"""
I started doing the incremental function and realized how
repetitive the code was getting because so much of it was
similar to the full_load function so I made this helpers file.
I have functions to upsert data for each table here.
"""

from datetime import datetime
from dateutil import parser as dateparser
from sqlalchemy import select, func

from models import Language, Inventory, Address, City, Country
from analytics import DimDate, DimFilm, DimActor, DimCategory, DimStore, DimCustomer, BridgeFilmActor, BridgeFilmCategory, FactRental, FactPayment

def get_key_from_id(id: int, multiplier=100) -> int:
    return id * multiplier + 1 if id is not None else None

def get_last_update(source_session, table_name):
    # ok so I wasn't sure exactly how to do this at first and I asked ChatGPT about it, but
    # I knew the last_update column would store whatever time of the prior update was, but I wasn't sure
    # how to update every last_update time efficiently, which is the point of the dictionary sync_timestamps.
    # It basically maps the new update time to each table by having the table name as the key and the max value of the last
    # update as the column. The SQL query here is SELECT MAX(last_update) from table_name, the scalar makes sureit is a datetime
    # value, and the or datetime makes sure that there is a fallback value in case it is None for some reason.
    return source_session.scalar(select(func.max(table_name.last_update))) or datetime(1970,1,1)

def upsert(session, obj):
    # https://stackoverflow.com/questions/1849567/can-sqlalchemys-session-merge-update-its-result-with-newer-data-from-the-data
    # make sure we avoid duplicates with merge, hence upsert; this all reminds me of Git
    session.merge(obj)

def date_key_from_datetime(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = dateparser.parse(dt)
    return int(dt.strftime("%Y%m%d"))

def dim_date_from_date(d):
    if d is None:
        return None
    if isinstance(d, datetime):
        d = d.date()
    return DimDate(
        date_key=int(d.strftime("%Y%m%d")),
        date=d,
        year=d.year,
        quarter=(d.month-1)//3 + 1,
        month=d.month,
        day_of_month=d.day,
        day_of_week=d.weekday(),
        is_weekend=(d.weekday() >= 5)
    )

def pct_diff(a, b):
        if a == 0 and b == 0:
            return 0.0
        if a == 0:
            return 100.0
        return abs((a - b) / max(a, 1)) * 100.0

def upsert_dim_film(target_session, source_session, row):
        upsert(target_session, DimFilm(
                            film_key=get_key_from_id(row.film_id),
                            film_id=row.film_id,
                            title=row.title,
                            rating=row.rating,
                            length=row.length,
                            language=(source_session.get(Language, row.language_id).name if row.language_id else None),
                            release_year=row.release_year,
                            last_update=row.last_update
                        ))

def upsert_dim_actor(target_session, row):
    upsert(target_session, DimActor(
                            actor_key=get_key_from_id(row.actor_id),
                            actor_id=row.actor_id,
                            first_name=row.first_name,
                            last_name=row.last_name,
                            last_update=row.last_update
                        ))
        
def upsert_dim_category(target_session, row):
     upsert(target_session, DimCategory(
                            category_key=get_key_from_id(row.category_id),
                            category_id=row.category_id,
                            name=row.name,
                            last_update=row.last_update
                        ))

def upsert_film_actor(target_session, row):
     upsert(target_session, BridgeFilmActor(
                            film_key=get_key_from_id(row.film_id),
                            actor_key=get_key_from_id(row.actor_id)
                        ))
     
def upsert_film_category(target_session, row):
     upsert(target_session, BridgeFilmCategory(
                            film_key=get_key_from_id(row.film_id),
                            category_key=get_key_from_id(row.category_id)
                        ))
     
def upsert_rental(target_session, source_session, row):

    inv = source_session.get(Inventory, row.inventory_id) if row.inventory_id else None
    film_id = inv.film_id if inv else None
    upsert(target_session, FactRental(
        fact_rental_key=row.rental_id * 10 + 1,
        rental_id=row.rental_id,
        date_key_rented=date_key_from_datetime(row.rental_date),
        date_key_returned=date_key_from_datetime(row.return_date),
        film_key=get_key_from_id(film_id),
        store_key=get_key_from_id(inv.store_id) if inv else None,
        customer_key=get_key_from_id(row.customer_id),
        staff_id=row.staff_id,
        rental_duration_days=((row.return_date - row.rental_date).days if row.return_date and row.rental_date else None)
    ))
    if row.rental_date:
        upsert(target_session, dim_date_from_date(row.rental_date))
    if row.return_date:
        upsert(target_session, dim_date_from_date(row.return_date))

def upsert_payment(target_session, row):
    upsert(target_session, FactPayment(
                            fact_payment_key=row.payment_id * 10 + 1,
                            payment_id=row.payment_id,
                            date_key_paid=date_key_from_datetime(row.payment_date),
                            customer_key=get_key_from_id(row.customer_id),
                            store_key=None,
                            staff_id=row.staff_id,
                            amount=row.amount
                        ))
    if row.payment_date:
        upsert(target_session, dim_date_from_date(row.payment_date))

def upsert_customer(target_session, source_session, row):

    city = country = None
    if row.address_id:
        adr = source_session.get(Address, row.address_id)
        if adr and adr.city_id:
            city_obj = source_session.get(City, adr.city_id)
            if city_obj:
                city = city_obj.city
                if city_obj.country_id:
                    cobj = source_session.get(Country, city_obj.country_id)
                    if cobj:
                        country = cobj.country
    upsert(target_session, DimCustomer(
        customer_key=get_key_from_id(row.customer_id),
        customer_id=row.customer_id,
        first_name=row.first_name,
        last_name=row.last_name,
        active=row.active,
        city=city,
        country=country,
        last_update=row.last_update
    ))

def upsert_store(target_session, source_session, row):
    city = country = None
    if row.address_id:
        adr = source_session.get(Address, row.address_id)
        if adr and adr.city_id:
            city_obj = source_session.get(City, adr.city_id)
            if city_obj:
                city = city_obj.city
                if city_obj.country_id:
                    cobj = source_session.get(Country, city_obj.country_id)
                    if cobj:
                        country = cobj.country
    upsert(target_session, DimStore(
        store_key=get_key_from_id(row.store_id),
        store_id=row.store_id,
        city=city,
        country=country,
        last_update=row.last_update
    ))