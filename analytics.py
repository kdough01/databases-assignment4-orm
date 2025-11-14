from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Numeric, Boolean, ForeignKey, Index
)
from sqlalchemy.orm import declarative_base

BaseAnalytics = declarative_base()

class DimDate(BaseAnalytics):
    __tablename__ = "dim_date"
    date_key = Column(Integer, primary_key=True)
    date = Column(Date)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    day_of_month = Column(Integer)
    day_of_week = Column(Integer)
    is_weekend = Column(Boolean)

class DimFilm(BaseAnalytics):
    __tablename__ = "dim_film"
    film_key = Column(Integer, primary_key=True)
    film_id = Column(Integer, unique=True, index=True)
    title = Column(String)
    rating = Column(String(10))
    length = Column(Integer)
    language = Column(String(50))
    release_year = Column(Integer)
    last_update = Column(DateTime)

class DimActor(BaseAnalytics):
    __tablename__ = "dim_actor"
    actor_key = Column(Integer, primary_key=True)
    actor_id = Column(Integer, unique=True, index=True)
    first_name = Column(String)
    last_name = Column(String)
    last_update = Column(DateTime)

class DimCategory(BaseAnalytics):
    __tablename__ = "dim_category"
    category_key = Column(Integer, primary_key=True)
    category_id = Column(Integer, unique=True, index=True)
    name = Column(String)
    last_update = Column(DateTime)

class DimStore(BaseAnalytics):
    __tablename__ = "dim_store"
    store_key = Column(Integer, primary_key=True)
    store_id = Column(Integer, unique=True, index=True)
    city = Column(String)
    country = Column(String)
    last_update = Column(DateTime)

class DimCustomer(BaseAnalytics):
    __tablename__ = "dim_customer"
    customer_key = Column(Integer, primary_key=True)
    customer_id = Column(Integer, unique=True, index=True)
    first_name = Column(String)
    last_name = Column(String)
    active = Column(Integer)
    city = Column(String)
    country = Column(String)
    last_update = Column(DateTime)

class BridgeFilmActor(BaseAnalytics):
    __tablename__ = "bridge_film_actor"
    film_key = Column(Integer, primary_key=True)
    actor_key = Column(Integer, primary_key=True)

class BridgeFilmCategory(BaseAnalytics):
    __tablename__ = "bridge_film_category"
    film_key = Column(Integer, primary_key=True)
    category_key = Column(Integer, primary_key=True)

class FactRental(BaseAnalytics):
    __tablename__ = "fact_rental"
    fact_rental_key = Column(Integer, primary_key=True)
    rental_id = Column(Integer, unique=True, index=True)
    date_key_rented = Column(Integer, ForeignKey("dim_date.date_key"))
    date_key_returned = Column(Integer, ForeignKey("dim_date.date_key"))
    film_key = Column(Integer, ForeignKey("dim_film.film_key"))
    store_key = Column(Integer, ForeignKey("dim_store.store_key"))
    customer_key = Column(Integer, ForeignKey("dim_customer.customer_key"))
    staff_id = Column(Integer)
    rental_duration_days = Column(Integer)

class FactPayment(BaseAnalytics):
    __tablename__ = "fact_payment"
    fact_payment_key = Column(Integer, primary_key=True)
    payment_id = Column(Integer, unique=True, index=True)
    date_key_paid = Column(Integer, ForeignKey("dim_date.date_key"))
    customer_key = Column(Integer, ForeignKey("dim_customer.customer_key"))
    store_key = Column(Integer, ForeignKey("dim_store.store_key"))
    staff_id = Column(Integer)
    amount = Column(Numeric(10,2))

class SyncState(BaseAnalytics):
    __tablename__ = "sync_state"
    table_name = Column(String, primary_key=True)
    last_synced = Column(DateTime)

Index("ix_fact_rental_store", FactRental.store_key)
Index("ix_fact_payment_store", FactPayment.store_key)