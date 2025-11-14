from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric
from sqlalchemy.orm import declarative_base, relationship

BaseSource = declarative_base()

class Language(BaseSource):
    __tablename__ = "language"
    language_id = Column(Integer, primary_key=True)
    name = Column(String(20))
    last_update = Column(DateTime)

class Film(BaseSource):
    __tablename__ = "film"
    film_id = Column(Integer, primary_key=True)
    title = Column(String(255))
    description = Column(String)
    release_year = Column(Integer)
    language_id = Column(Integer, ForeignKey("language.language_id"))
    rental_duration = Column(Integer)
    rental_rate = Column(Numeric(4,2))
    length = Column(Integer)
    rating = Column(String(10))
    last_update = Column(DateTime)

    language = relationship("Language", lazy="joined")

class Actor(BaseSource):
    __tablename__ = "actor"
    actor_id = Column(Integer, primary_key=True)
    first_name = Column(String(45))
    last_name = Column(String(45))
    last_update = Column(DateTime)

class Category(BaseSource):
    __tablename__ = "category"
    category_id = Column(Integer, primary_key=True)
    name = Column(String(25))
    last_update = Column(DateTime)

class FilmActor(BaseSource):
    __tablename__ = "film_actor"
    actor_id = Column(Integer, ForeignKey("actor.actor_id"), primary_key=True)
    film_id = Column(Integer, ForeignKey("film.film_id"), primary_key=True)
    last_update = Column(DateTime)

class FilmCategory(BaseSource):
    __tablename__ = "film_category"
    film_id = Column(Integer, ForeignKey("film.film_id"), primary_key=True)
    category_id = Column(Integer, ForeignKey("category.category_id"), primary_key=True)
    last_update = Column(DateTime)

class Address(BaseSource):
    __tablename__ = "address"
    address_id = Column(Integer, primary_key=True)
    address = Column(String(50))
    city_id = Column(Integer, ForeignKey("city.city_id"))
    last_update = Column(DateTime)

class City(BaseSource):
    __tablename__ = "city"
    city_id = Column(Integer, primary_key=True)
    city = Column(String(50))
    country_id = Column(Integer, ForeignKey("country.country_id"))
    last_update = Column(DateTime)

class Country(BaseSource):
    __tablename__ = "country"
    country_id = Column(Integer, primary_key=True)
    country = Column(String(50))
    last_update = Column(DateTime)

class Store(BaseSource):
    __tablename__ = "store"
    store_id = Column(Integer, primary_key=True)
    manager_staff_id = Column(Integer)
    address_id = Column(Integer, ForeignKey("address.address_id"))
    last_update = Column(DateTime)

class Customer(BaseSource):
    __tablename__ = "customer"
    customer_id = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    first_name = Column(String(45))
    last_name = Column(String(45))
    email = Column(String(50))
    active = Column(Integer)
    create_date = Column(DateTime)
    last_update = Column(DateTime)
    address_id = Column(Integer, ForeignKey("address.address_id"))

class Inventory(BaseSource):
    __tablename__ = "inventory"
    inventory_id = Column(Integer, primary_key=True)
    film_id = Column(Integer, ForeignKey("film.film_id"))
    store_id = Column(Integer)
    last_update = Column(DateTime)

class Rental(BaseSource):
    __tablename__ = "rental"
    rental_id = Column(Integer, primary_key=True)
    rental_date = Column(DateTime)
    inventory_id = Column(Integer, ForeignKey("inventory.inventory_id"))
    customer_id = Column(Integer, ForeignKey("customer.customer_id"))
    return_date = Column(DateTime)
    staff_id = Column(Integer)
    last_update = Column(DateTime)

class Payment(BaseSource):
    __tablename__ = "payment"
    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customer.customer_id"))
    staff_id = Column(Integer)
    rental_id = Column(Integer, ForeignKey("rental.rental_id"))
    amount = Column(Numeric(5,2))
    payment_date = Column(DateTime)
    last_update = Column(DateTime)