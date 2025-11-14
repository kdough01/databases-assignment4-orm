# databases-assignment4-orm

# Models
The models are set up exactly how you'd set them up in Django since this is what I am already familiar with. This file is mostly just about making sure the types are defined correctly in the file, nothing else. The tables are just the SakilaDB tables.

# Analytics
Again, these are just more tables that are setup in a similar manner to how you would do it in Django. I also included the bridges in this file.

# Helpers
I started doing the incremental function and realized how repetitive the code was getting because so much of it was similar to the full_load function so I made the helpers file. I have functions to upsert data for each table there.

# Pytests
I wanted to keep these tests as basic as possible, so we have the five tests specified in the assignment:
- init test - add a table, see if it is in the database
- full load test - make sure the test movie, which was added to the source session in one of our setup functions, now also appears in the target session
- incremental new data test - make sure the new movie, which we add to the source session is in the target session after running the incremental function
- incremental update data test - make sure the updated title, which was changed in the source session is updated in the target session after running the incremental function
- validate test - make sure that our validate function returns True and everything is ok and matched between the two databases

(Very happy we just started using pytest a lot in distributed systems, so this was a fairly familiar setup. I kinda see the first two functions as connecting to a server, and then the rest of the functions as performing the tests.)

# To run the code with CLI:
python sync.py --source "mysql+pymysql://root:password@localhost/sakila" init
python sync.py --source "mysql+pymysql://root:password@localhost/sakila" full-load
python sync.py --source "mysql+pymysql://root:password@localhost/sakila" incremental
python sync.py --source "mysql+pymysql://root:password@localhost/sakila" validate

# Example output in terminal:
- Note I have omitted my password here in the commands and replaced it with PASSWORD

- Init
(base) kevindougherty@Kevins-MacBook-Pro databases-assignment4-orm % python sync.py --source "mysql+pymysql://root:PASSWORD@localhost/sakila" init
INFO:sync:Analytics schema created.

- Full load
(base) kevindougherty@Kevins-MacBook-Pro databases-assignment4-orm % python sync.py --source "mysql+pymysql://root:PASSWORD@localhost/sakila" full-load
INFO:sync:Starting full load
/Users/kevindougherty/Documents/GitHub/databases-assignment4-orm/helpers.py:38: SAWarning: Dialect sqlite+pysqlite does *not* support Decimal objects natively, and SQLAlchemy must convert from floating point - rounding errors and other issues may occur. Please consider storing Decimal numbers as strings or integers on this platform for lossless storage.
  session.merge(obj)
INFO:sync:Full load completed and committed.

- Incremental
(base) kevindougherty@Kevins-MacBook-Pro databases-assignment4-orm % python sync.py --source "mysql+pymysql://root:PASSWORD@localhost/sakila" incremental
INFO:sync:Starting incremental sync
INFO:sync:Checking table film (since 2025-10-31 19:54:07)
INFO:sync:Synced 74 rows for film (new_last=2025-10-31 19:54:07)
INFO:sync:Checking table actor (since 2006-02-15 04:34:33)
INFO:sync:Synced 200 rows for actor (new_last=2006-02-15 04:34:33)
INFO:sync:Checking table category (since 2006-02-15 04:46:27)
INFO:sync:Synced 16 rows for category (new_last=2006-02-15 04:46:27)
INFO:sync:Checking table film_actor (since 2006-02-15 05:05:03)
INFO:sync:Synced 5462 rows for film_actor (new_last=2006-02-15 05:05:03)
INFO:sync:Checking table film_category (since 2006-02-15 05:07:09)
INFO:sync:Synced 1000 rows for film_category (new_last=2006-02-15 05:07:09)
INFO:sync:Checking table inventory (since 2006-02-15 05:09:17)
INFO:sync:Synced 4581 rows for inventory (new_last=2006-02-15 05:09:17)
INFO:sync:Checking table rental (since 2006-02-23 04:12:08)
INFO:sync:Synced 1 rows for rental (new_last=2006-02-23 04:12:08)
INFO:sync:Checking table payment (since 2006-02-15 22:24:11)
/Users/kevindougherty/Documents/GitHub/databases-assignment4-orm/helpers.py:38: SAWarning: Dialect sqlite+pysqlite does *not* support Decimal objects natively, and SQLAlchemy must convert from floating point - rounding errors and other issues may occur. Please consider storing Decimal numbers as strings or integers on this platform for lossless storage.
  session.merge(obj)
INFO:sync:Synced 3 rows for payment (new_last=2006-02-15 22:24:11)
INFO:sync:Checking table customer (since 2006-02-15 04:57:20)
INFO:sync:Synced 599 rows for customer (new_last=2006-02-15 04:57:20)
INFO:sync:Checking table store (since 2006-02-15 04:57:12)
INFO:sync:Synced 2 rows for store (new_last=2006-02-15 04:57:12)

- Validate
(base) kevindougherty@Kevins-MacBook-Pro databases-assignment4-orm % python sync.py --source "mysql+pymysql://root:PASSWORD@localhost/sakila" validate   
INFO:sync:Validating data consistency (last 30 days)
INFO:sync:Films - source: 1000, target: 1000
INFO:sync:Rentals (last 30 days) - source: 0, target: 0
/Users/kevindougherty/Documents/GitHub/databases-assignment4-orm/sync.py:215: SAWarning: Dialect sqlite+pysqlite does *not* support Decimal objects natively, and SQLAlchemy must convert from floating point - rounding errors and other issues may occur. Please consider storing Decimal numbers as strings or integers on this platform for lossless storage.
  tgt_pay = target_session.execute(select(func.count(FactPayment.payment_id), func.coalesce(func.sum(FactPayment.amount), 0)).where(FactPayment.date_key_paid >= int(cutoff.strftime("%Y%m%d")))).first()
INFO:sync:Payments (last 30 days) - source count,sum: 0,0.00 ; target count,sum: 0,0.00
INFO:sync:Validation passed.

# Sources
I used this to reference a few things for SQLAlchemy
- https://docs.sqlalchemy.org/en/20/orm/quickstart.html
- https://stackoverflow.com/questions/1849567/can-sqlalchemys-session-merge-update-its-result-with-newer-data-from-the-data
- https://towardsdatascience.com/the-easiest-way-to-upsert-with-sqlalchemy-9dae87a75c35/