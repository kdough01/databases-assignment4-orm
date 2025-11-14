import argparse
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from models import Film, Language, Actor, Category, FilmActor, FilmCategory, Rental, Payment, Inventory, Store, Customer
from analytics import BaseAnalytics, DimFilm, FactRental, FactPayment, SyncState
from helpers import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync")

def make_sessions(source_url: str, target_url: str):
    """
    So basically we need to connect the two databases together somehow and that's
    sort of what this function is doing, it is establishing connections with both
    databases so we can transfer data between them.
    """
    source_engine = create_engine(source_url, pool_pre_ping=True)
    target_engine = create_engine(target_url, connect_args={"check_same_thread": False})
    SourceSession = sessionmaker(bind=source_engine)
    TargetSession = sessionmaker(bind=target_engine)
    return source_engine, target_engine, SourceSession, TargetSession

def init(target_engine):
    """
    This creates the analytics database defined in the analytics.py file.
    We need to do this to be able to transfer the data.
    """
    BaseAnalytics.metadata.create_all(bind=target_engine)
    logger.info("Analytics schema created.")

def ensure_sync_state(target_session, table_names):
    """
    When we do these incremental syncs we need to know what changed, if anything, since our last sync
    and that's what this does. existing is a boolean value telling us if the table is in the table_names,
    if it is't in the tables, we need to add it and then commit the message

    This is again just like in Git we do
    git add .
    git commit -m "commit message"

    Also I did ask ChatGPT here what to do when we first initialize or sync the databases, and it said to
    use 1970-01-01, I assume because no data could actually be added before this, but after the first sync
    this data reflects the current time.
    """
    for t in table_names:
        existing = target_session.get(SyncState, t)
        if not existing:
            target_session.add(SyncState(table_name=t, last_synced=datetime(1970,1,1)))
    target_session.commit()

def full_load(source_session, target_session):
    logger.info("Starting full load")
    try:
        with target_session.begin():
            langs = source_session.scalars(select(Language)).all()
            lang_map = {l.language_id: l.name for l in langs}

            films = source_session.scalars(select(Film)).all()
            for f in films:
                upsert_dim_film(target_session, source_session, row=f)

            actors = source_session.scalars(select(Actor)).all()
            for a in actors:
                upsert_dim_actor(target_session, row=a)

            cats = source_session.scalars(select(Category)).all()
            for c in cats:
                upsert_dim_category(target_session, row=c)

            fas = source_session.scalars(select(FilmActor)).all()
            for fa in fas:
                upsert_film_actor(target_session, row=fa)

            fcs = source_session.scalars(select(FilmCategory)).all()
            for fc in fcs:
                upsert_film_category(target_session, row=fc)

            stores = source_session.scalars(select(Store)).all()
            for s in stores:
                upsert_store(target_session, source_session, row=s)

            customers = source_session.scalars(select(Customer)).all()
            for c in customers:
                upsert_customer(target_session, source_session, row=c)

            rentals = source_session.scalars(select(Rental)).all()
            for r in rentals:
                upsert_rental(target_session, source_session, row=r)

            payments = source_session.scalars(select(Payment)).all()
            for p in payments:
                upsert_payment(target_session, row=p)

            sync_timestamps = {
                "film": get_last_update(source_session, Film),
                "actor": get_last_update(source_session, Actor),
                "category": get_last_update(source_session, Category),
                "film_actor": get_last_update(source_session, FilmActor),
                "film_category": get_last_update(source_session, FilmCategory),
                "inventory": get_last_update(source_session, Inventory),
                "rental": get_last_update(source_session, Rental),
                "payment": get_last_update(source_session, Payment),
                "customer": get_last_update(source_session, Customer),
                "store": get_last_update(source_session, Store)
            }

            for table_name, max_ts in sync_timestamps.items():
                existing = target_session.get(SyncState, table_name)
                if existing:
                    existing.last_synced = max_ts
                else:
                    target_session.add(SyncState(table_name=table_name, last_synced=max_ts))
        
        target_session.commit()
        logger.info("Full load completed and committed.")

    except SQLAlchemyError as e:
        target_session.rollback()
        logger.exception("Full load failed; transaction rolled back.")
        raise

def incremental(source_session, target_session):
    logger.info("Starting incremental sync")
    sync_plan = [
        ("film", Film, "last_update"),
        ("actor", Actor, "last_update"),
        ("category", Category, "last_update"),
        ("film_actor", FilmActor, "last_update"),
        ("film_category", FilmCategory, "last_update"),
        ("inventory", Inventory, "last_update"),
        ("rental", Rental, "last_update"),
        ("payment", Payment, "last_update"),
        ("customer", Customer, "last_update"),
        ("store", Store, "last_update"),
    ]
    for table_name, model, ts_field in sync_plan:
        state = target_session.get(SyncState, table_name)
        last_synced = state.last_synced if state else datetime(1970,1,1)
        logger.info("Checking table %s (since %s)", table_name, last_synced)
        field = getattr(model, ts_field)
        rows = source_session.scalars(select(model).where(field >= last_synced).order_by(field)).all()
        
        if not rows:
            logger.info("No changes for %s", table_name)
            continue

        try:
            new_last = max(getattr(r, ts_field) for r in rows if getattr(r, ts_field) is not None)
            
            with target_session.begin_nested():
                for r in rows:
                    if isinstance(r, Film):
                        upsert_dim_film(target_session, source_session, row=r)
                    elif isinstance(r, Actor):
                        upsert_dim_actor(target_session, row=r)
                    elif isinstance(r, Category):
                        upsert_dim_category(target_session, row=r)
                    elif isinstance(r, FilmActor):
                        upsert_film_actor(target_session, r)
                    elif isinstance(r, FilmCategory):
                        upsert_film_category(target_session, row=r)
                    elif isinstance(r, Inventory):
                        pass
                    elif isinstance(r, Rental):
                        upsert_rental(target_session, source_session, row=r)
                    elif isinstance(r, Payment):
                        upsert_payment(target_session, row=r)
                    elif isinstance(r, Customer):
                        upsert_customer(target_session, source_session, row=r)
                    elif isinstance(r, Store):
                        upsert_store(target_session, source_session, row=r)

                st = target_session.get(SyncState, table_name)
                if st:
                    st.last_synced = new_last
                else:
                    target_session.add(SyncState(table_name=table_name, last_synced=new_last))

            logger.info("Synced %d rows for %s (new_last=%s)", len(rows), table_name, new_last)

        except SQLAlchemyError:
            logger.exception("Error syncing table %s; skipping and rolling back that table.", table_name)

def validate(source_session, target_session, days=30, threshold_pct=1.0):
    logger.info("Validating data consistency (last %d days)", days)
    cutoff = datetime.now() - timedelta(days=days)

    # for all of these I just compare the number of rows in each table and see if they're the same
    src_films = source_session.scalar(select(func.count(Film.film_id)))
    tgt_films = target_session.scalar(select(func.count(DimFilm.film_key)))
    logger.info("Films - source: %s, target: %s", src_films, tgt_films)

    src_rentals = source_session.scalar(select(func.count(Rental.rental_id)).where(Rental.rental_date >= cutoff))
    tgt_rentals = target_session.scalar(select(func.count(FactRental.rental_id)).where(FactRental.date_key_rented >= int(cutoff.strftime("%Y%m%d"))))
    logger.info("Rentals (last %d days) - source: %s, target: %s", days, src_rentals, tgt_rentals)

    src_pay = source_session.execute(select(func.count(Payment.payment_id), func.coalesce(func.sum(Payment.amount), 0)).where(Payment.payment_date >= cutoff)).first()

    src_pay_count = src_pay[0] or 0
    src_pay_sum = float(src_pay[1] or 0)

    tgt_pay = target_session.execute(select(func.count(FactPayment.payment_id), func.coalesce(func.sum(FactPayment.amount), 0)).where(FactPayment.date_key_paid >= int(cutoff.strftime("%Y%m%d")))).first()
    tgt_pay_count = tgt_pay[0] or 0
    tgt_pay_sum = float(tgt_pay[1] or 0)

    logger.info("Payments (last %d days) - source count,sum: %s,%.2f ; target count,sum: %s,%.2f",
                days, src_pay_count, src_pay_sum, tgt_pay_count, tgt_pay_sum)

    # essentially if any of the above checks cause an issue we add it to this problems list and can print it out in our logs
    # we can also return the lists, and a boolean if everything is ok
    problems = []
    if pct_diff(src_films or 0, tgt_films or 0) > threshold_pct:
        problems.append(f"Film count differs more than {threshold_pct}% (source={src_films} target={tgt_films})")
    if pct_diff(src_rentals or 0, tgt_rentals or 0) > threshold_pct:
        problems.append(f"Rental count differs more than {threshold_pct}% (last {days} days)")
    if pct_diff(src_pay_sum, tgt_pay_sum) > threshold_pct:
        problems.append(f"Payment total differs more than {threshold_pct}% (last {days} days)")

    if problems:
        logger.warning("Validation problems found:\n%s", "\n".join(problems))
        return False, problems
    logger.info("Validation passed.")
    return True, []

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", default="sqlite:///analytics.db")
    parser.add_argument("command", choices=["init", "full-load", "incremental", "validate"])
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    source_engine, target_engine, SourceSession, TargetSession = make_sessions(args.source, args.target)
    source_session = SourceSession()
    target_session = TargetSession()

    if args.command == "init":
        init(target_engine)
    elif args.command == "full-load":
        full_load(source_session, target_session)
    elif args.command == "incremental":
        incremental(source_session, target_session)
    elif args.command == "validate":
        ok, problems = validate(source_session, target_session)
        if not ok:
            raise SystemExit(2)
    else:
        print("Unknown command")