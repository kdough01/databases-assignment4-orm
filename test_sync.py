import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import BaseSource, Film, Language
from analytics import BaseAnalytics, DimFilm, SyncState
import sync

@pytest.fixture
def source_engine_and_session():
    engine = create_engine("sqlite:///:memory:")
    BaseSource.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    sess = Session()
    sess.add(Language(language_id=1, name="English", last_update=datetime.utcnow()))
    sess.add(Film(film_id=1, title="Test Movie", language_id=1, rating="PG",
                  length=100, release_year=2020, last_update=datetime.utcnow()))
    sess.commit()
    yield engine, sess
    sess.close()

@pytest.fixture
def target_engine_and_session(tmp_path):
    db_url = f"sqlite:///{tmp_path/'analytics.db'}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    BaseAnalytics.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    sess = Session()
    yield engine, sess
    sess.close()

def test_init_command(tmp_path):
    db_url = f"sqlite:///{tmp_path/'analytics_init.db'}"
    engine = create_engine(db_url)
    sync.init(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.add(SyncState(table_name="film", last_synced=datetime(1970,1,1)))
    s.commit()
    assert s.get(SyncState, "film") is not None
    s.close()

def test_full_load(source_engine_and_session, tmp_path):
    source_engine, source_sess = source_engine_and_session
    db_url = f"sqlite:///{tmp_path/'analytics_full.db'}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    TargetSession = sessionmaker(bind=engine)
    target_sess = TargetSession()
    sync.init(engine)
    sync.full_load(source_sess, target_sess)
    f = target_sess.query(DimFilm).filter_by(film_id=1).one_or_none()
    assert f is not None and f.title == "Test Movie"
    target_sess.close()

def test_incremental_new(source_engine_and_session, tmp_path):
    source_engine, source_sess = source_engine_and_session
    db_url = f"sqlite:///{tmp_path/'analytics_inc_new.db'}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    TargetSession = sessionmaker(bind=engine)
    target_sess = TargetSession()
    sync.init(engine)
    sync.full_load(source_sess, target_sess)

    new_film = Film(film_id=2, title="New Movie", language_id=1,
                    rating="G", length=90, release_year=2021, last_update=datetime.utcnow())
    source_sess.add(new_film)
    source_sess.commit()

    sync.incremental(source_sess, target_sess)
    f2 = target_sess.query(DimFilm).filter_by(film_id=2).one_or_none()
    assert f2 is not None and f2.title == "New Movie"
    target_sess.close()

def test_incremental_update(source_engine_and_session, tmp_path):
    source_engine, source_sess = source_engine_and_session
    db_url = f"sqlite:///{tmp_path/'analytics_inc_update.db'}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    TargetSession = sessionmaker(bind=engine)
    target_sess = TargetSession()
    sync.init(engine)
    sync.full_load(source_sess, target_sess)

    film = source_sess.get(Film, 1)
    film.title = "Updated Title"
    film.last_update = datetime.utcnow()
    source_sess.commit()

    sync.incremental(source_sess, target_sess)
    f1 = target_sess.query(DimFilm).filter_by(film_id=1).one()
    assert f1.title == "Updated Title"
    target_sess.close()

def test_validate(source_engine_and_session, tmp_path):
    source_engine, source_sess = source_engine_and_session
    db_url = f"sqlite:///{tmp_path/'analytics_validate.db'}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    TargetSession = sessionmaker(bind=engine)
    target_sess = TargetSession()
    sync.init(engine)
    sync.full_load(source_sess, target_sess)

    ok, problems = sync.validate(source_sess, target_sess)
    assert ok is True
    target_sess.close()