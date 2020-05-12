from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dataloader import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


def init_session(db_url):
    session = scoped_session(sessionmaker())
    engine = create_engine(
        db_url, pool_pre_ping=True, pool_size=2, max_overflow=10
    )
    session.configure(bind=engine)

    return session


def commit_session(session):
    try:
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.exception(exc)
    finally:
        session.close()
