import psycopg2.extras
from mysql import connector

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dataloader import logging
from dataloader.helper import StringIteratorIO
from dataloader.error import UnsupportError

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


def flush_bulk_data(dbcfg, full_tbname, buff_iter):
    src_dat_iter = StringIteratorIO(buff_iter)
    if dbcfg['scheme'] == 'postgresql':
        conn = psycopg2.connect(dbcfg['url'])
        cursor = conn.cursor()
        cursor.copy_from(src_dat_iter, full_tbname, sep='|')
        conn.commit()
        cursor.close()
        conn.close()
    elif dbcfg['scheme'] == 'mysql+mysqlconnector':
        conn = connector.connect(
            host=dbcfg['hostname'],
            user=dbcfg['username'],
            passwd=dbcfg['password'],
            port=dbcfg['port'],
            database=dbcfg['database']
        )
        cursor = conn.cursor()
        
        cursor.copy_from(src_dat_iter, full_tbname, sep='|')
        
        conn.commit()
        cursor.close()
        conn.close()
    else:
        raise UnsupportError("Not support yet.")