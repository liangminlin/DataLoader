import psycopg2.extras
from mysql import connector

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dataloader import logging
from dataloader.cfg import ITER_CHUCK_SIZE, FLUSH_BUFF_SIZE
from dataloader.error import UnsupportError
from dataloader.helper import StringIteratorIO
from dataloader.helper import clean_csv_value, iter_chunks

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


def _flush_postgres_buff(db_session, rec_buff, leftover=False):
    key_for_del = []

    logger.info(f"Runing _flush_postgres_buff, FLUSH_BUFF_SIZE: {FLUSH_BUFF_SIZE}")

    for full_tbname, buff in rec_buff.items():
        buff_size = len(buff)
        if not leftover and buff_size < FLUSH_BUFF_SIZE:
            logger.info(f"Continue cause of {buff_size} < FLUSH_BUFF_SIZE")
            continue

        key_for_del.append(full_tbname)
        std_data_iter = StringIteratorIO(iter(buff))
    
        cursor = db_session.connection().connection.cursor()
        cursor.copy_from(std_data_iter, full_tbname, sep='|')
    
        db_session.commit()

        del std_data_iter

    for k in key_for_del:
        logger.info(f"Del data of {k} from rec_buff")
        del rec_buff[k]

    if leftover:
        logger.info("True for leftover, clear rec_buff.")
        del rec_buff


def flush_bulk_data(dbcfg, rec_iter):
    if dbcfg['scheme'] == 'postgresql':  # psycopg2
        rec_buff = {}
        db_session = dbcfg['session']

        for iter_chuck in iter_chunks(rec_iter, ITER_CHUCK_SIZE):
            logger.info(f"Flush for an iter_chuck, ITER_CHUCK_SIZE: {ITER_CHUCK_SIZE}")
            
            for rec in iter_chuck:
                if rec.__ftable_name__ not in rec_buff:
                    rec_buff[rec.__ftable_name__] = []
                rec_buff[rec.__ftable_name__].append(
                    '|'.join(map(clean_csv_value, rec.csvalue())) + "\n"
                )

            logger.info(f"Keys in rec_buff: {rec_buff.keys()} before flush")

            _flush_postgres_buff(db_session, rec_buff)

            logger.info(f"Keys in rec_buff: {rec_buff.keys()} after flush")

        logger.info(f"Flushing the leftover data in  rec_buff: {rec_buff.keys()}")
        _flush_postgres_buff(db_session, rec_buff, True)

        logger.info("Done.")
    elif dbcfg['scheme'] == 'mysql':  # MySQLdb
        conn = connector.connect(
            host=dbcfg['hostname'],
            user=dbcfg['username'],
            passwd=dbcfg['password'],
            port=dbcfg['port'],
            database=dbcfg['database']
        )
        cursor = conn.cursor()
        
        # cursor.copy_from(src_dat_iter, full_tbname, sep='|')
        
        conn.commit()
        cursor.close()
        conn.close()
    elif dbcfg['scheme'] == 'mysql+mysqlconnector':  # mysql-connector
        conn = connector.connect(
            host=dbcfg['hostname'],
            user=dbcfg['username'],
            passwd=dbcfg['password'],
            port=dbcfg['port'],
            database=dbcfg['database']
        )
        cursor = conn.cursor()
        
        # cursor.copy_from(src_dat_iter, full_tbname, sep='|')
        
        conn.commit()
        cursor.close()
        conn.close()
    else:
        raise UnsupportError("Not support yet.")