from mysql import connector

from dataloader import logging
from dataloader.cfg import ITER_CHUNK_SIZE, FLUSH_BUFF_SIZE
from dataloader.error import UnsupportError
from dataloader.helper import StringIteratorIO
from dataloader.helper import (
    clean_csv_value, iter_chunks, time_stat
)

logger = logging.getLogger(__name__)


@time_stat
def _flush_postgres_buff(db_session, rec_buff, leftover=False):
    key_for_del = []

    for full_tbname, buff in rec_buff.items():
        buff_size = len(buff)
        if not leftover and buff_size < FLUSH_BUFF_SIZE:
            continue

        key_for_del.append(full_tbname)
        std_data_iter = StringIteratorIO(iter(buff))

        cursor = db_session.connection().connection.cursor()
        cursor.copy_from(std_data_iter, full_tbname, sep='|')

        db_session.commit()

        del std_data_iter

    for k in key_for_del:
        del rec_buff[k]

    if leftover:
        del rec_buff


@time_stat
def flush_data(dbcfg, rec_iter):
    if dbcfg['scheme'] == 'postgresql':  # psycopg2
        rec_buff = {}
        db_session = dbcfg['session']

        @time_stat
        def _iter_chunk(rec_buff, iter_chunk):
            for rec in iter_chunk:
                if rec.__ftable_name__ not in rec_buff:
                    rec_buff[rec.__ftable_name__] = []
                rec_buff[rec.__ftable_name__].append(
                    '|'.join(map(clean_csv_value, rec.csvalue())) + "\n"
                )

        for iter_chunk in iter_chunks(rec_iter, ITER_CHUNK_SIZE):
            _iter_chunk(rec_buff, iter_chunk)

            logger.info("[FLSH] Generated chunk data, flushing ...")

            _flush_postgres_buff(db_session, rec_buff)
        _flush_postgres_buff(db_session, rec_buff, True)

        logger.info("[FLSH] =-=-= Done. =-=-=")
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
