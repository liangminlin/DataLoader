from dataloader import logging
from dataloader.cfg import ITER_CHUNK_SIZE, FLUSH_BUFF_SIZE
from dataloader.helper import iter_chunks, time_stat

logger = logging.getLogger(__name__)


@time_stat
def _flush_chunk_buff(db_session, flusher, rec_buff, leftover=False):
    key_for_del = []

    for full_tbname, data in rec_buff.items():
        if not leftover and len(data["buff"]) < FLUSH_BUFF_SIZE:
            continue

        key_for_del.append(full_tbname)

        cursor = db_session.connection().connection.cursor()

        flushed = flusher(cursor, full_tbname, data["sql"], data["buff"])

        db_session.commit()

        del flushed

    for k in key_for_del:
        del rec_buff[k]

    if leftover:
        del rec_buff


@time_stat
def flush_data(dbcfg, rec_iter):
    rec_buff = {}
    db_session = dbcfg['session']
    flusher = dbcfg['flusher']
    rec_filter = dbcfg['rec_filter']

    @time_stat
    def _iter_chunk(rec_buff, iter_chunk):
        for rec in iter_chunk:
            if rec.__ftable_name__ not in rec_buff:
                rec_buff[rec.__ftable_name__] = {
                    "buff": [], "sql": rec.__insert_sql__
                }
            rec_buff[rec.__ftable_name__]["buff"].append(
                rec_filter(rec)
            )

    for iter_chunk in iter_chunks(rec_iter, ITER_CHUNK_SIZE):
        _iter_chunk(rec_buff, iter_chunk)

        logger.info("[FLSH] Generated chunk data, flushing ...")

        _flush_chunk_buff(db_session, flusher, rec_buff)
    _flush_chunk_buff(db_session, flusher, rec_buff, True)

    logger.info("[FLSH] =-=-= Done. =-=-=")
