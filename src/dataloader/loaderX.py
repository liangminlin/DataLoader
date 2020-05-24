import tempfile

from dataloader import logging
from dataloader.helper import iter_chunks, time_stat

logger = logging.getLogger(__name__)


@time_stat
def _flush_chunk_buff(db_session, flusher, rec_buff, flush_buff_size, leftover=False):
    key_for_del = []

    for full_tbname, data in rec_buff.items():
        if not leftover and data["bfsz"] < flush_buff_size:
            continue

        key_for_del.append(full_tbname)

        flushed = flusher(db_session, full_tbname, data["sql"], data["buff"])

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
    flush_buff_size = dbcfg['flush_buff_size']
    iter_chunk_size = dbcfg['iter_chunk_size']

    @time_stat
    def _iter_chunk(rec_buff, iter_chunk):
        for rec in iter_chunk:
            if rec.__ftable_name__ not in rec_buff:
                rec.__class__.__dbcfg_ref__ = dbcfg
                rec_buff[rec.__ftable_name__] = {
                    "bfsz": 0, "buff": [], "sql": rec.__load_mysql__
                }
                if dbcfg['scheme'] == 'mysql':
                    f = tempfile.NamedTemporaryFile(
                        mode='w', buffering=81920, delete=False
                    )
                    rec_buff[rec.__ftable_name__]["buff"] = f

            rec_buff[rec.__ftable_name__]["bfsz"] += 1
            rec_filter(rec_buff[rec.__ftable_name__], rec)

    for iter_chunk in iter_chunks(rec_iter, iter_chunk_size):
        logger.info("[GENR] Generating chunk data, please wait ......")

        _iter_chunk(rec_buff, iter_chunk)

        logger.info("[FLSH] Generated chunk data, flushing ......")

        _flush_chunk_buff(db_session, flusher, rec_buff, flush_buff_size)
    _flush_chunk_buff(db_session, flusher, rec_buff, flush_buff_size, True)
