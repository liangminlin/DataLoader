import uuid
from itertools import tee
from functools import wraps

from dataloader import db
from dataloader import logging
from dataloader import reflector
from dataloader.helper import time_stat
from dataloader.ctx import LoaderContext

logger = logging.getLogger(__name__)


class DataLoader(object):
    """ This is the main core module of this framework:

        loader = DataLoader(__name__, Config)
    """
    @time_stat
    def __init__(self, import_name, config_class):
        self._ctx = LoaderContext(
            import_name, config_class
        )

        self._reflect_target()

    @time_stat
    def _reflect_target(self):
        """ Reflect database table construct and
        generate target codes automatically. """
        reflector.reflect_targets(
            self._ctx.import_name,
            self._ctx.config.dbconfigs
        )

    @time_stat
    def _flush_data(self, dbname, full_tbname, buff_iter):
        """ Flush datas into table 'full_tbname' of database 'dbname' """
        try:
            db.flush_bulk_data(
                self._ctx.config.dbconfigs[dbname], full_tbname, buff_iter
            )
        except Exception as exc:
            logger.exception(exc)

    @time_stat
    def register_session(self, session):
        """ Register session into loader context """
        self._ctx.push_session(session)

    @time_stat
    def load(self):
        """ This is the running entrance for the user. """
        @time_stat
        def _concurren_load(session):
            from dataloader.helper import clean_csv_value

            dbconfigs = self._ctx.config.dbconfigs
            for s_item in session.registed_sessions:
                dbname = s_item['database']
                tables = dbconfigs[dbname]['tables']

                rec_iter = s_item['executor']()

                rec_iters = tee(rec_iter, len(tables))
                for ft, tbl_rec_iter in zip(tables, rec_iters):
                    batch_iter = (
                        ('|'.join(map(clean_csv_value, x.csvalue())) + "\n") 
                        for x in tbl_rec_iter if x.__ftable_name__==ft
                    )
                    self._flush_data(dbname, ft, batch_iter)

        while(self._ctx.has_session()):
            # 改成多进程方式
            _concurren_load( self._ctx.pop_session() )


class LoadSession(object):
    """ Each logically independent session should wrap by a LoadSession,
        a LoadSession will run in a single process for increase efficiency.

        A LoadSession can have multiple sessions, and a session is define by:

        ls = LoadSession(__name__)

        @ls.regist_for("<dbname>")      # We call this as a session.
        def load_some_data():
            # from target.<dbname> import iter_<tbname>
            # for item in iter_<tbname>(<total>):
            #     ls.collect(item)
    """
    @time_stat
    def __init__(self, import_name):
        """ import_name is not used at this moment """
        self.registed_sessions = []

    def collect(self, item):
        """ collect generated data """
        collected = item.__ftable_name__, item.csvalue()

        logger.info("collected: %s", collected)

        return collected

    @time_stat
    def regist_for(self, dbname):
        """ Define a session """
        def decorator(func):
            @wraps(func)
            def executor(*args, **kwargs):
                return func(*args, **kwargs)

            self.registed_sessions.append({
                'database': dbname, 'executor': executor
            })

            return executor
        return decorator
