from functools import wraps

from dataloader import loader
from dataloader import logging
from dataloader import reflector
from dataloader.ctx import LoaderContext
from dataloader.helper import time_stat

logger = logging.getLogger(__name__)


class DataLoader(object):
    """ This is the main core module of this framework:

        app = DataLoader(__name__, Config)
    """
    @time_stat
    def __init__(self, import_name, config_class):
        logging.init_logger(
            import_name, config_class
        )

        logger.info("================== START ==================")

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
    def _flush_session_data(self, dbcfg, rec_iter):
        try:
            loader.flush_data(dbcfg, rec_iter)
        except Exception as exc:
            logger.exception(exc)

    @time_stat
    def register_sessions(self, sessions):
        """ Register sessions into loader context """
        if not isinstance(sessions, list):
            raise ValueError("sessions should be list")
        for session in sessions:
            self._ctx.push_session(session)

    @time_stat
    def register_session(self, session):
        """ Register session into loader context """
        self._ctx.push_session(session)

    def run(self):
        """ This is the running entrance for the user. """
        @time_stat
        def _concurren_load(session):
            dbconfigs = self._ctx.config.dbconfigs
            for s_item in session.registed_sessions:
                rec_iter = s_item['executor']()  # collect data

                logger.info(
                    ">>> >>> >>>"
                    f"START SESSION OF DB {s_item['database']}"
                    "<<< <<< <<<"
                )

                self._flush_session_data(
                    dbconfigs[s_item['database']], rec_iter
                )

        while(self._ctx.has_session()):
            # TODO: 改成多进程/线程方式
            _concurren_load(
                self._ctx.pop_session()
            )
        logger.info("=================== END ===================\n\n\n\n\n")


class LoadSession(object):
    """ Each logically independent session should wrap by a LoadSession,
        a LoadSession will run in a single process for increase efficiency.

        A LoadSession can have multiple sessions, and a session is define by:

        ls = LoadSession(__name__)

        @ls.regist_for("<dbname>")      # We call this as a session.
        def load_some_data():
            # from target.<dbname> import iter_<tbname>
            # for idx, item in iter_<tbname>(<total>):
            #     yield item
    """
    @time_stat
    def __init__(self, import_name):
        """ import_name is not used at this moment """
        self.registed_sessions = []

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
