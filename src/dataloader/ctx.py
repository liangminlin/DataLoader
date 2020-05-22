from dataloader import logging
from dataloader.cfg import Configuration

logger = logging.getLogger(__name__)


class LoaderContext(object):
    """ The loader context """
    def __init__(self, import_name, config_class):
        self.load_sessions = []
        self.import_name = import_name
        self.config = Configuration(config_class)

    def has_session(self):
        return len(self.load_sessions) > 0

    def push_session(self, session):
        self.load_sessions.append(session)

    def pop_session(self):
        try:
            return self.load_sessions.pop()
        except Exception as exc:             # IndexError
            logger.exception(f"[Error] Failed to pop session: {exc}")
            return None
