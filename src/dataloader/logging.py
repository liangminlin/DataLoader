import logging
import datetime

from .error import ConfigError


def init_logger(import_name, config_class):
    from .helper import get_root_path

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("[%(asctime)-15s] %(message)s")
    )
    handlers = [stream_handler]

    if hasattr(config_class, 'SAVE_LOG_TO_FILE'):
        if config_class.SAVE_LOG_TO_FILE not in (True, False):
            raise ConfigError(
                f"SAVE_LOG_TO_FILE expected boolean value but get "
                f"{type(config_class.SAVE_LOG_TO_FILE)}: {config_class.SAVE_LOG_TO_FILE}."
            )

        logfile = None
        if hasattr(config_class, 'LOG_FILE_LOCATION'):
            if type(config_class.LOG_FILE_LOCATION) is not str:
                raise ConfigError("LOG_FILE_LOCATION should be string value")
            if len(config_class.LOG_FILE_LOCATION) == 0:
                raise ConfigError("LOG_FILE_LOCATION can't be empty string")
            logfile = config_class.LOG_FILE_LOCATION
            if logfile[-1] == '/':
                logfile = logfile[:-1]

        if not logfile:
            r = get_root_path(import_name)
            logfile = "/".join(r.split("/")[:-1])

        dt = datetime.datetime.now().strftime("%Y-%m-%d")
        logfile += "/dataloader." + dt + ".log"

        file_handler = logging.FileHandler(logfile, mode='a')
        file_handler.setFormatter(
            logging.Formatter("[%(asctime)-15s] %(message)s")
        )
        handlers.append(file_handler)

    logging.basicConfig(handlers=handlers)
    logger = logging.getLogger(import_name)

    logging_level = logging.INFO
    if hasattr(config_class, 'LOG_LEVEL'):
        if config_class.LOG_LEVEL not in (
            logging.INFO, logging.DEBUG, logging.NOTSET,
            logging.CRITICAL, logging.ERROR, logging.WARNING
        ):
            raise ValueError(f"""
            Invalid logging level, level should be on of:
                logging.CRITICAL
                logging.ERROR
                logging.WARNING
                logging.INFO
                logging.DEBUG
                logging.NOTSET
            Config setting should like:

            class Config(object):
                LOG_LEVEL = logging.INFO

            see: https://docs.python.org/3/library/logging.html#levels
            """)
        logging_level = config_class.LOG_LEVEL
    logger.setLevel(logging_level)


def getLogger(import_name, level=None):
    logger = logging.getLogger(import_name)

    logger.setLevel(logging.INFO)
    if level:
        logger.setLevel(level)

    return logger
