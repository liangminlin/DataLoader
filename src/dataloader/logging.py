import logging
import datetime


def init_logger(import_name):
    from .helper import get_root_path

    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    r = get_root_path(import_name)
    logfile = "/".join(r.split("/")[:-1]) + "/dataloader." + dt + ".log"

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("[%(asctime)-15s] %(message)s")
    )

    file_handler = logging.FileHandler(logfile, mode='a')
    file_handler.setFormatter(
        logging.Formatter("[%(asctime)-15s] %(message)s")
    )

    logging.basicConfig(handlers=[
        stream_handler, file_handler
    ])

    logger = logging.getLogger(import_name)
    logger.setLevel(logging.INFO)


def getLogger(import_name, level=None):
    logger = logging.getLogger(import_name)

    logger.setLevel(logging.INFO)
    if level:
        logger.setLevel(level)

    return logger
