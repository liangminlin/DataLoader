import logging

logging.basicConfig(format="[%(asctime)-15s] %(message)s")


def getLogger(import_name, level=None):
    logger = logging.getLogger(import_name)

    logger.setLevel(logging.INFO)
    if level:
        logger.setLevel(level)

    return logger
