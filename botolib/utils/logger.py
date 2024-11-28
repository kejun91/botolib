import logging


def get_logger(name = "default"):
    return logging.getLogger(f'botolib_{name}')