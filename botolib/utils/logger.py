import logging


def get_logger(name):
    return logging.getLogger(f'botolib_{name}')