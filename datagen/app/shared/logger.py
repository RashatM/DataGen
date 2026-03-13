import logging


LOGGER_FORMAT = "[%(name)s] %(asctime)s %(levelname)s: %(message)s"
LOGGER_DATE_FORMAT = "%d-%m-%y %H:%M:%S"


def create_logger(name: str) -> logging.Logger:
    custom_logger = logging.getLogger(name)
    custom_logger.setLevel(logging.INFO)

    if not custom_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt=LOGGER_FORMAT,
            datefmt=LOGGER_DATE_FORMAT,
        )
        handler.setFormatter(formatter)
        custom_logger.addHandler(handler)

    custom_logger.propagate = False
    return custom_logger


def get_logger(name: str) -> logging.Logger:
    return create_logger(name)
