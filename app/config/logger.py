import logging

def create_logger() -> logging.Logger:
    custom_logger = logging.getLogger("dq_check_logger")
    custom_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s: %(message)s', datefmt='%d-%m-%y %H:%M:%S')
    handler.setFormatter(formatter)
    custom_logger.addHandler(handler)
    return custom_logger

logger = create_logger()