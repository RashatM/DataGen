import logging


LOGGER_FORMAT = "[%(name)s] %(asctime)s %(levelname)s: %(message)s"
LOGGER_DATE_FORMAT = "%d-%m-%y %H:%M:%S"
PIPELINE_LOGGER_NAME = "datagen.pipeline"
GENERATION_LOGGER_NAME = "datagen.generation"
PUBLICATION_LOGGER_NAME = "datagen.publication"
AIRFLOW_LOGGER_NAME = "datagen.airflow"
COMPARISON_LOGGER_NAME = "datagen.comparison"


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


pipeline_logger = get_logger(PIPELINE_LOGGER_NAME)
generation_logger = get_logger(GENERATION_LOGGER_NAME)
publication_logger = get_logger(PUBLICATION_LOGGER_NAME)
airflow_logger = get_logger(AIRFLOW_LOGGER_NAME)
comparison_logger = get_logger(COMPARISON_LOGGER_NAME)
