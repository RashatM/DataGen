from typing import Any, List


from app.providers import provide_pipeline_use_case
from app.shared.config import load_app_settings
from app.shared.logger import logger


def run():
    config = load_app_settings()
    raw_tables: List[Any] = []

    for env_name in config.s3:
        if env_name not in config.airflow:
            logger.warning("No Airflow config for env=%s, skipping", env_name)
            continue

        pipeline = provide_pipeline_use_case(env_name, config)
        dag_result = pipeline.execute(raw_tables)

        logger.info(
            "env=%s dag_run_id=%s status=%s",
            env_name,
            dag_result.run_id,
            dag_result.status.value,
        )


if __name__ == "__main__":
    run()
