from typing import Any, Dict, List


from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.core.application.use_cases.data_pipeline_use_case import DataPipelineUseCase
from app.infrastructure.airflow.airflow_client import AirflowClient
from app.infrastructure.airflow.airflow_dag_runner import AirflowDagRunner
from app.providers import provide_generation_service, provide_publication_service
from app.shared.config import AppConfig, AirflowConfig, load_app_settings
from app.shared.logger import logger


def build_dag_runner(airflow_config: AirflowConfig) -> DagRunnerPort:
    return AirflowDagRunner(client=AirflowClient(airflow_config))


def build_dag_runners(config: AppConfig) -> Dict[str, DagRunnerPort]:
    return {
        env_name: build_dag_runner(airflow_config)
        for env_name, airflow_config in config.airflow.items()
    }


def build_publication_services(config: AppConfig):
    return {
        env_name: provide_publication_service(s3_config=s3_config)
        for env_name, s3_config in config.s3.items()
        if s3_config.bucket
    }


def build_pipeline(config: AppConfig) -> DataPipelineUseCase:
    publication_services = build_publication_services(config)
    dag_runners = build_dag_runners(config)

    if publication_services.keys() != dag_runners.keys():
        raise ValueError(
            f"S3 envs {set(publication_services)} and Airflow envs {set(dag_runners)} do not match"
        )

    return DataPipelineUseCase(
        generation_service=provide_generation_service(),
        publication_services=publication_services,
    )


def run():
    config = load_app_settings()
    raw_tables: List[Any] = []

    pipeline = build_pipeline(config)
    results = pipeline.execute(raw_tables)

    for env_name, dag_result in results.items():
        logger.info(
            "env=%s dag_run_id=%s status=%s",
            env_name,
            dag_result.run_id,
            dag_result.status.value,
        )


if __name__ == "__main__":
    run()