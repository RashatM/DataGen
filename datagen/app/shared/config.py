import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class S3Config:
    """Конфигурация подключения к object storage, совместимому с S3 API."""
    bucket: str
    endpoint_url: str
    region_name: str
    use_ssl: bool
    ssl_cert: str
    aws_access_key_id: str
    aws_secret_access_key: str


@dataclass(slots=True)
class AirflowConfig:
    """Конфигурация подключения к Airflow и параметров ожидания DAG-run."""
    url: str
    dag_id: str
    username: str
    password: str
    dag_run_id_prefix: str
    poll_interval_seconds: int
    dag_timeout_seconds: int
    max_retries: int
    retry_backoff_base: int


@dataclass(slots=True)
class AppConfig:
    """Собранная конфигурация приложения: object storage и Airflow."""
    s3: S3Config
    airflow: AirflowConfig


class ConfigurationError(ValueError):
    """Поднимается, когда config.yaml или обязательные env vars неполны либо неконсистентны."""
    pass


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigurationError(f"Required environment variable '{name}' is not set")
    return value


def merge(defaults: dict[str, Any], env_data: dict[str, Any]) -> dict[str, Any]:
    return {**defaults, **env_data}


def load_yaml_file(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise ConfigurationError(f"Config file not found: {config_path}")
    with config_path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_s3_config(config_data: dict[str, Any]) -> S3Config:
    s3 = config_data.get("s3", {})
    if not s3:
        raise ConfigurationError("Missing 's3' section in config.yaml")

    bucket = s3.get("bucket", "")
    if not bucket:
        raise ConfigurationError("S3 missing 'bucket'")

    return S3Config(
        bucket=bucket,
        endpoint_url=s3.get("endpoint_url", ""),
        region_name=s3.get("region_name", ""),
        use_ssl=s3.get("use_ssl", True),
        ssl_cert=s3.get("ssl_cert", ""),
        aws_access_key_id=require_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=require_env("AWS_SECRET_ACCESS_KEY"),
    )


def parse_airflow_env_config(config_data: dict[str, Any], env_name: str) -> AirflowConfig:
    airflow_section = config_data.get("airflow", {})
    if not airflow_section:
        raise ConfigurationError("Missing 'airflow' section in config.yaml")

    defaults = airflow_section.get("defaults", {})
    environments = airflow_section.get("environments", {})
    if not environments:
        raise ConfigurationError("Missing 'airflow.environments' section in config.yaml")

    environment_data = environments.get(env_name)
    if not environment_data:
        raise ConfigurationError(f"Airflow environment '{env_name}' not found in config.yaml")

    data = merge(defaults, environment_data)

    base_url = data.get("url", "")
    if not base_url:
        raise ConfigurationError(f"Airflow environment '{env_name}' missing 'url'")

    dag_id = data.get("dag_id", "")
    if not dag_id:
        raise ConfigurationError(f"Airflow environment '{env_name}' missing 'dag_id'")

    return AirflowConfig(
        url=base_url,
        dag_id=dag_id,
        dag_run_id_prefix=data.get("dag_run_id_prefix", "datagen"),
        poll_interval_seconds=data.get("poll_interval_seconds", 10),
        dag_timeout_seconds=data.get("dag_timeout_seconds", 3600),
        max_retries=data.get("max_retries", 3),
        retry_backoff_base=data.get("retry_backoff_base", 2),
        username=require_env("AIRFLOW_USERNAME"),
        password=require_env("AIRFLOW_PASSWORD"),
    )

def load_app_settings(environment_name: str) -> AppConfig:
    path = Path(__file__).resolve().parents[2] / "configuration" / "config.yaml"
    config_data = load_yaml_file(path)

    return AppConfig(
        s3=parse_s3_config(config_data),
        airflow=parse_airflow_env_config(config_data, environment_name),
    )
