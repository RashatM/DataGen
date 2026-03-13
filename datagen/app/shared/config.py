import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass(slots=True)
class S3Config:
    bucket: str
    endpoint_url: str
    region_name: str
    use_ssl: bool
    ssl_cert: str
    aws_access_key_id: str
    aws_secret_access_key: str


@dataclass(slots=True)
class AirflowConfig:
    base_url: str
    dag_id: str
    username: str
    password: str
    dag_run_id_prefix: str
    poll_interval_seconds: int
    max_retries: int
    retry_backoff_base: int


@dataclass(slots=True)
class TargetEngineConfig:
    database_name: str


@dataclass(slots=True)
class TargetStorageConfig:
    hive: TargetEngineConfig
    iceberg: TargetEngineConfig


@dataclass(slots=True)
class AppConfig:
    s3: S3Config
    airflow: AirflowConfig
    target_storage: TargetStorageConfig


class ConfigurationError(ValueError):
    pass


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigurationError(f"Required environment variable '{name}' is not set")
    return value


def merge(defaults: Dict[str, Any], env_data: Dict[str, Any]) -> Dict[str, Any]:
    return {**defaults, **env_data}


def load_yaml_file(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise ConfigurationError(f"Config file not found: {config_path}")
    with config_path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_s3_config(config_data: Dict[str, Any]) -> S3Config:
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


def parse_airflow_env_config(config_data: Dict[str, Any], env_name: str) -> AirflowConfig:
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

    base_url = data.get("base_url", "")
    if not base_url:
        raise ConfigurationError(f"Airflow environment '{env_name}' missing 'base_url'")

    return AirflowConfig(
        base_url=base_url,
        dag_id=data.get("dag_id", ""),
        dag_run_id_prefix=data.get("dag_run_id_prefix", "datagen"),
        poll_interval_seconds=data.get("poll_interval_seconds", 10),
        max_retries=data.get("max_retries", 3),
        retry_backoff_base=data.get("retry_backoff_base", 2),
        username=require_env("AIRFLOW_USERNAME"),
        password=require_env("AIRFLOW_PASSWORD"),
    )


def parse_engine_config(config_data: Dict[str, Any], engine_name: str) -> TargetEngineConfig:
    engine_data = config_data.get(engine_name, {})
    if not engine_data:
        raise ConfigurationError(f"target_storage missing '{engine_name}' section")

    database_name = engine_data.get("database_name", "")
    if not database_name:
        raise ConfigurationError(f"target_storage.{engine_name} missing 'database_name'")

    return TargetEngineConfig(database_name=database_name)


def parse_target_storage_config(config_data: Dict[str, Any]) -> TargetStorageConfig:
    section = config_data.get("target_storage", {})
    if not section:
        raise ConfigurationError("Missing 'target_storage' section in config.yaml")

    return TargetStorageConfig(
        hive=parse_engine_config(section, "hive"),
        iceberg=parse_engine_config(section, "iceberg"),
    )


def load_app_settings(environment_name: str) -> AppConfig:
    path = Path(__file__).resolve().parents[2] / "configuration" / "config.yaml"
    config_data = load_yaml_file(path)

    return AppConfig(
        s3=parse_s3_config(config_data),
        airflow=parse_airflow_env_config(config_data, environment_name),
        target_storage=parse_target_storage_config(config_data),
    )
