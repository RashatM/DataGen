import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import yaml


@dataclass(slots=True)
class S3Config:
    bucket: str
    prefix: str
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
class AppConfig:
    s3: Dict[str, S3Config]
    airflow: Dict[str, AirflowConfig]


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


def parse_s3_config(env_name: str, data: Dict[str, Any]) -> S3Config:
    bucket = data.get("bucket", "")
    if not bucket:
        raise ConfigurationError(f"S3 env '{env_name}' missing 'bucket'")

    return S3Config(
        bucket=bucket,
        prefix=data.get("prefix", "").strip("/"),
        endpoint_url=data.get("endpoint_url", ""),
        region_name=data.get("region_name", ""),
        use_ssl=data.get("use_ssl", True),
        ssl_cert=data.get("ssl_cert", ""),
        aws_access_key_id=require_env(f"S3_{env_name.upper()}_ACCESS_KEY_ID"),
        aws_secret_access_key=require_env(f"S3_{env_name.upper()}_SECRET_ACCESS_KEY"),
    )


def parse_s3_configs(config_data: Dict[str, Any]) -> Dict[str, S3Config]:
    s3_section = config_data.get("s3", {})
    if not s3_section:
        raise ConfigurationError("Missing 's3' section in config.yaml")

    defaults = s3_section.get("defaults", {})
    envs = s3_section.get("envs", {})
    if not envs:
        raise ConfigurationError("Missing 's3.envs' section in config.yaml")

    return {
        env_name: parse_s3_config(env_name, merge(defaults, env_data))
        for env_name, env_data in envs.items()
    }


def parse_airflow_config(env_name: str, data: Dict[str, Any]) -> AirflowConfig:
    base_url = data.get("base_url", "")
    if not base_url:
        raise ConfigurationError(f"Airflow env '{env_name}' missing 'base_url'")

    return AirflowConfig(
        base_url=base_url,
        dag_id=data.get("dag_id", ""),
        dag_run_id_prefix=data.get("dag_run_id_prefix", "datagen"),
        poll_interval_seconds=data.get("poll_interval_seconds", 10),
        max_retries=data.get("max_retries", 3),
        retry_backoff_base=data.get("retry_backoff_base", 2),
        username=require_env(f"AIRFLOW_{env_name.upper()}_USERNAME"),
        password=require_env(f"AIRFLOW_{env_name.upper()}_PASSWORD"),
    )


def parse_airflow_configs(config_data: Dict[str, Any]) -> Dict[str, AirflowConfig]:
    airflow_section = config_data.get("airflow", {})
    if not airflow_section:
        raise ConfigurationError("Missing 'airflow' section in config.yaml")

    defaults = airflow_section.get("defaults", {})
    envs = airflow_section.get("envs", {})
    if not envs:
        raise ConfigurationError("Missing 'airflow.envs' section in config.yaml")

    return {
        env_name: parse_airflow_config(env_name, merge(defaults, env_data))
        for env_name, env_data in envs.items()
    }


def load_app_settings() -> AppConfig:
    path = Path(__file__).resolve().parents[2] / "configuration" / "config.yaml"
    config_data = load_yaml_file(path)

    return AppConfig(
        s3=parse_s3_configs(config_data),
        airflow=parse_airflow_configs(config_data),
    )