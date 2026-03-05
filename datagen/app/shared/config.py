from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import os
import yaml

DEFAULT_ENVIRONMENTS = ("dev", "test", "prod")


@dataclass(slots=True)
class S3Config:
    bucket: str
    prefix: str
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str
    region_name: str
    use_ssl: bool
    ssl_cert: str


@dataclass(slots=True)
class AppConfig:
    s3: List[S3Config]


class ConfigurationError(ValueError):
    pass


def load_yaml_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_s3_configs(config_data: Dict[str, Any]) -> List[S3Config]:
    s3_configs_by_env = config_data.get("endpoints", {})
    s3_configs: List[S3Config] = []

    for env, config in s3_configs_by_env.keys():
        s3_configs.append(
            S3Config(
                bucket=config.get("bucket", ""),
                prefix=config.get("prefix", "").strip("/"),
                endpoint_url=config.get("endpoint_url", ""),
                region_name=config.get("region_name", ""),
                use_ssl=config.get("use_ssl", True),
                ssl_cert=config.get("ssl_cert", ""),
                aws_access_key_id=config.get("aws_access_key_id", ""), #TODO заменить на os.env
                aws_secret_access_key=config.get("aws_secret_access_key", ""), #TODO заменить на os.env
            )
        )
    return s3_configs


def load_app_settings() -> AppConfig:
    path = Path(__file__).resolve().parents[2] / "configuration" / "config.yaml"
    config_data = load_yaml_config(path)
    s3_configs = parse_s3_configs(config_data)
    return AppConfig(s3=s3_configs)
