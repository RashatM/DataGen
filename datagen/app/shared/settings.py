from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import os
import yaml

DEFAULT_ENVIRONMENTS = ("dev", "test", "prod")


@dataclass(frozen=True)
class S3TargetSettings:
    name: str
    bucket: str
    prefix: str
    endpoint_url: str
    region_name: str
    verify_ssl: bool


@dataclass(frozen=True)
class S3Settings:
    enabled: bool
    run_id: str
    targets: List[S3TargetSettings]


@dataclass(frozen=True)
class AppSettings:
    s3: S3Settings


class ConfigurationError(ValueError):
    pass


def load_yaml_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_s3_targets(s3_loaded: Dict[str, Any]) -> List[S3TargetSettings]:
    targets_loaded = s3_loaded.get("targets", {})
    targets: List[S3TargetSettings] = []

    for env in DEFAULT_ENVIRONMENTS:
        cfg = targets_loaded.get(env, {})
        targets.append(
            S3TargetSettings(
                name=env,
                bucket=cfg.get("bucket", ""),
                prefix=cfg.get("prefix", "").strip("/"),
                endpoint_url=cfg.get("endpoint_url", ""),
                region_name=cfg.get("region_name", ""),
                verify_ssl=cfg.get("verify_ssl", True),
            )
        )
    return targets


def create_default_config_path() -> Path:
    return Path(__file__).resolve().parents[2] / "configuration" / "config.yaml"


def load_app_settings(config_path: str | None = None) -> AppSettings:
    path = Path(config_path) if config_path else create_default_config_path()
    loaded = load_yaml_config(path)
    s3_loaded = loaded.get("s3", {})

    run_id = os.getenv("DATAGEN_RUN_ID", s3_loaded.get("run_id", "")).strip()
    targets = parse_s3_targets(s3_loaded)
    enabled = bool(s3_loaded.get("enabled", True))

    return AppSettings(s3=S3Settings(enabled=enabled, run_id=run_id, targets=targets))