"""Configuration management for storage service"""

from typing import Optional
import yaml
from pathlib import Path
from src.shared_utils.config_utils import expand_env_vars


def load_config(config_path: str = "config/dev.yaml") -> dict:
    """Load YAML configuration file with env var expansion"""
    path = Path(config_path)
    if path.exists():
        with open(path) as f:
            config = yaml.safe_load(f) or {}
            return expand_env_vars(config)
    return {}


def get_kafka_bootstrap_servers(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("bootstrap_servers", ["localhost:9092"])


def get_kafka_group_id(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("storage_group_id", "reddit-storage-group")


def get_kafka_input_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("storage_input_topic", "reddit-events-enriched")


def get_minio_endpoint(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("endpoint", "localhost:9000")


def get_minio_access_key(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("access_key", "minioadmin")


def get_minio_secret_key(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("secret_key", "minioadmin")


def get_minio_bucket(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("bucket", "reddit-events")


def get_minio_secure(config: Optional[dict] = None) -> bool:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("secure", False)


def get_batch_size(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("storage", {}).get("batch_size", 10)