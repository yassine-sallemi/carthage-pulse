"""Configuration management for processing service"""

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
            # Expand environment variables (${VAR_NAME})
            return expand_env_vars(config)
    return {}


def get_llm_provider(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("provider", "openrouter")


def get_llm_api_key(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("api_key", "")


def get_llm_model(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("model", "gpt-4o-mini")


def get_kafka_bootstrap_servers(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("bootstrap_servers", ["localhost:9092"])


def get_kafka_group_id(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("processing_group_id", "reddit-enricher-group")


def get_kafka_input_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("processing_input_topic", "reddit-events")


def get_kafka_output_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get(
        "processing_output_topic", "reddit-events-enriched"
    )


def get_kafka_dlq_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("processing_dlq_topic", "reddit-events-dlq")


def get_batch_size(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("processing", {}).get("batch_size", 5)


def get_max_retries(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("processing", {}).get("max_retries", 3)


def get_prompt(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("prompt", "")
