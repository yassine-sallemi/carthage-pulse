import yaml
from pathlib import Path


def load_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


def get_llm_provider(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("provider", "openrouter")


def get_llm_api_key(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("api_key", "")


def get_llm_model(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("model", "gpt-4o-mini")


def get_kafka_bootstrap_servers(config: dict | None = None) -> list:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("bootstrap_servers", ["localhost:9092"])


def get_kafka_group_id(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("group_id", "reddit-enricher-group")


def get_kafka_input_topic(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("input_topic", "reddit-events")


def get_kafka_output_topic(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("output_topic", "reddit-events-enriched")


def get_kafka_dlq_topic(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("dlq_topic", "reddit-events-dlq")


def get_batch_size(config: dict | None = None) -> int:
    if config is None:
        config = load_config()
    return config.get("batch_size", 5)


def get_max_retries(config: dict | None = None) -> int:
    if config is None:
        config = load_config()
    return config.get("max_retries", 3)


def get_prompt(config: dict | None = None) -> str:
    if config is None:
        config = load_config()
    return config.get("prompt", "")
