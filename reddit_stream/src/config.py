from typing import Optional
import yaml
from pathlib import Path


def load_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


def get_subreddits(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("subreddits", ["Tunisia"])


def get_poll_interval(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("poll_interval", 30)


def get_fetch_limit(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("fetch_limit", 100)


def get_user_agent(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("user_agent", "reddit-rss/1.0")


def get_endpoints(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("endpoints", ["new", "comments"])


def build_subreddit_url(subreddits: list) -> str:
    return "+".join(subreddits)


def get_kafka_bootstrap_servers(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("bootstrap_servers", ["localhost:9092"])


def get_kafka_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("topic", "reddit-events")


def get_initial_fetch(config: Optional[dict] = None) -> bool:
    if config is None:
        config = load_config()
    return config.get("initial_fetch", True)
