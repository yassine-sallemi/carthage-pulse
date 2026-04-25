"""Ingestion service - fetches Reddit posts and sends to Kafka"""

from .reddit_client import RedditClient
from .producer import KafkaProducer

__all__ = ["RedditClient", "KafkaProducer"]
