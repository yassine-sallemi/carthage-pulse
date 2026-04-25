"""Kafka producer for sending Reddit events to Topic A"""

import json
import logging
from typing import Optional, List
from kafka import KafkaProducer as KafkaProducerClient
from .config import get_kafka_bootstrap_servers, get_kafka_topic
from src.shared_utils import RedditEvent

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Produces RedditEvents to Kafka topic"""

    def __init__(self, config: Optional[dict] = None):
        self.topic = get_kafka_topic(config)
        bootstrap_servers = get_kafka_bootstrap_servers(config)

        try:
            self.producer = KafkaProducerClient(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v.model_dump(mode="json")).encode(
                    "utf-8"
                ),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
                retry_backoff_ms=100,
            )
            logger.info(f"Connected to Kafka topic: {self.topic}")
        except Exception as e:
            logger.error(f"Kafka connection failed: {type(e).__name__}")
            raise

    def send(self, event: RedditEvent, key: Optional[str] = None) -> bool:
        """Send a single event to Kafka"""
        try:
            future = self.producer.send(self.topic, value=event, key=key)
            future.get(timeout=10)
            return True
        except Exception:
            return False

    def send_batch(self, events: List[RedditEvent]) -> int:
        """Send batch of events to Kafka"""
        sent = 0
        for event in events:
            if self.send(event):
                sent += 1
        logger.info(f"Batch delivery: {sent}/{len(events)} events")
        return sent

    def flush(self):
        """Flush pending messages"""
        try:
            self.producer.flush(timeout=10)
        except Exception as e:
            logger.error(f"Error flushing producer: {e}")

    def close(self):
        """Close producer connection"""
        try:
            self.producer.close(timeout=10)
            logger.info("Kafka producer closed")
        except Exception:
            pass
