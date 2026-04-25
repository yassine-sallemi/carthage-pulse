"""MinIO storage consumer for persisting enriched Reddit events"""

import json
import logging
import signal
from datetime import datetime
from typing import List
from io import BytesIO
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import pyarrow as pa
import pyarrow.parquet as pq
from src.shared_utils.config import (
    load_config,
    get_kafka_bootstrap_servers,
    get_kafka_storage_group_id,
    get_kafka_storage_input_topic,
    get_minio_endpoint,
    get_minio_access_key,
    get_minio_secret_key,
    get_minio_bucket,
    get_minio_secure,
    get_storage_batch_size,
)
from src.shared_utils import RedditEvent

logger = logging.getLogger(__name__)


class StorageConsumer:
    """Kafka consumer that persists enriched events to MinIO"""

    def __init__(self):
        self.config = load_config()
        logger.info("Initializing StorageConsumer")
        self.consumer = KafkaConsumer(
            get_kafka_storage_input_topic(self.config),
            bootstrap_servers=get_kafka_bootstrap_servers(self.config),
            group_id=get_kafka_storage_group_id(self.config),
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=-1,
        )
        self.minio_client = Minio(
            get_minio_endpoint(self.config),
            access_key=get_minio_access_key(self.config),
            secret_key=get_minio_secret_key(self.config),
            secure=get_minio_secure(self.config),
        )
        self.bucket = get_minio_bucket(self.config)
        self.batch_size = get_storage_batch_size(self.config)
        self._ensure_bucket()
        logger.info(
            f"StorageConsumer ready - endpoint: {get_minio_endpoint(self.config)}, bucket: {self.bucket}"
        )

        self.batch: List[RedditEvent] = []
        self.running = True

    def _ensure_bucket(self):
        """Ensure the bucket exists"""
        if not self.minio_client.bucket_exists(self.bucket):
            self.minio_client.make_bucket(self.bucket)
            logger.info(f"Created bucket: {self.bucket}")

    def _get_partition_path(self, event: RedditEvent) -> str:
        """Generate partition path based on event timestamp"""
        dt = event.timestamp
        return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{event.event_id}.parquet"

    def _write_event(self, event: RedditEvent) -> bool:
        """Write single event to MinIO as Parquet"""
        try:
            key = self._get_partition_path(event)
            table = pa.Table.from_pylist([self._event_to_dict(event)])
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            self.minio_client.put_object(
                self.bucket,
                key,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream",
            )
            logger.debug(f"Stored event {event.event_id} to {key}")
            return True
        except S3Error as e:
            logger.error(f"S3Error writing event {event.event_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error writing event {event.event_id}: {e}")
            return False

    def _event_to_dict(self, event: RedditEvent) -> dict:
        """Convert RedditEvent to dict for Parquet"""
        d = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "posted_in_subreddit": event.posted_in_subreddit,
            "author": event.author,
            "url": event.url,
            "title": event.title,
            "content": event.content,
            "timestamp": event.timestamp.isoformat(),
            "has_media": event.has_media,
            "media_urls": event.media_urls,
            "score": event.score,
            "upvote_ratio": event.upvote_ratio,
            "num_comments": event.num_comments,
            "is_crosspost": event.is_crosspost,
            "original_subreddit": event.original_subreddit,
        }
        if event.enrichment:
            e = event.enrichment
            d.update(
                {
                    "enrichment_languages": (
                        [l.value for l in e.languages] if e.languages else []
                    ),
                    "enrichment_translation": e.translation,
                    "enrichment_sentiment": e.sentiment_score,
                    "enrichment_intent": e.intent,
                    "enrichment_topics": e.topics or [],
                    "enrichment_entity_names": (
                        [ent.name for ent in e.entities] if e.entities else []
                    ),
                    "enrichment_entity_types": (
                        [ent.type for ent in e.entities] if e.entities else []
                    ),
                }
            )
        return d

    def _write_batch(self, events: List[RedditEvent]) -> int:
        """Write batch of events to MinIO as Parquet"""
        if not events:
            return 0
        try:
            first_ts = events[0].timestamp
            partition_path = f"batch/year={first_ts.year}/month={first_ts.month:02d}/day={first_ts.day:02d}/{datetime.now().strftime('%H%M%S')}.parquet"
            records = [self._event_to_dict(e) for e in events]
            table = pa.Table.from_pylist(records)
            buffer = BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)
            self.minio_client.put_object(
                self.bucket,
                partition_path,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream",
            )
            logger.info(f"Wrote batch of {len(events)} events to {partition_path}")
            return len(events)
        except S3Error as e:
            logger.error(f"S3Error writing batch: {e}")
            return 0
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
            return 0

    def shutdown(self, signum, frame):
        """Handle shutdown signal"""
        logger.info("Shutdown signal received")
        self.running = False
        if self.batch:
            logger.info(f"Writing final batch of {len(self.batch)} events")
            self._write_batch(self.batch)
        self.close()

    def run(self):
        """Main consumer loop"""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        logger.info(f"Listening on: {get_kafka_storage_input_topic(self.config)}")

        try:
            for message in self.consumer:
                if not self.running:
                    break
                if not message.value.get("event_id"):
                    continue

                try:
                    event = RedditEvent(**message.value)
                    if event.enrichment:
                        self.batch.append(event)
                        logger.debug(
                            f"Buffered event {event.event_id} ({len(self.batch)}/{self.batch_size})"
                        )
                    else:
                        logger.debug(f"Skipping event {event.event_id} - no enrichment")
                except Exception as e:
                    logger.error(f"Invalid event format: {e}")
                    continue

                if len(self.batch) >= self.batch_size:
                    written = self._write_batch(self.batch)
                    logger.info(f"Batch processed: {written} events written")
                    self.batch = []

            if self.batch and self.running:
                logger.info(f"Writing final batch of {len(self.batch)} events")
                written = self._write_batch(self.batch)
                logger.info(f"Final batch: {written} events written")
                self.batch = []
        except Exception as e:
            logger.exception(f"Unexpected error in consumer loop: {e}")
            raise

    def close(self):
        """Close consumer"""
        logger.info("Closing consumer")
        try:
            self.consumer.close()
        except Exception:
            pass
        logger.info("StorageConsumer shutdown complete")
