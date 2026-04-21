import json
import signal
from kafka import KafkaConsumer, KafkaProducer
from .config import (
    load_config,
    get_kafka_bootstrap_servers,
    get_kafka_group_id,
    get_kafka_input_topic,
    get_kafka_output_topic,
    get_kafka_dlq_topic,
    get_llm_provider,
    get_llm_api_key,
    get_llm_model,
    get_prompt,
    get_batch_size,
    get_max_retries,
)
from .providers import get_provider
from .enricher import TextEnricher


class RedditEnricherConsumer:
    def __init__(self):
        self.config = load_config()
        self.consumer = KafkaConsumer(
            get_kafka_input_topic(self.config),
            bootstrap_servers=get_kafka_bootstrap_servers(self.config),
            group_id=get_kafka_group_id(self.config),
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=5000,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=get_kafka_bootstrap_servers(self.config),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.output_topic = get_kafka_output_topic(self.config)
        self.dlq_topic = get_kafka_dlq_topic(self.config)
        self.batch_size = get_batch_size(self.config)
        self.max_retries = get_max_retries(self.config)
        self.enricher = TextEnricher(
            provider=get_provider(
                get_llm_provider(self.config),
                get_llm_api_key(self.config),
                get_llm_model(self.config),
                get_prompt(self.config),
            )
        )
        self.batch = []
        self.running = True

    def process_batch(self, batch):
        failed = []
        success = False
        for attempt in range(self.max_retries):
            enriched_events = self.enricher.enrich_batch(batch)
            if enriched_events:
                success = True
                for event in enriched_events:
                    if event.get("enrichment"):
                        self.producer.send(self.output_topic, event)
                        print(f"Enriched: {event.get('event_id')}")
                    else:
                        failed.append(event)
                break
            else:
                print(f"Retry {attempt + 1}/{self.max_retries} for batch")
                failed.extend(batch)

        if not success:
            failed.extend(batch)

        self.producer.flush()
        return failed

    def shutdown(self, signum, frame):
        print("Shutting down...")
        self.running = False
        if self.batch:
            failed = self.process_batch(self.batch)
            for event in failed:
                self.producer.send(self.dlq_topic, event)
                print(f"Sent to DLQ: {event.get('event_id')}")
            self.producer.flush()
        self.close()

    def run(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        print(f"Listening to {get_kafka_input_topic(self.config)}...")
        for message in self.consumer:
            if not self.running:
                break
            if not message.value.get("event_id"):
                continue
            self.batch.append(message.value)
            print(f"Received: {message.value.get('event_id')}")

            if len(self.batch) >= self.batch_size:
                failed = self.process_batch(self.batch)
                for event in failed:
                    self.producer.send(self.dlq_topic, event)
                    print(f"Sent to DLQ: {event.get('event_id')}")
                self.batch = []

        if self.batch and self.running:
            failed = self.process_batch(self.batch)
            for event in failed:
                self.producer.send(self.dlq_topic, event)
                print(f"Sent to DLQ: {event.get('event_id')}")
            self.batch = []

    def close(self):
        self.consumer.close()
        self.producer.flush()
        self.producer.close()
