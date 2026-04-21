import json
from kafka import KafkaProducer
from .config import get_kafka_bootstrap_servers, get_kafka_topic


class RedditKafkaProducer:
    def __init__(self):
        self.topic = get_kafka_topic()
        self.producer = KafkaProducer(
            bootstrap_servers=get_kafka_bootstrap_servers(),
            value_serializer=lambda v: json.dumps(v.model_dump(mode="json")).encode(
                "utf-8"
            ),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    def send(self, event, key: str | None = None) -> bool:
        try:
            future = self.producer.send(self.topic, value=event, key=key)
            future.get(timeout=10)
            return True
        except Exception as e:
            print(f"Error sending: {e}")
            return False

    def send_batch(self, events) -> int:
        sent = 0
        for event in events:
            if self.send(event):
                sent += 1
        return sent

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()
