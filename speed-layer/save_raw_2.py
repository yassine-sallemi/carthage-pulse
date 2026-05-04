from pyspark.sql import SparkSession
from spark_connection import KAFKA_BROKER, SPARK_CONNECT_TARGET, run_kafka_to_postgres_stream
from event_json_types import raw_event_json_schema
from spark_columns import get_raw_columns


KAFKA_TOPIC = "reddit-events"

spark = (
    SparkSession.builder
    .appName(KAFKA_TOPIC)
    .remote(SPARK_CONNECT_TARGET)
    .getOrCreate()
)
print(f"Connected to Spark Connect Server at {SPARK_CONNECT_TARGET}.")

run_kafka_to_postgres_stream(
    spark=spark,
    kafka_broker=KAFKA_BROKER,
    topic=KAFKA_TOPIC,
    json_schema=raw_event_json_schema,
    select_columns=get_raw_columns(),
    db_table="raw_events"
)