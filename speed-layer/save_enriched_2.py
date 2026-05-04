from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json
from spark_connection import KAFKA_BROKER, run_kafka_to_postgres_stream, \
    SPARK_CONNECT_TARGET
from event_json_types import enriched_event_json_schema
from spark_columns import get_enriched_columns

KAFKA_TOPIC = "reddit-events-enriched"

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
    json_schema=enriched_event_json_schema,
    select_columns=get_enriched_columns(),
    db_table="enriched_events"
)