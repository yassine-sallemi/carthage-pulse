from typing import Any

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, from_json, to_json
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    IntegerType, FloatType, ArrayType, TimestampType
)

from spark_connection import KAFKA_BROKER, SPARK_CONNECT_TARGET
from event_json_types import  raw_event_json_schema

KAFKA_TOPIC = "reddit-events"


spark_builder: Any = SparkSession.builder
spark = spark_builder.appName(KAFKA_TOPIC).remote(SPARK_CONNECT_TARGET).getOrCreate()

print(f"Connected to Spark Connect Server at {SPARK_CONNECT_TARGET}. Reading from {KAFKA_TOPIC}...")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = df.selectExpr("CAST(value AS STRING) AS payload") \
              .withColumn("data", from_json(col("payload"), raw_event_json_schema))

flattened_df = parsed_df.select(
    col("data.event_id"),
    col("data.event_type"),
    col("data.posted_in_subreddit"),
    col("data.author"),
    col("data.url"),
    col("data.title"),
    col("data.content"),
    col("data.timestamp"),
    col("data.has_media"),
    col("data.media_urls"),
    col("data.score"),
    col("data.upvote_ratio"),
    col("data.num_comments"),
    col("data.is_crosspost"),
    col("data.original_subreddit"),
)

def write_to_postgres(batch_df, batch_id):
    """Function to write each micro-batch to the database."""
    print(f"Writing batch {batch_id} to Postgres...")
    count = batch_df.count()

    print("-------------------------------------------")
    print(f"BATCH ID: {batch_id}")
    print(f"RECORDS RECEIVED: {count}")

    # We use the BATCH 'write' here, even though the source is a stream
    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/reddit") #TODO: env var
        .option("dbtable", "raw_events")
        .option("user", "reddit") # TODO: env var
        .option("password", "reddit") # TODO: env var
        .option("driver", "org.postgresql.Driver")
        .option("stringtype", "unspecified")
        .mode("append") # Add new data to the table
        .save()
    )
    print("-------------------------------------------")


query = (
    flattened_df.writeStream
    .outputMode("append")
    .format("console")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/data/checkpoints/spark_checkpoints_"+KAFKA_TOPIC)
    .start()
)
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping stream...")
    query.stop()