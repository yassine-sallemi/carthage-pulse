from typing import Any

from pyspark.sql import SparkSession

from spark_connection import KAFKA_BROKER, KAFKA_TOPIC, SPARK_CONNECT_TARGET



spark_builder: Any = SparkSession.builder
spark = spark_builder.appName("TestKafkaRedditConnect").remote(SPARK_CONNECT_TARGET).getOrCreate()

print(f"Connected to Spark Connect Server at {SPARK_CONNECT_TARGET}. Reading from {KAFKA_TOPIC}...")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS payload")

def write_to_postgres(batch_df, batch_id):
    """Function to write each micro-batch to the database."""
    print(f"Writing batch {batch_id} to Postgres...")
    count = batch_df.count()

    print("-------------------------------------------")
    print(f"BATCH ID: {batch_id}")
    print(f"RECORDS RECEIVED: {count}")

    # We use the BATCH 'write' here, even though the source is a stream
    result = (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/reddit") #TODO: env var
        .option("dbtable", "enriched_events")
        .option("user", "reddit") # TODO: env var
        .option("password", "reddit") # TODO: env var
        .option("driver", "org.postgresql.Driver")
        .mode("append") # Add new data to the table
        .save()
    )
    print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA result: ", result)


def simple_batch_processor(batch_df, batch_id):
    """
    A simple function that doesn't touch external databases.
    It runs on the Spark Server side.
    """
    # Perform a basic action to trigger computation
    count = batch_df.count()

    print("-------------------------------------------")
    print(f"BATCH ID: {batch_id}")
    print(f"RECORDS RECEIVED: {count}")

    if count > 0:
        # Show a snippet of the data in the server logs
        batch_df.show(5, truncate=False)
    print("-------------------------------------------")
"""
class PostgresRowWriter:
    def open(self, partition_id, epoch_id):
        # This runs on the executor
        import psycopg2 # Ensure this is installed in the container!
        self.conn = psycopg2.connect("host=postgres dbname=reddit user=reddit password=reddit")
        self.cursor = self.conn.cursor()
        return True

    def process(self, row):
        # Write individual row
        self.cursor.execute(
            "INSERT INTO enriched_events (key, payload) VALUES (%s, %s)",
            (row.key, row.payload)
        )

    def close(self, error):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
"""
"""
query = (
    parsed_df.writeStream
    .foreach(PostgresRowWriter()) # Notice: .foreach(), NOT .foreachBatch()
    .option("checkpointLocation", "/tmp/spark_checkpoints_new")
    .start()
)
"""
query = (
    parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/spark_checkpoints") # Needed to keep track of progress
    .start()
)
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping stream...")
    query.stop()