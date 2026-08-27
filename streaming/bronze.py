import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from streaming.consumer import GTFSKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Configuration
DELTA_BUCKET = os.getenv("DELTA_BUCKET_NAME", "gtfs-lakehouse")
BRONZE_BASE_PATH = f"s3a://{DELTA_BUCKET}/bronze"
CHECKPOINT_BASE_PATH = f"s3a://{DELTA_BUCKET}/checkpoints/bronze"

RAW_TOPICS = ["raw.vehicle_positions", "raw.trip_updates", "raw.service_alerts"]


def run_bronze_pipeline(spark: SparkSession):
    logger.info("Starting Kafka to Bronze Streaming Pipeline")

    consumer = GTFSKafkaConsumer(spark, RAW_TOPICS)
    raw_stream = consumer.read_raw_stream()

    bronze_df = raw_stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingestion_date"),
    )

    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .partitionBy("topic", "ingestion_date")
        .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/raw_topics")
        .option("path", BRONZE_BASE_PATH)
        .start()
    )

    logger.info(f"Bronze stream actively writing to {BRONZE_BASE_PATH}...")
    return query