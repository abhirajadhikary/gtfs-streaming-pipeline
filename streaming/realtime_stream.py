import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DELTA_BUCKET = os.getenv("DELTA_BUCKET_NAME", "gtfs-lakehouse")
SILVER_BASE_PATH = f"s3a://{DELTA_BUCKET}/silver"
CHECKPOINT_RESULT = f"s3a://{DELTA_BUCKET}/checkpoints/results"

def wait_for_delta_table(spark: SparkSession, path: str):
    delta_log_path = f"{path.rstrip('/')}/_delta_log"
    fs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(spark._jsc.hadoopConfiguration())

    while not fs.exists(fs_path):
        logger.info(f"waiting for Silver Delta table to initialize at {path}....")
        time.sleep(2)

def stream_vehicle_status(spark: SparkSession):
    """Stream clean vehicle positions from Silver Delta to Kafka result.vehicle_status."""
    table_path = f"{SILVER_BASE_PATH}/vehicle_positions"
    wait_for_delta_table(spark, table_path)
    
    df = spark.readStream.format("delta").option("ignoreChanges", "true").load(table_path)

    out_df = df.select(
        F.col("vehicle_id").cast("string").alias("key"),
        F.to_json(
            F.struct(
                "vehicle_id",
                "trip_id",
                "route_id",
                "latitude",
                "longitude",
                "speed",
                "bearing",
                "event_time",
                "processing_time",
            )
        ).alias("value"),
    )

    return (
        out_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "result.vehicle_status")
        .option("checkpointLocation", f"{CHECKPOINT_RESULT}/vehicle_status")
        .trigger(processingTime="10 seconds")
        .outputMode("append")
        .start()
    )


def stream_route_health(spark: SparkSession):
    """Calculate 5-minute windowed delay averages per route and stream to result.route_health."""
    table_path = f"{SILVER_BASE_PATH}/trip_updates"
    wait_for_delta_table(spark, table_path)

    df = spark.readStream.format("delta").option("ignoreChanges", "true").load(table_path)

    route_agg = (
        df.withWatermark("event_time", "10 minutes")
        .groupBy(F.col("route_id"), F.window("event_time", "5 minutes"))
        .agg(
            F.avg("arrival_delay").alias("avg_arrival_delay_sec"),
            F.avg("departure_delay").alias("avg_departure_delay_sec"),
            F.count("trip_id").alias("total_updates_processed"),
        )
        .select(
            F.col("route_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "route_id",
                    "window.start",
                    "window.end",
                    "avg_arrival_delay_sec",
                    "avg_departure_delay_sec",
                    "total_updates_processed",
                )
            ).alias("value"),
        )
    )

    return (
        route_agg.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "result.route_health")
        .option("checkpointLocation", f"{CHECKPOINT_RESULT}/route_health")
        .trigger(processingTime="10 seconds")
        .outputMode("update")
        .start()
    )


def stream_network_health(spark: SparkSession):
    """Stream active service alerts from Silver Delta to Kafka result.network_health."""
    table_path = f"{SILVER_BASE_PATH}/service_alerts"
    wait_for_delta_table(spark, table_path)
    
    df = spark.readStream.format("delta").option("ignoreChanges", "true").load(table_path)

    out_df = df.select(
        F.col("alert_id").cast("string").alias("key"),
        F.to_json(
            F.struct(
                "alert_id",
                "cause",
                "effect",
                "header_text",
                "processing_time",
            )
        ).alias("value"),
    )

    return (
        out_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "result.network_health")
        .option("checkpointLocation", f"{CHECKPOINT_RESULT}/network_health")
        .trigger(processingTime="10 seconds")
        .outputMode("append")
        .start()
    )


def run_realtime_producer(spark: SparkSession):
    logger.info("Starting Real-time Kafka Producer streams for result.* topics...")

    q1 = stream_vehicle_status(spark)
    q2 = stream_route_health(spark)
    q3 = stream_network_health(spark)

    logger.info("All 3 result streams are actively running...")
    return [q1, q2, q3]