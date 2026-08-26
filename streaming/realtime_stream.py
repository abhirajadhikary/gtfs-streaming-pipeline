import os
import logging
from pyspark.sql import functions as F
from streaming.utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DELTA_BUCKET = os.getenv("DELTA_BUCKET_NAME", "gtfs-lakehouse")
SILVER_BASE_PATH = f"s3a://{DELTA_BUCKET}/silver"
CHECKPOINT_RESULT = f"s3a://{DELTA_BUCKET}/checkpoints/results"


def stream_vehicle_status(spark):
    """Stream clean vehicle positions from Silver Delta to Kafka result.vehicle_status."""
    df = spark.readStream.format("delta").load(f"{SILVER_BASE_PATH}/vehicle_positions")

    out_df = df.select(
        F.col("vehicle_id").alias("key"),
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
                "processing_time"
            )
        ).alias("value")
    )

    return (
        out_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "result.vehicle_status")
        .option("checkpointLocation", f"{CHECKPOINT_RESULT}/vehicle_status")
        .outputMode("append")
        .start()
    )


def stream_route_health(spark):
    """Calculate 5-minute windowed delay averages per route and stream to result.route_health."""
    df = spark.readStream.format("delta").load(f"{SILVER_BASE_PATH}/trip_updates")

    # Aggregate delays using event-time windows and watermarking
    route_agg = (
        df.withWatermark("event_time", "10 minutes")
        .groupBy(F.col("route_id"), F.window("event_time", "5 minutes"))
        .agg(
            F.avg("arrival_delay").alias("avg_arrival_delay_sec"),
            F.avg("departure_delay").alias("avg_departure_delay_sec"),
            F.count("trip_id").alias("total_updates_processed")
        )
        .select(
            F.col("route_id").alias("key"),
            F.to_json(
                F.struct(
                    "route_id",
                    "window.start",
                    "window.end",
                    "avg_arrival_delay_sec",
                    "avg_departure_delay_sec",
                    "total_updates_processed"
                )
            ).alias("value")
        )
    )

    return (
        route_agg.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "result.route_health")
        .option("checkpointLocation", f"{CHECKPOINT_RESULT}/route_health")
        .outputMode("update")
        .start()
    )


def stream_network_health(spark):
    """Stream active service alerts from Silver Delta to Kafka result.network_health."""
    df = spark.readStream.format("delta").load(f"{SILVER_BASE_PATH}/service_alerts")

    out_df = df.select(
        F.col("alert_id").alias("key"),
        F.to_json(
            F.struct(
                "alert_id",
                "cause",
                "effect",
                "header_text",
                "processing_time"
            )
        ).alias("value")
    )

    return (
        out_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "result.network_health")
        .option("checkpointLocation", f"{CHECKPOINT_RESULT}/network_health")
        .outputMode("append")
        .start()
    )


def run_realtime_producer():
    spark = get_spark_session("GTFS-Realtime-Producer")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Real-time Kafka Producer streams for result.* topics...")

    # Start all streaming output queries
    q1 = stream_vehicle_status(spark)
    q2 = stream_route_health(spark)
    q3 = stream_network_health(spark)

    logger.info("All 3 result streams are actively running...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_realtime_producer()