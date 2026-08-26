import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)
from streaming.utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DELTA_BUCKET = os.getenv("DELTA_BUCKET_NAME", "gtfs-lakehouse")
BRONZE_BASE_PATH = f"s3a://{DELTA_BUCKET}/bronze"
SILVER_BASE_PATH = f"s3a://{DELTA_BUCKET}/silver"
QUARANTINE_BASE_PATH = f"s3a://{DELTA_BUCKET}/quarantine"
CHECKPOINT_SILVER = f"s3a://{DELTA_BUCKET}/checkpoints/silver"
CHECKPOINT_QUARANTINE = f"s3a://{DELTA_BUCKET}/checkpoints/quarantine"

# Schemas
VEHICLE_POSITION_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle", StructType([
        StructField("trip", StructType([
            StructField("tripId", StringType(), True),
            StructField("routeId", StringType(), True),
            StructField("startDate", StringType(), True),
        ]), True),
        StructField("position", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("bearing", DoubleType(), True),
            StructField("speed", DoubleType(), True),
        ]), True),
        StructField("timestamp", StringType(), True),
        StructField("vehicle", StructType([
            StructField("id", StringType(), True),
            StructField("label", StringType(), True),
        ]), True),
    ]), True),
])

TRIP_UPDATE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("tripUpdate", StructType([
        StructField("trip", StructType([
            StructField("tripId", StringType(), True),
            StructField("routeId", StringType(), True),
        ]), True),
        StructField("stopTimeUpdate", ArrayType(StructType([
            StructField("stopSequence", LongType(), True),
            StructField("stopId", StringType(), True),
            StructField("arrival", StructType([
                StructField("delay", LongType(), True),
                StructField("time", StringType(), True)
            ]), True),
            StructField("departure", StructType([
                StructField("delay", LongType(), True),
                StructField("time", StringType(), True)
            ]), True),
        ])), True),
        StructField("timestamp", StringType(), True),
    ]), True)
])

SERVICE_ALERT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("alert", StructType([
        StructField("cause", StringType(), True),
        StructField("effect", StringType(), True),
        StructField("headerText", StructType([
            StructField("translation", ArrayType(StructType([
                StructField("text", StringType(), True)
            ])), True)
        ]), True),
        StructField("informedEntity", ArrayType(StructType([
            StructField("routeId", StringType(), True),
            StructField("stopId", StringType(), True),
        ])), True),
    ]), True),
])

def process_vehicle_positions(bronze_df):
    vp_stream = bronze_df.filter(F.col("topic") == "raw.vehicle_positions")
    parsed = vp_stream.withColumn("parsed", F.from_json(F.col("payload"), VEHICLE_POSITION_SCHEMA))

    valid_cond = (
        F.col("parsed.vehicle.vehicle.id").isNotNull() &
        F.col("parsed.vehicle.position.latitude").between(-90, 90) &
        F.col("parsed.vehicle.position.longitude").between(-180, 180)
    )

    valid_df = parsed.filter(valid_cond).select(
        F.col("parsed.vehicle.vehicle.id").alias("vehicle_id"),
        F.col("parsed.vehicle.trip.tripId").alias("trip_id"),
        F.col("parsed.vehicle.trip.routeId").alias("route_id"),
        F.col("parsed.vehicle.position.latitude").alias("latitude"),
        F.col("parsed.vehicle.position.longitude").alias("longitude"),
        F.col("parsed.vehicle.position.speed").alias("speed"),
        F.col("parsed.vehicle.position.bearing").alias("bearing"),
        F.to_timestamp(F.from_unixtime(F.col("parsed.vehicle.timestamp").cast("long"))).alias("event_time"),
        F.col("ingestion_timestamp").alias("processing_time")
    ).withWatermark("event_time", "10 minutes").dropDuplicates(["vehicle_id", "event_time"])

    return valid_df.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_SILVER}/vehicle_positions") \
        .option("path", f"{SILVER_BASE_PATH}/vehicle_positions").start()

def process_trip_updates(bronze_df):
    tu_stream = bronze_df.filter(F.col("topic") == "raw.trip_updates")
    parsed = tu_stream.withColumn("parsed", F.from_json(F.col("payload"), TRIP_UPDATE_SCHEMA))

    valid_df = parsed.filter(F.col("parsed.tripUpdate.trip.tripId").isNotNull()).select(
        F.col("parsed.tripUpdate.trip.tripId").alias("trip_id"),
        F.col("parsed.tripUpdate.trip.routeId").alias("route_id"),
        F.explode("parsed.tripUpdate.stopTimeUpdate").alias("stop_update"),
        F.to_timestamp(F.from_unixtime(F.col("parsed.tripUpdate.timestamp").cast("long"))).alias("event_time"),
        F.col("ingestion_timestamp").alias("processing_time")
    ).select(
        "trip_id", "route_id", "event_time", "processing_time",
        F.col("stop_update.stopSequence").alias("stop_sequence"),
        F.col("stop_update.stopId").alias("stop_id"),
        F.col("stop_update.arrival.delay").alias("arrival_delay"),
        F.col("stop_update.departure.delay").alias("departure_delay")
    ).withWatermark("event_time", "10 minutes").dropDuplicates(["trip_id", "stop_sequence", "event_time"])

    return valid_df.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_SILVER}/trip_updates") \
        .option("path", f"{SILVER_BASE_PATH}/trip_updates").start()

def process_service_alerts(bronze_df):
    sa_stream = bronze_df.filter(F.col("topic") == "raw.service_alerts")
    parsed = sa_stream.withColumn("parsed", F.from_json(F.col("payload"), SERVICE_ALERT_SCHEMA))

    # Safely extract text from array using element_at
    valid_df = parsed.filter(F.col("parsed.id").isNotNull()).select(
        F.col("parsed.id").alias("alert_id"),
        F.col("parsed.alert.cause").alias("cause"),
        F.col("parsed.alert.effect").alias("effect"),
        F.element_at(F.col("parsed.alert.headerText.translation"), 1).getItem("text").alias("header_text"),
        F.col("ingestion_timestamp").alias("processing_time")
    )

    return valid_df.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_SILVER}/service_alerts") \
        .option("path", f"{SILVER_BASE_PATH}/service_alerts").start()

def run_silver_pipeline():
    spark = get_spark_session("GTFS-Silver-Processing")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Silver Processing Streams for all topics...")
    bronze_df = spark.readStream.format("delta").load(BRONZE_BASE_PATH)

    q1 = process_vehicle_positions(bronze_df)
    q2 = process_trip_updates(bronze_df)
    q3 = process_service_alerts(bronze_df)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_silver_pipeline()