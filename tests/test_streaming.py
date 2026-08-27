import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from streaming.silver import deduplicate_and_watermark_vehicles

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("GTFS-Streaming-Tests")
        .master("local[2]")
        .getOrCreate()
    )

def test_silver_deduplication_and_watermarking(spark):
    schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("event_time", LongType(), True)
    ])

    # Sample input containing exact duplicates
    data = [
        ("v1", "M15", 22.5, 1700000000),
        ("v1", "M15", 22.5, 1700000000),  # Duplicate
        ("v2", "M34", 15.0, 1700000005)
    ]

    df = spark.createDataFrame(data, schema)
    deduped_df = deduplicate_and_watermark_vehicles(df)

    assert deduped_df.count() == 2