import os
import logging
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

class GTFSKafkaConsumer:

    def __init__(self, spark: SparkSession, topics: list = None):
        self.spark = spark
        self.topics = topics or [
            "raw.vehicle_positions",
            "raw.trip_updates",
            "raw.service_alerts",
        ]

    def read_raw_stream(self) -> DataFrame:
        logger.info(f"Subscribing to Kafka topics: {self.topics}")
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", ",".join(self.topics))
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", "1000")
            .load()
        )