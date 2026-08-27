import json
import time
import logging
from kafka import KafkaProducer
from google.protobuf.json_format import MessageToDict

from ingestion.config import(
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_VEHICLE_POSITIONS,
    TOPIC_TRIP_UPDATES,
    TOPIC_SERVICE_ALERTS,
    POLL_INTERVAL_SECONDS,
    GTFS_FEEDS,
)

from ingestion.fetcher import GTFSFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
    )

def run_ingestion_loop():
    producer = create_kafka_producer()
    fetchers = {
        TOPIC_VEHICLE_POSITIONS: GTFSFetcher(GTFS_FEEDS['vehicle_positions']),
        TOPIC_TRIP_UPDATES: GTFSFetcher(GTFS_FEEDS['trip_updates']),
        TOPIC_SERVICE_ALERTS: GTFSFetcher(GTFS_FEEDS['service_alerts']),
    }

    logger.info("Starting ingestion loop...")

    while True:
        for topic, fetcher in fetchers.items():
            feed = fetcher.fetch_feed()
            if not feed:
                continue

            for entity in feed.entity:
                entity_dict = MessageToDict(entity)
                key = entity.id
                if entity.HasField("vehicle") and entity.vehicle.vehicle.id:
                    key = entity.vehicle.vehicle.id
                elif entity.HasField("trip_update") and entity.trip_update.trip.trip_id:
                    key = entity.trip_update.trip.trip_id
                
                producer.send(topic, key=key, value=entity_dict)

        producer.flush()  
        logger.info(f"Published entities to topic: {topic}")

    time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_ingestion_loop()  