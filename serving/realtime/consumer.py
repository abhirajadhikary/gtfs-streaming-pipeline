"""Materialize realtime Kafka results into Redis for the serving API."""

import json
import logging
import os
from typing import Any

import redis
from kafka import KafkaConsumer


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CONSUMER_GROUP = os.getenv("SERVING_KAFKA_GROUP", "gtfs-serving-redis-v2")
VEHICLE_TTL_SECONDS = int(os.getenv("VEHICLE_TTL_SECONDS", "3600"))
ROUTE_TTL_SECONDS = int(os.getenv("ROUTE_TTL_SECONDS", "3600"))
NETWORK_TTL_SECONDS = int(os.getenv("NETWORK_TTL_SECONDS", "3600"))


def _json_value(message: Any) -> dict[str, Any] | None:
    try:
        value = message.value.decode("utf-8") if isinstance(message.value, bytes) else message.value
        payload = json.loads(value)
    except (UnicodeDecodeError, TypeError, json.JSONDecodeError) as error:
        logger.warning("Skipping invalid Kafka message from %s: %s", message.topic, error)
        return None
    return payload if isinstance(payload, dict) else None


def _route_key(payload: dict[str, Any]) -> str | None:
    route_id = payload.get("route_id")
    window_start = payload.get("start") or payload.get("window_start")
    if route_id is None:
        return None
    return f"route_health:{route_id}:{window_start or 'current'}"


def _redis_record(client: redis.Redis, topic: str, payload: dict[str, Any]) -> tuple[str, int] | None:
    if topic == "result.vehicle_status":
        identifier = payload.get("vehicle_id")
        return (f"vehicle:{identifier}", VEHICLE_TTL_SECONDS) if identifier is not None else None
    if topic == "result.route_health":
        key = _route_key(payload)
        return (key, ROUTE_TTL_SECONDS) if key else None
    if topic == "result.network_health":
        identifier = payload.get("alert_id") or payload.get("window_start") or payload.get("processing_time")
        return (f"network_health:{identifier}", NETWORK_TTL_SECONDS) if identifier is not None else None
    return None


def run_consumer() -> None:
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    consumer = KafkaConsumer(
        "result.vehicle_status",
        "result.route_health",
        "result.network_health",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value,
    )
    logger.info("Serving consumer listening to result topics from %s", KAFKA_BOOTSTRAP_SERVERS)
    try:
        for message in consumer:
            payload = _json_value(message)
            if payload is None:
                continue
            record = _redis_record(client, message.topic, payload)
            if record is None:
                logger.warning("Skipping result without an identifier from %s: %s", message.topic, payload)
                continue
            key, ttl = record
            client.set(key, json.dumps(payload), ex=ttl)
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()