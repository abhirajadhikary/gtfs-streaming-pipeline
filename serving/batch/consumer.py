import json
import logging
import os
import shutil
from datetime import UTC, datetime
from typing import Any

import duckdb
from kafka import KafkaConsumer

from serving.batch.processor import create_batch_views

DB_PATH = os.getenv("BATCH_DB_PATH", "gtfs_batch.db")
QUERY_DB_PATH = os.getenv("BATCH_QUERY_DB_PATH", "gtfs_batch_read.db")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("BATCH_CONSUMER_GROUP", "batch-consumer-v2")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def create_batch_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_status_history (
            vehicle_id VARCHAR, route_id VARCHAR, latitude DOUBLE,
            longitude DOUBLE, speed DOUBLE, status VARCHAR, timestamp TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS route_health_history (
            route_id VARCHAR, avg_delay_sec DOUBLE, active_buses INTEGER,
            status VARCHAR, timestamp TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS network_health_history (
            active_routes INTEGER, total_vehicles INTEGER, on_time_pct DOUBLE,
            timestamp TIMESTAMP
        )
    """)


def parse_timestamp(payload: dict[str, Any]) -> Any:
    value = payload.get("event_time") or payload.get("processing_time") or payload.get("timestamp")
    return value or datetime.now(UTC)


def route_delay(payload: dict[str, Any]) -> float | None:
    delays = [payload.get("avg_arrival_delay_sec"), payload.get("avg_departure_delay_sec")]
    values = [float(delay) for delay in delays if delay is not None]
    return sum(values) / len(values) if values else None


def insert_result(conn: duckdb.DuckDBPyConnection, topic: str, payload: dict[str, Any]) -> None:
    if topic == "result.vehicle_status":
        conn.execute(
            "INSERT INTO vehicle_status_history VALUES (?, ?, ?, ?, ?, ?, ?)",
            [payload.get("vehicle_id"), payload.get("route_id"), payload.get("latitude"),
             payload.get("longitude"), payload.get("speed"), payload.get("status"),
             parse_timestamp(payload)],
        )
    elif topic == "result.route_health":
        conn.execute(
            "INSERT INTO route_health_history VALUES (?, ?, ?, ?, ?)",
            [payload.get("route_id"), route_delay(payload), payload.get("total_updates_processed"),
             payload.get("status"), parse_timestamp(payload)],
        )
    elif topic == "result.network_health":
        conn.execute(
            "INSERT INTO network_health_history VALUES (?, ?, ?, ?)",
            [payload.get("active_routes"), payload.get("total_vehicles"),
             payload.get("on_time_pct"), parse_timestamp(payload)],
        )


def publish_query_snapshot() -> None:
    temporary_path = f"{QUERY_DB_PATH}.tmp"
    shutil.copyfile(DB_PATH, temporary_path)
    os.replace(temporary_path, QUERY_DB_PATH)


def run_consumer() -> None:
    conn = duckdb.connect(DB_PATH)
    try:
        create_batch_tables(conn)
        create_batch_views(conn)
        conn.commit()
    finally:
        conn.close()
    publish_query_snapshot()

    consumer = KafkaConsumer(
        "result.vehicle_status", "result.route_health", "result.network_health",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    logger.info("Batch consumer listening on %s", KAFKA_BOOTSTRAP_SERVERS)
    try:
        while True:
            messages = consumer.poll(timeout_ms=1000, max_records=100)
            if not messages:
                continue
            conn = duckdb.connect(DB_PATH)
            try:
                create_batch_tables(conn)
                for records in messages.values():
                    for message in records:
                        if isinstance(message.value, dict):
                            insert_result(conn, message.topic, message.value)
                create_batch_views(conn)
                conn.commit()
            finally:
                conn.close()
            publish_query_snapshot()
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
