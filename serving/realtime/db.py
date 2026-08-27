import os
import json
from typing import Any

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def get_redis_client():
    return r

# --- Generic Helper to Fetch Pattern Keys ---
def get_records_by_prefix(prefix: str):
    records: list[dict[str, Any]] = []
    for key in sorted(r.scan_iter(match=f"{prefix}:*")):
        value = r.get(key)
        if value is None:
            hash_value = r.hgetall(key)
            if hash_value:
                records.append(hash_value)
            continue
        try:
            record = json.loads(value)
        except json.JSONDecodeError:
            record = {"raw_data": value}
        if isinstance(record, dict):
            records.append(record)
    return records

# --- Section-Specific Fetchers ---
def get_all_vehicles():
    return get_records_by_prefix("vehicle")

def get_route_health():
    return get_records_by_prefix("route_health")

def get_network_health():
    return get_records_by_prefix("network_health")