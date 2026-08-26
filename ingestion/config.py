import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topic configurations
TOPIC_VEHICLE_POSITIONS = "raw.vehicle_positions"
TOPIC_TRIP_UPDATES = "raw.trip_updates"
TOPIC_SERVICE_ALERTS = "raw.service_alerts"

# Fresh data polling interval
POLL_INTERVAL_SECONDS = int(os.getenv("GTFS_POLL_INTERVAL", 60))

# Public endpoints for data
GTFS_FEEDS = {
    "vehicle_positions": os.getenv("GTFS_VEHICLE_POSITIONS_URL", "https://cdn.mbta.com/realtime/VehiclePositions.pb"),
    "trip_updates": os.getenv("GTFS_TRIP_UPDATES_URL", "https://cdn.mbta.com/realtime/TripUpdates.pb"),
    "service_alerts": os.getenv("GTFS_SERVICE_ALERTS_URL", "https://cdn.mbta.com/realtime/Alerts.pb"),
}