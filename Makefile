.PHONY: setup up down build status logs create-topics clean test

COMPOSE_FILE = infrastructure/docker-compose.yml
KAFKA_CONTAINER = gtfs-kafka
BOOTSTRAP_SERVER = localhost:9092

setup:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	pip install -r requirements.txt

build:
	docker compose -f $(COMPOSE_FILE) build spark-master

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

status:
	docker compose -f $(COMPOSE_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

create-topics:
	@echo "Creating Kafka topics according to Blueprint Section 4..."
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --bootstrap-server $(BOOTSTRAP_SERVER) --partitions 3 --replication-factor 1 --topic raw.vehicle_positions
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --bootstrap-server $(BOOTSTRAP_SERVER) --partitions 3 --replication-factor 1 --topic raw.trip_updates
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --bootstrap-server $(BOOTSTRAP_SERVER) --partitions 3 --replication-factor 1 --topic raw.service_alerts
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --bootstrap-server $(BOOTSTRAP_SERVER) --partitions 3 --replication-factor 1 --topic result.vehicle_status
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --bootstrap-server $(BOOTSTRAP_SERVER) --partitions 3 --replication-factor 1 --topic result.route_health
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --bootstrap-server $(BOOTSTRAP_SERVER) --partitions 3 --replication-factor 1 --topic result.network_health
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --bootstrap-server $(BOOTSTRAP_SERVER) --partitions 3 --replication-factor 1 --topic quarantine.events
	@echo "All topics created successfully."

test:
	pytest tests/

clean:
	docker compose -f $(COMPOSE_FILE) down -v