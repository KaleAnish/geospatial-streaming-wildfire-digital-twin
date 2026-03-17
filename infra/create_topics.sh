#!/usr/bin/env bash
set -e

TOPIC=${1:-fire_events}
OUTPUT_TOPIC="at_risk_assets"

# Create main fire_events topic
docker compose -f infra/docker-compose.yml exec -T kafka \
  kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

# Create output spatial join topic
docker compose -f infra/docker-compose.yml exec -T kafka \
  kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists --topic "$OUTPUT_TOPIC" --partitions 1 --replication-factor 1

echo "✅ Topics ready: $TOPIC, $OUTPUT_TOPIC"
