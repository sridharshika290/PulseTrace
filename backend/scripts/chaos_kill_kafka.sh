#!/usr/bin/env bash
set -euo pipefail

echo "Simulating Kafka outage by stopping kafka container in docker-compose"
docker compose stop kafka || docker compose stop pulsetrace_kafka || true
sleep 10
echo "Bringing kafka back up"
docker compose start kafka || docker compose start pulsetrace_kafka || true
