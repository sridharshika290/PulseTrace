#!/usr/bin/env bash
set -euo pipefail

host_check() {
  local host=$1
  local port=$2
  for i in {1..30}; do
    if nc -z "$host" "$port"; then
      echo "$host:$port is available"
      return 0
    fi
    echo "Waiting for $host:$port... ($i)"
    sleep 2
  done
  echo "Timed out waiting for $host:$port" >&2
  return 1
}

# Wait for Redis
REDIS_HOST=$(echo "${REDIS_URL:-redis://redis:6379}" | sed -E 's#^[a-z]+://([^:/]+):?([0-9]*)/?#\1#')
REDIS_PORT=$(echo "${REDIS_URL:-redis://redis:6379}" | sed -E 's#^[a-z]+://[^:/]+:([0-9]+).*#\1#')
host_check "$REDIS_HOST" "$REDIS_PORT"

# Wait for Kafka
KAFKA_HOST=$(echo "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" | sed -E 's#^([^:]+):([0-9]+).*#\1#')
KAFKA_PORT=$(echo "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" | sed -E 's#^([^:]+):([0-9]+).*#\2#')
host_check "$KAFKA_HOST" "$KAFKA_PORT"

exec "$@"
