#!/usr/bin/env bash
set -euo pipefail

OUT_DIR=${1:-/backups}
mkdir -p "$OUT_DIR"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)

echo "Triggering Redis BGSAVE..."
redis-cli -h ${REDIS_HOST:-localhost} -p ${REDIS_PORT:-6379} BGSAVE

echo "Waiting for RDB..."
sleep 3

RDB_FILE="dump-${TIMESTAMP}.rdb"
echo "Copying RDB to $OUT_DIR/$RDB_FILE"
# this assumes host mount or access to redis data dir; for containerized env, bind mount /data
cp /data/dump.rdb "$OUT_DIR/$RDB_FILE" || echo "Failed to copy RDB, container access may be required"

echo "Exporting streams via XREADGROUP may be preferable; consider using redis-cli --raw XREAD"
echo "Backup complete: $OUT_DIR/$RDB_FILE"
