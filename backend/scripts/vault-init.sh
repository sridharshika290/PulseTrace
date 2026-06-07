#!/bin/sh
set -eu

VAULT_ADDR=${VAULT_ADDR:-http://vault:8200}
VAULT_TOKEN=${VAULT_TOKEN:-root}
SECRET_PATH=${SECRET_PATH:-secret/data/API_KEY}
SECRET_VALUE=${VAULT_SECRET_API_KEY:-local-api-key}

echo "Waiting for Vault at ${VAULT_ADDR}..."
until curl -sSf "${VAULT_ADDR}/v1/sys/health" >/dev/null 2>&1; do
  printf '.'
  sleep 1
done

echo "
Vault is responding — writing secret to ${SECRET_PATH}"

curl -sSf -X POST "${VAULT_ADDR}/v1/${SECRET_PATH}" \
  -H "X-Vault-Token: ${VAULT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"data\": { \"value\": \"${SECRET_VALUE}\" }}"

echo "Secret written."

exit 0
