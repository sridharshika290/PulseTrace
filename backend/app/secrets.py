from __future__ import annotations

import os
from typing import Optional

import requests


def fetch_from_vault(path: str, token: str, addr: str) -> Optional[str]:
    url = f"{addr.rstrip('/')}/v1/{path.lstrip('/')}"
    headers = {"X-Vault-Token": token}
    try:
        r = requests.get(url, headers=headers, timeout=5)
        r.raise_for_status()
        data = r.json().get("data")
        # kv v2: the secret is under data.data
        if isinstance(data, dict) and "data" in data:
            return data["data"].get("value") or data["data"].get("api_key")
        if isinstance(data, dict):
            return next(iter(data.values()))
        return None
    except Exception:
        return None


def get_secret(name: str) -> Optional[str]:
    # Priority: ENV, then Vault
    env_val = os.getenv(name.upper())
    if env_val:
        return env_val
    vault_addr = os.getenv("VAULT_ADDR")
    vault_token = os.getenv("VAULT_TOKEN")
    if vault_addr and vault_token:
        # Try standard kv path
        val = fetch_from_vault(f"secret/data/{name}", vault_token, vault_addr)
        if val:
            return val
    return None
