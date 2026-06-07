import json
from jsonschema import validate, Draft7Validator
from pathlib import Path


def load_schema():
    base = Path(__file__).resolve().parents[2]
    schema_path = base / 'schemas' / 'message.json'
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def test_message_schema_valid_sample():
    schema = load_schema()
    sample = {
        "message_id": "test-1",
        "topic": "payments",
        "partition": 0,
        "sequence": 1,
        "kind": "producer",
        "timestamp": "2026-06-07T12:00:00Z",
        "processing_ms": 120,
        "lag_ms": 20,
    }
    v = Draft7Validator(schema)
    errors = sorted(v.iter_errors(sample), key=lambda e: e.path)
    assert not errors, f"Schema validation errors: {errors}"
