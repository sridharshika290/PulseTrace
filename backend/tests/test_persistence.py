from datetime import datetime, timezone

import pytest

from app.models import AnomalyKind, AnomalyRecord, AuditEvent, StreamKind
from app.persistence import RedisEventStore


class FakeRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[dict[str, str]]] = {}

    async def xadd(self, key: str, fields: dict[str, str], maxlen: int | None = None, approximate: bool = False) -> None:
        self.streams.setdefault(key, []).append(fields)

    async def xrevrange(self, key: str, count: int | None = None):
        entries = self.streams.get(key, [])
        selected = list(reversed(entries[:]))[:count]
        return [(f"id-{index}", {"payload": item["payload"].encode("utf-8")}) for index, item in enumerate(selected)]


@pytest.mark.asyncio
async def test_redis_stream_round_trip() -> None:
    redis_client = FakeRedis()
    store = RedisEventStore(redis_client, "events", "anomalies")
    event = AuditEvent(
        message_id="evt-1",
        topic="payments",
        partition=0,
        sequence=1,
        timestamp=datetime.now(timezone.utc),
        kind=StreamKind.PRODUCER,
        payload={"amount": 42},
    )
    anomaly = AnomalyRecord(
        id="anom-1",
        kind=AnomalyKind.DUPLICATE,
        severity="high",
        message="Duplicate message detected",
        topic="payments",
        partition=0,
    )

    await store.append(event, [anomaly])

    events = await store.recent_events(10)
    anomalies = await store.recent_anomalies(10)

    assert events[0].message_id == "evt-1"
    assert anomalies[0].kind is AnomalyKind.DUPLICATE
