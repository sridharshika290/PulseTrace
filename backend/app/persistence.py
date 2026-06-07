from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from collections import deque

from .models import AnomalyRecord, AuditEvent


@dataclass(slots=True)
class PersistedBatch:
    events: list[AuditEvent]
    anomalies: list[AnomalyRecord]


class RedisEventStore:
    def __init__(self, redis_client: Any, event_stream: str, anomaly_stream: str) -> None:
        self.redis = redis_client
        self.event_stream = event_stream
        self.anomaly_stream = anomaly_stream

    async def append(self, event: AuditEvent, anomalies: list[AnomalyRecord]) -> None:
        event_payload = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
        await self.redis.xadd(self.event_stream, {"payload": event_payload}, maxlen=10_000, approximate=True)
        for anomaly in anomalies:
            anomaly_payload = json.dumps(anomaly.model_dump(mode="json"), separators=(",", ":"))
            await self.redis.xadd(self.anomaly_stream, {"payload": anomaly_payload}, maxlen=10_000, approximate=True)

    async def recent_events(self, limit: int) -> list[AuditEvent]:
        return await self._read_stream(self.event_stream, limit, AuditEvent)

    async def recent_anomalies(self, limit: int) -> list[AnomalyRecord]:
        return await self._read_stream(self.anomaly_stream, limit, AnomalyRecord)

    async def hydrate(self, limit: int) -> list[AuditEvent]:
        return await self.recent_events(limit)

    async def _read_stream(self, key: str, limit: int, model: type[AuditEvent] | type[AnomalyRecord]) -> list[Any]:
        entries = await self.redis.xrevrange(key, count=limit)
        records: list[Any] = []
        for _, payload in reversed(entries):
            raw_value = payload.get(b"payload") or payload.get("payload")
            if raw_value is None:
                continue
            decoded = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value
            records.append(model.model_validate_json(decoded))
        return records


class InMemoryEventStore:
    def __init__(self, event_limit: int = 5000, anomaly_limit: int = 5000) -> None:
        self._events = deque(maxlen=event_limit)
        self._anomalies = deque(maxlen=anomaly_limit)

    async def append(self, event: AuditEvent, anomalies: list[AnomalyRecord]) -> None:
        self._events.append(event)
        self._anomalies.extend(anomalies)

    async def recent_events(self, limit: int) -> list[AuditEvent]:
        return list(self._events)[-limit:]

    async def recent_anomalies(self, limit: int) -> list[AnomalyRecord]:
        return list(self._anomalies)[-limit:]
