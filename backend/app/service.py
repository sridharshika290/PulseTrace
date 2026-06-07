from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .engine import AnomalyDetector
from .models import AnomalyRecord, AuditEvent
from .persistence import RedisEventStore
from .state import AuditState


@dataclass(slots=True)
class IngestResult:
    event: AuditEvent
    anomalies: list[AnomalyRecord]


class EventIngestService:
    def __init__(self, state: AuditState, detector: AnomalyDetector, store: RedisEventStore | None = None) -> None:
        self.state = state
        self.detector = detector
        self.store = store

    async def ingest(self, event: AuditEvent, persist: bool = True) -> IngestResult:
        anomalies = await self.detector.inspect(event)
        if persist and self.store is not None:
            await self.store.append(event, anomalies)
        return IngestResult(event=event, anomalies=anomalies)

    async def hydrate(self, limit: int) -> None:
        if self.store is None:
            return
        events = await self.store.recent_events(limit)
        anomalies = await self.store.recent_anomalies(limit)
        for event in events:
            await self.state.seed_event(event)
        for anomaly in anomalies:
            await self.state.seed_anomaly(anomaly)


def extract_event_payload(payload: dict[str, Any], *, source: str = "api") -> AuditEvent:
    payload_data = dict(payload)
    payload_data.setdefault("source", source)
    payload_data.setdefault("payload", {})
    return AuditEvent.model_validate(payload_data)
