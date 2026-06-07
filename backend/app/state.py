from __future__ import annotations

import asyncio
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .models import AnomalyRecord, AuditEvent, HeatmapPoint


def bucket_label(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


@dataclass
class AuditState:
    recent_events: deque[AuditEvent] = field(default_factory=lambda: deque(maxlen=200))
    anomalies: deque[AnomalyRecord] = field(default_factory=lambda: deque(maxlen=200))
    duplicate_seen: dict[str, datetime] = field(default_factory=dict)
    last_sequence: dict[tuple[str, int], int] = field(default_factory=dict)
    lag_history: deque[tuple[datetime, float]] = field(default_factory=lambda: deque(maxlen=300))
    processing_samples: deque[float] = field(default_factory=lambda: deque(maxlen=300))
    heatmap: Counter[tuple[str, str]] = field(default_factory=Counter)
    metrics: Counter[str] = field(default_factory=Counter)
    topics: set[str] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record_event(self, event: AuditEvent) -> None:
        async with self.lock:
            self.recent_events.appendleft(event)
            self.topics.add(event.topic)
            self.metrics["messages_total"] += 1
            if event.lag_ms is not None:
                self.lag_history.append((event.timestamp, event.lag_ms))
            if event.processing_ms is not None:
                self.processing_samples.append(event.processing_ms)

    async def seed_event(self, event: AuditEvent) -> None:
        async with self.lock:
            self.recent_events.appendleft(event)
            self.topics.add(event.topic)
            self.metrics["messages_total"] += 1
            if event.lag_ms is not None:
                self.lag_history.append((event.timestamp, event.lag_ms))
            if event.processing_ms is not None:
                self.processing_samples.append(event.processing_ms)

    async def seed_anomaly(self, anomaly: AnomalyRecord) -> None:
        async with self.lock:
            self.anomalies.appendleft(anomaly)
            self.metrics[f"anomalies_{anomaly.kind.value}"] += 1

    async def record_anomaly(self, anomaly: AnomalyRecord) -> None:
        async with self.lock:
            self.anomalies.appendleft(anomaly)
            self.metrics[f"anomalies_{anomaly.kind.value}"] += 1

    async def mark_duplicate(self, message_id: str, created_at: datetime, ttl_seconds: int) -> bool:
        async with self.lock:
            cutoff = created_at.timestamp() - ttl_seconds
            stale = [key for key, value in self.duplicate_seen.items() if value.timestamp() < cutoff]
            for key in stale:
                self.duplicate_seen.pop(key, None)
            if message_id in self.duplicate_seen:
                return True
            self.duplicate_seen[message_id] = created_at
            return False

    async def update_sequence(self, topic: str, partition: int, sequence: int) -> int | None:
        async with self.lock:
            key = (topic, partition)
            previous = self.last_sequence.get(key)
            self.last_sequence[key] = sequence
            return previous

    async def add_heatmap_point(self, event: AuditEvent) -> None:
        if event.kind.value != "dlq":
            return
        async with self.lock:
            self.heatmap[(bucket_label(event.timestamp), event.topic)] += 1

    async def snapshot(self) -> dict[str, Any]:
        async with self.lock:
            return {
                "recent_events": list(self.recent_events),
                "anomalies": list(self.anomalies),
                "lag_history": list(self.lag_history),
                "processing_samples": list(self.processing_samples),
                "heatmap": [
                    HeatmapPoint(bucket=bucket, topic=topic, count=count)
                    for (bucket, topic), count in self.heatmap.items()
                ],
                "metrics": dict(self.metrics),
                "topics": sorted(self.topics),
            }
