from __future__ import annotations

from collections import Counter
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class StreamKind(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"
    DLQ = "dlq"


class AnomalyKind(str, Enum):
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    LAG_SPIKE = "lag_spike"
    DLQ_BURST = "dlq_burst"
    PROCESSING_OUTLIER = "processing_outlier"


class AuditEvent(BaseModel):
    message_id: str
    topic: str
    partition: int = 0
    sequence: int
    kind: StreamKind = StreamKind.PRODUCER
    source: str = "producer"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    processed_at: datetime | None = None
    committed_at: datetime | None = None
    processing_ms: float | None = None
    lag_ms: float | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class AnomalyRecord(BaseModel):
    id: str
    kind: AnomalyKind
    severity: str
    message: str
    topic: str
    partition: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    details: dict[str, Any] = Field(default_factory=dict)


class DashboardSummary(BaseModel):
    total_messages: int
    duplicates: int
    out_of_order: int
    lag_spikes: int
    dlq_bursts: int
    slow_processes: int
    active_topics: int


class HeatmapPoint(BaseModel):
    bucket: str
    topic: str
    count: int


class Snapshot(BaseModel):
    summary: DashboardSummary
    recent_events: list[AuditEvent]
    anomalies: list[AnomalyRecord]
    heatmap: list[HeatmapPoint]


class MetricCounter:
    def __init__(self) -> None:
        self.counters = Counter[str]()

    def inc(self, key: str, amount: int = 1) -> None:
        self.counters[key] += amount

    def value(self, key: str) -> int:
        return int(self.counters[key])
