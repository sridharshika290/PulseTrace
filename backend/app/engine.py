from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from statistics import quantiles
from uuid import uuid4

from .models import AnomalyKind, AnomalyRecord, AuditEvent
from .state import AuditState


@dataclass
class DetectionConfig:
    duplicate_ttl_seconds: int = 60
    lag_spike_threshold_ms: float = 1500.0
    processing_outlier_ms: float = 1200.0
    lag_jump_factor: float = 1.6
    dlq_window_events: int = 12
    dlq_burst_threshold: int = 4
    out_of_order_delta: int = 0


class AnomalyDetector:
    def __init__(self, state: AuditState, config: DetectionConfig | None = None) -> None:
        self.state = state
        self.config = config or DetectionConfig()
        self._recent_lags: dict[tuple[str, int], deque[float]] = {}
        self._recent_dlq: dict[str, deque[int]] = {}

    async def inspect(self, event: AuditEvent) -> list[AnomalyRecord]:
        anomalies: list[AnomalyRecord] = []
        if await self.state.mark_duplicate(event.message_id, event.timestamp, self.config.duplicate_ttl_seconds):
            anomalies.append(self._build_anomaly(event, AnomalyKind.DUPLICATE, "high", "Duplicate message detected", {"message_id": event.message_id}))

        previous_sequence = await self.state.update_sequence(event.topic, event.partition, event.sequence)
        if previous_sequence is not None and event.sequence <= previous_sequence:
            anomalies.append(
                self._build_anomaly(
                    event,
                    AnomalyKind.OUT_OF_ORDER,
                    "high",
                    "Out-of-order sequence observed",
                    {"previous_sequence": previous_sequence, "sequence": event.sequence},
                )
            )

        if event.lag_ms is not None:
            lag_window = self._recent_lags.setdefault((event.topic, event.partition), deque(maxlen=10))
            lag_window.append(event.lag_ms)
            if len(lag_window) >= 3:
                baseline = sum(list(lag_window)[:-1]) / (len(lag_window) - 1)
                current = lag_window[-1]
                if current >= self.config.lag_spike_threshold_ms or (baseline > 0 and current >= baseline * self.config.lag_jump_factor):
                    anomalies.append(
                        self._build_anomaly(
                            event,
                            AnomalyKind.LAG_SPIKE,
                            "medium",
                            "Consumer lag spike detected",
                            {"lag_ms": current, "baseline_ms": baseline},
                        )
                    )

        if event.kind.value == "dlq":
            dlq_window = self._recent_dlq.setdefault(event.topic, deque(maxlen=self.config.dlq_window_events))
            dlq_window.append(1)
            if len(dlq_window) >= self.config.dlq_burst_threshold:
                anomalies.append(
                    self._build_anomaly(
                        event,
                        AnomalyKind.DLQ_BURST,
                        "medium",
                        "Dead-letter queue burst detected",
                        {"window_events": len(dlq_window), "threshold": self.config.dlq_burst_threshold},
                    )
                )

        if event.processing_ms is not None:
            samples = list(self.state.processing_samples)
            threshold = self.config.processing_outlier_ms
            if len(samples) >= 8:
                try:
                    threshold = max(threshold, quantiles(samples, n=100)[98])
                except ValueError:
                    threshold = max(threshold, max(samples))
            if event.processing_ms >= threshold:
                anomalies.append(
                    self._build_anomaly(
                        event,
                        AnomalyKind.PROCESSING_OUTLIER,
                        "medium",
                        "Slow consumer processing outlier",
                        {"processing_ms": event.processing_ms, "threshold_ms": threshold},
                    )
                )

        for anomaly in anomalies:
            await self.state.record_anomaly(anomaly)
        await self.state.record_event(event)
        await self.state.add_heatmap_point(event)
        return anomalies

    def _build_anomaly(
        self,
        event: AuditEvent,
        kind: AnomalyKind,
        severity: str,
        message: str,
        details: dict[str, object],
    ) -> AnomalyRecord:
        return AnomalyRecord(
            id=str(uuid4()),
            kind=kind,
            severity=severity,
            message=message,
            topic=event.topic,
            partition=event.partition,
            created_at=datetime.utcnow(),
            details=details,
        )
