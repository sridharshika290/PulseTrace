from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta

from .engine import AnomalyDetector
from .models import AuditEvent, StreamKind


class StreamSimulator:
    def __init__(self, detector: AnomalyDetector, seed: int = 7) -> None:
        self.detector = detector
        self.random = random.Random(seed)
        self.sequence = 0

    async def run(self) -> None:
        topic_cycle = ["contracts", "payments", "notifications"]
        while True:
            await self.detector.inspect(self._next_event(topic_cycle[self.sequence % len(topic_cycle)]))
            await asyncio.sleep(0.9)

    def _next_event(self, topic: str) -> AuditEvent:
        self.sequence += 1
        partition = self.sequence % 3
        message_id = f"msg-{self.sequence}"
        processing_ms = self.random.choice([120.0, 180.0, 240.0, 260.0, 1800.0 if self.sequence % 11 == 0 else 320.0])
        lag_ms = self.random.choice([80.0, 120.0, 140.0, 2600.0 if self.sequence % 9 == 0 else 220.0])
        kind = StreamKind.PRODUCER
        payload = {"amount": self.random.randint(10, 500), "tenant": f"team-{self.sequence % 4}"}

        if self.sequence % 8 == 0:
            message_id = f"msg-{self.sequence - 1}"
        if self.sequence % 10 == 0:
            kind = StreamKind.DLQ
        if self.sequence % 13 == 0:
            sequence = max(1, self.sequence - 4)
        else:
            sequence = self.sequence

        return AuditEvent(
            message_id=message_id,
            topic=topic,
            partition=partition,
            sequence=sequence,
            kind=kind,
            source="simulator",
            timestamp=datetime.utcnow(),
            processed_at=datetime.utcnow() + timedelta(milliseconds=processing_ms / 2),
            committed_at=datetime.utcnow() + timedelta(milliseconds=processing_ms),
            processing_ms=processing_ms,
            lag_ms=lag_ms,
            payload=payload,
        )
