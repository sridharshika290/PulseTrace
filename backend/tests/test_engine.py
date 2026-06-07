from datetime import datetime

import pytest

from app.engine import AnomalyDetector, DetectionConfig
from app.models import AuditEvent, AnomalyKind, StreamKind
from app.state import AuditState


@pytest.mark.asyncio
async def test_duplicate_detection_triggers() -> None:
    state = AuditState()
    detector = AnomalyDetector(state, DetectionConfig(duplicate_ttl_seconds=60))
    event = AuditEvent(message_id="abc", topic="orders", partition=0, sequence=1, timestamp=datetime.utcnow(), kind=StreamKind.PRODUCER)

    first = await detector.inspect(event)
    second = await detector.inspect(event.model_copy(update={"timestamp": datetime.utcnow()}))

    assert first == []
    assert any(anomaly.kind is AnomalyKind.DUPLICATE for anomaly in second)


@pytest.mark.asyncio
async def test_out_of_order_detection_triggers() -> None:
    state = AuditState()
    detector = AnomalyDetector(state)
    first = AuditEvent(message_id="1", topic="orders", partition=0, sequence=10, timestamp=datetime.utcnow(), kind=StreamKind.PRODUCER)
    second = AuditEvent(message_id="2", topic="orders", partition=0, sequence=9, timestamp=datetime.utcnow(), kind=StreamKind.PRODUCER)

    await detector.inspect(first)
    result = await detector.inspect(second)

    assert any(anomaly.kind is AnomalyKind.OUT_OF_ORDER for anomaly in result)


@pytest.mark.asyncio
async def test_lag_spike_detection_triggers() -> None:
    state = AuditState()
    detector = AnomalyDetector(state, DetectionConfig(lag_spike_threshold_ms=1000))
    events = [
        AuditEvent(message_id="1", topic="contracts", partition=1, sequence=1, timestamp=datetime.utcnow(), kind=StreamKind.PRODUCER, lag_ms=100),
        AuditEvent(message_id="2", topic="contracts", partition=1, sequence=2, timestamp=datetime.utcnow(), kind=StreamKind.PRODUCER, lag_ms=120),
        AuditEvent(message_id="3", topic="contracts", partition=1, sequence=3, timestamp=datetime.utcnow(), kind=StreamKind.PRODUCER, lag_ms=2500),
    ]

    result = []
    for event in events:
        result = await detector.inspect(event)

    assert any(anomaly.kind is AnomalyKind.LAG_SPIKE for anomaly in result)


@pytest.mark.asyncio
async def test_processing_outlier_detection_triggers() -> None:
    state = AuditState()
    detector = AnomalyDetector(state, DetectionConfig(processing_outlier_ms=500))
    for idx in range(8):
        await detector.inspect(
            AuditEvent(
                message_id=f"seed-{idx}",
                topic="payments",
                partition=0,
                sequence=idx + 1,
                timestamp=datetime.utcnow(),
                kind=StreamKind.PRODUCER,
                processing_ms=120,
            )
        )

    result = await detector.inspect(
        AuditEvent(
            message_id="slow",
            topic="payments",
            partition=0,
            sequence=99,
            timestamp=datetime.utcnow(),
            kind=StreamKind.PRODUCER,
            processing_ms=2000,
        )
    )

    assert any(anomaly.kind is AnomalyKind.PROCESSING_OUTLIER for anomaly in result)
