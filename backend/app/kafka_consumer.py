from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaConsumer
from opentelemetry import trace

from .models import AuditEvent, StreamKind
from .service import EventIngestService


def _header_value(headers: list[tuple[str, bytes | None]], name: str) -> str | None:
    for header_name, raw_value in headers:
        if header_name == name and raw_value is not None:
            return raw_value.decode("utf-8")
    return None


def _decode_message(message_value: bytes | None) -> dict[str, Any]:
    if not message_value:
        return {}
    decoded = message_value.decode("utf-8")
    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        return {"payload": {"raw": decoded}}


def message_to_event(message: Any, topic: str) -> AuditEvent:
    body = _decode_message(message.value)
    headers = [(name, value) for name, value in (message.headers or [])]
    timestamp_value = body.get("timestamp") or getattr(message, "timestamp", None)
    if isinstance(timestamp_value, (int, float)):
        timestamp_value = datetime.fromtimestamp(timestamp_value / 1000.0, tz=timezone.utc)
    payload = dict(body)
    payload.update({
        "topic": topic,
        "partition": getattr(message, "partition", 0) or 0,
        "sequence": payload.get("sequence") or int(getattr(message, "offset", 0) or 0),
        "message_id": payload.get("message_id") or _header_value(headers, "message_id") or f"{topic}-{message.partition}-{message.offset}",
        "kind": payload.get("kind") or StreamKind.CONSUMER.value,
        "source": payload.get("source") or "kafka",
        "payload": payload.get("payload") or body,
        "timestamp": timestamp_value,
        "lag_ms": payload.get("lag_ms"),
        "processing_ms": payload.get("processing_ms"),
    })
    return AuditEvent.model_validate(payload)


@dataclass(slots=True)
class KafkaConsumerRunner:
    service: EventIngestService
    bootstrap_servers: str
    topic: str
    group_id: str
    _consumer: AIOKafkaConsumer | None = field(default=None, init=False, repr=False)

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            metadata_max_age_ms=10_000,
            heartbeat_interval_ms=3_000,
        )
        await self._consumer.start()

    async def run(self) -> None:
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None
        try:
                tracer = trace.get_tracer(__name__)
                async for message in self._consumer:
                    # create a span per consumed message
                    with tracer.start_as_current_span("kafka.consume", attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": self.topic,
                        "messaging.kafka.partition": getattr(message, 'partition', None),
                        "messaging.kafka.offset": getattr(message, 'offset', None),
                    }):
                        event = message_to_event(message, self.topic)
                        await self.service.ingest(event)
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
