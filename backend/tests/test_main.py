from __future__ import annotations

import pytest

from app.main import _run_kafka_consumer, app, healthz


class BrokenConsumer:
    async def run(self) -> None:
        raise RuntimeError("broker unavailable")


@pytest.mark.asyncio
async def test_kafka_consumer_failure_updates_status() -> None:
    app.state.kafka_status = {"enabled": True, "running": True, "error": None}

    await _run_kafka_consumer(app, BrokenConsumer())

    assert app.state.kafka_status["running"] is False
    assert app.state.kafka_status["error"] == "broker unavailable"


@pytest.mark.asyncio
async def test_healthz_reports_degraded_when_kafka_fails() -> None:
    app.state.kafka_status = {"enabled": True, "running": False, "error": "broker unavailable"}

    response = await healthz()

    assert response["status"] == "degraded"
    assert response["kafka"] == "unavailable"