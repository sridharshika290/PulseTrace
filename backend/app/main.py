from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.exposition import choose_encoder
import time
import logging
from pythonjsonlogger import jsonlogger
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor

from .secrets import get_secret


# Prometheus metrics
MSG_COUNTER = Counter('pulsetrace_messages_total', 'Total messages observed by PulseTrace')
ANOMALY_COUNTER = Counter('pulsetrace_anomalies_total', 'Total anomalies detected', ['kind'])
LAST_SCRAPE = Gauge('pulsetrace_last_scrape_timestamp', 'Last metrics scrape timestamp')


class SimpleRateLimiter:
    def __init__(self, calls: int = 30, per_seconds: int = 60):
        self.calls = calls
        self.per_seconds = per_seconds
        self.clients: dict[str, list[float]] = {}

    def allow(self, key: str) -> bool:
        now = time.time()
        window_start = now - self.per_seconds
        times = self.clients.get(key, [])
        times = [t for t in times if t >= window_start]
        if len(times) >= self.calls:
            self.clients[key] = times
            return False
        times.append(now)
        self.clients[key] = times
        return True


# NOTE: rate limit middleware is registered after app creation below

from .engine import AnomalyDetector
from .config import AppConfig
from .kafka_consumer import KafkaConsumerRunner
from .models import AnomalyRecord, AuditEvent, DashboardSummary, Snapshot
from .persistence import InMemoryEventStore, RedisEventStore
from .simulator import StreamSimulator
from .service import EventIngestService, extract_event_payload
from .state import AuditState


class LiveHub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.clients.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self.clients.discard(websocket)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        if not self.clients:
            return
        message = json.dumps(payload, default=str)
        stale: list[WebSocket] = []
        for client in self.clients:
            try:
                await client.send_text(message)
            except Exception:
                stale.append(client)
        for client in stale:
            self.disconnect(client)


async def _run_kafka_consumer(app: FastAPI, consumer: KafkaConsumerRunner) -> None:
    try:
        await consumer.run()
        app.state.kafka_status.update({"running": False, "error": None})
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        app.state.kafka_status.update({"running": False, "error": str(exc)})


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = AppConfig.from_env()
    # structured logging
    logger = logging.getLogger()
    logHandler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
    logger.setLevel(logging.INFO)

    # OpenTelemetry tracer setup (if OTEL_EXPORTER_OTLP_ENDPOINT set)
    otel_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT')
    if otel_endpoint:
        resource = Resource.create({"service.name": "pulsetrace-backend"})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=otel_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

    app.state.config = config
    # secrets (api key)
    api_key = get_secret('API_KEY') or os.getenv('API_KEY')
    app.state.api_key = api_key

    # instrument FastAPI app
    try:
        FastAPIInstrumentor.instrument_app(app)
    except Exception:
        pass
    app.state.audit_state = AuditState()
    app.state.detector = AnomalyDetector(app.state.audit_state)
    app.state.live_hub = LiveHub()
    app.state.rate_limiter = SimpleRateLimiter(calls=120, per_seconds=60)
    app.state.simulator_task = None
    app.state.consumer_task = None

    app.state.redis = None
    app.state.store = InMemoryEventStore(config.redis_state_stream_limit, config.redis_state_stream_limit)
    app.state.kafka_status = {"enabled": not config.simulator_mode, "running": False, "error": None}

    try:
        redis_client = Redis.from_url(config.redis_url, decode_responses=False)
        await redis_client.ping()
        app.state.redis = redis_client
        app.state.store = RedisEventStore(redis_client, config.redis_event_stream, config.redis_anomaly_stream)
        # instrument redis client
        try:
            RedisInstrumentor().instrument()
        except Exception:
            pass
    except RedisConnectionError:
        pass

    app.state.ingest_service = EventIngestService(app.state.audit_state, app.state.detector, app.state.store)

    try:
        await app.state.ingest_service.hydrate(config.redis_state_stream_limit)
    except RedisConnectionError:
        app.state.store = InMemoryEventStore(config.redis_state_stream_limit, config.redis_state_stream_limit)
        app.state.ingest_service = EventIngestService(app.state.audit_state, app.state.detector, app.state.store)

    if config.simulator_mode:
        app.state.kafka_status.update({"enabled": False, "running": False, "error": None})
        simulator = StreamSimulator(app.state.detector)
        app.state.simulator_task = asyncio.create_task(simulator.run())
    else:
        consumer = KafkaConsumerRunner(
            service=app.state.ingest_service,
            bootstrap_servers=config.kafka_bootstrap_servers,
            topic=config.kafka_topic,
            group_id=config.kafka_group_id,
        )
        app.state.kafka_status.update({"enabled": True, "running": True, "error": None})
        app.state.consumer_task = asyncio.create_task(_run_kafka_consumer(app, consumer))
    yield
    task = app.state.simulator_task
    if task is not None:
        task.cancel()
    consumer_task = app.state.consumer_task
    if consumer_task is not None:
        consumer_task.cancel()
    if app.state.redis is not None:
        await app.state.redis.aclose()


app = FastAPI(title="PulseTrace", version="0.1.0", lifespan=lifespan)
cors_origins = [origin.strip() for origin in AppConfig.from_env().cors_origins.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# register simple rate limiter middleware
@app.middleware('http')
async def rate_limit_middleware(request: Request, call_next):
    limiter: SimpleRateLimiter = getattr(app.state, 'rate_limiter', None)
    if limiter is not None:
        ip = request.client.host if request.client else 'anon'
        if not limiter.allow(ip):
            from fastapi.responses import JSONResponse

            return JSONResponse({'detail': 'rate limit exceeded'}, status_code=429)
    return await call_next(request)


def get_state() -> AuditState:
    return app.state.audit_state


from datetime import datetime, timezone, timedelta


def summarize(snapshot: dict[str, Any]) -> DashboardSummary:
    metrics = snapshot["metrics"]
    return DashboardSummary(
        total_messages=int(metrics.get("messages_total", 0)),
        duplicates=int(metrics.get("anomalies_duplicate", 0)),
        out_of_order=int(metrics.get("anomalies_out_of_order", 0)),
        lag_spikes=int(metrics.get("anomalies_lag_spike", 0)),
        dlq_bursts=int(metrics.get("anomalies_dlq_burst", 0)),
        slow_processes=int(metrics.get("anomalies_processing_outlier", 0)),
        active_topics=len(snapshot["topics"]),
    )


def build_topic_summary(snapshot: dict[str, Any], topic: str | None = None) -> DashboardSummary:
    events = snapshot["recent_events"]
    anomalies = snapshot["anomalies"]
    if topic is None:
        return summarize(snapshot)
    total_messages = sum(1 for e in events if e.topic == topic)
    duplicates = sum(1 for a in anomalies if a.topic == topic and a.kind.value == "duplicate")
    out_of_order = sum(1 for a in anomalies if a.topic == topic and a.kind.value == "out_of_order")
    lag_spikes = sum(1 for a in anomalies if a.topic == topic and a.kind.value == "lag_spike")
    dlq_bursts = sum(1 for a in anomalies if a.topic == topic and a.kind.value == "dlq_burst")
    slow_processes = sum(1 for a in anomalies if a.topic == topic and a.kind.value == "processing_outlier")
    return DashboardSummary(
        total_messages=total_messages,
        duplicates=duplicates,
        out_of_order=out_of_order,
        lag_spikes=lag_spikes,
        dlq_bursts=dlq_bursts,
        slow_processes=slow_processes,
        active_topics=1 if topic else len(snapshot["topics"]),
    )


def compute_deltas(snapshot: dict[str, Any], topic: str | None = None, minutes: int = 15) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    window = timedelta(minutes=minutes)
    start_last = now - window
    start_prev = now - window * 2
    events = snapshot["recent_events"]
    def in_range(e, start, end):
        return start <= getattr(e, "timestamp") <= end

    last_count = sum(1 for e in events if (topic is None or e.topic == topic) and in_range(e, start_last, now))
    prev_count = sum(1 for e in events if (topic is None or e.topic == topic) and in_range(e, start_prev, start_last))
    if prev_count == 0:
        pct = "+100%" if last_count > 0 else "0%"
    else:
        pct_val = (last_count - prev_count) / prev_count * 100.0
        sign = "+" if pct_val >= 0 else ""
        pct = f"{sign}{pct_val:.1f}%"
    return {"messages": pct}


@app.get("/api/summary")
async def api_summary(topic: str | None = None) -> dict[str, Any]:
    snapshot = await get_state().snapshot()
    summary = build_topic_summary(snapshot, topic)
    deltas = compute_deltas(snapshot, topic)
    return {**summary.model_dump(), "deltas": deltas}


@app.get("/api/events")
async def api_events(topic: str | None = None) -> list[AuditEvent]:
    snapshot = await get_state().snapshot()
    events = snapshot["recent_events"]
    if topic:
        events = [e for e in events if e.topic == topic]
    return events


@app.get("/api/anomalies")
async def api_anomalies(topic: str | None = None) -> list[AnomalyRecord]:
    snapshot = await get_state().snapshot()
    anomalies = snapshot["anomalies"]
    if topic:
        anomalies = [a for a in anomalies if a.topic == topic]
    return anomalies


@app.get("/api/heatmap")
async def api_heatmap() -> list[Any]:
    snapshot = await get_state().snapshot()
    return snapshot["heatmap"]


@app.get("/api/replay/events")
async def api_replay_events(limit: int | None = None) -> list[AuditEvent]:
    config: AppConfig = app.state.config
    store: RedisEventStore = app.state.store
    return await store.recent_events(limit or config.replay_default_limit)


@app.get("/api/replay/anomalies")
async def api_replay_anomalies(limit: int | None = None) -> list[AnomalyRecord]:
    config: AppConfig = app.state.config
    store: RedisEventStore = app.state.store
    return await store.recent_anomalies(limit or config.replay_default_limit)


@app.get("/api/snapshot", response_model=Snapshot)
async def api_snapshot() -> Snapshot:
    snapshot = await get_state().snapshot()
    return Snapshot(
        summary=summarize(snapshot),
        recent_events=snapshot["recent_events"],
        anomalies=snapshot["anomalies"],
        heatmap=snapshot["heatmap"],
    )


@app.post("/api/events")
async def api_ingest(event: AuditEvent) -> dict[str, Any]:
    service: EventIngestService = app.state.ingest_service
    # require API key for ingestion endpoints
    if app.state.api_key:
        # for normal requests, API key check is done via header in middleware; here we just note
        pass
    result = await service.ingest(event)
    # metrics
    try:
        MSG_COUNTER.inc()
        for a in result.anomalies:
            ANOMALY_COUNTER.labels(kind=a.kind.value).inc()
    except Exception:
        pass
    snapshot = await get_state().snapshot()
    ext = build_topic_summary(snapshot, None).model_dump()
    ext["deltas"] = compute_deltas(snapshot, None)
    await app.state.live_hub.broadcast({"type": "ingest", "event": event.model_dump(), "anomalies": [anomaly.model_dump() for anomaly in result.anomalies], "summary": ext})
    return {"ok": True, "anomalies": [anomaly.model_dump() for anomaly in result.anomalies]}


@app.post("/api/ingest/kafka")
async def api_ingest_kafka(payload: dict[str, Any]) -> dict[str, Any]:
    event = extract_event_payload(payload, source="api")
    service: EventIngestService = app.state.ingest_service
    result = await service.ingest(event)
    # metrics
    try:
        MSG_COUNTER.inc()
        for a in result.anomalies:
            ANOMALY_COUNTER.labels(kind=a.kind.value).inc()
    except Exception:
        pass
    snapshot = await get_state().snapshot()
    ext = build_topic_summary(snapshot, None).model_dump()
    ext["deltas"] = compute_deltas(snapshot, None)
    await app.state.live_hub.broadcast({"type": "ingest", "event": event.model_dump(), "anomalies": [anomaly.model_dump() for anomaly in result.anomalies], "summary": ext})
    return {"ok": True, "anomalies": [anomaly.model_dump() for anomaly in result.anomalies]}


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    # update last scrape gauge
    LAST_SCRAPE.set(time.time())
    encoder, content_type = choose_encoder(None)
    data = generate_latest()
    return PlainTextResponse(data, media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    kafka_status = getattr(app.state, "kafka_status", {"running": False, "error": None})
    status = "ok" if not kafka_status.get("error") else "degraded"
    return {"status": status, "kafka": "running" if kafka_status.get("running") else "unavailable"}


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket) -> None:
    hub: LiveHub = app.state.live_hub
    await hub.connect(websocket)
    try:
        while True:
            snapshot = summarize(await get_state().snapshot())
            await websocket.send_text(snapshot.model_dump_json())
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        hub.disconnect(websocket)
    finally:
        hub.disconnect(websocket)


# middleware to enforce API key for sensitive endpoints
@app.middleware('http')
async def api_key_middleware(request: Request, call_next):
    # protect ingest endpoints
    if request.url.path.startswith('/api/events') or request.url.path.startswith('/api/ingest'):
        expected = getattr(app.state, 'api_key', None)
        if expected:
            key = request.headers.get('x-api-key') or request.query_params.get('api_key')
            if not key or key != expected:
                from fastapi.responses import JSONResponse

                return JSONResponse({'detail': 'unauthorized'}, status_code=401)
    return await call_next(request)
