from __future__ import annotations

from dataclasses import dataclass
import os


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


DEFAULT_SIMULATOR_MODE = False
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC = "pulsetrace.events"
DEFAULT_KAFKA_GROUP_ID = "pulsetrace-sidecar"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_REDIS_EVENT_STREAM = "pulsetrace:events"
DEFAULT_REDIS_ANOMALY_STREAM = "pulsetrace:anomalies"
DEFAULT_REDIS_STATE_STREAM_LIMIT = 5000
DEFAULT_REPLAY_DEFAULT_LIMIT = 100
DEFAULT_CORS_ORIGINS = "http://localhost:5173"


@dataclass(slots=True)
class AppConfig:
    simulator_mode: bool = DEFAULT_SIMULATOR_MODE
    kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    kafka_topic: str = DEFAULT_KAFKA_TOPIC
    kafka_group_id: str = DEFAULT_KAFKA_GROUP_ID
    redis_url: str = DEFAULT_REDIS_URL
    redis_event_stream: str = DEFAULT_REDIS_EVENT_STREAM
    redis_anomaly_stream: str = DEFAULT_REDIS_ANOMALY_STREAM
    redis_state_stream_limit: int = DEFAULT_REDIS_STATE_STREAM_LIMIT
    replay_default_limit: int = DEFAULT_REPLAY_DEFAULT_LIMIT
    cors_origins: str = DEFAULT_CORS_ORIGINS

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            simulator_mode=_as_bool(os.getenv("SIMULATOR_MODE"), DEFAULT_SIMULATOR_MODE),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
            kafka_topic=os.getenv("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", DEFAULT_KAFKA_GROUP_ID),
            redis_url=os.getenv("REDIS_URL", DEFAULT_REDIS_URL),
            redis_event_stream=os.getenv("REDIS_EVENT_STREAM", DEFAULT_REDIS_EVENT_STREAM),
            redis_anomaly_stream=os.getenv("REDIS_ANOMALY_STREAM", DEFAULT_REDIS_ANOMALY_STREAM),
            redis_state_stream_limit=int(os.getenv("REDIS_STATE_STREAM_LIMIT", str(DEFAULT_REDIS_STATE_STREAM_LIMIT))),
            replay_default_limit=int(os.getenv("REPLAY_DEFAULT_LIMIT", str(DEFAULT_REPLAY_DEFAULT_LIMIT))),
            cors_origins=os.getenv("CORS_ORIGINS", DEFAULT_CORS_ORIGINS),
        )
