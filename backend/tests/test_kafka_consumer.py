from datetime import datetime, timezone

from app.kafka_consumer import message_to_event
from app.models import StreamKind


class FakeMessage:
    def __init__(self) -> None:
        self.value = b'{"message_id":"abc","sequence":7,"payload":{"amount":99},"kind":"consumer"}'
        self.headers = [("message_id", b"header-abc")]
        self.partition = 2
        self.offset = 14
        self.timestamp = 1710000000000


def test_message_to_event_converts_timestamp_and_headers() -> None:
    event = message_to_event(FakeMessage(), "topic-a")

    assert event.message_id == "abc"
    assert event.topic == "topic-a"
    assert event.partition == 2
    assert event.sequence == 7
    assert event.kind is StreamKind.CONSUMER
    assert event.timestamp == datetime.fromtimestamp(1710000000.0, tz=timezone.utc)
