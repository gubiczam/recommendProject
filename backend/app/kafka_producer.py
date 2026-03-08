from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer


class UserInteractionProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all",
        )

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        future = self._producer.send(topic=topic, key=key, value=event)
        future.get(timeout=10)
