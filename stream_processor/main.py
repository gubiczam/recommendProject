from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer
from neo4j import GraphDatabase


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [stream_processor] %(message)s",
)
logger = logging.getLogger(__name__)


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_USER_INTERACTIONS", "user-interactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "interaction-writer-v1")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

_running = True


def _handle_shutdown(signum: int, _frame: Any) -> None:
    global _running
    logger.info("Received signal %s, shutting down...", signum)
    _running = False


class Neo4jInteractionWriter:
    def __init__(self, uri: str, user: str, password: str) -> None:
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self._driver.close()

    def upsert_interaction(self, user_id: str, product_id: str, action: str, event_time: str) -> None:
        query = """
        MERGE (u:User {id: $user_id})
        ON CREATE SET u.name = $user_id
        MERGE (p:Product {id: $product_id})
        ON CREATE SET p.name = $product_id, p.category = 'Unknown', p.price = 0.0
        WITH u, p
        FOREACH (_ IN CASE WHEN $action = 'VIEWED' THEN [1] ELSE [] END |
          MERGE (u)-[r:VIEWED]->(p)
          ON CREATE SET r.first_event_at = datetime($event_time), r.count = 0
          SET r.last_event_at = datetime($event_time), r.count = coalesce(r.count, 0) + 1
        )
        FOREACH (_ IN CASE WHEN $action = 'PURCHASED' THEN [1] ELSE [] END |
          MERGE (u)-[r:PURCHASED]->(p)
          ON CREATE SET r.first_event_at = datetime($event_time), r.count = 0
          SET r.last_event_at = datetime($event_time), r.count = coalesce(r.count, 0) + 1
        )
        """
        with self._driver.session() as session:
            session.run(
                query,
                user_id=user_id,
                product_id=product_id,
                action=action,
                event_time=event_time,
            )


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )


def parse_event(raw_value: bytes) -> dict[str, Any]:
    payload = json.loads(raw_value.decode("utf-8"))
    required = {"user_id", "product_id", "action"}
    missing = required - payload.keys()
    if missing:
        raise ValueError(f"Missing required fields: {sorted(missing)}")

    action = str(payload["action"]).upper()
    if action not in {"VIEWED", "PURCHASED"}:
        raise ValueError(f"Unsupported action: {action}")

    event_time = payload.get("event_time") or datetime.now(timezone.utc).isoformat()
    return {
        "user_id": str(payload["user_id"]),
        "product_id": str(payload["product_id"]),
        "action": action,
        "event_time": str(event_time),
    }


def run() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    writer = Neo4jInteractionWriter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    consumer: KafkaConsumer | None = None

    try:
        while _running and consumer is None:
            try:
                consumer = build_consumer()
            except Exception as exc:
                logger.warning("Kafka not ready (%s). Retrying in 3s...", exc)
                time.sleep(3)

        if consumer is None:
            return

        logger.info(
            "Stream processor started. topic=%s group_id=%s broker=%s",
            KAFKA_TOPIC,
            KAFKA_GROUP_ID,
            KAFKA_BOOTSTRAP_SERVERS,
        )

        while _running:
            for message in consumer:
                if not _running:
                    break

                try:
                    event = parse_event(message.value)
                except Exception as exc:
                    logger.error(
                        "Dropping malformed event at offset=%s partition=%s: %s",
                        message.offset,
                        message.partition,
                        exc,
                    )
                    consumer.commit()
                    continue

                try:
                    writer.upsert_interaction(
                        user_id=event["user_id"],
                        product_id=event["product_id"],
                        action=event["action"],
                        event_time=event["event_time"],
                    )
                    consumer.commit()
                    logger.info(
                        "Processed event offset=%s user_id=%s product_id=%s action=%s",
                        message.offset,
                        event["user_id"],
                        event["product_id"],
                        event["action"],
                    )
                except Exception as exc:
                    logger.exception(
                        "Neo4j write failed for offset=%s (will retry after restart): %s",
                        message.offset,
                        exc,
                    )
                    time.sleep(1)
    finally:
        if consumer is not None:
            consumer.close()
        writer.close()
        logger.info("Stream processor stopped.")


if __name__ == "__main__":
    run()
