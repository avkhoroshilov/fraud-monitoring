"""At-least-once Kafka delivery and idempotent PostgreSQL inserts."""
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server
from fraud_pipeline.metrics import WriterMetrics
from fraud_pipeline.stream import BROKERS, send

logger = logging.getLogger(__name__)
INSERT = """
INSERT INTO transaction_scores
  (transaction_id, score, fraud_flag, model_version, processed_at, us_state, merch, cat_id, amount)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id
"""


def validate_batch(payload):
    if not isinstance(payload, list) or not payload:
        raise ValueError("scoring must contain a nonempty JSON array")
    for row in payload:
        if not isinstance(row, dict):
            raise ValueError("Score must be an object")
        for key in ("transaction_id", "model_version"):
            value = row.get(key)
            if not isinstance(value, str) or not value.strip() or len(value) > 200 or "\x00" in value:
                raise ValueError(f"Invalid {key}")
        score = row.get("score")
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("Invalid score")
        if type(row.get("fraud_flag")) is not int or row["fraud_flag"] not in (0, 1):
            raise ValueError("Invalid fraud_flag")
        metadata = row.setdefault("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("Invalid metadata")
        for key in ("us_state", "merch", "cat_id"):
            value = metadata.setdefault(key, "unknown")
            if not isinstance(value, str) or not value or len(value) > 500 or "\x00" in value:
                raise ValueError(f"Invalid metadata.{key}")
        amount = metadata.get("amount")
        if amount is not None and (isinstance(amount, bool) or not isinstance(amount, (int, float)) or not math.isfinite(amount) or amount < 0):
            raise ValueError("Invalid amount")
        stamp = metadata.setdefault("processed_at", datetime.now(timezone.utc).isoformat())
        if not isinstance(stamp, str):
            raise ValueError("Invalid processed_at")
        parsed = datetime.fromisoformat(stamp)
        if parsed.tzinfo is None:
            raise ValueError("processed_at needs a timezone")
    return payload


def save_batch(connection, batch, metrics):
    started = time.perf_counter()
    inserted = []
    with connection.transaction():
        with connection.cursor() as cursor:
            for row in batch:
                meta = row["metadata"]
                cursor.execute(INSERT, (row["transaction_id"], row["score"], row["fraud_flag"], row["model_version"],
                                        meta["processed_at"], meta["us_state"], meta["merch"], meta["cat_id"], meta.get("amount")))
                if cursor.fetchone():
                    inserted.append(row)
    # Commit has completed. Retried Kafka records do not inflate these metrics.
    metrics.recorded(inserted, len(batch), time.perf_counter() - started)


def main():
    logging.basicConfig(level=logging.INFO)
    metrics = WriterMetrics()
    start_http_server(int(os.getenv("METRICS_PORT", "8001")))
    consumer = Consumer({"bootstrap.servers": BROKERS, "group.id": "fraud-postgres-writer-v1",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True})
    consumer.subscribe(["scoring"])
    try:
        with psycopg.connect(os.environ["SCORES_DSN"], autocommit=True) as connection:
            # Also works if the application starts against an existing database.
            connection.execute(Path("sql/schema.sql").read_text())
            while True:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(message.error())
                try:
                    batch = validate_batch(json.loads(message.value()))
                except (ValueError, TypeError, KeyError) as exc:
                    metrics.errors.labels("invalid_input").inc()
                    send(producer, "writer_errors", {"error": str(exc), "input": message.value().decode(errors="replace")},
                         f"{message.partition()}:{message.offset()}")
                else:
                    try:
                        save_batch(connection, batch, metrics)
                    except psycopg.Error:
                        metrics.errors.labels("database").inc()
                        logger.exception("Database write failed; Kafka offset remains uncommitted")
                        raise
                consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
