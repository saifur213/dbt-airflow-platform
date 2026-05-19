"""
Kafka Producer: polls rows from source Postgres and publishes them to a Kafka topic.

Strategy: uses a watermark column (updated_at by default) to track already-processed
rows, so restarts are safe and idempotent. The watermark is stored in a local state
file so the producer survives container restarts.

Environment variables (set via docker-compose env_file or env section):
  SOURCE_PG_HOST        - usually host.docker.internal
  SOURCE_PG_PORT        - default 5432
  SOURCE_PG_DB
  SOURCE_PG_USER
  SOURCE_PG_PASSWORD
  SOURCE_TABLE          - schema.table to stream, e.g. public.orders
  WATERMARK_COLUMN      - column used for incremental fetch, e.g. updated_at
  KAFKA_BOOTSTRAP       - e.g. kafka:9092
  KAFKA_TOPIC           - topic name, e.g. pg.orders
  POLL_INTERVAL_SEC     - seconds between polls, default 10
  BATCH_SIZE            - rows per poll, default 500
"""

import json
import logging
import os
import time
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("kafka-producer")

# ── Config ──────────────────────────────────────────────────────────────────

SOURCE_PG_HOST     = "host.docker.internal"
SOURCE_PG_PORT     = int(os.getenv("SOURCE_PG_PORT", "5432"))
SOURCE_PG_DB       = os.environ["SOURCE_PG_DB"]
SOURCE_PG_USER     = os.environ["SOURCE_PG_USER"]
SOURCE_PG_PASSWORD = os.environ["SOURCE_PG_PASSWORD"]

SOURCE_TABLE       = os.environ["SOURCE_TABLE"]          # e.g. public.orders
WATERMARK_COLUMN   = os.getenv("WATERMARK_COLUMN", "updated_at")

KAFKA_BOOTSTRAP    = os.environ["KAFKA_BOOTSTRAP"]       # e.g. kafka:9092
KAFKA_TOPIC        = os.environ["KAFKA_TOPIC"]

POLL_INTERVAL_SEC  = int(os.getenv("POLL_INTERVAL_SEC", "10"))
BATCH_SIZE         = int(os.getenv("BATCH_SIZE", "500"))

WATERMARK_FILE     = Path("/tmp/watermark.txt")


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_watermark() -> str:
    """Load last-seen watermark value from disk (survives restarts)."""
    if WATERMARK_FILE.exists():
        val = WATERMARK_FILE.read_text().strip()
        log.info("Resumed from watermark: %s", val)
        return val
    # First run: start from epoch so all existing rows are captured
    return "1970-01-01T00:00:00+00:00"


def save_watermark(val: str) -> None:
    WATERMARK_FILE.write_text(val)


def get_pg_connection():
    return psycopg2.connect(
        host=SOURCE_PG_HOST,
        port=SOURCE_PG_PORT,
        dbname=SOURCE_PG_DB,
        user=SOURCE_PG_USER,
        password=SOURCE_PG_PASSWORD,
        connect_timeout=10,
    )


def ensure_topic(bootstrap: str, topic: str) -> None:
    """Create the Kafka topic if it doesn't exist yet."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        futures = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        for t, f in futures.items():
            try:
                f.result()
                log.info("Created topic: %s", t)
            except Exception as exc:
                log.warning("Topic creation warning (may already exist): %s", exc)


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)


def serialize_row(row: dict) -> str:
    """Convert a Postgres row dict to JSON, handling special types."""
    
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()

        elif isinstance(o, Decimal):
            # preserve decimal precision
            return str(o)

        raise TypeError(f"Unserializable: {type(o)}")

    return json.dumps(row, default=default)


# ── Main loop ────────────────────────────────────────────────────────────────

def main():
    log.info("Starting producer | table=%s topic=%s poll=%ss", SOURCE_TABLE, KAFKA_TOPIC, POLL_INTERVAL_SEC)

    # Wait for Kafka to be ready
    for attempt in range(30):
        try:
            ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
            break
        except Exception as exc:
            log.warning("Kafka not ready yet (attempt %d/30): %s", attempt + 1, exc)
            time.sleep(5)
    else:
        raise RuntimeError("Kafka unavailable after 30 attempts")

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 1000,
    })

    watermark = load_watermark()

    while True:
        try:
            conn = get_pg_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = f"""
                    SELECT *
                    FROM {SOURCE_TABLE}
                    WHERE {WATERMARK_COLUMN} > %s
                    ORDER BY {WATERMARK_COLUMN} ASC
                    LIMIT %s
                """
                cur.execute(query, (watermark, BATCH_SIZE))
                rows = cur.fetchall()

            conn.close()

            if rows:
                log.info("Fetched %d rows (watermark: %s)", len(rows), watermark)
                new_watermark = watermark

                for row in rows:
                    row_dict = dict(row)
                    key = str(row_dict.get("id", ""))
                    value = serialize_row(row_dict)

                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=key.encode("utf-8"),
                        value=value.encode("utf-8"),
                        callback=delivery_report,
                    )

                    wm_val = row_dict.get(WATERMARK_COLUMN)
                    if isinstance(wm_val, datetime):
                        wm_val = wm_val.isoformat()
                    new_watermark = str(wm_val)

                producer.flush()
                save_watermark(new_watermark)
                watermark = new_watermark
                log.info("Published %d messages | new watermark: %s", len(rows), watermark)
            else:
                log.debug("No new rows. Sleeping %ss...", POLL_INTERVAL_SEC)

        except psycopg2.OperationalError as exc:
            log.error("Postgres connection error: %s – retrying in %ss", exc, POLL_INTERVAL_SEC)
        except Exception as exc:
            log.exception("Unexpected error: %s", exc)

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()