"""
Kafka Consumer: reads messages from a Kafka topic and upserts rows into
the destination Postgres database.

Design decisions:
  - Uses INSERT … ON CONFLICT (id) DO UPDATE so re-delivered messages are safe.
  - Commits Kafka offsets only after a successful Postgres commit (at-least-once).
  - Processes messages in micro-batches (COMMIT_BATCH_SIZE) for throughput.
  - Destination table is auto-created from the first message's keys if
    AUTO_CREATE_TABLE=true (useful for dev; disable in prod).

Environment variables:
  DEST_PG_HOST          - usually host.docker.internal
  DEST_PG_PORT          - default 5432
  DEST_PG_DB
  DEST_PG_USER
  DEST_PG_PASSWORD
  DEST_TABLE            - target schema.table, e.g. public.orders_sink
  KAFKA_BOOTSTRAP       - e.g. kafka:9092
  KAFKA_TOPIC           - topic to consume
  KAFKA_CONSUMER_GROUP  - consumer group id, e.g. pg-sink-group
  COMMIT_BATCH_SIZE     - rows per Postgres commit, default 100
  AUTO_CREATE_TABLE     - true/false, default true
"""

import json
import logging
import os
import time
from typing import Any

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("kafka-consumer")

# ── Config ──────────────────────────────────────────────────────────────────

DEST_PG_HOST        = "host.docker.internal"
DEST_PG_PORT        = int(os.getenv("DEST_PG_PORT", "5432"))
DEST_PG_DB          = os.environ["DEST_PG_DB"]
DEST_PG_USER        = os.environ["DEST_PG_USER"]
DEST_PG_PASSWORD    = os.environ["DEST_PG_PASSWORD"]

DEST_TABLE          = os.environ["DEST_TABLE"]

KAFKA_BOOTSTRAP     = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC         = os.environ["KAFKA_TOPIC"]
KAFKA_GROUP         = os.getenv("KAFKA_CONSUMER_GROUP", "pg-sink-group")

COMMIT_BATCH_SIZE   = int(os.getenv("COMMIT_BATCH_SIZE", "100"))
AUTO_CREATE_TABLE   = os.getenv("AUTO_CREATE_TABLE", "true").lower() == "true"


# ── Helpers ──────────────────────────────────────────────────────────────────
PRIMARY_KEY = "order_id"

def get_pg_connection():
    return psycopg2.connect(
        host=DEST_PG_HOST,
        port=DEST_PG_PORT,
        dbname=DEST_PG_DB,
        user=DEST_PG_USER,
        password=DEST_PG_PASSWORD,
        connect_timeout=10,
    )


def infer_pg_type(value: Any) -> str:
    """Infer a reasonable Postgres column type from a Python value."""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    return "TEXT"


def auto_create_table(conn, table: str, row: dict) -> None:
    """Create the destination table if it doesn't exist, inferring types."""
    schema, tbl = (table.split(".", 1) + [""])[:2] or ("public", table)
    if not tbl:
        schema, tbl = "public", schema

    col_defs = []
    for col, val in row.items():
        pg_type = infer_pg_type(val)
        if col == PRIMARY_KEY:
            col_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
        else:
            col_defs.append(f'"{col}" {pg_type}')

    ddl = f'CREATE TABLE IF NOT EXISTS {table} ({", ".join(col_defs)})'
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Ensured table: %s", table)


def build_upsert(table: str, row: dict) -> tuple[str, list]:
    """Build an INSERT … ON CONFLICT DO UPDATE statement."""
    cols    = list(row.keys())
    values  = list(row.values())
    placeholders = ", ".join(["%s"] * len(cols))
    col_list     = ", ".join([f'"{c}"' for c in cols])
    updates = ", ".join([
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols if c != PRIMARY_KEY
    ])

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({PRIMARY_KEY}) DO UPDATE SET {updates}
    """
    return sql.strip(), values


# ── Main loop ────────────────────────────────────────────────────────────────

def main():
    log.info("Starting consumer | topic=%s group=%s table=%s", KAFKA_TOPIC, KAFKA_GROUP, DEST_TABLE)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # manual commit after Postgres write
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000,
    })

    # Wait for Kafka to be ready
    for attempt in range(30):
        try:
            consumer.subscribe([KAFKA_TOPIC])
            log.info("Subscribed to topic: %s", KAFKA_TOPIC)
            break
        except KafkaException as exc:
            log.warning("Kafka not ready yet (attempt %d/30): %s", attempt + 1, exc)
            time.sleep(5)
    else:
        raise RuntimeError("Kafka unavailable after 30 attempts")

    table_ensured = False
    batch: list[dict] = []
    offsets_to_commit = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush any partial batch after idle
                if batch:
                    _flush_batch(batch, offsets_to_commit, consumer, table_ensured)
                    batch.clear()
                    offsets_to_commit.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("Reached end of partition %s/%s", msg.topic(), msg.partition())
                else:
                    log.error("Kafka error: %s", msg.error())
                continue

            try:
                row = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as exc:
                log.error("Failed to decode message: %s", exc)
                continue

            if not table_ensured and AUTO_CREATE_TABLE:
                conn = get_pg_connection()
                auto_create_table(conn, DEST_TABLE, row)
                conn.close()
                table_ensured = True

            batch.append(row)
            offsets_to_commit.append(msg)

            if len(batch) >= COMMIT_BATCH_SIZE:
                _flush_batch(batch, offsets_to_commit, consumer, table_ensured)
                batch.clear()
                offsets_to_commit.clear()

    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    finally:
        consumer.close()


def _flush_batch(batch: list[dict], messages: list, consumer: Consumer, table_ensured: bool) -> None:
    """Write a batch to Postgres and commit Kafka offsets."""
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            for row in batch:
                sql, values = build_upsert(DEST_TABLE, row)
                cur.execute(sql, values)
        conn.commit()
        conn.close()

        # Commit Kafka offset only after successful Postgres commit
        consumer.commit(message=messages[-1], asynchronous=False)
        log.info("Flushed %d rows → %s", len(batch), DEST_TABLE)

    except psycopg2.OperationalError as exc:
        log.error("Postgres write error: %s – batch will be retried", exc)
        raise
    except Exception as exc:
        log.exception("Unexpected flush error: %s", exc)
        raise


if __name__ == "__main__":
    main()