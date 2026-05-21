import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("kafka-consumer")

CONFIG_PATH = Path("/app/config/consumer.yml")

PRIMARY_KEY = "order_id"


# ── Config loading ────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f)

    # ── dest_postgres overrides ───────────────────────────────────────────────
    pg = cfg.setdefault("dest_postgres", {})
    if v := os.getenv("TARGET_POSTGRES_HOST"):
        pg["host"] = v
    if v := os.getenv("TARGET_POSTGRES_PORT"):
        pg["port"] = int(v)
    if v := os.getenv("TARGET_POSTGRES_DB"):
        pg["database"] = v
    if v := os.getenv("TARGET_POSTGRES_USER"):
        pg["user"] = v

    # Password must come from env — never stored in the config file
    password = os.environ.get("TARGET_POSTGRES_PASSWORD")
    if not password:
        raise EnvironmentError("Required env var not set: TARGET_POSTGRES_PASSWORD")
    pg["password"] = password

    # ── single-stream env overrides (applies to first stream entry) ───────────
    streams = cfg.setdefault("streams", [{}])
    s = streams[0]
    if v := os.getenv("KAFKA_TOPIC"):
        s["kafka_topic"] = v
    if v := os.getenv("KAFKA_CONSUMER_GROUP"):
        s["consumer_group"] = v
    if v := os.getenv("TARGET_STREAM_TABLE"):
        s["dest_table"] = v
    if v := os.getenv("COMMIT_BATCH_SIZE"):
        s["commit_batch_size"] = int(v)
    if v := os.getenv("AUTO_CREATE_TABLE"):
        s["auto_create_table"] = v.lower() == "true"

    # ── kafka overrides ───────────────────────────────────────────────────────
    kafka = cfg.setdefault("kafka", {})
    if v := os.getenv("KAFKA_BOOTSTRAP"):
        kafka["bootstrap_servers"] = v

    return cfg


# ── Postgres helpers ──────────────────────────────────────────────────────────

def get_pg_connection(pg_cfg: dict):
    return psycopg2.connect(
        host=pg_cfg["host"],
        port=int(pg_cfg.get("port", 5432)),
        dbname=pg_cfg["database"],
        user=pg_cfg["user"],
        password=pg_cfg["password"],
        connect_timeout=pg_cfg.get("connect_timeout", 10),
    )


def infer_pg_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    return "TEXT"


def ensure_table(conn, table: str, row: dict) -> None:
    """Create the destination table if it doesn't exist, inferring column types."""
    col_defs = []
    for col, val in row.items():
        pg_type = infer_pg_type(val)
        if col == PRIMARY_KEY:
            col_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
        else:
            col_defs.append(f'"{col}" {pg_type}')

    # Ensure schema exists
    schema = table.split(".")[0] if "." in table else "public"
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table} ({", ".join(col_defs)})')
    conn.commit()
    log.info("Ensured table: %s", table)


def build_upsert(table: str, row: dict) -> tuple[str, list]:
    cols         = list(row.keys())
    values       = list(row.values())
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


# ── Stream worker ─────────────────────────────────────────────────────────────

def run_stream(stream_cfg: dict, pg_cfg: dict, kafka_cfg: dict) -> None:
    """
    Consumes one Kafka topic and writes to one destination table.
    Runs in its own thread so multiple streams work concurrently.
    """
    topic             = stream_cfg["kafka_topic"]
    group             = stream_cfg.get("consumer_group", "pg-sink-group")
    dest_table        = stream_cfg["dest_table"]
    commit_batch_size = int(stream_cfg.get("commit_batch_size", 100))
    auto_create       = str(stream_cfg.get("auto_create_table", True)).lower() == "true"
    bootstrap         = kafka_cfg["bootstrap_servers"]
    consumer_cfg      = kafka_cfg.get("consumer", {})

    log.info("[%s] Starting consumer | group=%s dest=%s", topic, group, dest_table)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": consumer_cfg.get("auto_offset_reset", "earliest"),
        "enable.auto.commit": False,
        "session.timeout.ms": consumer_cfg.get("session_timeout_ms", 30000),
        "max.poll.interval.ms": consumer_cfg.get("max_poll_interval_ms", 300000),
    })

    # Wait for Kafka and subscribe
    for attempt in range(30):
        try:
            consumer.subscribe([topic])
            log.info("[%s] Subscribed", topic)
            break
        except KafkaException as exc:
            log.warning("[%s] Kafka not ready (attempt %d/30): %s", topic, attempt + 1, exc)
            time.sleep(5)
    else:
        raise RuntimeError(f"[{topic}] Kafka unavailable after 30 attempts")

    table_ensured = False
    batch: list[dict] = []
    pending_msgs: list = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if batch:
                    _flush(batch, pending_msgs, consumer, pg_cfg, dest_table, topic)
                    batch.clear()
                    pending_msgs.clear()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("[%s] Kafka error: %s", topic, msg.error())
                continue

            try:
                row = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as exc:
                log.error("[%s] Failed to decode message: %s", topic, exc)
                continue

            if not table_ensured and auto_create:
                conn = get_pg_connection(pg_cfg)
                ensure_table(conn, dest_table, row)
                conn.close()
                table_ensured = True

            batch.append(row)
            pending_msgs.append(msg)

            if len(batch) >= commit_batch_size:
                _flush(batch, pending_msgs, consumer, pg_cfg, dest_table, topic)
                batch.clear()
                pending_msgs.clear()

    except KeyboardInterrupt:
        log.info("[%s] Shutting down...", topic)
    finally:
        consumer.close()


def _flush(batch: list[dict], messages: list, consumer: Consumer,
           pg_cfg: dict, dest_table: str, topic: str) -> None:
    """Write batch to Postgres then commit Kafka offset."""
    try:
        conn = get_pg_connection(pg_cfg)
        with conn.cursor() as cur:
            for row in batch:
                sql, values = build_upsert(dest_table, row)
                cur.execute(sql, values)
        conn.commit()
        conn.close()

        consumer.commit(message=messages[-1], asynchronous=False)
        log.info("[%s] Flushed %d rows → %s", topic, len(batch), dest_table)

    except psycopg2.OperationalError as exc:
        log.error("[%s] Postgres write error: %s – batch will be retried", topic, exc)
        raise
    except Exception as exc:
        log.exception("[%s] Unexpected flush error: %s", topic, exc)
        raise


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    cfg       = load_config()
    pg_cfg    = cfg["dest_postgres"]
    kafka_cfg = cfg["kafka"]
    streams   = cfg.get("streams", [])

    if not streams:
        raise ValueError("No streams defined in consumer.yml under 'streams:'")

    log.info("Loaded %d stream(s) from config", len(streams))

    if len(streams) == 1:
        run_stream(streams[0], pg_cfg, kafka_cfg)
    else:
        threads = []
        for stream_cfg in streams:
            t = threading.Thread(
                target=run_stream,
                args=(stream_cfg, pg_cfg, kafka_cfg),
                name=stream_cfg["kafka_topic"],
                daemon=True,
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()


if __name__ == "__main__":
    main()