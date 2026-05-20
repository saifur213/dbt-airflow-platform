import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("kafka-producer")

CONFIG_PATH = Path("/app/config/producer.yml")


# ── Config loading ────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f)

    # ── source_postgres overrides ─────────────────────────────────────────────
    pg = cfg.setdefault("source_postgres", {})
    if v := os.getenv("SOURCE_POSTGRES_HOST"):
        pg["host"] = v
    if v := os.getenv("SOURCE_POSTGRES_PORT"):
        pg["port"] = int(v)
    if v := os.getenv("SOURCE_POSTGRES_DB"):
        pg["database"] = v
    if v := os.getenv("SOURCE_POSTGRES_USER"):
        pg["user"] = v

    # Password must come from env — never stored in the config file
    password = os.environ.get("SOURCE_POSTGRES_PASSWORD")
    if not password:
        raise EnvironmentError("Required env var not set: SOURCE_POSTGRES_PASSWORD")
    pg["password"] = password

    # ── single-stream env overrides (applies to first stream entry) ───────────
    streams = cfg.setdefault("streams", [{}])
    s = streams[0]
    if v := os.getenv("SOURCE_POSTGRES_TABLE"):
        s["source_table"] = v
    if v := os.getenv("WATERMARK_COLUMN"):
        s["watermark_column"] = v
    if v := os.getenv("KAFKA_TOPIC"):
        s["kafka_topic"] = v
    if v := os.getenv("BATCH_SIZE"):
        s["batch_size"] = int(v)
    if v := os.getenv("POLL_INTERVAL_SEC"):
        s["poll_interval_sec"] = int(v)

    # ── kafka overrides ───────────────────────────────────────────────────────
    kafka = cfg.setdefault("kafka", {})
    if v := os.getenv("KAFKA_BOOTSTRAP"):
        kafka["bootstrap_servers"] = v

    return cfg


# ── Watermark helpers ─────────────────────────────────────────────────────────

def watermark_path(state_dir: str, topic: str) -> Path:
    """One watermark file per stream topic, stored in the state directory."""
    safe = topic.replace("/", "_").replace(".", "_")
    return Path(state_dir) / f"{safe}.watermark"


def load_watermark(state_dir: str, topic: str) -> str:
    path = watermark_path(state_dir, topic)
    if path.exists():
        val = path.read_text().strip()
        log.info("[%s] Resumed from watermark: %s", topic, val)
        return val
    return "1970-01-01T00:00:00+00:00"


def save_watermark(state_dir: str, topic: str, val: str) -> None:
    path = watermark_path(state_dir, topic)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(val)


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def ensure_topic(admin: AdminClient, topic: str, defaults: dict) -> None:
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        futures = admin.create_topics([
            NewTopic(
                topic,
                num_partitions=defaults.get("num_partitions", 3),
                replication_factor=defaults.get("replication_factor", 1),
            )
        ])
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
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Unserializable type: {type(o)}")
    return json.dumps(row, default=default)


# ── Postgres helper ───────────────────────────────────────────────────────────

def get_pg_connection(pg_cfg: dict):
    return psycopg2.connect(
        host=pg_cfg["host"],
        port=int(pg_cfg.get("port", 5432)),
        dbname=pg_cfg["database"],
        user=pg_cfg["user"],
        password=pg_cfg["password"],
        connect_timeout=pg_cfg.get("connect_timeout", 10),
    )


# ── Stream worker ─────────────────────────────────────────────────────────────

def run_stream(stream_cfg: dict, pg_cfg: dict, kafka_cfg: dict, state_dir: str) -> None:
    """
    Polls one source table and publishes to one Kafka topic.
    Runs in its own thread so multiple streams work concurrently.
    """
    topic         = stream_cfg["kafka_topic"]
    source_table  = stream_cfg["source_table"]
    watermark_col = stream_cfg.get("watermark_column", "updated_at")
    batch_size    = int(stream_cfg.get("batch_size", 500))
    poll_interval = int(stream_cfg.get("poll_interval_sec", 10))
    topic_defaults = kafka_cfg.get("topic_defaults", {})
    bootstrap     = kafka_cfg["bootstrap_servers"]

    log.info("[%s] Starting stream | table=%s watermark_col=%s", topic, source_table, watermark_col)

    # Wait for Kafka
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for attempt in range(30):
        try:
            ensure_topic(admin, topic, topic_defaults)
            break
        except Exception as exc:
            log.warning("[%s] Kafka not ready (attempt %d/30): %s", topic, attempt + 1, exc)
            time.sleep(5)
    else:
        raise RuntimeError(f"[{topic}] Kafka unavailable after 30 attempts")

    producer = Producer({
        "bootstrap.servers": bootstrap,
        **{k.replace("_", "."): v for k, v in kafka_cfg.get("producer", {}).items()},
    })

    watermark = load_watermark(state_dir, topic)

    while True:
        try:
            conn = get_pg_connection(pg_cfg)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT * FROM {source_table}
                    WHERE {watermark_col} > %s
                    ORDER BY {watermark_col} ASC
                    LIMIT %s
                    """,
                    (watermark, batch_size),
                )
                rows = cur.fetchall()
            conn.close()

            if rows:
                log.info("[%s] Fetched %d rows (watermark: %s)", topic, len(rows), watermark)
                new_watermark = watermark

                for row in rows:
                    row_dict = dict(row)
                    key = str(row_dict.get("id", ""))
                    producer.produce(
                        topic=topic,
                        key=key.encode("utf-8"),
                        value=serialize_row(row_dict).encode("utf-8"),
                        callback=delivery_report,
                    )
                    wm_val = row_dict.get(watermark_col)
                    if isinstance(wm_val, datetime):
                        wm_val = wm_val.isoformat()
                    new_watermark = str(wm_val)

                producer.flush()
                save_watermark(state_dir, topic, new_watermark)
                watermark = new_watermark
                log.info("[%s] Published %d messages | new watermark: %s", topic, len(rows), watermark)
            else:
                log.debug("[%s] No new rows. Sleeping %ss...", topic, poll_interval)

        except psycopg2.OperationalError as exc:
            log.error("[%s] Postgres error: %s – retrying in %ss", topic, exc, poll_interval)
        except Exception as exc:
            log.exception("[%s] Unexpected error: %s", topic, exc)

        time.sleep(poll_interval)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    cfg       = load_config()
    pg_cfg    = cfg["source_postgres"]
    kafka_cfg = cfg["kafka"]
    streams   = cfg.get("streams", [])
    state_dir = cfg.get("watermark", {}).get("state_dir", "/tmp/watermarks")

    if not streams:
        raise ValueError("No streams defined in producer.yml under 'streams:'")

    log.info("Loaded %d stream(s) from config", len(streams))

    if len(streams) == 1:
        # Single stream — run directly (simpler stack trace)
        run_stream(streams[0], pg_cfg, kafka_cfg, state_dir)
    else:
        # Multiple streams — one thread each
        threads = []
        for stream_cfg in streams:
            t = threading.Thread(
                target=run_stream,
                args=(stream_cfg, pg_cfg, kafka_cfg, state_dir),
                name=stream_cfg["kafka_topic"],
                daemon=True,
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()


if __name__ == "__main__":
    main()