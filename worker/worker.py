"""
Sentinel Worker — Redis Streams consumer that processes AI tasks with Gemini.

Changes from original:
  - All print() replaced with structured logging
  - Main loop guarded by if __name__ == "__main__"
  - Prometheus metrics exposed via HTTP server on port 9100
  - Broad except Exception narrowed where safe
  - XTRIM "MAXAGE" replaced with MAXLEN (MAXAGE is not a standard Redis 7 command arg)
"""

import json
import logging
import os
import sys
import time
import threading
from datetime import datetime

import psycopg2
import redis
from google import genai
from prometheus_client import Counter, Histogram, start_http_server

# ---------------------------------------------------------------------------
# Path fix — allow running as `python worker/worker.py` from repo root
# ---------------------------------------------------------------------------
_worker_dir = os.path.dirname(os.path.abspath(__file__))
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)

from snowflake import generate_snowflake_id  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STREAM_NAME  = "ai_task_queue"
GROUP_NAME   = "ai_workers"
CONSUMER_NAME = os.getenv("WORKER_NAME", "worker-1")

STREAM_TRIM_MAXLEN          = int(os.getenv("STREAM_TRIM_MAXLEN", "10000"))
PROCESSING_DELAY_SEC        = int(os.getenv("PROCESSING_DELAY_SEC", "0"))
PROCESS_MAX_RETRIES         = int(os.getenv("PROCESS_MAX_RETRIES", "3"))
PROCESS_RETRY_BASE_DELAY_SEC = float(os.getenv("PROCESS_RETRY_BASE_DELAY_SEC", "1.0"))
METRICS_PORT                = int(os.getenv("METRICS_PORT", "9100"))

REDIS_HOST               = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT               = int(os.getenv("REDIS_PORT", "6379"))
REDIS_SOCKET_TIMEOUT     = int(os.getenv("REDIS_SOCKET_TIMEOUT", "10"))
REDIS_CONNECT_RETRIES    = int(os.getenv("REDIS_CONNECT_RETRIES", "5"))
REDIS_CONNECT_RETRY_DELAY = float(os.getenv("REDIS_CONNECT_RETRY_DELAY", "2.0"))
PUBSUB_CHANNEL           = "task_events"

DB_HOST                = os.getenv("DB_HOST", "localhost")
DB_PORT                = int(os.getenv("DB_PORT", "5432"))
DB_NAME                = os.getenv("DB_NAME", "sentinel_db")
DB_USER                = os.getenv("DB_USER", "sentinel")
DB_PASSWORD            = os.getenv("DB_PASSWORD", "sentinel")
DB_CONNECT_RETRIES     = int(os.getenv("DB_CONNECT_RETRIES", "5"))
DB_CONNECT_RETRY_DELAY = float(os.getenv("DB_CONNECT_RETRY_DELAY", "1.0"))

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
TASKS_PROCESSED = Counter(
    "worker_tasks_processed_total", "Tasks successfully completed", ["worker"]
)
TASKS_FAILED = Counter(
    "worker_tasks_failed_total", "Tasks that exhausted all retries", ["worker"]
)
TASK_DURATION = Histogram(
    "worker_task_processing_seconds",
    "Time spent processing a single task (Gemini + DB write)",
    ["worker"],
)

# ---------------------------------------------------------------------------
# Redis client (sync, for stream consumer)
# ---------------------------------------------------------------------------
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_timeout=REDIS_SOCKET_TIMEOUT,
    socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
)


# ---------------------------------------------------------------------------
# Gemini client
# ---------------------------------------------------------------------------
def _build_gemini_client():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is not set")
    return genai.Client(api_key=api_key)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def publish_event(event_type: str, task_id: str, extra: dict | None = None) -> None:
    """Publish a JSON event to the Redis Pub/Sub channel. Never raises."""
    payload = {
        "event_id": str(generate_snowflake_id()),
        "event": event_type,
        "task_id": str(task_id),
        "worker": CONSUMER_NAME,
    }
    if extra:
        payload.update(extra)
    try:
        redis_client.publish(PUBSUB_CHANNEL, json.dumps(payload))
    except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        logger.warning("Failed to publish event event_type=%s error=%s", event_type, e)


def get_db_connection():
    """Connect to PostgreSQL with retries. Raises on persistent failure."""
    last_err = None
    for attempt in range(DB_CONNECT_RETRIES):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=10,
            )
        except psycopg2.OperationalError as e:
            last_err = e
            logger.warning(
                "DB connect attempt %d/%d failed: %s", attempt + 1, DB_CONNECT_RETRIES, e
            )
            if attempt < DB_CONNECT_RETRIES - 1:
                time.sleep(DB_CONNECT_RETRY_DELAY)
    raise last_err


def ensure_consumer_group() -> None:
    try:
        redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        logger.info("Consumer group created stream=%s group=%s", STREAM_NAME, GROUP_NAME)
    except redis.exceptions.ResponseError:
        logger.debug("Consumer group already exists stream=%s group=%s", STREAM_NAME, GROUP_NAME)


def reclaim_stuck_tasks() -> list:
    """Reclaim messages idle > 10 s from other consumers."""
    try:
        pending = redis_client.xpending_range(STREAM_NAME, GROUP_NAME, min="-", max="+", count=10)
    except (redis.ConnectionError, redis.TimeoutError) as e:
        logger.warning("Could not check pending messages: %s", e)
        return []

    reclaimed = []
    for entry in pending:
        message_id = entry["message_id"]
        idle_time = entry["time_since_delivered"]
        if idle_time > 10_000:
            try:
                messages = redis_client.xclaim(
                    STREAM_NAME, GROUP_NAME, CONSUMER_NAME,
                    min_idle_time=10_000, message_ids=[message_id],
                )
                reclaimed.extend(messages)
                logger.info("Reclaimed stuck message message_id=%s idle_ms=%d", message_id, idle_time)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning("XCLAIM failed for message_id=%s: %s", message_id, e)
                break
    return reclaimed


def trim_stream_if_due() -> None:
    """Trim stream to STREAM_TRIM_MAXLEN entries (approximate, ~)."""
    if STREAM_TRIM_MAXLEN <= 0:
        return
    try:
        redis_client.xtrim(STREAM_NAME, maxlen=STREAM_TRIM_MAXLEN, approximate=True)
    except Exception as e:
        logger.warning("Stream trim failed: %s", e)


def _parse_task_id(task_id_raw) -> tuple[int | None, str | None]:
    """Parse task_id from stream. Returns (int_id, str_id) or (None, None) on error."""
    try:
        s = str(task_id_raw).strip()
        n = int(s)
        if n < 0:
            return None, None
        return n, s
    except (ValueError, TypeError):
        return None, None


def _ack(message_id: str) -> None:
    """ACK a stream message, swallowing Redis errors."""
    try:
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
    except (redis.ConnectionError, redis.TimeoutError) as e:
        logger.warning("Failed to ACK message_id=%s: %s", message_id, e)


# ---------------------------------------------------------------------------
# Core task processor
# ---------------------------------------------------------------------------

def process_task(message_id: str, data: dict, gemini_client) -> None:
    task_id_int, task_id_str = _parse_task_id(data.get("task_id"))

    if task_id_int is None:
        logger.error("Invalid stream payload — bad task_id: %r", data.get("task_id"))
        _ack(message_id)
        return

    task_type  = data.get("task_type")
    input_text = data.get("input_text")

    if not task_type or not input_text:
        logger.error(
            "Invalid stream payload task_id=%s: missing task_type or input_text", task_id_str
        )
        _ack(message_id)
        return

    if PROCESSING_DELAY_SEC > 0:
        logger.info("Task %s: artificial delay %ds", task_id_str, PROCESSING_DELAY_SEC)
        time.sleep(PROCESSING_DELAY_SEC)

    logger.info("Processing task task_id=%s type=%s", task_id_str, task_type)

    last_error = None
    conn = None
    cur = None

    for attempt in range(PROCESS_MAX_RETRIES):
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # Fetch or materialise task row
            cur.execute(
                "SELECT status, task_type, input_text FROM tasks WHERE id=%s",
                (task_id_int,),
            )
            row = cur.fetchone()

            if not row:
                now = datetime.utcnow()
                cur.execute(
                    """
                    INSERT INTO tasks (id, task_type, status, input_text, created_at, updated_at, queued_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (task_id_int, task_type, "QUEUED", input_text, now, now, now),
                )
                conn.commit()
                status = "QUEUED"
            else:
                status, task_type, input_text = row

            if status in ("COMPLETED", "FAILED"):
                logger.info(
                    "Skipping already-finished task task_id=%s status=%s", task_id_str, status
                )
                _ack(message_id)
                cur.close()
                conn.close()
                return

            cur.execute(
                "UPDATE tasks SET status=%s, started_at=NOW(), updated_at=NOW() WHERE id=%s",
                ("PROCESSING", task_id_int),
            )
            conn.commit()
            publish_event("TASK_PROCESSING", task_id_str)

            t0 = time.monotonic()
            MODEL_NAME = "gemini-2.5-flash"
            prompt = f"Task: {task_type}\n\nInput:\n{input_text}\n\nReturn only the result."
            response = gemini_client.models.generate_content(model=MODEL_NAME, contents=prompt)
            result = response.text
            elapsed = time.monotonic() - t0

            cur.execute(
                "UPDATE tasks SET status=%s, output_text=%s, completed_at=NOW(), updated_at=NOW() WHERE id=%s",
                ("COMPLETED", result, task_id_int),
            )
            conn.commit()
            publish_event("TASK_COMPLETED", task_id_str)
            _ack(message_id)

            TASKS_PROCESSED.labels(worker=CONSUMER_NAME).inc()
            TASK_DURATION.labels(worker=CONSUMER_NAME).observe(elapsed)
            logger.info(
                "Task completed task_id=%s duration=%.2fs", task_id_str, elapsed
            )
            cur.close()
            conn.close()
            return

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last_error = e
            _cleanup_db(conn, cur)
            delay = PROCESS_RETRY_BASE_DELAY_SEC * (2 ** attempt)
            logger.warning(
                "DB error on attempt %d/%d for task_id=%s: %s — retry in %.1fs",
                attempt + 1, PROCESS_MAX_RETRIES, task_id_str, e, delay,
            )
            if attempt < PROCESS_MAX_RETRIES - 1:
                time.sleep(delay)

        except Exception as e:
            last_error = e
            _cleanup_db(conn, cur)
            delay = PROCESS_RETRY_BASE_DELAY_SEC * (2 ** attempt)
            logger.warning(
                "Error on attempt %d/%d for task_id=%s: %s — retry in %.1fs",
                attempt + 1, PROCESS_MAX_RETRIES, task_id_str, e, delay,
            )
            if attempt < PROCESS_MAX_RETRIES - 1:
                time.sleep(delay)

    # All retries exhausted — mark task FAILED
    logger.error(
        "Task failed after %d attempts task_id=%s error=%s",
        PROCESS_MAX_RETRIES, task_id_str, last_error,
    )
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE tasks SET status=%s, error=%s, completed_at=NOW(), updated_at=NOW() WHERE id=%s",
            ("FAILED", str(last_error), task_id_int),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as db_err:
        logger.error("Could not mark task %s as FAILED: %s", task_id_str, db_err)

    publish_event("TASK_FAILED", task_id_str, {"error": str(last_error)})
    _ack(message_id)
    TASKS_FAILED.labels(worker=CONSUMER_NAME).inc()


def _cleanup_db(conn, cur) -> None:
    """Safely roll back and close DB resources."""
    try:
        if conn:
            conn.rollback()
    except Exception:
        pass
    try:
        if cur:
            cur.close()
    except Exception:
        pass
    try:
        if conn:
            conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Worker entry point — start Prometheus metrics server then run consume loop."""
    gemini_client = _build_gemini_client()

    # Start Prometheus HTTP server on a background thread
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics server started port=%d", METRICS_PORT)

    ensure_consumer_group()
    logger.info(
        "Worker started consumer=%s stream=%s | trim_maxlen=%d | delay=%ds | retries=%d",
        CONSUMER_NAME, STREAM_NAME, STREAM_TRIM_MAXLEN, PROCESSING_DELAY_SEC, PROCESS_MAX_RETRIES,
    )

    last_trim = time.monotonic()
    redis_backoff = 0.0

    while True:
        try:
            if redis_backoff > 0:
                time.sleep(redis_backoff)
                redis_backoff = min(redis_backoff * 2, 60.0)

            # Periodic stream trim
            if (time.monotonic() - last_trim) >= 300:  # every 5 min
                trim_stream_if_due()
                last_trim = time.monotonic()

            messages = redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=1,
                block=5000,
            )
            redis_backoff = 0.0

            if messages:
                for _stream, entries in messages:
                    for message_id, data in entries:
                        process_task(message_id, data, gemini_client)

            # Check for and reclaim stuck tasks
            reclaimed = reclaim_stuck_tasks()
            for message_id, data in reclaimed:
                process_task(message_id, data, gemini_client)

        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
            if redis_backoff == 0:
                redis_backoff = REDIS_CONNECT_RETRY_DELAY
            logger.error("Redis error (retry in %.1fs): %s", redis_backoff, e)


if __name__ == "__main__":
    main()
