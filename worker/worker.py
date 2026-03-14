import time
import redis
import psycopg2
import os
import sys
from datetime import datetime
from google import genai
import json

# Allow running as python worker/worker.py from repo root
_worker_dir = os.path.dirname(os.path.abspath(__file__))
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)
from snowflake import generate_snowflake_id

STREAM_NAME = "ai_task_queue"
GROUP_NAME = "ai_workers"
CONSUMER_NAME = "worker-1"

# Flush/trim stream: remove entries older than this (minutes). 0 = disabled.
STREAM_TRIM_MAXAGE_MINUTES = int(os.getenv("STREAM_TRIM_MAXAGE_MINUTES", "15"))
# Delay before processing each task (seconds), to avoid hammering DB under high traffic. 0 = no delay.
PROCESSING_DELAY_SEC = int(os.getenv("PROCESSING_DELAY_SEC", "120"))

# Retry config for processing (Gemini/DB transient failures)
PROCESS_MAX_RETRIES = int(os.getenv("PROCESS_MAX_RETRIES", "3"))
PROCESS_RETRY_BASE_DELAY_SEC = float(os.getenv("PROCESS_RETRY_BASE_DELAY_SEC", "1.0"))

# Redis connection (with timeouts)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", "10"))
REDIS_CONNECT_RETRIES = int(os.getenv("REDIS_CONNECT_RETRIES", "5"))
REDIS_CONNECT_RETRY_DELAY = float(os.getenv("REDIS_CONNECT_RETRY_DELAY", "2.0"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_timeout=REDIS_SOCKET_TIMEOUT,
    socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
)

PUBSUB_CHANNEL = "task_events"


def publish_event(event_type, task_id, extra=None):
    """Publish event with Snowflake event_id. Swallows Redis errors so worker keeps running."""
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
        print(f"Warning: failed to publish event ({event_type}): {e}")

# Gemini setup

# Gemini setup (NEW SDK)
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise RuntimeError("GEMINI_API_KEY environment variable is not set")

client = genai.Client(api_key=api_key)
MODEL_NAME = "gemini-2.5-flash"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "sentinel_db")
DB_USER = os.getenv("DB_USER", "sentinel")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sentinel")
DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "5"))
DB_CONNECT_RETRY_DELAY = float(os.getenv("DB_CONNECT_RETRY_DELAY", "1.0"))


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
            print(f"DB connect attempt {attempt + 1}/{DB_CONNECT_RETRIES} failed: {e}")
            if attempt < DB_CONNECT_RETRIES - 1:
                time.sleep(DB_CONNECT_RETRY_DELAY)
    raise last_err


def ensure_consumer_group():
    try:
        redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except redis.exceptions.ResponseError:
        pass  # group exists

def reclaim_stuck_tasks():
    try:
        pending = redis_client.xpending_range(STREAM_NAME, GROUP_NAME, min="-", max="+", count=10)
    except (redis.ConnectionError, redis.TimeoutError):
        return []
    reclaimed = []
    for entry in pending:
        message_id = entry["message_id"]
        idle_time = entry["time_since_delivered"]
        if idle_time > 10000:
            try:
                messages = redis_client.xclaim(
                    STREAM_NAME, GROUP_NAME, CONSUMER_NAME,
                    min_idle_time=10000, message_ids=[message_id],
                )
                reclaimed.extend(messages)
            except (redis.ConnectionError, redis.TimeoutError):
                break
    return reclaimed
def trim_stream_if_due():
    """Trim stream to remove entries older than STREAM_TRIM_MAXAGE_MINUTES (run periodically)."""
    if STREAM_TRIM_MAXAGE_MINUTES <= 0:
        return
    try:
        maxage_ms = STREAM_TRIM_MAXAGE_MINUTES * 60 * 1000
        redis_client.execute_command("XTRIM", STREAM_NAME, "MAXAGE", maxage_ms)
    except Exception as e:
        print(f"Stream trim failed: {e}")


def _parse_task_id(task_id_raw):
    """Parse task_id from stream to int for DB (Snowflake BIGINT). Returns (int_id, str_id) or (None, None)."""
    try:
        s = str(task_id_raw).strip()
        n = int(s)
        if n < 0:
            return None, None
        return n, s
    except (ValueError, TypeError):
        return None, None


def process_task(message_id, data):
    task_id_str = data.get("task_id")
    task_id_int, task_id_str = _parse_task_id(task_id_str)
    if task_id_int is None:
        print(f"Invalid stream payload: bad task_id {task_id_str!r}")
        try:
            redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        except (redis.ConnectionError, redis.TimeoutError):
            pass
        return

    task_type = data.get("task_type")
    input_text = data.get("input_text")
    if not task_type or not input_text:
        print(f"Invalid stream payload for {task_id_str}: missing task_type or input_text")
        try:
            redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        except (redis.ConnectionError, redis.TimeoutError):
            pass
        return

    if PROCESSING_DELAY_SEC > 0:
        print(f"Task {task_id_str}: waiting {PROCESSING_DELAY_SEC}s before processing...")
        time.sleep(PROCESSING_DELAY_SEC)
    print(f"Processing task {task_id_str}")

    last_error = None
    for attempt in range(PROCESS_MAX_RETRIES):
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # 1) Fetch or materialize task row (id is BIGINT / Snowflake)
            cur.execute("SELECT status, task_type, input_text FROM tasks WHERE id=%s", (task_id_int,))
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
                status, task_type, input_text = "QUEUED", task_type, input_text
            else:
                status, task_type, input_text = row[0], row[1], row[2]

            if status in ("COMPLETED", "FAILED"):
                print(f"Skipping already finished task {task_id_str} (status={status})")
                redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
                cur.close()
                conn.close()
                return

            cur.execute("UPDATE tasks SET status=%s, started_at=NOW() WHERE id=%s", ("PROCESSING", task_id_int))
            conn.commit()
            publish_event("TASK_PROCESSING", task_id_str)

            prompt = f"Task: {task_type}\n\nInput:\n{input_text}\n\nReturn only the result."
            response = client.models.generate_content(model=MODEL_NAME, contents=prompt)
            result = response.text

            cur.execute(
                "UPDATE tasks SET status=%s, output_text=%s, completed_at=NOW() WHERE id=%s",
                ("COMPLETED", result, task_id_int),
            )
            conn.commit()
            publish_event("TASK_COMPLETED", task_id_str)
            redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
            print(f"Completed task {task_id_str}")
            cur.close()
            conn.close()
            return

        except Exception as e:
            last_error = e
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            if attempt < PROCESS_MAX_RETRIES - 1:
                delay = PROCESS_RETRY_BASE_DELAY_SEC * (2 ** attempt)
                print(f"Task {task_id_str} attempt {attempt + 1} failed: {e}. Retry in {delay}s...")
                time.sleep(delay)
            else:
                break

    # Final failure: mark FAILED in DB and ACK so we don't infinite retry
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE tasks SET status=%s, error=%s, completed_at=NOW() WHERE id=%s",
            ("FAILED", str(last_error), task_id_int),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as db_err:
        print(f"Could not mark task {task_id_str} as FAILED in DB: {db_err}")
    publish_event("TASK_FAILED", task_id_str, {"error": str(last_error)})
    try:
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
    except (redis.ConnectionError, redis.TimeoutError):
        pass
    print(f"Failed task {task_id_str} after {PROCESS_MAX_RETRIES} attempts: {last_error}")

ensure_consumer_group()
print("Worker started, waiting for tasks...")
print(f"Stream trim: every {STREAM_TRIM_MAXAGE_MINUTES} min | Processing delay: {PROCESSING_DELAY_SEC}s | Max retries: {PROCESS_MAX_RETRIES}")
last_trim = time.monotonic()
redis_backoff = 0.0

while True:
    try:
        if redis_backoff > 0:
            time.sleep(redis_backoff)
            redis_backoff = min(redis_backoff * 2, 60.0)

        if STREAM_TRIM_MAXAGE_MINUTES > 0 and (time.monotonic() - last_trim) >= 60 * STREAM_TRIM_MAXAGE_MINUTES:
            trim_stream_if_due()
            last_trim = time.monotonic()

        messages = redis_client.xreadgroup(
            groupname=GROUP_NAME,
            consumername=CONSUMER_NAME,
            streams={STREAM_NAME: ">"},
            count=1,
            block=5000,
        )
        redis_backoff = 0.0  # success

        if messages:
            for stream, entries in messages:
                for message_id, data in entries:
                    process_task(message_id, data)

        reclaimed = reclaim_stuck_tasks()
        for message_id, data in reclaimed:
            process_task(message_id, data)

    except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        if redis_backoff == 0:
            redis_backoff = REDIS_CONNECT_RETRY_DELAY
        print(f"Redis error (will retry in {redis_backoff}s): {e}")
