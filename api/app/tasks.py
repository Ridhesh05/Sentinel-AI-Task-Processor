import json
import redis
from datetime import datetime
from app.db import get_db_connection
from app.redis_client import get_redis_client, STREAM_NAME
from app.snowflake import generate_snowflake_id
from app.exceptions import RedisUnavailableError, DatabaseUnavailableError

# Redis key for task metadata before worker materializes into DB (GET returns this when row missing)
TASK_META_PREFIX = "task:meta:"
TASK_META_TTL_SEC = 3600  # 1 hour; worker will have materialized by then


def _parse_task_id(task_id: str):
    """Validate task_id is a numeric Snowflake ID string. Returns int or None if invalid."""
    if not task_id or not isinstance(task_id, str):
        return None
    try:
        n = int(task_id)
        if n < 0:
            return None
        return n
    except ValueError:
        return None


def create_task(task_type: str, input_text: str):
    """
    Redis-first ingestion: no PostgreSQL on create.
    Push full payload to stream; optionally cache metadata so GET /tasks/:id works before worker runs.
    Uses Snowflake ID for task_id (time-ordered, compact). Raises RedisUnavailableError if Redis is down.
    """
    task_id = str(generate_snowflake_id())
    now = datetime.utcnow().isoformat() + "Z"

    try:
        r = get_redis_client()
        r.xadd(
            STREAM_NAME,
            {
                "task_id": task_id,
                "task_type": task_type,
                "input_text": input_text,
                "created_at": now,
            },
        )
        meta = {
            "id": task_id,
            "task_type": task_type,
            "status": "QUEUED",
            "input_text": input_text,
            "output_text": None,
            "error": None,
            "queued_at": now,
            "started_at": None,
            "completed_at": None,
        }
        r.setex(
            f"{TASK_META_PREFIX}{task_id}",
            TASK_META_TTL_SEC,
            json.dumps(meta),
        )
    except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        raise RedisUnavailableError(f"Redis unavailable: {e}") from e
    return task_id

def get_task(task_id):
    """
    Return task from DB if materialized by worker; else from Redis cache (QUEUED) if present.
    Raises DatabaseUnavailableError or RedisUnavailableError when dependencies are down.
    """
    tid = _parse_task_id(task_id)
    if tid is None:
        return None

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, task_type, status, output_text, error, queued_at, started_at, completed_at
            FROM tasks
            WHERE id = %s
            """,
            (tid,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
    except DatabaseUnavailableError:
        raise
    except Exception as e:
        raise DatabaseUnavailableError(f"Database error: {e}") from e

    if row is not None:
        return {
            "id": str(row[0]),  # Snowflake id as string in API
            "task_type": row[1],
            "status": row[2],
            "output_text": row[3],
            "error": row[4],
            "queued_at": str(row[5]) if row[5] else None,
            "started_at": str(row[6]) if row[6] else None,
            "completed_at": str(row[7]) if row[7] else None,
        }

    try:
        r = get_redis_client()
        raw = r.get(f"{TASK_META_PREFIX}{task_id}")
    except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        raise RedisUnavailableError(f"Redis unavailable: {e}") from e
    if raw:
        return json.loads(raw)
    return None


def retry_task(task_id: str):
    """
    Only for tasks already in DB with status FAILED. Reset and re-push to stream (full payload).
    Raises DatabaseUnavailableError or RedisUnavailableError when dependencies are down.
    """
    tid = _parse_task_id(task_id)
    if tid is None:
        return {"error": "Task not found"}

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT status, task_type, input_text FROM tasks WHERE id=%s", (tid,))
        row = cur.fetchone()
    except DatabaseUnavailableError:
        raise
    except Exception as e:
        raise DatabaseUnavailableError(f"Database error: {e}") from e

    if not row:
        cur.close()
        conn.close()
        return {"error": "Task not found"}

    status, task_type, input_text = row[0], row[1], row[2]
    if status != "FAILED":
        cur.close()
        conn.close()
        return {"error": f"Task not retryable (status={status})"}

    now = datetime.utcnow()
    try:
        cur.execute(
            """
            UPDATE tasks
            SET status=%s,
                error=NULL,
                output_text=NULL,
                started_at=NULL,
                completed_at=NULL,
                queued_at=%s,
                updated_at=%s
            WHERE id=%s
            """,
            ("QUEUED", now, now, tid),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        if cur:
            cur.close()
        if conn:
            conn.close()
        raise DatabaseUnavailableError(f"Database error: {e}") from e

    now_iso = datetime.utcnow().isoformat() + "Z"
    task_id_str = str(tid)
    try:
        r = get_redis_client()
        r.xadd(
            STREAM_NAME,
            {
                "task_id": task_id_str,
                "task_type": task_type,
                "input_text": input_text,
                "created_at": now_iso,
            },
        )
    except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        raise RedisUnavailableError(f"Redis unavailable: {e}") from e
    return {"task_id": task_id_str, "status": "QUEUED"}


def get_recent_tasks(limit: int = 10):
    """Raises DatabaseUnavailableError if PostgreSQL is down."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, task_type, status, created_at, queued_at, started_at, completed_at
        FROM tasks
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    tasks = []
    for r in rows:
        tasks.append({
            "id": str(r[0]),  # Snowflake id as string in API
            "task_type": r[1],
            "status": r[2],
            "created_at": str(r[3]) if r[3] else None,
            "queued_at": str(r[4]) if r[4] else None,
            "started_at": str(r[5]) if r[5] else None,
            "completed_at": str(r[6]) if r[6] else None,
        })
    return {"tasks": tasks}
