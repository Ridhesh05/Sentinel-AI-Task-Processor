"""
Task CRUD operations for the API service.

All functions are Redis-first: writes go to Redis Streams (fast path),
reads fall back to PostgreSQL then Redis cache.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from core import (
    generate_snowflake_id,
    get_api_config,
    RedisUnavailableError,
    DatabaseUnavailableError,
)

logger = logging.getLogger(__name__)

config = get_api_config()

STREAM_NAME = config.redis.stream_name
TASK_META_PREFIX = "task:meta:"
TASK_META_TTL_SEC = 3600


def _parse_task_id(task_id: str) -> int | None:
    """
    Validate task_id is a non-negative Snowflake ID string.

    Args:
        task_id: String task ID from request.

    Returns:
        Integer task ID or None if invalid.
    """
    if not task_id or not isinstance(task_id, str):
        return None
    try:
        n = int(task_id)
        return n if n >= 0 else None
    except ValueError:
        return None


def create_task(task_type: str, input_text: str) -> int:
    """
    Redis-first ingestion: push payload to stream and cache metadata.

    No PostgreSQL write on the hot path — the worker materialises the row.
    The returned task_id is a Snowflake integer.

    Args:
        task_type: Type of AI task (e.g., summarize, classify).
        input_text: Input text for the task.

    Returns:
        Snowflake task_id as integer.

    Raises:
        RedisUnavailableError: if Redis is down.
    """
    task_id = generate_snowflake_id()
    now = datetime.now(timezone.utc).isoformat()

    try:
        from core.redis import get_redis_client
        r = get_redis_client()

        r.xadd(
            STREAM_NAME,
            {
                "task_id": str(task_id),
                "task_type": task_type,
                "input_text": input_text,
                "created_at": now,
            },
        )

        meta = {
            "id": str(task_id),
            "task_type": task_type,
            "status": "QUEUED",
            "input_text": input_text,
            "output_text": None,
            "error": None,
            "queued_at": now,
            "started_at": None,
            "completed_at": None,
        }
        r.setex(f"{TASK_META_PREFIX}{task_id}", TASK_META_TTL_SEC, json.dumps(meta))
    except RedisUnavailableError:
        raise
    except Exception as e:
        raise RedisUnavailableError(f"Redis operation failed: {e}") from e

    logger.debug("Task queued task_id=%s type=%s", task_id, task_type)
    return task_id


def get_task(task_id: str) -> dict | None:
    """
    Return task from PostgreSQL (materialised rows) or Redis cache (QUEUED state).

    Args:
        task_id: Snowflake task ID string.

    Returns:
        Task dict or None if not found.

    Raises:
        DatabaseUnavailableError: if PostgreSQL is down.
        RedisUnavailableError: if Redis is down (and cache lookup needed).
    """
    tid = _parse_task_id(task_id)
    if tid is None:
        return None

    # Try PostgreSQL first (materialised rows)
    try:
        from core.db import get_db_client
        db = get_db_client()
        row = db.execute(
            """
            SELECT id, task_type, status, output_text, error,
                   queued_at, started_at, completed_at
            FROM tasks
            WHERE id = %s
            """,
            (tid,),
            fetch_one=True,
        )
        if row:
            return {
                "id": str(row[0]),
                "task_type": row[1],
                "status": row[2],
                "output_text": row[3],
                "error": row[4],
                "queued_at": str(row[5]) if row[5] else None,
                "started_at": str(row[6]) if row[6] else None,
                "completed_at": str(row[7]) if row[7] else None,
            }
    except DatabaseUnavailableError:
        raise
    except Exception as e:
        raise DatabaseUnavailableError(f"Database error: {e}") from e

    # DB miss — fall back to Redis cache (task still queued, not yet picked up by worker)
    try:
        from core.redis import get_redis_client
        r = get_redis_client()
        raw = r.get(f"{TASK_META_PREFIX}{task_id}")
    except RedisUnavailableError:
        raise

    if raw:
        logger.debug("Task task_id=%s served from Redis metadata cache", task_id)
        return json.loads(raw)

    return None


def retry_task(task_id: str) -> dict:
    """
    Re-queue a FAILED task for reprocessing.

    Resets status to QUEUED in PostgreSQL and pushes a new entry to the Redis Stream.

    Args:
        task_id: Snowflake task ID string.

    Returns:
        {"task_id": str, "status": "QUEUED"} on success, or {"error": str} on failure.

    Raises:
        DatabaseUnavailableError: if PostgreSQL is down.
        RedisUnavailableError: if Redis is down.
    """
    tid = _parse_task_id(task_id)
    if tid is None:
        return {"error": "Task not found"}

    # Fetch current task state from DB
    try:
        from core.db import get_db_client
        db = get_db_client()
        row = db.execute(
            "SELECT status, task_type, input_text FROM tasks WHERE id=%s",
            (tid,),
            fetch_one=True,
        )
    except DatabaseUnavailableError:
        raise
    except Exception as e:
        raise DatabaseUnavailableError(f"Database error: {e}") from e

    if not row:
        return {"error": "Task not found"}

    status, task_type, input_text = row

    if status != "FAILED":
        return {"error": f"Task not retryable (status={status})"}

    now = datetime.now(timezone.utc)

    # Update DB — reset status and timestamps
    try:
        with db.transaction() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET status=%s, error=NULL, output_text=NULL,
                    started_at=NULL, completed_at=NULL,
                    queued_at=%s, updated_at=%s
                WHERE id=%s
                """,
                ("QUEUED", now, now, tid),
            )
    except Exception as e:
        raise DatabaseUnavailableError(f"Database error during retry: {e}") from e

    # Push to Redis Stream
    now_iso = now.isoformat()
    task_id_str = str(tid)
    try:
        from core.redis import get_redis_client
        r = get_redis_client()
        r.xadd(
            STREAM_NAME,
            {
                "task_id": task_id_str,
                "task_type": task_type,
                "input_text": input_text or "",
                "created_at": now_iso,
            },
        )
    except RedisUnavailableError:
        raise

    logger.info("Task retry queued task_id=%s type=%s", task_id_str, task_type)
    return {"task_id": task_id_str, "status": "QUEUED"}


def get_recent_tasks(limit: int = 10) -> dict:
    """
    Return the most recent tasks ordered by created_at DESC.

    Args:
        limit: Maximum number of tasks to return.

    Returns:
        {"tasks": [...]} dict.
    """
    from core.db import get_db_client
    db = get_db_client()
    rows = db.execute(
        """
        SELECT id, task_type, status, created_at, queued_at, started_at, completed_at
        FROM tasks
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
        fetch_all=True,
    ) or []

    tasks = [
        {
            "id": str(r[0]),
            "task_type": r[1],
            "status": r[2],
            "created_at": str(r[3]) if r[3] else None,
            "queued_at": str(r[4]) if r[4] else None,
            "started_at": str(r[5]) if r[5] else None,
            "completed_at": str(r[6]) if r[6] else None,
        }
        for r in rows
    ]
    return {"tasks": tasks}