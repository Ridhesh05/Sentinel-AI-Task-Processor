import uuid
from datetime import datetime
from api.app.db import get_db_connection
from api.app.redis_client import get_redis_client, STREAM_NAME

def create_task(task_type: str, input_text: str):
    task_id = uuid.uuid4()
    now = datetime.utcnow()

    conn = get_db_connection()
    cur = conn.cursor()

    # 1. Insert task as CREATED
    cur.execute(
        """
        INSERT INTO tasks (id, task_type, status, input_text, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (str(task_id), task_type, "CREATED", input_text, now, now)
    )

    # 2. Push task_id to Redis Stream
    redis_client = get_redis_client()
    redis_client.xadd(
        STREAM_NAME,
        {"task_id": str(task_id)}
    )

    # 3. Update status to QUEUED
    cur.execute(
        """
        UPDATE tasks
        SET status = %s, updated_at = %s
        WHERE id = %s
        """,
        ("QUEUED", now, str(task_id))
    )

    conn.commit()
    cur.close()
    conn.close()

    return task_id

def get_task(task_id):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, task_type, status, output_text, error
        FROM tasks
        WHERE id = %s
        """,
        (task_id,)
    )

    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        return None

    return {
        "id": row[0],
        "task_type": row[1],
        "status": row[2],
        "output_text": row[3],
        "error": row[4],
    }
