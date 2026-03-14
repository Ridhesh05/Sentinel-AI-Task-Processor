# # check api is running 
# from fastapi import FastAPI
# app = FastAPI(title="Sentinel AI API")
# @app.get("/health")
# def health():
#     return {"status": "ok"}

from app.db import get_db_connection
from app.tasks import create_task, get_task
from app.redis_client import get_redis_client, redis_ping
from app.exceptions import RedisUnavailableError, DatabaseUnavailableError
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.tasks import retry_task
from fastapi import Request, HTTPException
from app.rate_limiter import check_rate_limit
from typing import Optional
from app.tasks import get_recent_tasks

app = FastAPI(title="Sentinel AI API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CreateTaskRequest(BaseModel):
    task_type: str
    input_text: str

@app.get("/health")
def health():
    """Unified health: 200 only if both Redis and PostgreSQL are up; 503 otherwise."""
    errors = []
    try:
        redis_ping()
    except RedisUnavailableError as e:
        errors.append(f"redis: {e}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
    except DatabaseUnavailableError as e:
        errors.append(f"postgres: {e}")
    if errors:
        raise HTTPException(status_code=503, detail={"status": "unhealthy", "errors": errors})
    return {"status": "ok", "redis": "up", "postgres": "up"}


@app.get("/db-check")
def db_check():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {"db": "connected", "result": result[0]}
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {e}") from e


@app.get("/redis-check")
def redis_check():
    try:
        redis_ping()
        return {"redis": "connected", "ping": True}
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}") from e
@app.post("/tasks")
def create_task_api(payload: CreateTaskRequest, request: Request):
    try:
        client_ip = request.client.host
        allowed, remaining, reset_in = check_rate_limit(client_ip)
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Try again in {reset_in} seconds."
        )
    try:
        task_id = create_task(payload.task_type, payload.input_text)
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e
    return {
        "task_id": str(task_id),
        "status": "QUEUED"
    }

@app.get("/tasks/{task_id}")
def get_task_api(task_id: str):
    try:
        task = get_task(task_id)
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: PostgreSQL down. {e}") from e
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e
    if task is None:
        return {"error": "Task not found"}
    return task


@app.post("/tasks/{task_id}/retry")
def retry_task_api(task_id: str):
    try:
        return retry_task(task_id)
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: PostgreSQL down. {e}") from e
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e


@app.get("/tasks")
def list_tasks(limit: int = 10):
    try:
        return get_recent_tasks(limit)
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: PostgreSQL down. {e}") from e