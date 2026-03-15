"""
Sentinel AI API — Task submission, retrieval, retry, and health endpoints.

Structured logging, Prometheus metrics, startup dependency checks,
per-client rate limiting (X-Client-ID / X-Forwarded-For aware),
and strict input validation are all included.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field
from starlette.responses import Response

from app.db import get_db_connection
from app.exceptions import DatabaseUnavailableError, RedisUnavailableError
from app.rate_limiter import check_rate_limit
from app.redis_client import redis_ping
from app.tasks import create_task, get_task, get_recent_tasks, retry_task

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
# Prometheus metrics
# ---------------------------------------------------------------------------
REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "api_request_duration_seconds",
    "API request latency",
    ["endpoint"],
)
TASK_CREATED = Counter("tasks_created_total", "Total tasks successfully created")
TASK_RETRIED = Counter("tasks_retried_total", "Total task retries triggered")

# ---------------------------------------------------------------------------
# Startup / shutdown lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Validate Redis and PostgreSQL are reachable before accepting traffic."""
    errors = []
    try:
        redis_ping()
        logger.info("Startup check: Redis OK")
    except RedisUnavailableError as e:
        errors.append(str(e))
        logger.error("Startup check: Redis FAILED — %s", e)

    try:
        conn = get_db_connection()
        conn.close()
        logger.info("Startup check: PostgreSQL OK")
    except DatabaseUnavailableError as e:
        errors.append(str(e))
        logger.error("Startup check: PostgreSQL FAILED — %s", e)

    if errors:
        logger.warning("Service starting with dependency errors: %s", errors)

    yield  # application runs here

    logger.info("Sentinel API shutting down")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
_cors_origins_raw = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
CORS_ORIGINS = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]

app = FastAPI(title="Sentinel AI API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------
INPUT_TEXT_MAX_LENGTH = int(os.getenv("INPUT_TEXT_MAX_LENGTH", "5000"))


class CreateTaskRequest(BaseModel):
    task_type: str = Field(..., min_length=1, max_length=100)
    input_text: str = Field(..., min_length=1, max_length=INPUT_TEXT_MAX_LENGTH)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_client_id(request: Request) -> str:
    """
    Determine the rate-limit key for this request.

    Priority:
      1. X-Client-ID header  (set by load tests or trusted proxies)
      2. X-Forwarded-For header (first IP, behind reverse proxy)
      3. request.client.host  (direct connection)
    """
    x_client_id = request.headers.get("X-Client-ID")
    if x_client_id:
        return x_client_id
    x_forwarded_for = request.headers.get("X-Forwarded-For")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
def health():
    """
    Unified health check.
    Returns 200 only if both Redis and PostgreSQL are reachable; 503 otherwise.
    """
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
        logger.warning("Health check failed: %s", errors)
        raise HTTPException(
            status_code=503,
            detail={"status": "unhealthy", "errors": errors},
        )

    return {"status": "ok", "redis": "up", "postgres": "up"}


@app.get("/metrics", tags=["ops"], include_in_schema=False)
def metrics():
    """Expose Prometheus metrics in text format."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/tasks", tags=["tasks"])
def create_task_api(payload: CreateTaskRequest, request: Request):
    """Submit a new AI task. Returns task_id immediately; processing is async."""
    client_id = _resolve_client_id(request)

    try:
        allowed, remaining, reset_in = check_rate_limit(client_id)
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e

    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. {remaining} requests remaining. Try again in {reset_in}s.",
        )

    try:
        task_id = create_task(payload.task_type, payload.input_text)
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e

    logger.info("Task created task_id=%s type=%s client=%s", task_id, payload.task_type, client_id)
    TASK_CREATED.inc()
    REQUEST_COUNT.labels(method="POST", endpoint="/tasks", status_code=200).inc()
    return {"task_id": str(task_id), "status": "QUEUED"}


@app.get("/tasks", tags=["tasks"])
def list_tasks(limit: int = 10):
    """Return the most recent *limit* tasks from the database."""
    try:
        result = get_recent_tasks(limit)
        REQUEST_COUNT.labels(method="GET", endpoint="/tasks", status_code=200).inc()
        return result
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: PostgreSQL down. {e}") from e


@app.get("/tasks/{task_id}", tags=["tasks"])
def get_task_api(task_id: str):
    """Retrieve current status and output of a task by its Snowflake ID."""
    try:
        task = get_task(task_id)
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: PostgreSQL down. {e}") from e
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e

    if task is None:
        REQUEST_COUNT.labels(method="GET", endpoint="/tasks/{id}", status_code=404).inc()
        raise HTTPException(status_code=404, detail="Task not found")

    REQUEST_COUNT.labels(method="GET", endpoint="/tasks/{id}", status_code=200).inc()
    return task


@app.post("/tasks/{task_id}/retry", tags=["tasks"])
def retry_task_api(task_id: str):
    """Re-queue a FAILED task for reprocessing."""
    try:
        result = retry_task(task_id)
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: PostgreSQL down. {e}") from e
    except RedisUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: Redis down. {e}") from e

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    logger.info("Task retry queued task_id=%s", task_id)
    TASK_RETRIED.inc()
    return result