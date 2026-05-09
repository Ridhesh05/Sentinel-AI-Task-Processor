"""
Sentinel AI API — FastAPI application entry point.

Responsibilities:
- Task submission (POST /tasks)
- Task retrieval (GET /tasks, GET /tasks/{id})
- Task retry (POST /tasks/{id}/retry)
- Health check (GET /health)
- Prometheus metrics (GET /metrics)
- Per-client rate limiting
- Per-request structured logging + metrics
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field
from typing import Optional

from core import (
    setup_logging,
    get_logger,
    get_api_config,
    DatabaseUnavailableError,
    RedisUnavailableError,
    RateLimitExceededError,
    TaskNotFoundError,
    TaskNotRetryableError,
)
from core.metrics import REQUEST_COUNT, REQUEST_LATENCY, TASK_CREATED, TASK_RETRIED
from services.api.app import tasks, rate_limiter, deps

__version__ = "1.0.0"

config = get_api_config()
setup_logging(level=config.log_level, json_output=config.log_format == "json")
logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Request / response models
# ─────────────────────────────────────────────────────────────────────────────

class CreateTaskRequest(BaseModel):
    """Payload for creating a new AI task."""

    task_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Type of AI task (e.g., summarize, classify, tag)",
    )
    input_text: str = Field(
        ...,
        min_length=1,
        max_length=config.input_text_max_length,
        description="Input text for the AI task",
    )
    session_id: Optional[str] = Field(
        None,
        description="Browser session ID for conversation memory",
    )

    model_config = {"json_schema_extra": {"example": {"task_type": "summarize", "input_text": "Long text to summarize..."}}}


class TaskResponse(BaseModel):
    """Response returned after creating a task."""

    task_id: str = Field(..., description="Snowflake ID of the created task")
    status: str = Field(..., description="Initial task status (always 'QUEUED')")


class ErrorDetail(BaseModel):
    """Standard error response body."""

    detail: str


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Validate Redis and PostgreSQL are reachable before accepting traffic."""
    errors: list[str] = []

    try:
        deps.redis_ping()
        logger.info("Startup check: Redis OK")
    except RedisUnavailableError as e:
        errors.append(str(e))
        logger.error("Startup check: Redis FAILED — %s", e)

    try:
        deps.db_health_check()
        logger.info("Startup check: PostgreSQL OK")
    except DatabaseUnavailableError as e:
        errors.append(str(e))
        logger.error("Startup check: PostgreSQL FAILED — %s", e)

    if errors:
        logger.warning("Service starting with dependency errors: %s", errors)
    else:
        logger.info("Sentinel API started successfully port=%d", config.port)

    yield

    logger.info("Sentinel API shutting down")


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title=config.app_name,
    version="1.0.0",
    description="Distributed AI background task processing API with Redis Streams, PostgreSQL, and real-time monitoring.",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Record request count and latency for Prometheus."""
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start

    endpoint = request.url.path
    method = request.method
    status = str(response.status_code)

    REQUEST_COUNT.labels(method=method, endpoint=endpoint, status_code=status).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(elapsed)

    return response


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/health",
    tags=["operations"],
    summary="Health check",
    responses={
        200: {"description": "All dependencies healthy"},
        503: {"description": "One or more dependencies unhealthy"},
    },
)
def health():
    """Unified health check. Returns 200 only if both Redis and PostgreSQL are reachable."""
    errors: list[str] = []

    try:
        deps.redis_ping()
    except RedisUnavailableError as e:
        errors.append(f"redis: {e}")

    try:
        deps.db_health_check()
    except DatabaseUnavailableError as e:
        errors.append(f"postgres: {e}")

    if errors:
        logger.warning("Health check failed: %s", errors)
        raise HTTPException(
            status_code=503,
            detail={"status": "unhealthy", "errors": errors},
        )

    return {"status": "ok", "redis": "up", "postgres": "up"}


@app.get(
    "/health/live",
    tags=["operations"],
    summary="Liveness probe",
)
def liveness():
    """Kubernetes liveness probe — always returns 200 if the process is alive."""
    return {"status": "alive"}


@app.get(
    "/health/ready",
    tags=["operations"],
    summary="Readiness probe",
    responses={
        200: {"description": "Service is ready to accept traffic"},
        503: {"description": "Service is not ready"},
    },
)
def readiness():
    """
    Kubernetes readiness probe — returns 200 only if both Redis and Postgres are reachable.
    Use this instead of /health for traffic routing decisions.
    """
    return health()


@app.get(
    "/metrics",
    tags=["operations"],
    summary="Prometheus metrics",
    include_in_schema=False,
)
def metrics():
    """Expose Prometheus metrics in text exposition format."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get(
    "/info",
    tags=["operations"],
    summary="Service information",
)
def info():
    """Return service metadata (version, config summary)."""
    return {
        "service": config.app_name,
        "version": __version__,
        "rate_limit": config.rate_limit.requests_per_window,
        "rate_window_sec": config.rate_limit.window_seconds,
        "input_max_length": config.input_text_max_length,
    }


@app.post(
    "/tasks",
    tags=["tasks"],
    response_model=TaskResponse,
    status_code=201,
    summary="Create a new AI task",
    responses={
        201: {"description": "Task created and queued successfully"},
        400: {"description": "Invalid request payload"},
        429: {"description": "Rate limit exceeded"},
        503: {"description": "Service unavailable (Redis down)"},
    },
)
def create_task_api(payload: CreateTaskRequest, request: Request):
    """
    Submit a new AI task for background processing.

    The task is immediately queued in Redis Streams. The returned `task_id`
    can be used to poll for completion via GET /tasks/{id}.

    Processing is asynchronous — this endpoint returns as soon as the task
    is queued, not when it completes.
    """
    client_id = deps.resolve_client_id(request)

    try:
        allowed, remaining, reset_in = rate_limiter.check_rate_limit(client_id)
    except RedisUnavailableError as e:
        logger.error("Rate limit check failed (Redis down): %s", e)
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: Redis down. {e}",
        ) from e

    if not allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "message": "Rate limit exceeded",
                "remaining": remaining,
                "reset_in_sec": reset_in,
            },
            headers={"Retry-After": str(reset_in)},
        )

    try:
        task_id = tasks.create_task(payload.task_type, payload.input_text, payload.session_id)
    except RedisUnavailableError as e:
        logger.error("Task creation failed (Redis down): %s", e)
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: Redis down. {e}",
        ) from e

    logger.info("Task created task_id=%s type=%s client=%s", task_id, payload.task_type, client_id)
    TASK_CREATED.inc()
    return TaskResponse(task_id=str(task_id), status="QUEUED")


@app.get(
    "/tasks",
    tags=["tasks"],
    summary="List recent tasks",
    responses={
        200: {"description": "List of recent tasks"},
        503: {"description": "PostgreSQL unavailable"},
    },
)
def list_tasks(limit: int = 10):
    """
    Return the most recent `limit` tasks from the database, ordered by created_at DESC.

    Args:
        limit: Maximum number of tasks to return (default: 10, max: 100).
    """
    limit = max(1, min(limit, 100))
    try:
        return tasks.get_recent_tasks(limit)
    except DatabaseUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: PostgreSQL down. {e}") from e


@app.get(
    "/tasks/{task_id}",
    tags=["tasks"],
    summary="Get task details",
    responses={
        200: {"description": "Task details"},
        404: {"description": "Task not found"},
        503: {"description": "Database or Redis unavailable"},
    },
)
def get_task_api(task_id: str):
    """
    Retrieve current status and output of a task by its Snowflake ID.

    The task is first fetched from PostgreSQL (materialized rows). If not found,
    Redis cache is checked (for tasks still in QUEUED state, not yet picked up by the worker).
    """
    try:
        task = tasks.get_task(task_id)
    except (DatabaseUnavailableError, RedisUnavailableError) as e:
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: {e}",
        ) from e

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return task


@app.post(
    "/tasks/{task_id}/retry",
    tags=["tasks"],
    summary="Retry a failed task",
    responses={
        200: {"description": "Task re-queued successfully"},
        400: {"description": "Task is not in a retryable state"},
        404: {"description": "Task not found"},
        503: {"description": "Redis or PostgreSQL unavailable"},
    },
)
def retry_task_api(task_id: str):
    """
    Re-queue a FAILED task for reprocessing.

    Only tasks with status=FAILED can be retried. The task status is reset
    to QUEUED and a new entry is pushed to the Redis Stream.
    """
    try:
        result = tasks.retry_task(task_id)
    except (DatabaseUnavailableError, RedisUnavailableError) as e:
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: {e}",
        ) from e

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    logger.info("Task retry queued task_id=%s", task_id)
    TASK_RETRIED.inc()
    return result


@app.get(
    "/",
    tags=["operations"],
    summary="Root endpoint",
    include_in_schema=False,
)
def root():
    """Redirect to API documentation."""
    return {"message": "Sentinel AI Task Processor API", "docs": "/docs"}