"""
Sentinel Monitor Service — Server-Sent Events (SSE) streaming.

Subscribes to Redis Pub/Sub and fans out task events to browser clients
via Server-Sent Events (SSE). SSE is simpler, proxy-friendly, and
auto-reconnects on the client side via the native EventSource API.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response, StreamingResponse

from core import (
    setup_logging,
    get_logger,
    get_monitor_config,
    MonitorConfig,
)
from core.metrics import SSE_CONNECTIONS, SSE_EVENTS_SENT
from core.redis import AsyncRedisClient, get_async_redis_client

setup_logging(level="INFO")
logger = get_logger(__name__)

config = get_monitor_config()

_subscribers: set[asyncio.Queue] = set()
_lock = asyncio.Lock()


async def _redis_listener() -> None:
    """
    Background task: subscribe to Redis Pub/Sub and fan out messages to all
    active SSE connections via their individual asyncio.Queue instances.

    Implements exponential backoff on reconnection.
    """
    backoff = 1.0

    while True:
        try:
            async_client = get_async_redis_client(config.redis)
            pubsub = await async_client.subscribe(config.redis.pubsub_channel)
            logger.info(
                "Redis Pub/Sub listener connected channel=%s",
                config.redis.pubsub_channel,
            )
            backoff = 1.0

            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue

                data = message.get("data", "")
                logger.debug("Pub/Sub event received: %s", data)

                async with _lock:
                    for q in list(_subscribers):
                        try:
                            q.put_nowait(data)
                        except asyncio.QueueFull:
                            logger.warning("SSE subscriber queue full — dropping event")

        except Exception as e:
            logger.error(
                "Redis listener error: %s — reconnecting in %ss",
                e,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background Redis listener, stop it on shutdown."""
    task = asyncio.create_task(_redis_listener())
    logger.info("Sentinel Monitor Service started port=%d", config.port)
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("Sentinel Monitor Service stopped")


app = FastAPI(
    title=config.app_name,
    version="1.0.0",
    description="Real-time task event streaming via Server-Sent Events (SSE)",
    lifespan=lifespan,
    docs_url="/docs",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get(
    "/health",
    tags=["operations"],
    summary="Health check",
)
def health():
    """Return service health status."""
    return {"status": "ok"}


@app.get(
    "/health/live",
    tags=["operations"],
    summary="Liveness probe",
)
def liveness():
    """Always returns 200 if the process is alive."""
    return {"status": "alive"}


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
    "/events",
    tags=["monitoring"],
    summary="Server-Sent Events stream",
    description=(
        "Streams real-time task events (TASK_PROCESSING, TASK_COMPLETED, TASK_FAILED) "
        "to connected clients. The browser's native EventSource API handles "
        "automatic reconnection on connection loss."
    ),
)
async def sse_events(request: Request):
    """
    Server-Sent Events stream.

    Each connected client receives a dedicated asyncio.Queue.
    The background Redis listener fans events to all active queues.

    SSE wire format: data: <json-payload>\\n\\n
    The browser EventSource API handles reconnection automatically.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=config.sse_queue_maxsize)

    async with _lock:
        _subscribers.add(queue)

    SSE_CONNECTIONS.inc()
    client_host = request.client.host if request.client else "unknown"
    logger.info(
        "SSE client connected client=%s total=%d",
        client_host,
        len(_subscribers),
    )

    async def event_generator():
        # Send an initial comment to confirm the stream is open
        yield ": connected\n\n"

        try:
            while True:
                if await request.is_disconnected():
                    logger.info("SSE client disconnected client=%s", client_host)
                    break

                try:
                    data = await asyncio.wait_for(
                        queue.get(),
                        timeout=config.sse_keepalive_sec,
                    )

                    # Validate JSON before forwarding
                    try:
                        json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("Dropping non-JSON event: %r", data)
                        continue

                    yield f"data: {data}\n\n"
                    SSE_EVENTS_SENT.inc()

                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"

        except asyncio.CancelledError:
            logger.info("SSE generator cancelled for client=%s", client_host)
        finally:
            async with _lock:
                _subscribers.discard(queue)
            logger.info(
                "SSE client removed client=%s remaining=%d",
                client_host,
                len(_subscribers),
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked",
        },
    )


@app.get(
    "/",
    tags=["operations"],
    summary="Root endpoint",
    include_in_schema=False,
)
def root():
    """Redirect to API documentation."""
    return {"message": "Sentinel Monitor Service", "docs": "/docs"}