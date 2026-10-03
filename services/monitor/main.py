"""
Sentinel Monitor Service — Server-Sent Events (SSE) streaming.

Each SSE client runs its own XREAD loop against the Redis events stream.
No Pub/Sub, no shared queues, no fan-out — just one clean loop per client:

    while True:
        msgs = await redis.xread({"events": last_id}, block=15000, count=50)
        if not msgs:
            yield ": keepalive\\n\\n"
            continue
        for eid, fields in msgs:
            yield f"id: {eid}\\ndata: {fields['data']}\\n\\n"

On reconnect the browser sends Last-Event-ID automatically. The monitor
replays everything after that ID from the stream. No missed events.
"""

from __future__ import annotations

import json

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response, StreamingResponse

from core import (
    setup_logging,
    get_logger,
    get_monitor_config,
)
from core.metrics import SSE_CONNECTIONS, SSE_EVENTS_SENT
from core.redis import get_async_redis_client

setup_logging(level="INFO")
logger = get_logger(__name__)

config = get_monitor_config()


app = FastAPI(
    title=config.app_name,
    version="1.0.0",
    description="Real-time task event streaming via Server-Sent Events (SSE)",
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
        "to connected clients. On reconnect the browser sends Last-Event-ID "
        "automatically and the server replays everything it missed."
    ),
)
async def sse_events(request: Request):
    """
    Server-Sent Events stream backed by Redis Streams.

    One XREAD loop per client.  block=15000 means "wait up to 15 seconds for
    an event."  If something arrives, send it.  If nothing arrives, send a
    keepalive comment and loop again.

    On reconnect the browser sends Last-Event-ID.  We start XREAD from that
    ID so every missed event is replayed immediately.
    """
    SSE_CONNECTIONS.inc()
    client_host = request.client.host if request.client else "unknown"

    # First connect → "$" (new events only)
    # Reconnect   → browser sends Last-Event-ID automatically
    last_id = request.headers.get("Last-Event-ID", "$")

    logger.info(
        "SSE client connected client=%s last_id=%s",
        client_host,
        last_id,
    )

    stream_name = config.redis.event_stream_name
    block_ms = config.sse_keepalive_sec * 1000  # 15s → 15000ms

    async def event_generator():
        nonlocal last_id

        yield ": connected\n\n"

        try:
            async_client = get_async_redis_client(config.redis)

            while True:
                if await request.is_disconnected():
                    break

                msgs = await async_client.xread(
                    {stream_name: last_id},
                    block=block_ms,
                    count=50,
                )

                if not msgs:
                    # 15s passed with no events — send a ping so proxies
                    # don't kill the connection for being idle.
                    yield ": keepalive\n\n"
                    continue

                for _stream, entries in msgs:
                    for eid, fields in entries:
                        last_id = eid
                        data = fields.get("data", "")

                        # Validate JSON before forwarding
                        try:
                            json.loads(data)
                        except (json.JSONDecodeError, TypeError):
                            logger.warning("Dropping non-JSON event: %r", data)
                            continue

                        yield f"id: {eid}\ndata: {data}\n\n"
                        SSE_EVENTS_SENT.inc()

        except Exception as e:
            logger.error("SSE stream error for client=%s: %s", client_host, e)
        finally:
            logger.info("SSE client disconnected client=%s", client_host)

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