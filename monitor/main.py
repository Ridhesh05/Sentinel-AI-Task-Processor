"""
Sentinel Monitor Service — Server-Sent Events (SSE) streaming.

Replaces WebSocket with SSE for real-time task event delivery.
SSE is simpler, proxy-friendly, and auto-reconnects on the client side.

Architecture:
  Redis Pub/Sub → background fan-out task → per-connection asyncio.Queue → SSE stream
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response, StreamingResponse

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
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PUBSUB_CHANNEL = "task_events"

_cors_origins_raw = os.getenv("CORS_ORIGINS", "*")
CORS_ORIGINS = (
    ["*"]
    if _cors_origins_raw.strip() == "*"
    else [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]
)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
SSE_CONNECTIONS = Counter(
    "monitor_sse_connections_total", "Total SSE connections established"
)
SSE_EVENTS_SENT = Counter(
    "monitor_sse_events_sent_total", "Total SSE events delivered to clients"
)

# ---------------------------------------------------------------------------
# Fan-out registry: set of queues, one per active SSE connection
# ---------------------------------------------------------------------------
_subscribers: set[asyncio.Queue] = set()


async def _redis_listener():
    """
    Background task: subscribe to Redis Pub/Sub and fan out messages to all active
    SSE connections via their individual asyncio.Queue instances.
    Reconnects automatically on Redis failure.
    """
    backoff = 1.0
    while True:
        try:
            client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
            )
            pubsub = client.pubsub()
            await pubsub.subscribe(PUBSUB_CHANNEL)
            logger.info("Redis Pub/Sub listener connected — channel=%s", PUBSUB_CHANNEL)
            backoff = 1.0  # reset on success

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                data = message["data"]
                logger.debug("Pub/Sub event received: %s", data)
                for q in list(_subscribers):
                    try:
                        q.put_nowait(data)
                    except asyncio.QueueFull:
                        logger.warning("SSE subscriber queue full — dropping event")

        except Exception as e:
            logger.error("Redis listener error: %s — reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


# ---------------------------------------------------------------------------
# Lifespan: start background listener
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_redis_listener())
    logger.info("Sentinel Monitor Service started")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("Sentinel Monitor Service stopped")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Sentinel Monitor Service", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok"}


@app.get("/metrics", tags=["ops"], include_in_schema=False)
def metrics():
    """Expose Prometheus metrics."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/events", tags=["monitoring"])
async def sse_events(request: Request):
    """
    Server-Sent Events stream.

    Each connected client receives a dedicated asyncio.Queue.
    The background Redis listener fans events to all active queues.

    SSE wire format:
        data: <json-payload>\\n\\n

    The browser EventSource API handles reconnection automatically.
    Proxy/Nginx: set X-Accel-Buffering: no to disable buffering.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=128)
    _subscribers.add(queue)
    SSE_CONNECTIONS.inc()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected client=%s total=%d", client, len(_subscribers))

    async def event_generator():
        # Send an initial comment to confirm the stream is open
        yield ": connected\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    logger.info("SSE client disconnected client=%s", client)
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15.0)
                    # Validate it's JSON before forwarding
                    try:
                        json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("Dropping non-JSON event: %r", data)
                        continue
                    yield f"data: {data}\n\n"
                    SSE_EVENTS_SENT.inc()
                except asyncio.TimeoutError:
                    # Send a keep-alive comment every 15 s to prevent proxy timeouts
                    yield ": keep-alive\n\n"
        except asyncio.CancelledError:
            logger.info("SSE generator cancelled for client=%s", client)
        finally:
            _subscribers.discard(queue)
            logger.info("SSE client removed client=%s remaining=%d", client, len(_subscribers))

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",       # disable Nginx proxy buffering
            "Transfer-Encoding": "chunked",
        },
    )
