import asyncio
import redis
import os
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Sentinel Monitor Service")

# allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in prod, restrict
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
CHANNEL = "task_events"

@app.get("/health")
def health():
    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    pubsub = redis_client.pubsub()
    pubsub.subscribe(CHANNEL)

    try:
        while True:
            msg = pubsub.get_message(ignore_subscribe_messages=True)
            if msg and msg["type"] == "message":
                await ws.send_text(msg["data"])
            await asyncio.sleep(0.1)
    finally:
        pubsub.close()
