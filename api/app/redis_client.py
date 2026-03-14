import os
import redis
from app.exceptions import RedisUnavailableError

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))

STREAM_NAME = "ai_task_queue"

# Retry config
REDIS_RETRIES = int(os.getenv("REDIS_RETRIES", "3"))
REDIS_RETRY_DELAY_SEC = float(os.getenv("REDIS_RETRY_DELAY_SEC", "0.3"))


def get_redis_client():
    """Return a Redis client. Does not ping; callers should catch redis.ConnectionError / TimeoutError."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
        socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
    )


def redis_ping():
    """Ping Redis. Raises RedisUnavailableError if down."""
    import time
    last_err = None
    for attempt in range(REDIS_RETRIES):
        try:
            r = get_redis_client()
            r.ping()
            return
        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
            last_err = e
            if attempt < REDIS_RETRIES - 1:
                time.sleep(REDIS_RETRY_DELAY_SEC)
            continue
    raise RedisUnavailableError(f"Redis unreachable after {REDIS_RETRIES} attempts: {last_err}")
