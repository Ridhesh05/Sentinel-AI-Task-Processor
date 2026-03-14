import os
import redis
from app.redis_client import get_redis_client
from app.exceptions import RedisUnavailableError

RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))   # requests
WINDOW_SEC = int(os.getenv("RATE_WINDOW", "60"))  # seconds

def check_rate_limit(client_id: str):
    """
    Returns: (allowed: bool, remaining: int, reset_in: int).
    Raises RedisUnavailableError if Redis is down.
    """
    try:
        r = get_redis_client()
        key = f"rl:{client_id}"
        current = r.incr(key)
        if current == 1:
            r.expire(key, WINDOW_SEC)
        ttl = r.ttl(key)
        remaining = max(0, RATE_LIMIT - current)
        allowed = current <= RATE_LIMIT
        reset_in = ttl if ttl > 0 else WINDOW_SEC
        return allowed, remaining, reset_in
    except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        raise RedisUnavailableError(f"Redis unavailable: {e}") from e
