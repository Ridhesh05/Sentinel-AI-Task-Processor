"""Redis-backed sliding-window rate limiter."""

import logging
import os

import redis

from app.exceptions import RedisUnavailableError
from app.redis_client import get_redis_client

logger = logging.getLogger(__name__)

RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))   # max requests
WINDOW_SEC = int(os.getenv("RATE_WINDOW", "60"))  # window in seconds


def check_rate_limit(client_id: str) -> tuple[bool, int, int]:
    """
    Apply a sliding-window rate limit keyed on *client_id*.

    Returns:
        (allowed, remaining, reset_in_seconds)

    Raises:
        RedisUnavailableError: if Redis is unreachable.
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
        if not allowed:
            logger.info(
                "Rate limit exceeded for client=%s current=%d limit=%d",
                client_id,
                current,
                RATE_LIMIT,
            )
        return allowed, remaining, reset_in
    except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        raise RedisUnavailableError(f"Redis unavailable during rate-limit check: {e}") from e
