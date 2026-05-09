"""Rate limiting for the API service."""

from __future__ import annotations

import logging
from typing import NamedTuple

from core import RedisUnavailableError, get_api_config

logger = logging.getLogger(__name__)

config = get_api_config()


class RateLimitResult(NamedTuple):
    """Result of a rate limit check."""

    allowed: bool
    remaining: int
    reset_in: int


def check_rate_limit(client_id: str) -> RateLimitResult:
    """
    Apply a sliding-window rate limit keyed on *client_id*.

    Args:
        client_id: Identifier for the rate-limit bucket (resolved from request headers).

    Returns:
        RateLimitResult with allowed flag, remaining requests, and reset time in seconds.

    Raises:
        RedisUnavailableError: if Redis is unreachable.
    """
    from core.redis import get_redis_client

    key = f"rl:{client_id}"
    r = get_redis_client()

    current = r.incr(key)

    if current == 1:
        r.expire(key, config.rate_limit.window_seconds)

    ttl = r.ttl(key)
    remaining = max(0, config.rate_limit.requests_per_window - current)
    allowed = current <= config.rate_limit.requests_per_window
    reset_in = ttl if ttl > 0 else config.rate_limit.window_seconds

    if not allowed:
        logger.info(
            "Rate limit exceeded client=%s current=%d limit=%d",
            client_id,
            current,
            config.rate_limit.requests_per_window,
        )

    return RateLimitResult(allowed=allowed, remaining=remaining, reset_in=reset_in)