"""Dependency injection helpers for the API service."""

from __future__ import annotations

from fastapi import Request

__all__ = ["resolve_client_id", "redis_ping", "db_health_check"]


def resolve_client_id(request: Request) -> str:
    """
    Determine the rate-limit key for the incoming request.

    Priority:
      1. X-Client-ID header  (set by load tests or trusted proxies)
      2. X-Forwarded-For header (first IP, behind reverse proxy)
      3. request.client.host  (direct connection)

    Returns:
        Client identifier string for rate limiting.
    """
    x_client_id = request.headers.get("X-Client-ID")
    if x_client_id:
        return x_client_id

    x_forwarded_for = request.headers.get("X-Forwarded-For")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()

    return request.client.host if request.client else "unknown"


def redis_ping() -> None:
    """Ping Redis with retry. Raises RedisUnavailableError on failure."""
    from core.redis import get_redis_client
    get_redis_client().ping()


def db_health_check() -> None:
    """Check PostgreSQL connectivity. Raises DatabaseUnavailableError on failure."""
    from core.db import get_db_client
    if not get_db_client().health_check():
        from core.exceptions import DatabaseUnavailableError
        raise DatabaseUnavailableError("PostgreSQL health check failed")