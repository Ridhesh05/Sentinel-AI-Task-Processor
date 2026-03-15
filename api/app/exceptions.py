"""Custom exceptions for dependency failures (Redis / PostgreSQL)."""


class RedisUnavailableError(Exception):
    """Raised when Redis is unreachable or an operation fails."""


class DatabaseUnavailableError(Exception):
    """Raised when PostgreSQL is unreachable or an operation fails."""
