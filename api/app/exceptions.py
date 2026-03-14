"""Custom exceptions for dependency failures (Redis/PostgreSQL)."""


class RedisUnavailableError(Exception):
    """Raised when Redis is unreachable or operation fails."""
    pass


class DatabaseUnavailableError(Exception):
    """Raised when PostgreSQL is unreachable or operation fails."""
    pass
