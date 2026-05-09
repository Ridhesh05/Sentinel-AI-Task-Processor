"""Custom exceptions for Sentinel AI Task Processor."""


class SentinelError(Exception):
    """Base exception for all Sentinel application errors."""

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class RedisUnavailableError(SentinelError):
    """Raised when Redis is unreachable or an operation fails."""


class DatabaseUnavailableError(SentinelError):
    """Raised when PostgreSQL is unreachable or an operation fails."""


class RateLimitExceededError(SentinelError):
    """Raised when a client exceeds the configured rate limit."""

    def __init__(
        self,
        message: str,
        *,
        remaining: int = 0,
        reset_in: int = 0,
        details: dict | None = None,
    ) -> None:
        super().__init__(message, details=details)
        self.remaining = remaining
        self.reset_in = reset_in


class TaskNotFoundError(SentinelError):
    """Raised when a requested task does not exist."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task not found: {task_id}", details={"task_id": task_id})
        self.task_id = task_id


class TaskNotRetryableError(SentinelError):
    """Raised when attempting to retry a task that is not in FAILED state."""

    def __init__(self, task_id: str, current_status: str) -> None:
        super().__init__(
            f"Task {task_id} cannot be retried (status={current_status})",
            details={"task_id": task_id, "status": current_status},
        )
        self.task_id = task_id
        self.current_status = current_status


class ConfigurationError(SentinelError):
    """Raised when required configuration is missing or invalid."""


class InvalidTaskPayloadError(SentinelError):
    """Raised when task payload fails validation."""


class GeminiAPIError(SentinelError):
    """Raised when Gemini API call fails after all retries."""


class StreamProcessingError(SentinelError):
    """Raised when processing a stream message fails."""


class WorkerStartupError(SentinelError):
    """Raised when worker fails to start (missing deps, config, etc.)."""