"""Core library package for Sentinel AI Task Processor."""

from core.config import (
    get_api_config,
    get_worker_config,
    get_monitor_config,
    APIConfig,
    WorkerConfig,
    MonitorConfig,
    DatabaseSettings,
    RedisSettings,
)
from core.db import DatabaseClient, get_db_client
from core.exceptions import (
    SentinelError,
    RedisUnavailableError,
    DatabaseUnavailableError,
    RateLimitExceededError,
    TaskNotFoundError,
    TaskNotRetryableError,
    ConfigurationError,
    InvalidTaskPayloadError,
    GeminiAPIError,
    StreamProcessingError,
    WorkerStartupError,
)
from core.logging import setup_logging, get_logger, StructuredLogger
from core.redis import RedisClient, AsyncRedisClient, get_redis_client, get_async_redis_client
from core.snowflake import generate_snowflake_id, SnowflakeGenerator

__version__ = "1.0.0"

__all__ = [
    # Config
    "get_api_config",
    "get_worker_config",
    "get_monitor_config",
    "APIConfig",
    "WorkerConfig",
    "MonitorConfig",
    "DatabaseSettings",
    "RedisSettings",
    # DB
    "DatabaseClient",
    "get_db_client",
    # Exceptions
    "SentinelError",
    "RedisUnavailableError",
    "DatabaseUnavailableError",
    "RateLimitExceededError",
    "TaskNotFoundError",
    "TaskNotRetryableError",
    "ConfigurationError",
    "InvalidTaskPayloadError",
    "GeminiAPIError",
    "StreamProcessingError",
    "WorkerStartupError",
    # Logging
    "setup_logging",
    "get_logger",
    "StructuredLogger",
    # Redis
    "RedisClient",
    "AsyncRedisClient",
    "get_redis_client",
    "get_async_redis_client",
    # Snowflake
    "generate_snowflake_id",
    "SnowflakeGenerator",
]