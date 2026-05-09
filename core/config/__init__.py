"""
Configuration management for Sentinel AI Task Processor.

Loads configuration from environment variables with Pydantic validation.
Each service defines its own config class that inherits from BaseSettings.

Environment variable precedence:
1. Explicit argument (when using .from_env() override)
2. OS environment variables
3. .env file (loaded via python-dotenv if .env exists)
4. Defaults defined in Pydantic models
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

__all__ = [
    "DatabaseSettings",
    "RedisSettings",
    "RateLimitSettings",
    "APIConfig",
    "WorkerConfig",
    "MonitorConfig",
    "get_api_config",
    "get_worker_config",
    "get_monitor_config",
]


# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

class DatabaseSettings(BaseModel):
    """PostgreSQL connection settings."""

    host: str = Field(default="localhost", validation_alias="DB_HOST")
    port: int = Field(default=5432, validation_alias="DB_PORT")
    name: str = Field(default="sentinel_db", validation_alias="DB_NAME")
    user: str = Field(default="sentinel", validation_alias="DB_USER")
    password: str = Field(default="sentinel", validation_alias="DB_PASSWORD")
    connect_retries: int = Field(default=3, validation_alias="DB_CONNECT_RETRIES")
    connect_retry_delay: float = Field(default=0.5, validation_alias="DB_CONNECT_RETRY_DELAY_SEC")
    connect_timeout: int = Field(default=10, validation_alias="DB_CONNECT_TIMEOUT")

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


# ─────────────────────────────────────────────────────────────────────────────
# Redis
# ─────────────────────────────────────────────────────────────────────────────

class RedisSettings(BaseModel):
    """Redis connection settings."""

    host: str = Field(default="localhost", validation_alias="REDIS_HOST")
    port: int = Field(default=6379, validation_alias="REDIS_PORT")
    socket_timeout: int = Field(default=5, validation_alias="REDIS_SOCKET_TIMEOUT")
    socket_connect_timeout: int = Field(default=5, validation_alias="REDIS_SOCKET_TIMEOUT")
    connect_retries: int = Field(default=3, validation_alias="REDIS_CONNECT_RETRIES")
    connect_retry_delay: float = Field(default=0.3, validation_alias="REDIS_CONNECT_RETRY_DELAY")
    decode_responses: bool = Field(default=True)

    stream_name: str = Field(default="ai_task_queue")
    stream_consumer_group: str = Field(default="ai_workers")
    pubsub_channel: str = Field(default="task_events")


# ─────────────────────────────────────────────────────────────────────────────
# Rate Limiting
# ─────────────────────────────────────────────────────────────────────────────

class RateLimitSettings(BaseModel):
    """Rate limiting configuration."""

    requests_per_window: int = Field(default=10, validation_alias="RATE_LIMIT")
    window_seconds: int = Field(default=60, validation_alias="RATE_WINDOW")

    @field_validator("requests_per_window", "window_seconds", mode="before")
    @classmethod
    def _coerce_positive_int(cls, v: int | str) -> int:
        val = int(v)
        if val <= 0:
            raise ValueError("must be positive")
        return val


# ─────────────────────────────────────────────────────────────────────────────
# API Service
# ─────────────────────────────────────────────────────────────────────────────

class APIConfig(BaseSettings):
    """Configuration for the API service."""

    app_name: str = "Sentinel AI API"
    host: str = "0.0.0.0"
    port: int = 8000

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)

    input_text_max_length: int = Field(default=5000, validation_alias="INPUT_TEXT_MAX_LENGTH")
    cors_origins: list[str] = Field(
        default=["http://localhost:3000", "http://127.0.0.1:3000"],
        validation_alias="CORS_ORIGINS",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_format: Literal["text", "json"] = Field(default="text")

    snowflake_node_id: int = Field(default=0, validation_alias="SNOWFLAKE_NODE_ID")

    model_config = {"env_prefix": "", "env_nested_delimiter": "__"}

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _parse_cors_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [o.strip() for o in v.split(",") if o.strip()]
        return v


# ─────────────────────────────────────────────────────────────────────────────
# Worker Service
# ─────────────────────────────────────────────────────────────────────────────

class WorkerConfig(BaseSettings):
    """Configuration for the Worker service."""

    app_name: str = "Sentinel AI Worker"
    host: str = "0.0.0.0"
    metrics_port: int = 9100

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)

    worker_name: str = Field(default="worker-1", validation_alias="WORKER_NAME")
    snowflake_node_id: int = Field(default=1, validation_alias="SNOWFLAKE_NODE_ID")

    gemini_api_key: str = Field(validation_alias="GEMINI_API_KEY")
    gemini_model: str = Field(default="gemini-2.5-flash", validation_alias="GEMINI_MODEL")

    stream_trim_maxlen: int = Field(default=10000, validation_alias="STREAM_TRIM_MAXLEN")
    processing_delay_sec: int = Field(default=0, validation_alias="PROCESSING_DELAY_SEC")
    process_max_retries: int = Field(default=3, validation_alias="PROCESS_MAX_RETRIES")
    process_retry_base_delay_sec: float = Field(default=1.0, validation_alias="PROCESS_RETRY_BASE_DELAY_SEC")
    process_retry_max_delay: float = Field(default=30.0, validation_alias="PROCESS_RETRY_MAX_DELAY")

    spacy_clean_enabled: bool = Field(default=True, validation_alias="SPACY_CLEAN_ENABLED")
    spacy_model: str = Field(default="en_core_web_sm", validation_alias="SPACY_MODEL")
    spacy_min_token_retention_ratio: float = Field(default=0.1, validation_alias="SPACY_MIN_RETENTION_RATIO")

    stuck_message_idle_ms: int = Field(default=10_000, validation_alias="STUCK_MESSAGE_IDLE_MS")
    stream_trim_interval_sec: int = Field(default=300, validation_alias="STREAM_TRIM_INTERVAL")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_format: Literal["text", "json"] = Field(default="text")

    model_config = {"env_prefix": "", "env_nested_delimiter": "__"}

    @field_validator("spacy_clean_enabled", mode="before")
    @classmethod
    def _coerce_bool(cls, v: bool | str | int) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        return v.lower() in ("1", "true", "yes", "on")


# ─────────────────────────────────────────────────────────────────────────────
# Monitor Service
# ─────────────────────────────────────────────────────────────────────────────

class MonitorConfig(BaseSettings):
    """Configuration for the Monitor service."""

    app_name: str = "Sentinel Monitor"
    host: str = "0.0.0.0"
    port: int = 9000

    redis: RedisSettings = Field(default_factory=RedisSettings)

    sse_keepalive_sec: int = Field(default=15, validation_alias="SSE_KEEPALIVE_SEC")
    sse_queue_maxsize: int = Field(default=128, validation_alias="SSE_QUEUE_MAXSIZE")
    sse_queue_timeout: float = Field(default=30.0, validation_alias="SSE_QUEUE_TIMEOUT")

    cors_origins: list[str] = Field(
        default=["http://localhost:3000", "http://127.0.0.1:3000"],
        validation_alias="CORS_ORIGINS",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_format: Literal["text", "json"] = Field(default="text")

    model_config = {"env_prefix": "", "env_nested_delimiter": "__"}

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _parse_cors_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [o.strip() for o in v.split(",") if o.strip()]
        return v


# ─────────────────────────────────────────────────────────────────────────────
# Factories (cached singletons per process)
# ─────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_api_config() -> APIConfig:
    return APIConfig()

@lru_cache(maxsize=1)
def get_worker_config() -> WorkerConfig:
    return WorkerConfig()

@lru_cache(maxsize=1)
def get_monitor_config() -> MonitorConfig:
    return MonitorConfig()