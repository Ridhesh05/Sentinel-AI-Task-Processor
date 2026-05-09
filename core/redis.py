"""
Redis client with retry logic, connection management, and typed helpers.

Provides both synchronous and asynchronous Redis operations.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Literal

import redis
from redis import asyncio as aioredis

from core.config import RedisSettings
from core.exceptions import RedisUnavailableError

__all__ = ["RedisClient", "AsyncRedisClient", "get_redis_client", "get_async_redis_client"]

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Synchronous Redis client with retry logic and helper methods.

    Args:
        settings: Redis connection settings.
    """

    def __init__(self, settings: RedisSettings) -> None:
        self._settings = settings
        self._client: redis.Redis | None = None

    @property
    def settings(self) -> RedisSettings:
        return self._settings

    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            self._client = redis.Redis(
                host=self._settings.host,
                port=self._settings.port,
                decode_responses=self._settings.decode_responses,
                socket_timeout=self._settings.socket_timeout,
                socket_connect_timeout=self._settings.socket_connect_timeout,
            )
        return self._client

    def health_check(self) -> bool:
        """Return True if Redis is reachable."""
        try:
            return self.client.ping()
        except Exception as e:
            logger.warning("Redis health check failed: %s", e)
            return False

    def ping(self) -> None:
        """Ping Redis with retries. Raises RedisUnavailableError on failure."""
        last_err: Exception | None = None
        for attempt in range(self._settings.connect_retries):
            try:
                self.client.ping()
                return
            except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
                last_err = e
                logger.warning(
                    "Redis ping attempt %d/%d failed: %s",
                    attempt + 1,
                    self._settings.connect_retries,
                    e,
                )
                if attempt < self._settings.connect_retries - 1:
                    time.sleep(self._settings.connect_retry_delay)

        raise RedisUnavailableError(
            f"Redis unreachable after {self._settings.connect_retries} attempts: {last_err}"
        )

    def publish(self, channel: str, message: str | dict) -> int:
        """
        Publish a message to a channel.

        Args:
            channel: Pub/Sub channel name.
            message: Message string or dict (will be JSON-serialized if dict).

        Returns:
            Number of subscribers that received the message.
        """
        if isinstance(message, dict):
            message = json.dumps(message)
        return self.client.publish(channel, message)

    def get_json(self, key: str) -> dict | list | None:
        """Get a JSON value from Redis."""
        raw = self.client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set_json(self, key: str, value: dict | list, *, ex: int | None = None) -> bool:
        """Set a JSON value in Redis with optional TTL (seconds)."""
        return self.client.set(key, json.dumps(value), ex=ex)

    def incr(self, key: str) -> int:
        """Atomically increment a key and return the new value."""
        return self.client.incr(key)

    def expire(self, key: str, seconds: int) -> bool:
        """Set expiry on a key."""
        return self.client.expire(key, seconds)

    def ttl(self, key: str) -> int:
        """Get remaining TTL on a key (-1 if no expiry, -2 if key doesn't exist)."""
        return self.client.ttl(key)

    def xadd(self, stream: str, fields: dict[str, str], *, maxlen: int | None = None) -> str:
        """
        Add an entry to a Redis Stream.

        Args:
            stream: Stream name.
            fields: Key-value pairs for the stream entry.
            maxlen: Optional max stream length.

        Returns:
            Stream entry ID.
        """
        if maxlen:
            return self.client.xadd(stream, fields, maxlen=maxlen, approximate=True)
        return self.client.xadd(stream, fields)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int = 1,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]] | None:
        """
        Read from a stream consumer group.

        Args:
            group: Consumer group name.
            consumer: Consumer name.
            streams: Dict of {stream_name: ">" (new only) | start_id}.
            count: Max entries to read.
            block: Block for N milliseconds (None = non-blocking).

        Returns:
            List of (stream, entries) tuples or None.
        """
        return self.client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams=streams,
            count=count,
            block=block,
        )

    def xack(self, stream: str, group: str, *message_ids: str) -> int:
        """Acknowledge one or more stream messages."""
        return self.client.xack(stream, group, *message_ids)

    def xpending(self, stream: str, group: str) -> list[dict[str, Any]]:
        """Get pending messages in a consumer group."""
        raw = self.client.xpending(stream, group)
        if not raw:
            return []
        return [
            {
                "message_id": r[0],
                "consumer": r[1],
                "idle_time": r[2],
                "last_delivered": r[3],
                "delivery_counter": r[4],
            }
            for r in raw
        ]

    def xclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        message_ids: list[str],
    ) -> list[tuple[str, dict[str, str]]]:
        """Claim pending messages from another consumer."""
        return self.client.xclaim(
            stream, group, consumer, min_idle_time, message_ids
        )

    def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str = "0",
        mkstream: bool = True,
    ) -> bool:
        """Create a consumer group (idempotent)."""
        return self.client.xgroup_create(stream, group, id=id, mkstream=mkstream)

    def xtrim(self, stream: str, maxlen: int, *, approximate: bool = True) -> int:
        """Trim a stream to approximately maxlen entries."""
        return self.client.xtrim(stream, maxlen=maxlen, approximate=approximate)

    def get(self, key: str) -> str | None:
        """Get a string value."""
        return self.client.get(key)

    def setex(self, key: str, seconds: int, value: str) -> bool:
        """Set a value with expiry."""
        return self.client.setex(key, seconds, value)

    def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        return self.client.delete(*keys)

    def scan_iter(self, match: str = "*", count: int = 100) -> Any:
        """Iterate over keys matching a pattern."""
        return self.client.scan_iter(match=match, count=count)

    def rpush(self, key: str, *values: str) -> int:
        """Append values to the end of a list."""
        return self.client.rpush(key, *values)

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        """Get a range of elements from a list."""
        return self.client.lrange(key, start, end)

    def ltrim(self, key: str, start: int, stop: int) -> bool:
        """Trim a list to the specified range."""
        return self.client.ltrim(key, start, stop)

    def close(self) -> None:
        """Close the Redis connection."""
        if self._client:
            self._client.close()
            self._client = None


class AsyncRedisClient:
    """
    Asynchronous Redis client for use in async contexts (e.g., Monitor service).
    """

    def __init__(self, settings: RedisSettings) -> None:
        self._settings = settings
        self._client: aioredis.Redis | None = None

    @property
    def settings(self) -> RedisSettings:
        return self._settings

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.Redis(
                host=self._settings.host,
                port=self._settings.port,
                decode_responses=self._settings.decode_responses,
            )
        return self._client

    async def publish(self, channel: str, message: str | dict) -> int:
        if isinstance(message, dict):
            message = json.dumps(message)
        return await self.client.publish(channel, message)

    async def subscribe(self, *channels: str) -> aioredis.client.PubSub:
        pubsub = self.client.pubsub()
        await pubsub.subscribe(*channels)
        return pubsub

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton factories
# ─────────────────────────────────────────────────────────────────────────────

_redis_client: RedisClient | None = None
_async_redis_client: AsyncRedisClient | None = None


def get_redis_client(settings: RedisSettings | None = None) -> RedisClient:
    """Return the shared RedisClient instance."""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient(settings or RedisSettings())
    return _redis_client


def get_async_redis_client(settings: RedisSettings | None = None) -> AsyncRedisClient:
    """Return the shared AsyncRedisClient instance."""
    global _async_redis_client
    if _async_redis_client is None:
        _async_redis_client = AsyncRedisClient(settings or RedisSettings())
    return _async_redis_client