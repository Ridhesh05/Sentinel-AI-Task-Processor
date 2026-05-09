"""
Snowflake ID generator (Twitter-style 64-bit).

Layout: 1 bit unused | 41 bits timestamp (ms) | 10 bits node_id | 12 bits sequence

Epoch: 2020-01-01 00:00:00 UTC
"""

from __future__ import annotations

import os
import threading
import time

__all__ = ["SnowflakeGenerator", "generate_snowflake_id"]

SNOWFLAKE_EPOCH_MS = 1577836800000
TIMESTAMP_BITS = 41
NODE_BITS = 10
SEQUENCE_BITS = 12

NODE_ID_MAX = (1 << NODE_BITS) - 1
SEQUENCE_MAX = (1 << SEQUENCE_BITS) - 1

NODE_ID: int = int(os.getenv("SNOWFLAKE_NODE_ID", "0")) & NODE_ID_MAX

_lock = threading.Lock()
_last_ms = 0
_sequence = 0


def generate_snowflake_id() -> int:
    """Generate a unique 64-bit Snowflake ID (thread-safe)."""
    global _last_ms, _sequence
    with _lock:
        ms = int(time.time() * 1000) - SNOWFLAKE_EPOCH_MS
        if ms < 0:
            ms = 0
        if ms == _last_ms:
            _sequence = (_sequence + 1) & SEQUENCE_MAX
            if _sequence == 0:
                while ms == _last_ms:
                    time.sleep(0.001)
                    ms = int(time.time() * 1000) - SNOWFLAKE_EPOCH_MS
                    if ms < 0:
                        ms = 0
                _last_ms = ms
        else:
            _sequence = 0
            _last_ms = ms
        sid = (ms << (NODE_BITS + SEQUENCE_BITS)) | (NODE_ID << SEQUENCE_BITS) | _sequence
    return sid


class SnowflakeGenerator:
    """Stateless Snowflake ID generator backed by module-level state."""

    __slots__ = ()

    @staticmethod
    def generate() -> int:
        """Generate and return a new Snowflake ID."""
        return generate_snowflake_id()

    @staticmethod
    def extract_timestamp(snowflake_id: int) -> int:
        """Extract timestamp (epoch ms) from a Snowflake ID."""
        return snowflake_id >> (NODE_BITS + SEQUENCE_BITS)

    @staticmethod
    def extract_node_id(snowflake_id: int) -> int:
        """Extract node ID from a Snowflake ID."""
        return (snowflake_id >> SEQUENCE_BITS) & NODE_ID_MAX

    @staticmethod
    def extract_sequence(snowflake_id: int) -> int:
        """Extract sequence number from a Snowflake ID."""
        return snowflake_id & SEQUENCE_MAX

    @staticmethod
    def decode(snowflake_id: int) -> dict[str, int]:
        """Decode a Snowflake ID into its components."""
        return {
            "timestamp_ms": SnowflakeGenerator.extract_timestamp(snowflake_id),
            "node_id": SnowflakeGenerator.extract_node_id(snowflake_id),
            "sequence": SnowflakeGenerator.extract_sequence(snowflake_id),
        }