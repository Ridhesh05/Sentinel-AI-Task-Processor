"""
Snowflake ID generator (Twitter-style 64-bit).
Useful: time-ordered, compact, good for distributed systems and indexing.
Layout: 1 bit unused | 41 bits timestamp (ms) | 10 bits node_id | 12 bits sequence
"""
import time
import threading
import os

# Epoch (ms) — 2020-01-01 00:00:00 UTC
SNOWFLAKE_EPOCH_MS = 1577836800000

# Bits
TIMESTAMP_BITS = 41
NODE_BITS = 10
SEQUENCE_BITS = 12

NODE_ID_MAX = (1 << NODE_BITS) - 1
SEQUENCE_MAX = (1 << SEQUENCE_BITS) - 1

# Node ID from env (0–1023) so multiple workers don't collide
NODE_ID = int(os.getenv("SNOWFLAKE_NODE_ID", "0")) & NODE_ID_MAX

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
                # Overflow in same ms — spin until next ms
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
