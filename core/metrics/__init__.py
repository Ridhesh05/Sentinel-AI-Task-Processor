"""
Prometheus metrics registry for Sentinel services.

Provides shared metric definitions that can be imported across all services.
Single responsibility: define and expose metrics. Do NOT create Gauge/Histogram
instances here — import the definitions and use them in each service.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram, Gauge

__all__ = [
    "REQUEST_COUNT",
    "REQUEST_LATENCY",
    "TASK_CREATED",
    "TASK_RETRIED",
    "TASKS_PROCESSED",
    "TASKS_FAILED",
    "TASK_DURATION",
    "SSE_CONNECTIONS",
    "SSE_EVENTS_SENT",
    "REDIS_OPERATIONS",
    "DB_OPERATIONS",
]

# ─── API Metrics ───────────────────────────────────────────────────────────────

REQUEST_COUNT = Counter(
    "sentinel_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status_code"],
)

REQUEST_LATENCY = Histogram(
    "sentinel_api_request_duration_seconds",
    "API request latency in seconds",
    ["endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

TASK_CREATED = Counter(
    "sentinel_tasks_created_total",
    "Total tasks successfully created via POST /tasks",
)

TASK_RETRIED = Counter(
    "sentinel_tasks_retried_total",
    "Total task retries triggered via POST /tasks/{id}/retry",
)

# ─── Worker Metrics ─────────────────────────────────────────────────────────────

TASKS_PROCESSED = Counter(
    "sentinel_worker_tasks_processed_total",
    "Tasks successfully completed by workers",
    ["worker"],
)

TASKS_FAILED = Counter(
    "sentinel_worker_tasks_failed_total",
    "Tasks that exhausted all retries and marked FAILED",
    ["worker"],
)

TASK_DURATION = Histogram(
    "sentinel_worker_task_processing_seconds",
    "Time spent processing a single task (Gemini + DB write)",
    ["worker"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)

# ─── Monitor Metrics ───────────────────────────────────────────────────────────

SSE_CONNECTIONS = Counter(
    "sentinel_monitor_sse_connections_total",
    "Total SSE connections established",
)

SSE_EVENTS_SENT = Counter(
    "sentinel_monitor_sse_events_sent_total",
    "Total SSE events delivered to clients",
)

# ─── Infrastructure Metrics ───────────────────────────────────────────────────

REDIS_OPERATIONS = Counter(
    "sentinel_redis_operations_total",
    "Total Redis operations",
    ["operation", "status"],
)

DB_OPERATIONS = Counter(
    "sentinel_db_operations_total",
    "Total database operations",
    ["operation", "status"],
)