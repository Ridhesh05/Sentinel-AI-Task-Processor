"""
Sentinel AI Worker — Redis Streams consumer that processes AI tasks with Gemini.

Responsibilities:
- Consume messages from Redis Stream via consumer group (XREADGROUP)
- Materialise task rows in PostgreSQL
- Call Gemini API for AI inference
- Handle retries with exponential backoff
- Publish events to Redis Pub/Sub
- Expose Prometheus metrics on port 9100
- Graceful shutdown handling
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from prometheus_client import start_http_server

from core import (
    setup_logging,
    get_logger,
    get_worker_config,
    WorkerConfig,
    RedisUnavailableError,
)
from core.metrics import TASKS_PROCESSED, TASKS_FAILED, TASK_DURATION
from core.redis import RedisClient, get_redis_client
from core.db import DatabaseClient, get_db_client
from core.snowflake import generate_snowflake_id

setup_logging(level="INFO")
logger = get_logger(__name__)


class Worker:
    """Redis Streams consumer that processes AI tasks with Gemini."""

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.redis: RedisClient = get_redis_client(config.redis)
        self.db: DatabaseClient = get_db_client(config.db)

        self._shutdown = False
        self._gemini_client: Any | None = None

    def _build_gemini_client(self) -> Any:
        """Build and return the Gemini client (lazy, one-time)."""
        from google import genai
        return genai.Client(api_key=self.config.gemini_api_key)

    @property
    def gemini_client(self) -> Any:
        if self._gemini_client is None:
            self._gemini_client = self._build_gemini_client()
        return self._gemini_client

    def publish_event(
        self,
        event_type: str,
        task_id: str,
        extra: dict | None = None,
    ) -> None:
        """Publish a JSON event to the Redis Pub/Sub channel. Never raises."""
        payload = {
            "event_id": str(generate_snowflake_id()),
            "event": event_type,
            "task_id": str(task_id),
            "worker": self.config.worker_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            payload.update(extra)

        try:
            self.redis.publish(self.config.redis.pubsub_channel, payload)
        except Exception as e:
            logger.warning("Failed to publish event event_type=%s error=%s", event_type, e)

    def _parse_task_id(self, raw: Any) -> tuple[int | None, str | None]:
        """Parse task_id from stream entry. Returns (int_id, str_id) or (None, None)."""
        try:
            s = str(raw).strip()
            n = int(s)
            return (n, s) if n >= 0 else (None, None)
        except (ValueError, TypeError):
            return None, None

    def _ack(self, message_id: str) -> None:
        """ACK a stream message, swallowing Redis errors."""
        try:
            self.redis.xack(
                self.config.redis.stream_name,
                self.config.redis.stream_consumer_group,
                message_id,
            )
        except Exception as e:
            logger.warning("Failed to ACK message_id=%s: %s", message_id, e)

    def _reclaim_stuck_tasks(self) -> list[tuple[str, dict]]:
        """
        Reclaim messages idle > configured threshold from other consumers.

        Uses XPENDING to find pending messages, then XCLAIM to take ownership.
        """
        reclaimed: list[tuple[str, dict]] = []
        try:
            pending = self.redis.xpending(
                self.config.redis.stream_name,
                self.config.redis.stream_consumer_group,
            )
        except Exception as e:
            logger.warning("Could not check pending messages: %s", e)
            return reclaimed

        for entry in pending:
            message_id = entry.get("message_id")
            idle_time = entry.get("idle_time", 0)

            if idle_time > self.config.stuck_message_idle_ms:
                try:
                    messages = self.redis.xclaim(
                        self.config.redis.stream_name,
                        self.config.redis.stream_consumer_group,
                        self.config.worker_name,
                        self.config.stuck_message_idle_ms,
                        [message_id],
                    )
                    for msg_id, data in messages:
                        reclaimed.append((msg_id, data))
                    logger.info(
                        "Reclaimed stuck message message_id=%s idle_ms=%d",
                        message_id,
                        idle_time,
                    )
                except Exception as e:
                    logger.warning("XCLAIM failed for message_id=%s: %s", message_id, e)
                    break

        return reclaimed

    def _trim_stream(self) -> None:
        """Trim stream to configured maxlen entries (approximate)."""
        if self.config.stream_trim_maxlen <= 0:
            return
        try:
            self.redis.xtrim(
                self.config.redis.stream_name,
                self.config.stream_trim_maxlen,
                approximate=True,
            )
        except Exception as e:
            logger.warning("Stream trim failed: %s", e)

    def _get_session_history(self, session_id: str) -> list[dict]:
        """
        Fetch the last 5 task outputs for a session from Redis.

        Used to build conversation memory for Gemini prompts.
        """
        if not session_id:
            return []

        try:
            history_key = f"session:{session_id}:history"
            entries = self.redis.lrange(history_key, -5, -1)
            history = []
            for entry in entries:
                try:
                    history.append(json.loads(entry))
                except (json.JSONDecodeError, TypeError):
                    continue
            return history
        except Exception as e:
            logger.warning("Failed to fetch session history session_id=%s: %s", session_id, e)
            return []

    def _save_to_session_history(
        self,
        session_id: str,
        task_id: str,
        task_type: str,
        input_text: str,
        output: str,
    ) -> None:
        """
        Save a completed task's result to the session history in Redis.

        Uses RPUSH to append and resets TTL to 30 minutes.
        """
        if not session_id:
            return

        try:
            history_key = f"session:{session_id}:history"
            entry = json.dumps({
                "task_id": str(task_id),
                "task_type": task_type,
                "input_text": input_text,
                "output": output,
            })
            self.redis.rpush(history_key, entry)
            self.redis.expire(history_key, 1800)
        except Exception as e:
            logger.warning("Failed to save session history session_id=%s: %s", session_id, e)

    def _build_memory_prompt(
        self,
        task_type: str,
        input_text: str,
        history: list[dict],
    ) -> str:
        """
        Build a Gemini prompt that includes conversation memory from previous tasks.

        If history exists, injects previous task outputs as context so Gemini
        understands references like "it", "this", "the result".

        Args:
            task_type: Current task type (e.g., summarize, translate).
            input_text: Current user input.
            history: List of previous session task entries.

        Returns:
            Prompt string with or without memory context.
        """
        if not history:
            return f"Task: {task_type}\n\nInput:\n{input_text}\n\nReturn only the result."

        context_parts = []
        for item in history:
            output = item.get("output", "")
            if len(output) > 500:
                output = output[:500] + "..."
            context_parts.append(
                f"Previous Task ({item.get('task_type', 'unknown')}): {output}"
            )

        context = "\n".join(context_parts)

        return f"""You are processing tasks in a conversation session.

Previous tasks in this session:
{context}

Current Task: {task_type}
Current Input: {input_text}

Important: If the current input refers to previous output (using words like 'it', 'this', 'the result', 'that'), use the most recent previous task output as the subject.

Return only the result of the current task."""

    def _cleanup_db(self, conn: Any, cur: Any) -> None:
        """Safely roll back and close DB resources."""
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        try:
            if cur and not cur.closed:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    def process_task(self, message_id: str, data: dict[str, str]) -> None:
        """
        Process a single task from the Redis Stream.

        Steps:
          1. Parse and validate task_id
          2. Extract session_id for conversation memory
          3. Fetch session history from Redis
          4. Fetch or materialise task row in PostgreSQL
          5. Idempotency check (skip if COMPLETED/FAILED)
          6. Update status to PROCESSING + publish event
          7. Build prompt with memory context, call Gemini
          8. Save to session history in Redis
          9. Update status to COMPLETED with output_text
          10. Publish TASK_COMPLETED event
          11. XACK the message

        On exhaustion of retries, marks task as FAILED and publishes TASK_FAILED.
        The original input_text is NEVER modified in the database.
        """
        task_id_int, task_id_str = self._parse_task_id(data.get("task_id"))
        if task_id_int is None:
            logger.error("Invalid stream payload — bad task_id: %r", data.get("task_id"))
            self._ack(message_id)
            return

        task_type = data.get("task_type", "")
        input_text = data.get("input_text", "")
        session_id = data.get("session_id", "")

        if not task_type or not input_text:
            logger.error(
                "Invalid stream payload task_id=%s: missing task_type or input_text",
                task_id_str,
            )
            self._ack(message_id)
            return

        if self.config.processing_delay_sec > 0:
            logger.info(
                "Task %s: artificial delay %ds",
                task_id_str,
                self.config.processing_delay_sec,
            )
            time.sleep(self.config.processing_delay_sec)

        logger.info("Processing task task_id=%s type=%s", task_id_str, task_type)

        last_error: Exception | None = None
        conn = None
        cur = None

        for attempt in range(self.config.process_max_retries):
            try:
                conn = get_db_client()._get_conn()
                cur = conn.cursor()

                # Fetch or materialise task row
                cur.execute(
                    "SELECT status, task_type, input_text FROM tasks WHERE id=%s",
                    (task_id_int,),
                )
                row = cur.fetchone()

                if not row:
                    now = datetime.now(timezone.utc)
                    cur.execute(
                        """
                        INSERT INTO tasks (id, task_type, status, input_text, created_at, updated_at, queued_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (task_id_int, task_type, "QUEUED", input_text, now, now, now),
                    )
                    conn.commit()
                    status = "QUEUED"
                else:
                    status, task_type, input_text = row

                # Idempotency — skip already finished tasks
                if status in ("COMPLETED", "FAILED"):
                    logger.info(
                        "Skipping already-finished task task_id=%s status=%s",
                        task_id_str,
                        status,
                    )
                    cur.close()
                    conn.close()
                    self._ack(message_id)
                    return

                # Fetch session history for conversation memory
                history = self._get_session_history(session_id)

                # Mark as PROCESSING
                cur.execute(
                    "UPDATE tasks SET status=%s, started_at=NOW(), updated_at=NOW() WHERE id=%s",
                    ("PROCESSING", task_id_int),
                )
                conn.commit()
                self.publish_event("TASK_PROCESSING", task_id_str)

                # Build prompt with memory context
                t0 = time.monotonic()

                from services.worker.text_cleaner import clean_text
                cleaned_input = clean_text(input_text)
                prompt = self._build_memory_prompt(task_type, cleaned_input, history)
                response = self.gemini_client.models.generate_content(
                    model=self.config.gemini_model,
                    contents=prompt,
                )
                result = response.text
                elapsed = time.monotonic() - t0

                # Save to session history in Redis
                self._save_to_session_history(session_id, task_id_str, task_type, input_text, result)

                # Mark COMPLETED
                cur.execute(
                    "UPDATE tasks SET status=%s, output_text=%s, completed_at=NOW(), updated_at=NOW() WHERE id=%s",
                    ("COMPLETED", result, task_id_int),
                )
                conn.commit()
                self.publish_event("TASK_COMPLETED", task_id_str)
                self._ack(message_id)

                TASKS_PROCESSED.labels(worker=self.config.worker_name).inc()
                TASK_DURATION.labels(worker=self.config.worker_name).observe(elapsed)
                logger.info("Task completed task_id=%s duration=%.2fs", task_id_str, elapsed)

                cur.close()
                conn.close()
                return

            except Exception as e:
                last_error = e
                self._cleanup_db(conn, cur)

                delay = self.config.process_retry_base_delay_sec * (2 ** attempt)
                delay = min(delay, self.config.process_retry_max_delay)

                logger.warning(
                    "Error on attempt %d/%d for task_id=%s: %s — retry in %.1fs",
                    attempt + 1,
                    self.config.process_max_retries,
                    task_id_str,
                    e,
                    delay,
                )

                if attempt < self.config.process_max_retries - 1:
                    time.sleep(delay)

        # All retries exhausted — mark FAILED
        logger.error(
            "Task failed after %d attempts task_id=%s error=%s",
            self.config.process_max_retries,
            task_id_str,
            last_error,
        )

        try:
            conn = get_db_client()._get_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE tasks SET status=%s, error=%s, completed_at=NOW(), updated_at=NOW() WHERE id=%s",
                ("FAILED", str(last_error), task_id_int),
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as db_err:
            logger.error("Could not mark task %s as FAILED: %s", task_id_str, db_err)

        self.publish_event("TASK_FAILED", task_id_str, {"error": str(last_error)})
        self._ack(message_id)
        TASKS_FAILED.labels(worker=self.config.worker_name).inc()

    def ensure_consumer_group(self) -> None:
        """Create the consumer group for the stream (idempotent)."""
        try:
            self.redis.xgroup_create(
                self.config.redis.stream_name,
                self.config.redis.stream_consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Consumer group created stream=%s group=%s",
                self.config.redis.stream_name,
                self.config.redis.stream_consumer_group,
            )
        except Exception:
            logger.debug(
                "Consumer group already exists stream=%s group=%s",
                self.config.redis.stream_name,
                self.config.redis.stream_consumer_group,
            )

    def run(self) -> None:
        """
        Main worker loop.

        Reads from Redis Stream via consumer group, processes tasks,
        periodically trims the stream, and reclaims stuck messages.
        """
        self.ensure_consumer_group()
        logger.info(
            "Worker started consumer=%s stream=%s | trim_maxlen=%d | delay=%ds | retries=%d",
            self.config.worker_name,
            self.config.redis.stream_name,
            self.config.stream_trim_maxlen,
            self.config.processing_delay_sec,
            self.config.process_max_retries,
        )

        last_trim = time.monotonic()
        backoff = 0.0

        while not self._shutdown:
            try:
                if backoff > 0:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)

                # Periodic stream trim
                if (time.monotonic() - last_trim) >= self.config.stream_trim_interval_sec:
                    self._trim_stream()
                    last_trim = time.monotonic()

                # Read from stream
                messages = self.redis.xreadgroup(
                    self.config.redis.stream_consumer_group,
                    self.config.worker_name,
                    {self.config.redis.stream_name: ">"},
                    count=1,
                    block=5000,
                )
                backoff = 0.0

                if messages:
                    for _stream, entries in messages:
                        for message_id, data in entries:
                            self.process_task(message_id, data)

                # Reclaim stuck tasks
                reclaimed = self._reclaim_stuck_tasks()
                for message_id, data in reclaimed:
                    self.process_task(message_id, data)

            except Exception as e:
                if backoff == 0:
                    backoff = self.config.redis.connect_retry_delay
                logger.error("Worker loop error (retry in %.1fs): %s", backoff, e)

    def stop(self) -> None:
        """Signal the worker to shut down gracefully."""
        logger.info("Worker shutdown requested")
        self._shutdown = True


def main() -> None:
    """Entry point: start metrics server, then run the worker loop."""
    config = get_worker_config()

    setup_logging(level=config.log_level, json_output=config.log_format == "json")

    # Start Prometheus metrics server on a background thread
    start_http_server(config.metrics_port)
    logger.info(
        "Prometheus metrics server started port=%d worker=%s",
        config.metrics_port,
        config.worker_name,
    )

    worker = Worker(config)

    # Graceful shutdown on SIGTERM / SIGINT
    def handle_signal(signum: int, frame) -> None:
        logger.info("Received signal %d — initiating graceful shutdown", signum)
        worker.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted — shutting down")
    finally:
        logger.info("Worker stopped")


if __name__ == "__main__":
    main()