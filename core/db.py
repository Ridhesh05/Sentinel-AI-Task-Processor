"""
PostgreSQL database client with retry logic and connection pooling.

Provides synchronous database operations using psycopg2.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Generator, Sequence

import psycopg2
from psycopg2 import pool

from core.config import DatabaseSettings
from core.exceptions import DatabaseUnavailableError

__all__ = ["DatabaseClient", "get_db_client", "get_connection"]

logger = logging.getLogger(__name__)


class DatabaseClient:
    """
    PostgreSQL client with retry logic and optional connection pooling.

    Args:
        settings: Database connection settings.
        use_pool: If True, uses a SimpleConnectionPool; otherwise creates a fresh connection per request.
    """

    def __init__(
        self,
        settings: DatabaseSettings,
        *,
        use_pool: bool = False,
        pool_min: int = 2,
        pool_max: int = 10,
    ) -> None:
        self._settings = settings
        self._use_pool = use_pool
        self._pool: pool.SimpleConnectionPool | None = None
        self._pool_min = pool_min
        self._pool_max = pool_max

    @property
    def settings(self) -> DatabaseSettings:
        return self._settings

    def _connect(self) -> psycopg2.extensions.connection:
        """Create a new database connection."""
        return psycopg2.connect(
            host=self._settings.host,
            port=self._settings.port,
            database=self._settings.name,
            user=self._settings.user,
            password=self._settings.password,
            connect_timeout=self._settings.connect_timeout,
        )

    def _get_conn(self) -> psycopg2.extensions.connection:
        """Get a connection (from pool or fresh)."""
        if self._pool:
            return self._pool.getconn()
        return self._connect()

    def _return_conn(self, conn: psycopg2.extensions.connection) -> None:
        """Return connection to pool if using pooling."""
        if self._pool:
            self._pool.putconn(conn)

    def initialize(self) -> None:
        """Initialize the connection pool (if enabled)."""
        if not self._use_pool:
            return
        self._pool = pool.SimpleConnectionPool(
            self._pool_min,
            self._pool_max,
            host=self._settings.host,
            port=self._settings.port,
            database=self._settings.name,
            user=self._settings.user,
            password=self._settings.password,
            connect_timeout=self._settings.connect_timeout,
        )
        logger.info("Database connection pool initialized")

    def close(self) -> None:
        """Close all pooled connections."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("Database connection pool closed")

    def health_check(self) -> bool:
        """Return True if database is reachable."""
        try:
            conn = self._get_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                return True
            finally:
                self._return_conn(conn)
        except Exception as e:
            logger.warning("Database health check failed: %s", e)
            return False

    def execute(
        self,
        query: str,
        params: Sequence[Any] | None = None,
        *,
        fetch_one: bool = False,
        fetch_all: bool = False,
    ) -> list[tuple[Any, ...]] | tuple[Any, ...] | None:
        """
        Execute a query with retry logic.

        Args:
            query: SQL query string.
            params: Query parameters.
            fetch_one: If True, returns a single row.
            fetch_all: If True, returns all rows.

        Returns:
            List of rows, single row, or None depending on fetch mode.
        """
        last_err: Exception | None = None
        for attempt in range(self._settings.connect_retries):
            conn = self._get_conn()
            try:
                cur = conn.cursor()
                cur.execute(query, params)
                result: list[tuple[Any, ...]] | tuple[Any, ...] | None = None
                if fetch_one:
                    result = cur.fetchone()
                elif fetch_all:
                    result = cur.fetchall()
                cur.close()
                self._return_conn(conn)
                return result
            except psycopg2.OperationalError as e:
                last_err = e
                self._return_conn(conn)
                logger.warning(
                    "DB execute attempt %d/%d failed: %s",
                    attempt + 1,
                    self._settings.connect_retries,
                    e,
                )
                if attempt < self._settings.connect_retries - 1:
                    time.sleep(self._settings.connect_retry_delay)
            except Exception:
                self._return_conn(conn)
                raise

        raise DatabaseUnavailableError(
            f"PostgreSQL unavailable after {self._settings.connect_retries} attempts: {last_err}"
        )

    @contextmanager
    def transaction(self) -> Generator[psycopg2.extensions.cursor, None, None]:
        """Context manager for a database transaction (auto-commit/rollback)."""
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            yield cur
            conn.commit()
            cur.close()
            self._return_conn(conn)
        except Exception:
            conn.rollback()
            if cur and not cur.closed:
                cur.close()
            self._return_conn(conn)
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton factory
# ─────────────────────────────────────────────────────────────────────────────

_db_client: DatabaseClient | None = None


def get_db_client(settings: DatabaseSettings | None = None) -> DatabaseClient:
    """Return the shared DatabaseClient instance."""
    global _db_client
    if _db_client is None:
        cfg = settings or DatabaseSettings()
        _db_client = DatabaseClient(cfg)
    return _db_client


def get_connection(settings: DatabaseSettings | None = None) -> psycopg2.extensions.connection:
    """Return a fresh database connection (direct, no pooling)."""
    cfg = settings or DatabaseSettings()
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.name,
        user=cfg.user,
        password=cfg.password,
        connect_timeout=cfg.connect_timeout,
    )