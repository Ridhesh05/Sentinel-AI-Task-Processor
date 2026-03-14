import os
import time
import psycopg2
from app.exceptions import DatabaseUnavailableError

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "sentinel_db")
DB_USER = os.getenv("DB_USER", "sentinel")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sentinel")

# Retry config for transient failures
DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "3"))
DB_CONNECT_RETRY_DELAY_SEC = float(os.getenv("DB_CONNECT_RETRY_DELAY_SEC", "0.5"))


def get_db_connection():
    """Return a DB connection. Raises DatabaseUnavailableError if PostgreSQL is down."""
    last_err = None
    for attempt in range(DB_CONNECT_RETRIES):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=5,
            )
        except psycopg2.OperationalError as e:
            last_err = e
            if attempt < DB_CONNECT_RETRIES - 1:
                time.sleep(DB_CONNECT_RETRY_DELAY_SEC)
            continue
    raise DatabaseUnavailableError(f"PostgreSQL unreachable after {DB_CONNECT_RETRIES} attempts: {last_err}")
