"""
Structured logging setup for Sentinel AI Task Processor.

Provides a consistent logging format across all services with support for
JSON output (production) and human-readable output (development).
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

__all__ = ["setup_logging", "get_logger"]

LOG_FORMAT_HUMAN = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
LOG_FORMAT_JSON = (
    '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}'
)


def _configure_root_logger(level: str, json_output: bool) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))

    if json_output:
        handler.setFormatter(logging.Formatter(LOG_FORMAT_JSON, datefmt=LOG_DATE_FORMAT))
    else:
        handler.setFormatter(
            logging.Formatter(LOG_FORMAT_HUMAN, datefmt=LOG_DATE_FORMAT)
        )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def setup_logging(
    level: str | None = None,
    json_output: bool | None = None,
    service_name: str | None = None,
) -> None:
    """
    Configure root logger for the current process.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to LOG_LEVEL env var or INFO.
        json_output: Emit JSON logs (for production). Defaults to LOG_FORMAT=json env var or False.
        service_name: Optional prefix for all loggers in this service.
    """
    _level = level or os.getenv("LOG_LEVEL", "INFO").upper()
    _json = json_output if json_output is not None else os.getenv("LOG_FORMAT", "").lower() == "json"

    _configure_root_logger(_level, _json)

    if service_name:
        logging.getLogger("sentinel").name = service_name


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger instance for the given name.

    Args:
        name: Logger name (typically __name__ of the calling module).

    Returns:
        Configured Logger instance.
    """
    return logging.getLogger(name)


class StructuredLogger:
    """
    Wraps a standard Logger with structured (key-value) logging support.

    Use this for services that need to emit structured logs in both
    human-readable and JSON formats.
    """

    __slots__ = ("_logger",)

    def __init__(self, logger: logging.Logger | str) -> None:
        self._logger = logger if isinstance(logger, logging.Logger) else logging.getLogger(logger)

    def _format(self, message: str, **kwargs: Any) -> str:
        if kwargs:
            pairs = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            return f"{message} {pairs}"
        return message

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug(self._format(message, **kwargs))

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(self._format(message, **kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(self._format(message, **kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(self._format(message, **kwargs))

    def critical(self, message: str, **kwargs: Any) -> None:
        self._logger.critical(self._format(message, **kwargs))

    def exception(self, message: str, **kwargs: Any) -> None:
        self._logger.exception(self._format(message, **kwargs))