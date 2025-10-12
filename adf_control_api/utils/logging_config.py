"""Structured logging configuration."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict

from adf_control_api.core.config import Settings, get_settings


class JsonFormatter(logging.Formatter):
    """Format log records as structured JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        # Include any extra attributes supplied via LoggerAdapter / contextual logging
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in {
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "message",
                "module",
                "msecs",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            }:
                continue
            log_record[key] = value

        return json.dumps(log_record, default=str)


def setup_logging(settings: Settings | None = None) -> None:
    """Configure root logger with JSON formatting."""
    settings = settings or get_settings()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel(settings.log_level.upper())

    # Remove existing handlers to avoid duplicate logs in reload scenarios
    for existing in list(root_logger.handlers):
        root_logger.removeHandler(existing)

    root_logger.addHandler(handler)
    # Silence overly noisy loggers by default
    logging.getLogger("azure").setLevel(logging.INFO)
    logging.getLogger("msal").setLevel(logging.INFO)

