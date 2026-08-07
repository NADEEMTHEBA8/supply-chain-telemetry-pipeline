"""
Centralised structured logging configuration.

All services import `configure_logging()` to get consistent, production-grade
JSON log output. This makes logs parseable by Cloud Logging, Datadog, Splunk,
and any ELK-stack deployment without regex parsing.

12-Factor XI (Logs):
    "A twelve-factor app never concerns itself with routing or storage of its
    output stream." Logs are written to stdout as structured JSON. The runtime
    (Docker, Kubernetes, Cloud Logging) handles aggregation and routing.

Usage:
    from config.logging_config import configure_logging
    logger = configure_logging("my_service")
    logger.info("Feature load complete", extra={"user_count": 950, "duration_ms": 142})

Output (JSON, one line per event):
    {
      "timestamp": "2026-07-15T14:30:00.123Z",
      "level": "INFO",
      "service": "feature_loader",
      "message": "Feature load complete",
      "user_count": 950,
      "duration_ms": 142
    }

Cloud Logging equivalent:
    jsonPayload.level = "ERROR"  AND  jsonPayload.service = "feature_api"
    → instant filtering without regex

GCP Cloud Logging severity mapping:
    INFO  → INFO
    WARNING → WARNING
    ERROR → ERROR
    (mapped automatically by the Cloud Logging agent)
"""

import logging
import os
import sys

try:
    from pythonjsonlogger import jsonlogger

    _JSON_AVAILABLE = True
except ImportError:  # pragma: no cover
    _JSON_AVAILABLE = False


class _JsonFormatter(jsonlogger.JsonFormatter if _JSON_AVAILABLE else logging.Formatter):
    """
    Extends pythonjsonlogger to add a consistent field set to every log record.

    Extra fields added automatically:
        timestamp  — ISO 8601 UTC timestamp
        level      — log severity string
        service    — the logger name (injected at configure_logging() time)
    """

    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        log_record["level"] = record.levelname
        log_record["service"] = record.name
        # Remove the default keys that duplicate our renamed fields
        log_record.pop("levelname", None)
        log_record.pop("name", None)


def configure_logging(service_name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configure and return a named logger with structured JSON output.

    Falls back to human-readable format if python-json-logger is not installed
    (e.g., during early development without the full venv).

    Args:
        service_name: Identifier emitted as the "service" field in every log
                      record. Use the module name (e.g., "feature_api").
        level:        Logging level. Defaults to INFO. Override via LOG_LEVEL
                      environment variable for debug sessions.

    Returns:
        A configured logging.Logger instance.
    """
    effective_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    effective_level = getattr(logging, effective_level_name, level)

    logger = logging.getLogger(service_name)
    logger.setLevel(effective_level)

    # Avoid adding duplicate handlers if called multiple times (e.g., in tests).
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(effective_level)

    if _JSON_AVAILABLE:
        formatter = _JsonFormatter(fmt="%(timestamp)s %(level)s %(service)s %(message)s")
    else:  # pragma: no cover
        # Graceful degradation: human-readable fallback
        formatter = logging.Formatter(fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # Prevent propagation to root logger to avoid duplicate output.
    logger.propagate = False

    return logger
