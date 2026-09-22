from __future__ import annotations

import contextvars
import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)

_RESERVED_LOG_RECORD_KEYS = frozenset(
    logging.LogRecord(
        name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None
    ).__dict__.keys()
) | {"message"}


def get_correlation_id() -> str | None:
    return _correlation_id.get()


def set_correlation_id(correlation_id: str) -> contextvars.Token[str | None]:
    return _correlation_id.set(correlation_id)


def reset_correlation_id(token: contextvars.Token[str | None]) -> None:
    _correlation_id.reset(token)


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        correlation_id = get_correlation_id()
        if correlation_id is not None:
            payload["correlation_id"] = correlation_id
        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_RECORD_KEYS:
                payload.setdefault(key, value)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)
