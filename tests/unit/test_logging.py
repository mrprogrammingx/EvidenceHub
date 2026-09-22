from __future__ import annotations

import json
import logging

from evidencehub.common.logging import (
    JSONFormatter,
    configure_logging,
    reset_correlation_id,
    set_correlation_id,
)


def _make_record(msg: str = "hi", args: tuple[object, ...] = ()) -> logging.LogRecord:
    return logging.LogRecord(
        name="evidencehub.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=args,
        exc_info=None,
    )


def test_json_formatter_includes_core_fields() -> None:
    payload = json.loads(JSONFormatter().format(_make_record("hello %s", ("world",))))
    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "evidencehub.test"
    assert "timestamp" in payload


def test_json_formatter_includes_correlation_id_when_set() -> None:
    token = set_correlation_id("req-123")
    try:
        payload = json.loads(JSONFormatter().format(_make_record()))
        assert payload["correlation_id"] == "req-123"
    finally:
        reset_correlation_id(token)


def test_json_formatter_omits_correlation_id_when_unset() -> None:
    payload = json.loads(JSONFormatter().format(_make_record()))
    assert "correlation_id" not in payload


def test_json_formatter_includes_extra_fields() -> None:
    record = _make_record()
    record.rows = 42  # type: ignore[attr-defined]
    payload = json.loads(JSONFormatter().format(record))
    assert payload["rows"] == 42


def test_configure_logging_sets_single_json_handler_on_root() -> None:
    configure_logging("DEBUG")
    try:
        root = logging.getLogger()
        assert root.level == logging.DEBUG
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JSONFormatter)
    finally:
        logging.getLogger().handlers.clear()
