"""Tests for the task `logger` helper (stdlib `logging` setup)."""

from __future__ import annotations

import logging

from logger import logger


def test_handler_formatter_matches_task_format() -> None:
    """Log lines use `LEVELNAME: message` on stderr."""
    assert logger.handlers, "release logger should have a handler from import"
    handler = logger.handlers[0]
    assert isinstance(handler.formatter, logging.Formatter)
    info_rec = logging.LogRecord("release", logging.INFO, __file__, 0, "hello", (), None)
    assert handler.format(info_rec) == "INFO: hello"
    warn_rec = logging.LogRecord("release", logging.WARNING, __file__, 0, "oops", (), None)
    assert handler.format(warn_rec) == "WARNING: oops"
