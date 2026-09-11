"""Tests for the task `logger` helper (stdlib `logging` setup)."""

from __future__ import annotations

import logging

from release_service_utils.helpers.logger import logger, setup_logger


def test_handler_formatter_matches_task_format() -> None:
    """Log lines use `LEVELNAME: message` on stderr."""
    assert logger.handlers, "release logger should have a handler from import"
    handler = logger.handlers[0]
    assert isinstance(handler.formatter, logging.Formatter)
    info_rec = logging.LogRecord("release", logging.INFO, __file__, 0, "hello", (), None)
    assert handler.format(info_rec) == "INFO: hello"
    warn_rec = logging.LogRecord("release", logging.WARNING, __file__, 0, "oops", (), None)
    assert handler.format(warn_rec) == "WARNING: oops"


def test_setup_logger_returns_named_logger() -> None:
    """setup_logger returns a logger with the given name."""
    log = setup_logger(name="test_named")
    assert log.name == "test_named"
    assert log.level == logging.DEBUG


def test_setup_logger_respects_level() -> None:
    """setup_logger sets the requested log level."""
    log = setup_logger(level=logging.WARNING, name="test_level")
    assert log.level == logging.WARNING


def test_setup_logger_formatter() -> None:
    """setup_logger configures the same formatter as the module-level logger."""
    log = setup_logger(name="test_formatter")
    assert len(log.handlers) == 1
    handler = log.handlers[0]
    rec = logging.LogRecord("test_formatter", logging.ERROR, __file__, 0, "boom", (), None)
    assert handler.format(rec) == "ERROR: boom"


def test_setup_logger_no_propagation() -> None:
    """setup_logger disables propagation to avoid duplicate messages."""
    log = setup_logger(name="test_propagate")
    assert log.propagate is False
