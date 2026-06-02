"""Configured logger for task scripts — writes to stderr."""

from __future__ import annotations

import logging

logger = logging.getLogger("release")
logger.setLevel(logging.DEBUG)

# Clear any existing handlers first
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_handler)
logger.propagate = False


def setup_logger(
    level: int = logging.DEBUG,
    name: str = "release",
) -> logging.Logger:
    """Create and return a configured logger with the given level and name."""
    log = logging.getLogger(name)
    log.setLevel(level)
    log.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    log.addHandler(handler)
    log.propagate = False
    return log
