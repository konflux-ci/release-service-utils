"""Configured logger for task scripts — writes to stderr."""

from __future__ import annotations

import logging

from release_service_utils.framework import Helper


class LoggerHelper(Helper):
    """Configured logger for task scripts — writes to stderr."""

    def setup_logger(
        self,
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


logger = logging.getLogger("release")
logger.setLevel(logging.DEBUG)

# Clear any existing handlers first
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_handler)
logger.propagate = False

_helper = LoggerHelper()
setup_logger = _helper.setup_logger
