"""Configured logger for task scripts — writes to stderr."""

from __future__ import annotations
import logging
from typing import Optional
import sys

logger = logging.getLogger("release")
logger.setLevel(logging.DEBUG)

# Clear any existing handlers first
logger.handlers.clear()
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_handler)
logger.propagate = False


def setup_logger(
    level: int = logging.INFO, log_format: Optional[str] = None, name: Optional[str] = None
) -> logging.Logger:
    """Set up and configure logger with stderr handlers.

    Logs at the specified level to to stderr.

    Args:
        level: Minimum logging level for stdout (default: logging.INFO)
        log_format: Logging message format (default: standard format with timestamp and level)
        name: Optional name for the logger (default: root logger)

    Returns:
        The root logger instance

    Example:
        import logging
        from logger import setup_logger

        # Basic usage with INFO level
        log = setup_logger()
        log.info("This goes to stdout")
        log.error("This goes to stderr")

        # Custom level and format
        log = setup_logger(
            level=logging.DEBUG,
            log_format="%(levelname)s - %(message)s"
        )

    """
    if log_format is None:
        log_format = "%(asctime)s [%(name)s] %(levelname)s %(message)s"

    if name:
        root = logging.getLogger(name)
    else:
        root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    formatter = logging.Formatter(log_format)

    # Add stderr handler for errors (ERROR and above)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(formatter)
    root.addHandler(stderr_handler)

    return root
