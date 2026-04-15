from typing import Any
import logging
import sys


def setup_logger(level: int = logging.INFO, log_format: Any = None):
    """
    Set up and configure logger with stdout and stderr handlers.

    Logs at passed level to stdout, ERROR and above to stderr.

    Args:
        level: Minimum logging level for stdout (default: logging.INFO)
        log_format: Logging message format (default: standard format)
    """
    if log_format is None:
        log_format = "%(asctime)s [%(name)s] %(levelname)s %(message)s"

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    formatter = logging.Formatter(log_format)

    # Add stdout and stderr handlers
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    root.addHandler(handler)
