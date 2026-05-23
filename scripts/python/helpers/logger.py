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
