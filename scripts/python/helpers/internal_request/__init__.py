"""Create and wait for InternalRequest resources."""

from __future__ import annotations

from .internal_request import InternalRequestWaitError, SPAWN_OVERHEAD_SECONDS, create

__all__ = ["InternalRequestWaitError", "SPAWN_OVERHEAD_SECONDS", "create"]
