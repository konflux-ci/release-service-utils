"""Create and wait for InternalRequest resources."""

from __future__ import annotations

from .internal_request import (
    InternalRequestWaitError,
    PIPELINERUN_UID_LABEL,
    SPAWN_OVERHEAD_SECONDS,
    create,
    create_internal_request,
    duration_to_seconds,
    fetch_results,
    wait_for_completion,
)

__all__ = [
    "InternalRequestWaitError",
    "PIPELINERUN_UID_LABEL",
    "SPAWN_OVERHEAD_SECONDS",
    "create",
    "create_internal_request",
    "duration_to_seconds",
    "fetch_results",
    "wait_for_completion",
]
