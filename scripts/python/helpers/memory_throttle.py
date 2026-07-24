"""Memory-aware job throttling for Tekton tasks.

Reads cgroup v2/v1 memory counters to determine container memory pressure
and blocks new work from being submitted when usage exceeds a threshold.
Gracefully degrades to a no-op when cgroup files are not available (e.g.
local development, unsupported container runtimes).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

logger = logging.getLogger("memory_throttle")

_CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_MAX = Path("/sys/fs/cgroup/memory.max")
_CGROUP_V1_USAGE = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
_CGROUP_V1_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")


def _read_cgroup_memory() -> tuple[int, int] | None:
    """Read current and max memory bytes from cgroups.

    Try cgroup v2 first, fall back to v1.  Return ``(current, max)``
    or ``None`` when the information is unavailable.
    """
    if _CGROUP_V2_CURRENT.is_file() and _CGROUP_V2_MAX.is_file():
        try:
            current = int(_CGROUP_V2_CURRENT.read_text().strip())
            max_text = _CGROUP_V2_MAX.read_text().strip()
            if max_text != "max":
                max_bytes = int(max_text)
                if max_bytes > 0:
                    return current, max_bytes
        except (ValueError, OSError):
            pass

    if _CGROUP_V1_USAGE.is_file() and _CGROUP_V1_LIMIT.is_file():
        try:
            current = int(_CGROUP_V1_USAGE.read_text().strip())
            max_bytes = int(_CGROUP_V1_LIMIT.read_text().strip())
            if max_bytes > 0:
                return current, max_bytes
        except (ValueError, OSError):
            pass

    return None


def _format_bytes(n: int) -> str:
    """Format *n* bytes as a human-readable string (Gi/Mi/Ki/B)."""
    if n >= 1 << 30:
        return f"{n >> 30}Gi"
    if n >= 1 << 20:
        return f"{n >> 20}Mi"
    if n >= 1 << 10:
        return f"{n >> 10}Ki"
    return f"{n}B"


def get_memory_usage_percent() -> int | None:
    """Return container memory usage as an integer percentage (0-100).

    Returns ``None`` when cgroup memory information is unavailable.
    """
    mem = _read_cgroup_memory()
    if mem is None:
        return None
    current, max_bytes = mem
    return current * 100 // max_bytes


def get_memory_stats() -> str:
    """Return a human-readable memory summary, e.g. ``"512Mi/1024Mi (50%)"``."""
    mem = _read_cgroup_memory()
    if mem is None:
        return "unavailable"
    current, max_bytes = mem
    pct = current * 100 // max_bytes
    return f"{_format_bytes(current)}/{_format_bytes(max_bytes)} ({pct}%)"


def wait_for_memory(
    threshold: int = 80,
    interval: float = 5.0,
) -> None:
    """Block until container memory usage drops below *threshold* percent.

    If cgroup memory information is unavailable, returns immediately so
    callers can fall back to concurrency-limit-only throttling.
    """
    usage = get_memory_usage_percent()
    if usage is None:
        return

    waited = False
    while usage is not None and usage >= threshold:
        if not waited:
            logger.info(
                "Memory throttle: usage above %d%% threshold, " "pausing new job spawns...",
                threshold,
            )
            waited = True
        logger.info(
            "  Memory: %s - waiting for running jobs to free memory...",
            get_memory_stats(),
        )
        time.sleep(interval)
        usage = get_memory_usage_percent()

    if waited:
        logger.info(
            "Memory throttle: usage now at %s, resuming...",
            get_memory_stats(),
        )


def log_memory_throttle_status(threshold: int = 80) -> None:
    """Log once at task start whether memory-based throttling is available."""
    stats = get_memory_stats()
    if stats == "unavailable":
        logger.info(
            "Memory throttle: cgroup memory info not available, " "using concurrentLimit only"
        )
    else:
        logger.info(
            "Memory throttle: enabled with %d%% threshold, " "current usage: %s",
            threshold,
            stats,
        )
