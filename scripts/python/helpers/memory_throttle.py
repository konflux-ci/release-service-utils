"""Memory-aware job throttling, ported from ``utils/memory-throttle.sh``.

Reads container memory usage from cgroups (v2, falling back to v1) so
parallel task runners can pause spawning new work while memory is under
pressure, reducing the frequency of OOMKills. Callers that cannot read
cgroup memory info (e.g. outside a container) get a no-op: throttling
relies solely on the caller's own concurrency limit in that case.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from logger import logger

DEFAULT_THRESHOLD = 80
DEFAULT_INTERVAL = 5.0

_CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_MAX = Path("/sys/fs/cgroup/memory.max")
_CGROUP_V1_CURRENT = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
_CGROUP_V1_MAX = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

ReadMemory = Callable[[], tuple[int, int] | None]


def _read_int(path: Path) -> int | None:
    """Read an integer value from *path*, or None if missing/unreadable."""
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not text or text == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def read_cgroup_memory(
    *,
    v2_current: Path = _CGROUP_V2_CURRENT,
    v2_max: Path = _CGROUP_V2_MAX,
    v1_current: Path = _CGROUP_V1_CURRENT,
    v1_max: Path = _CGROUP_V1_MAX,
) -> tuple[int, int] | None:
    """Return ``(current, max)`` memory bytes from cgroups v2 or v1.

    Returns None when neither cgroup interface is readable, matching the
    original bash helper's "memory monitoring unavailable" case.
    """
    current = _read_int(v2_current)
    maximum = _read_int(v2_max)
    if current is not None and maximum is not None and maximum > 0:
        return current, maximum

    current = _read_int(v1_current)
    maximum = _read_int(v1_max)
    if current is not None and maximum is not None and maximum > 0:
        return current, maximum

    return None


def get_memory_usage_percent(*, read_memory: ReadMemory = read_cgroup_memory) -> int | None:
    """Return current memory usage as an integer percentage, or None if unavailable."""
    values = read_memory()
    if values is None:
        return None
    current, maximum = values
    return current * 100 // maximum


def format_bytes(num_bytes: int) -> str:
    """Format *num_bytes* as a human-readable Gi/Mi/Ki/B string."""
    if num_bytes >= 1024**3:
        return f"{num_bytes // 1024**3}Gi"
    if num_bytes >= 1024**2:
        return f"{num_bytes // 1024**2}Mi"
    if num_bytes >= 1024:
        return f"{num_bytes // 1024}Ki"
    return f"{num_bytes}B"


def get_memory_stats(*, read_memory: ReadMemory = read_cgroup_memory) -> str:
    """Return ``"used/limit (XX%)"``, or ``"unavailable"`` when unreadable."""
    values = read_memory()
    if values is None:
        return "unavailable"
    current, maximum = values
    usage = current * 100 // maximum
    return f"{format_bytes(current)}/{format_bytes(maximum)} ({usage}%)"


def log_memory_throttle_status(
    threshold: int = DEFAULT_THRESHOLD,
    *,
    read_memory: ReadMemory = read_cgroup_memory,
) -> None:
    """Log whether memory-based throttling is available. Call once at task start."""
    stats = get_memory_stats(read_memory=read_memory)
    if stats == "unavailable":
        logger.info(
            "Memory throttle: cgroup memory info not available, using concurrent-limit only"
        )
    else:
        logger.info(
            "Memory throttle: enabled with %s%% threshold, current usage: %s", threshold, stats
        )


def wait_for_memory(
    threshold: int = DEFAULT_THRESHOLD,
    interval: float = DEFAULT_INTERVAL,
    *,
    read_memory: ReadMemory = read_cgroup_memory,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Block the calling thread until memory usage is below *threshold* percent.

    Returns immediately without blocking when cgroup memory info cannot be
    read, so callers fall back to relying on their own concurrency limit.
    """
    usage = get_memory_usage_percent(read_memory=read_memory)
    if usage is None:
        return

    waited = False
    while usage is not None and usage >= threshold:
        if not waited:
            logger.info(
                "Memory throttle: usage above %s%% threshold, pausing new job spawns...",
                threshold,
            )
            waited = True
        logger.info(
            "  Memory: %s - waiting for running jobs to free memory...",
            get_memory_stats(read_memory=read_memory),
        )
        sleep(interval)
        usage = get_memory_usage_percent(read_memory=read_memory)

    if waited:
        logger.info(
            "Memory throttle: usage now at %s, resuming...",
            get_memory_stats(read_memory=read_memory),
        )
