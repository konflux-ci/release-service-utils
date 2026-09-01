"""Memory-aware job throttling, ported from ``utils/memory-throttle.sh``.

Reads container memory usage from cgroups (v2, falling back to v1) so
parallel task runners can pause spawning new work while memory is under
pressure, reducing the frequency of OOMKills. Callers that cannot read
cgroup memory info (e.g. outside a container) get a no-op: throttling
relies solely on the caller's own concurrency limit in that case.
"""

from .memory_throttle import (  # noqa: F401
    DEFAULT_INTERVAL,
    DEFAULT_THRESHOLD,
    ReadMemory,
    format_bytes,
    get_memory_stats,
    get_memory_usage_percent,
    log_memory_throttle_status,
    read_cgroup_memory,
    wait_for_memory,
)
