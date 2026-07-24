"""Tests for ``memory_throttle`` helper."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch

import memory_throttle
import pytest

# ── _read_cgroup_memory ────────────────────────────────────────────


class TestReadCgroupMemory:
    """Tests for ``_read_cgroup_memory``."""

    def test_cgroup_v2(self, tmp_path: Path) -> None:
        """Read memory from cgroup v2 files."""
        current = tmp_path / "memory.current"
        maximum = tmp_path / "memory.max"
        current.write_text("524288000\n")
        maximum.write_text("1073741824\n")
        with (
            patch.object(memory_throttle, "_CGROUP_V2_CURRENT", current),
            patch.object(memory_throttle, "_CGROUP_V2_MAX", maximum),
        ):
            assert memory_throttle._read_cgroup_memory() == (524288000, 1073741824)

    def test_cgroup_v2_max_unlimited(self, tmp_path: Path) -> None:
        """When max is 'max' (unlimited), v2 is skipped."""
        current = tmp_path / "memory.current"
        maximum = tmp_path / "memory.max"
        current.write_text("100\n")
        maximum.write_text("max\n")

        v1_usage = tmp_path / "usage_in_bytes"
        v1_limit = tmp_path / "limit_in_bytes"
        v1_usage.write_text("200\n")
        v1_limit.write_text("400\n")

        with (
            patch.object(memory_throttle, "_CGROUP_V2_CURRENT", current),
            patch.object(memory_throttle, "_CGROUP_V2_MAX", maximum),
            patch.object(memory_throttle, "_CGROUP_V1_USAGE", v1_usage),
            patch.object(memory_throttle, "_CGROUP_V1_LIMIT", v1_limit),
        ):
            assert memory_throttle._read_cgroup_memory() == (200, 400)

    def test_cgroup_v1_fallback(self, tmp_path: Path) -> None:
        """Fall back to cgroup v1 when v2 files are absent."""
        usage = tmp_path / "usage_in_bytes"
        limit = tmp_path / "limit_in_bytes"
        usage.write_text("200\n")
        limit.write_text("400\n")
        with (
            patch.object(memory_throttle, "_CGROUP_V2_CURRENT", tmp_path / "no"),
            patch.object(memory_throttle, "_CGROUP_V2_MAX", tmp_path / "no"),
            patch.object(memory_throttle, "_CGROUP_V1_USAGE", usage),
            patch.object(memory_throttle, "_CGROUP_V1_LIMIT", limit),
        ):
            assert memory_throttle._read_cgroup_memory() == (200, 400)

    def test_unavailable(self, tmp_path: Path) -> None:
        """Return None when no cgroup files exist."""
        with (
            patch.object(memory_throttle, "_CGROUP_V2_CURRENT", tmp_path / "no"),
            patch.object(memory_throttle, "_CGROUP_V2_MAX", tmp_path / "no"),
            patch.object(memory_throttle, "_CGROUP_V1_USAGE", tmp_path / "no"),
            patch.object(memory_throttle, "_CGROUP_V1_LIMIT", tmp_path / "no"),
        ):
            assert memory_throttle._read_cgroup_memory() is None

    def test_v2_non_numeric(self, tmp_path: Path) -> None:
        """Non-numeric content in v2 files falls through to v1 / None."""
        current = tmp_path / "memory.current"
        maximum = tmp_path / "memory.max"
        current.write_text("garbage\n")
        maximum.write_text("1024\n")
        with (
            patch.object(memory_throttle, "_CGROUP_V2_CURRENT", current),
            patch.object(memory_throttle, "_CGROUP_V2_MAX", maximum),
            patch.object(memory_throttle, "_CGROUP_V1_USAGE", tmp_path / "no"),
            patch.object(memory_throttle, "_CGROUP_V1_LIMIT", tmp_path / "no"),
        ):
            assert memory_throttle._read_cgroup_memory() is None


# ── _format_bytes ──────────────────────────────────────────────────


class TestFormatBytes:
    """Tests for ``_format_bytes``."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (2 * (1 << 30), "2Gi"),
            (512 * (1 << 20), "512Mi"),
            (64 * (1 << 10), "64Ki"),
            (999, "999B"),
        ],
    )
    def test_format(self, value: int, expected: str) -> None:
        """Format byte values to human-readable units."""
        assert memory_throttle._format_bytes(value) == expected


# ── get_memory_usage_percent ───────────────────────────────────────


class TestGetMemoryUsagePercent:
    """Tests for ``get_memory_usage_percent``."""

    def test_returns_percentage(self) -> None:
        """Return integer percentage of memory usage."""
        with patch.object(memory_throttle, "_read_cgroup_memory", return_value=(750, 1000)):
            assert memory_throttle.get_memory_usage_percent() == 75

    def test_returns_none_when_unavailable(self) -> None:
        """Return None when cgroup memory is unavailable."""
        with patch.object(memory_throttle, "_read_cgroup_memory", return_value=None):
            assert memory_throttle.get_memory_usage_percent() is None


# ── get_memory_stats ───────────────────────────────────────────────


class TestGetMemoryStats:
    """Tests for ``get_memory_stats``."""

    def test_formatted(self) -> None:
        """Return formatted usage/limit string with percentage."""
        with patch.object(
            memory_throttle,
            "_read_cgroup_memory",
            return_value=(512 * (1 << 20), 1024 * (1 << 20)),
        ):
            assert memory_throttle.get_memory_stats() == "512Mi/1Gi (50%)"

    def test_unavailable(self) -> None:
        """Return 'unavailable' when cgroup memory is not readable."""
        with patch.object(memory_throttle, "_read_cgroup_memory", return_value=None):
            assert memory_throttle.get_memory_stats() == "unavailable"


# ── wait_for_memory ────────────────────────────────────────────────


class TestWaitForMemory:
    """Tests for ``wait_for_memory``."""

    def test_returns_immediately_when_below_threshold(self) -> None:
        """Return immediately when usage is below threshold."""
        with patch.object(memory_throttle, "get_memory_usage_percent", return_value=50):
            start = time.monotonic()
            memory_throttle.wait_for_memory(80)
            assert time.monotonic() - start < 1.0

    def test_returns_immediately_when_unavailable(self) -> None:
        """Return immediately when cgroup memory is unavailable."""
        with patch.object(memory_throttle, "get_memory_usage_percent", return_value=None):
            start = time.monotonic()
            memory_throttle.wait_for_memory(80)
            assert time.monotonic() - start < 1.0

    def test_blocks_until_below_threshold(self) -> None:
        """Block until memory usage drops below threshold."""
        call_count = 0

        def declining_usage() -> int | None:
            nonlocal call_count
            call_count += 1
            return 90 if call_count <= 2 else 70

        with (
            patch.object(
                memory_throttle,
                "get_memory_usage_percent",
                side_effect=declining_usage,
            ),
            patch.object(
                memory_throttle,
                "get_memory_stats",
                return_value="900Mi/1024Mi (90%)",
            ),
            patch.object(memory_throttle.time, "sleep"),
        ):
            memory_throttle.wait_for_memory(80, interval=1.0)
        assert call_count == 3

    def test_breaks_if_cgroups_lost(self) -> None:
        """If cgroups become unavailable mid-wait, stop blocking."""
        calls = [85, None]

        with (
            patch.object(
                memory_throttle,
                "get_memory_usage_percent",
                side_effect=calls,
            ),
            patch.object(
                memory_throttle,
                "get_memory_stats",
                return_value="test",
            ),
            patch.object(memory_throttle.time, "sleep"),
        ):
            memory_throttle.wait_for_memory(80, interval=0.01)


# ── log_memory_throttle_status ─────────────────────────────────────


class TestLogMemoryThrottleStatus:
    """Tests for ``log_memory_throttle_status``."""

    def test_available(self) -> None:
        """Log memory stats when available."""
        with patch.object(
            memory_throttle, "get_memory_stats", return_value="512Mi/1024Mi (50%)"
        ):
            memory_throttle.log_memory_throttle_status(80)

    def test_unavailable(self) -> None:
        """Log when memory stats are unavailable."""
        with patch.object(memory_throttle, "get_memory_stats", return_value="unavailable"):
            memory_throttle.log_memory_throttle_status(80)
