"""Tests for memory_throttle."""

from __future__ import annotations

from pathlib import Path

import memory_throttle as mt
import pytest


def _write(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8")


# --- read_cgroup_memory ---


def test_read_cgroup_memory_v2(tmp_path: Path) -> None:
    """Cgroups v2 files are preferred and parsed as (current, max)."""
    v2_current = tmp_path / "memory.current"
    v2_max = tmp_path / "memory.max"
    _write(v2_current, "1000\n")
    _write(v2_max, "4000\n")

    result = mt.read_cgroup_memory(
        v2_current=v2_current,
        v2_max=v2_max,
        v1_current=tmp_path / "missing-v1-current",
        v1_max=tmp_path / "missing-v1-max",
    )
    assert result == (1000, 4000)


def test_read_cgroup_memory_v2_unlimited_falls_back_to_v1(tmp_path: Path) -> None:
    """A v2 max of 'max' (unlimited) is ignored in favor of v1 values."""
    v2_current = tmp_path / "memory.current"
    v2_max = tmp_path / "memory.max"
    v1_current = tmp_path / "usage_in_bytes"
    v1_max = tmp_path / "limit_in_bytes"
    _write(v2_current, "1000")
    _write(v2_max, "max")
    _write(v1_current, "2000")
    _write(v1_max, "8000")

    result = mt.read_cgroup_memory(
        v2_current=v2_current, v2_max=v2_max, v1_current=v1_current, v1_max=v1_max
    )
    assert result == (2000, 8000)


def test_read_cgroup_memory_unavailable(tmp_path: Path) -> None:
    """None is returned when no cgroup interface is readable."""
    missing = tmp_path / "missing"
    result = mt.read_cgroup_memory(
        v2_current=missing, v2_max=missing, v1_current=missing, v1_max=missing
    )
    assert result is None


def test_read_cgroup_memory_non_numeric_content(tmp_path: Path) -> None:
    """Non-numeric file content (corrupt/unexpected) is treated as unreadable."""
    v2_current = tmp_path / "memory.current"
    v2_max = tmp_path / "memory.max"
    missing = tmp_path / "missing"
    _write(v2_current, "not-a-number")
    _write(v2_max, "4000")

    result = mt.read_cgroup_memory(
        v2_current=v2_current, v2_max=v2_max, v1_current=missing, v1_max=missing
    )
    assert result is None


# --- get_memory_usage_percent / format_bytes / get_memory_stats ---


def test_get_memory_usage_percent() -> None:
    """Usage percent is computed as an integer floor division."""
    assert mt.get_memory_usage_percent(read_memory=lambda: (500, 1000)) == 50
    assert mt.get_memory_usage_percent(read_memory=lambda: (333, 1000)) == 33


def test_get_memory_usage_percent_unavailable() -> None:
    """None is returned when the reader has no data."""
    assert mt.get_memory_usage_percent(read_memory=lambda: None) is None


@pytest.mark.parametrize(
    ("num_bytes", "expected"),
    [
        (512, "512B"),
        (2545, "2Ki"),
        (5 * 1024 * 1024 + 777, "5Mi"),
        (3 * 1024 * 1024 * 1024 + 123456, "3Gi"),
    ],
)
def test_format_bytes(num_bytes: int, expected: str) -> None:
    """Byte counts are formatted with the largest whole unit, truncated."""
    assert mt.format_bytes(num_bytes) == expected


def test_get_memory_stats() -> None:
    """Stats string reports used/limit and the usage percentage."""
    assert mt.get_memory_stats(
        read_memory=lambda: (512 * 1024 * 1024, 1024 * 1024 * 1024)
    ) == ("512Mi/1Gi (50%)")


def test_get_memory_stats_unavailable() -> None:
    """'unavailable' is reported when the reader has no data."""
    assert mt.get_memory_stats(read_memory=lambda: None) == "unavailable"


# --- log_memory_throttle_status ---


def test_log_memory_throttle_status_available() -> None:
    """No exception is raised when logging available memory stats."""
    mt.log_memory_throttle_status(80, read_memory=lambda: (500, 1000))


def test_log_memory_throttle_status_unavailable() -> None:
    """No exception is raised when memory info is unavailable."""
    mt.log_memory_throttle_status(80, read_memory=lambda: None)


# --- wait_for_memory ---


def test_wait_for_memory_returns_immediately_when_unavailable() -> None:
    """No sleep occurs when cgroup memory info cannot be read."""
    sleeps: list[float] = []
    mt.wait_for_memory(read_memory=lambda: None, sleep=sleeps.append)
    assert sleeps == []


def test_wait_for_memory_returns_immediately_when_below_threshold() -> None:
    """No sleep occurs when usage is already below the threshold."""
    sleeps: list[float] = []
    mt.wait_for_memory(80, read_memory=lambda: (100, 1000), sleep=sleeps.append)
    assert sleeps == []


def test_wait_for_memory_polls_until_below_threshold() -> None:
    """Sleeps until a subsequent read reports usage below the threshold.

    Each loop iteration reads memory twice (once for the logged stats, once
    for the loop condition), so two full high-usage iterations require four
    high readings before the usage drops on the fifth read.
    """
    calls = {"n": 0}

    def reader() -> tuple[int, int]:
        calls["n"] += 1
        return (900, 1000) if calls["n"] <= 4 else (500, 1000)

    sleeps: list[float] = []
    mt.wait_for_memory(80, interval=1.5, read_memory=reader, sleep=sleeps.append)
    assert sleeps == [1.5, 1.5]


def test_wait_for_memory_stops_if_reader_loses_access_mid_wait() -> None:
    """Polling stops if the reader starts returning None mid-wait."""
    calls = {"n": 0}

    def flaky_reader() -> tuple[int, int] | None:
        calls["n"] += 1
        return (900, 1000) if calls["n"] == 1 else None

    sleeps: list[float] = []
    mt.wait_for_memory(80, read_memory=flaky_reader, sleep=sleeps.append)
    assert sleeps == [5.0]
