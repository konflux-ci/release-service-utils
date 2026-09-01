"""Tests for the ``date_format`` helper module."""

from __future__ import annotations

import subprocess
import unittest.mock as mock

import pytest

from release_service_utils.helpers import date_format


def test_current_timestamp_builds_correct_command() -> None:
    """The correct ``date`` invocation is used and output is stripped."""
    with mock.patch(
        "release_service_utils.helpers.date_format.run_cmd_text",
        return_value="20240115 10:30:00\n",
    ) as run_mock:
        result = date_format.current_timestamp()

    run_mock.assert_called_once_with(["date", "+%Y%m%d %T"])
    assert result == "20240115 10:30:00"


def test_format_date_builds_correct_command() -> None:
    """``format_date`` passes ``-d`` and the format string with a leading ``+``."""
    with mock.patch(
        "release_service_utils.helpers.date_format.run_cmd_text",
        return_value="1705318200\n",
    ) as run_mock:
        result = date_format.format_date("2024-01-15T10:30:00Z", "%s")

    run_mock.assert_called_once_with(["date", "-d", "2024-01-15T10:30:00Z", "+%s"])
    assert result == "1705318200"


def test_format_date_custom_format() -> None:
    """Arbitrary format strings are passed through untouched."""
    with mock.patch(
        "release_service_utils.helpers.date_format.run_cmd_text",
        return_value="2024-01-15\n",
    ):
        result = date_format.format_date("20240115 10:30:00", "%Y-%m-%d")

    assert result == "2024-01-15"


def test_format_date_invalid_date_raises() -> None:
    """An unparsable date string propagates ``CalledProcessError`` from ``date``."""
    with mock.patch(
        "release_service_utils.helpers.date_format.run_cmd_text",
        side_effect=subprocess.CalledProcessError(1, ["date"]),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            date_format.format_date("not-a-date", "%s")
