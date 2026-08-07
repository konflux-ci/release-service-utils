"""Tests for the ``image_architectures`` helper module."""

from __future__ import annotations

import subprocess
import unittest.mock as mock

import pytest

import image_architectures


def _completed(
    stdout: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=""
    )


def test_get_image_architectures_single_arch() -> None:
    """A single JSON line is parsed into a one-item list."""
    stdout = (
        '{"platform": {"architecture": "amd64", "os": "linux"}, '
        '"digest": "sha256:abc", "multiarch": false}\n'
    )
    with mock.patch(
        "image_architectures.run_cmd",
        return_value=_completed(stdout=stdout),
    ) as run_mock:
        result = image_architectures.get_image_architectures("registry.io/repo@sha256:abc")

    assert result == [
        {
            "platform": {"architecture": "amd64", "os": "linux"},
            "digest": "sha256:abc",
            "multiarch": False,
        }
    ]
    cmd = run_mock.call_args[0][0]
    assert cmd == ["get-image-architectures", "registry.io/repo@sha256:abc"]
    assert run_mock.call_args[1]["check"] is False


def test_get_image_architectures_multi_arch() -> None:
    """Multiple newline-delimited JSON objects are all parsed."""
    stdout = (
        '{"platform": {"architecture": "amd64", "os": "linux"}, '
        '"digest": "sha256:aaa", "multiarch": true}\n'
        '{"platform": {"architecture": "arm64", "os": "linux"}, '
        '"digest": "sha256:bbb", "multiarch": true}\n'
    )
    with mock.patch(
        "image_architectures.run_cmd",
        return_value=_completed(stdout=stdout),
    ):
        result = image_architectures.get_image_architectures("registry.io/repo@sha256:idx")

    assert len(result) == 2
    assert result[0]["platform"]["architecture"] == "amd64"
    assert result[1]["platform"]["architecture"] == "arm64"


def test_get_image_architectures_skips_blank_lines() -> None:
    """Blank lines in the output are ignored."""
    stdout = (
        '{"platform": {"architecture": "amd64", "os": "linux"}, "digest": "sha256:aaa"}\n\n'
    )
    with mock.patch(
        "image_architectures.run_cmd",
        return_value=_completed(stdout=stdout),
    ):
        result = image_architectures.get_image_architectures("registry.io/repo@sha256:aaa")

    assert len(result) == 1


def test_get_image_architectures_custom_retry_times() -> None:
    """``retry_times`` is forwarded as ``--skopeo-retries``."""
    with mock.patch(
        "image_architectures.run_cmd",
        return_value=_completed(stdout="{}\n"),
    ) as run_mock:
        image_architectures.get_image_architectures("img:v1", retry_times=5)

    cmd = run_mock.call_args[0][0]
    assert cmd == ["get-image-architectures", "--skopeo-retries", "5", "img:v1"]


def test_get_image_architectures_no_retry_flag_by_default() -> None:
    """No ``--skopeo-retries`` flag is added unless explicitly requested."""
    with mock.patch(
        "image_architectures.run_cmd",
        return_value=_completed(stdout="{}\n"),
    ) as run_mock:
        image_architectures.get_image_architectures("img:v1")

    cmd = run_mock.call_args[0][0]
    assert "--skopeo-retries" not in cmd


def test_get_image_architectures_failure_raises() -> None:
    """A non-zero exit code raises ``RuntimeError`` with the stderr message."""
    with mock.patch(
        "image_architectures.run_cmd",
        return_value=_completed(returncode=1, stdout=""),
    ):
        with pytest.raises(RuntimeError, match="get-image-architectures failed"):
            image_architectures.get_image_architectures("img:v1")


def test_get_image_architectures_failure_includes_stderr() -> None:
    """The raised error message includes the underlying stderr text."""
    completed = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr="boom: auth failed"
    )
    with mock.patch("image_architectures.run_cmd", return_value=completed):
        with pytest.raises(RuntimeError, match="boom: auth failed"):
            image_architectures.get_image_architectures("img:v1")
