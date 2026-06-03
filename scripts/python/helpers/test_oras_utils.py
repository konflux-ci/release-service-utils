"""Unit tests for oras_utils."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from oras_utils import oras_resolve


def test_oras_resolve_calls_select_oci_auth_with_reference() -> None:
    """select-oci-auth is called with the image reference."""
    with patch("oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout='{"auths": {}}'),
            MagicMock(returncode=0, stdout="sha256:abc\n"),
        ]
        oras_resolve("registry.io/repo:tag")

    first_call = mock_run.call_args_list[0]
    assert first_call == call(["select-oci-auth", "registry.io/repo:tag"])


def test_oras_resolve_passes_auth_file_to_oras() -> None:
    """Oras resolve is called with --registry-config pointing to the auth temp file."""
    with patch("oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout='{"auths": {}}'),
            MagicMock(returncode=0, stdout="sha256:abc\n"),
        ]
        oras_resolve("registry.io/repo:tag")

    second_call = mock_run.call_args_list[1]
    cmd = second_call.args[0]
    assert cmd[0] == "oras"
    assert cmd[1] == "resolve"
    assert "--registry-config" in cmd
    assert "registry.io/repo:tag" in cmd


def test_oras_resolve_returns_stripped_digest() -> None:
    """Returns the digest from oras resolve output, stripped of whitespace."""
    with patch("oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(returncode=0, stdout="sha256:deadbeef\n"),
        ]
        result = oras_resolve("registry.io/repo:tag")

    assert result == "sha256:deadbeef"


def test_oras_resolve_raises_on_nonzero_returncode() -> None:
    """Raises RuntimeError when oras resolve exits non-zero."""
    with patch("oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(returncode=1, stdout="", stderr="unauthorized"),
        ]
        with pytest.raises(RuntimeError):
            oras_resolve("registry.io/repo:tag")
