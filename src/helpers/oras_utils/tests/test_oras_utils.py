"""Unit tests for oras_utils."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import oras_utils
from oras_utils import oras_utils as _oras_utils
import pytest

from release_service_utils.helpers.oras_utils.oras_utils import oras_resolve


def test_oras_resolve_calls_select_oci_auth_with_reference() -> None:
    """select-oci-auth is called with the image reference."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout='{"auths": {}}'),
            MagicMock(returncode=0, stdout="sha256:abc\n"),
        ]
        oras_resolve("registry.io/repo:tag")

    first_call = mock_run.call_args_list[0]
    assert first_call == call(["select-oci-auth", "registry.io/repo:tag"], check=False)


def test_oras_resolve_passes_auth_file_to_oras() -> None:
    """Oras resolve is called with --registry-config pointing to the auth temp file."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
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
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(returncode=0, stdout="sha256:deadbeef\n"),
        ]
        result = oras_resolve("registry.io/repo:tag")

    assert result == "sha256:deadbeef"


def test_oras_resolve_raises_on_nonzero_returncode() -> None:
    """Raises RuntimeError when oras resolve exits non-zero."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(returncode=1, stdout="", stderr="unauthorized"),
        ]
        with pytest.raises(RuntimeError):
            oras_resolve("registry.io/repo:tag")


def test_oras_resolve_returns_none_when_check_false() -> None:
    """Returns None instead of raising when check=False."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(returncode=1, stdout="", stderr="not found"),
        ]
        result = oras_resolve("registry.io/repo:tag", check=False)

    assert result is None


def test_oras_resolve_returns_none_on_empty_output_when_check_false() -> None:
    """Returns None when oras outputs only whitespace (check=False)."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(returncode=0, stdout="  \n"),
        ]
        result = oras_resolve("registry.io/repo:tag", check=False)

    assert result is None


def test_oras_resolve_uses_auth_ref_for_select_oci_auth() -> None:
    """auth_ref overrides reference for select-oci-auth."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(returncode=0, stdout="sha256:abc\n"),
        ]
        oras_resolve("registry.io/repo:v1", auth_ref="registry.io/repo")

    auth_call = mock_run.call_args_list[0]
    assert auth_call == call(["select-oci-auth", "registry.io/repo"], check=False)
    resolve_call = mock_run.call_args_list[1]
    assert "registry.io/repo:v1" in resolve_call.args[0]


def test_oras_resolve_falls_back_to_empty_auth_on_select_oci_auth_failure() -> None:
    """select-oci-auth failure falls back to empty auth and still resolves."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="no creds"),
            MagicMock(returncode=0, stdout="sha256:abc\n"),
        ]
        result = oras_resolve("registry.io/repo:tag")

    assert result == "sha256:abc"
    auth_call = mock_run.call_args_list[0]
    assert auth_call == call(["select-oci-auth", "registry.io/repo:tag"], check=False)


def test_oras_resolve_falls_back_to_empty_auth_on_empty_stdout() -> None:
    """select-oci-auth returning empty stdout falls back to empty auth."""
    with patch("release_service_utils.helpers.oras_utils.oras_utils.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="  \n"),
            MagicMock(returncode=0, stdout="sha256:abc\n"),
        ]
        result = oras_resolve("registry.io/repo:tag")

    assert result == "sha256:abc"


def test_oras_pull_runs_select_oci_auth_and_oras(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`oras_pull` writes auth config then pulls the artifact into *download_dir*."""
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append([str(x) for x in cmd])
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout='{"auths":{}}', stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(_oras_utils.subprocess_cmd, "run_cmd", fake_run_cmd)
    oras_utils.oras_pull("quay.io/org/image@sha256:abc", tmp_path)

    assert calls[0] == ["select-oci-auth", "quay.io/org/image@sha256:abc"]
    assert calls[1][0:3] == ["oras", "pull", "--registry-config"]
    assert calls[1][-1] == "quay.io/org/image@sha256:abc"


def test_oras_pull_cleans_up_auth_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Temporary auth file created for oras is removed after pull completes."""
    created: list[Path] = []
    original = _oras_utils.file.make_tempfile_path

    def track_tempfile(prefix: str, data: bytes | None = None) -> Path:
        path = original(prefix, data)
        created.append(path)
        return path

    monkeypatch.setattr(_oras_utils.file, "make_tempfile_path", track_tempfile)

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(_oras_utils.subprocess_cmd, "run_cmd", fake_run_cmd)
    oras_utils.oras_pull("quay.io/org/image:tag", tmp_path)

    assert len(created) == 1
    assert not created[0].exists()


def test_oras_pull_raises_when_subprocess_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Subprocess failures propagate to callers after auth file cleanup."""

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        raise subprocess.CalledProcessError(1, cmd, stderr="oras pull failed")

    monkeypatch.setattr(_oras_utils.subprocess_cmd, "run_cmd", fake_run_cmd)

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        oras_utils.oras_pull("quay.io/org/image:tag", tmp_path)

    assert exc_info.value.returncode == 1
