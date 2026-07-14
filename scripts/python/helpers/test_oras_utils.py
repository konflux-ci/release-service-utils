"""Unit tests for oras_utils."""

from __future__ import annotations

import io
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import oras_utils
import pytest

from oras_utils import oras_resolve

# ---------------------------------------------------------------------------
# oras_resolve
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# oras_pull
# ---------------------------------------------------------------------------


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

    monkeypatch.setattr(oras_utils.subprocess_cmd, "run_cmd", fake_run_cmd)
    oras_utils.oras_pull("quay.io/org/image@sha256:abc", tmp_path)

    assert calls[0] == ["select-oci-auth", "quay.io/org/image@sha256:abc"]
    assert calls[1][0:3] == ["oras", "pull", "--registry-config"]
    assert calls[1][-1] == "quay.io/org/image@sha256:abc"


def test_oras_pull_cleans_up_auth_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Temporary auth file created for oras is removed after pull completes."""
    created: list[Path] = []
    original = oras_utils.file.make_tempfile_path

    def track_tempfile(prefix: str, data: bytes | None = None) -> Path:
        path = original(prefix, data)
        created.append(path)
        return path

    monkeypatch.setattr(oras_utils.file, "make_tempfile_path", track_tempfile)

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(oras_utils.subprocess_cmd, "run_cmd", fake_run_cmd)
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

    monkeypatch.setattr(oras_utils.subprocess_cmd, "run_cmd", fake_run_cmd)

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        oras_utils.oras_pull("quay.io/org/image:tag", tmp_path)

    assert exc_info.value.returncode == 1


# ---------------------------------------------------------------------------
# safe_extract_archive
# ---------------------------------------------------------------------------


def _make_tar(path: Path, files: dict[str, bytes]) -> None:
    with tarfile.open(str(path), "w:gz") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def test_safe_extract_archive_extracts_files(tmp_path: Path) -> None:
    """Regular files are extracted into target_dir."""
    archive = tmp_path / "test.tar.gz"
    _make_tar(archive, {"hello.txt": b"hello", "sub/world.txt": b"world"})
    target = tmp_path / "out"
    target.mkdir()
    with tarfile.open(str(archive)) as tf:
        oras_utils.safe_extract_archive(tf, target, "test.tar.gz")
    assert (target / "hello.txt").read_bytes() == b"hello"
    assert (target / "sub" / "world.txt").read_bytes() == b"world"


def test_safe_extract_archive_rejects_path_traversal(tmp_path: Path) -> None:
    """Archives with path traversal entries raise RuntimeError."""
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(str(archive), "w:gz") as tf:
        info = tarfile.TarInfo(name="../../escape.txt")
        info.size = 4
        tf.addfile(info, io.BytesIO(b"oops"))
    target = tmp_path / "out"
    target.mkdir()
    with tarfile.open(str(archive)) as tf:
        with pytest.raises(RuntimeError, match="unsafe path"):
            oras_utils.safe_extract_archive(tf, target, "bad.tar.gz")


def test_safe_extract_archive_rejects_symlinks(tmp_path: Path) -> None:
    """Archives with symlink entries raise RuntimeError."""
    archive = tmp_path / "sym.tar.gz"
    with tarfile.open(str(archive), "w:gz") as tf:
        info = tarfile.TarInfo(name="link")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tf.addfile(info)
    target = tmp_path / "out"
    target.mkdir()
    with tarfile.open(str(archive)) as tf:
        with pytest.raises(RuntimeError, match="unsupported entry type"):
            oras_utils.safe_extract_archive(tf, target, "sym.tar.gz")


# ---------------------------------------------------------------------------
# os_arch_dir
# ---------------------------------------------------------------------------


def test_os_arch_dir_darwin(tmp_path: Path) -> None:
    result = oras_utils.os_arch_dir(
        "darwin",
        "arm64",
        mac_windows_base=tmp_path / "unsigned",
        linux_base=tmp_path / "linux",
    )
    assert result == tmp_path / "unsigned" / "macos" / "arm64"


def test_os_arch_dir_windows(tmp_path: Path) -> None:
    result = oras_utils.os_arch_dir(
        "windows",
        "amd64",
        mac_windows_base=tmp_path / "unsigned",
        linux_base=tmp_path / "linux",
    )
    assert result == tmp_path / "unsigned" / "windows" / "amd64"


def test_os_arch_dir_linux(tmp_path: Path) -> None:
    result = oras_utils.os_arch_dir(
        "linux", "amd64", mac_windows_base=tmp_path / "unsigned", linux_base=tmp_path / "linux"
    )
    assert result == tmp_path / "linux" / "amd64"


def test_os_arch_dir_unknown_returns_none(tmp_path: Path) -> None:
    result = oras_utils.os_arch_dir(
        "freebsd",
        "amd64",
        mac_windows_base=tmp_path / "unsigned",
        linux_base=tmp_path / "linux",
    )
    assert result is None


# ---------------------------------------------------------------------------
# oras_login
# ---------------------------------------------------------------------------


def test_oras_login_passes_password_via_stdin() -> None:
    """Password is piped via stdin, not on the command line."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = Mock(returncode=0)
        oras_utils.oras_login("quay.io", "myuser", "mypass")
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert "mypass" not in cmd
    assert mock_run.call_args[1]["input"] == "mypass"
    assert "--password-stdin" in cmd


# ---------------------------------------------------------------------------
# oras_push
# ---------------------------------------------------------------------------


def test_oras_push_returns_digest(tmp_path: Path) -> None:
    """Digest is parsed from oras push output."""
    with patch("subprocess.check_output", return_value="Digest: sha256:abc123\n"):
        digest = oras_utils.oras_push("quay.io/org/repo:tag", tmp_path, "macos", "mycomp")
    assert digest == "sha256:abc123"


def test_oras_push_raises_on_missing_digest(tmp_path: Path) -> None:
    """RuntimeError is raised when digest cannot be parsed from oras output."""
    with patch("subprocess.check_output", return_value="no digest here\n"):
        with pytest.raises(RuntimeError, match="Could not extract digest"):
            oras_utils.oras_push("quay.io/org/repo:tag", tmp_path, "macos", "mycomp")
