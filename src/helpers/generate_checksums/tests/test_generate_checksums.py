"""Tests for generate_checksums.py."""

from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.helpers import generate_checksums

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _setup_checksum_creds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "checksum_creds"
    mount.mkdir()
    (mount / "user").write_text("csuser")
    (mount / "host").write_text("cshost.example.com")
    (mount / "fingerprint").write_text("ssh-rsa AAAA...")
    (mount / "keytab").write_bytes(base64.b64encode(b"fake-keytab"))
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "CHECKSUM_CREDENTIALS_MOUNT", mount
    )
    return mount


def _make_ready_dir(base: Path, name: str, files: dict[str, bytes]) -> Path:
    d = base / name / "ready_for_distribution"
    d.mkdir(parents=True)
    for fname, content in files.items():
        (d / fname).write_bytes(content)
    return d


# ---------------------------------------------------------------------------
# file_utils.sha256 (shared helper, tested in test_file.py)
# ---------------------------------------------------------------------------


def test_sha256sum_correct(tmp_path: Path) -> None:
    """SHA-256 hex digest matches hashlib reference for a known file."""
    import file as file_utils

    f = tmp_path / "data.bin"
    content = b"hello world"
    f.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert file_utils.sha256(f) == expected


# ---------------------------------------------------------------------------
# _kinit
# ---------------------------------------------------------------------------


def test_kinit_calls_kinit_with_keytab(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_kinit passes the correct principal (user@REALM) to kinit_with_retry."""
    monkeypatch.setenv("HOME", str(tmp_path))
    calls = []

    def fake_kinit_with_retry(princ, keytab, extra_env, **kwargs):
        calls.append(princ)

    with mock.patch(
        "release_service_utils.helpers.authentication.kinit_with_retry",
        side_effect=fake_kinit_with_retry,
    ):
        generate_checksums.generate_checksums._kinit(
            "user", "REALM.COM", base64.b64encode(b"fakekey")
        )

    assert calls == ["user@REALM.COM"]


def test_kinit_cleans_up_keytab(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Temporary keytab file is deleted from disk after kinit completes."""
    monkeypatch.setenv("HOME", str(tmp_path))
    created_keytabs = []

    orig_write_bytes = Path.write_bytes

    def track_write(self, data):
        if str(self).endswith(".keytab"):
            created_keytabs.append(self)
        return orig_write_bytes(self, data)

    with (
        mock.patch("release_service_utils.helpers.authentication.kinit_with_retry"),
        mock.patch.object(Path, "write_bytes", track_write),
    ):
        generate_checksums.generate_checksums._kinit("user", "REALM", base64.b64encode(b"key"))

    # keytab file should be deleted after kinit
    for p in created_keytabs:
        assert not p.exists()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def _mock_subprocess_for_run(monkeypatch, first_ready_dir: Path) -> list:
    """Patch subprocess.run to simulate SSH/SCP by writing the .sig/.gpg files."""
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        cmd_list = cmd if isinstance(cmd, list) else [str(cmd)]
        cmd_str = " ".join(str(c) for c in cmd_list)
        # Simulate scp of .sig and .gpg files
        if "sha256sum.txt.sig" in cmd_str and cmd_list and cmd_list[0] == "scp":
            (first_ready_dir / "sha256sum.txt.sig").write_bytes(b"SIG")
        if "sha256sum.txt.gpg" in cmd_str and cmd_list and cmd_list[0] == "scp":
            (first_ready_dir / "sha256sum.txt.gpg").write_bytes(b"GPG")
        return mock.MagicMock(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    return calls


def _patch_checksum_ssh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch chmod calls that target hardcoded /tmp/.ssh paths."""
    monkeypatch.setattr("pathlib.Path.chmod", lambda self, mode, **kw: None)


def test_run_generates_checksums(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() creates sha256sum.txt listing all archives in ready_for_distribution."""
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "CONTENT_DIR", tmp_path / "artifacts"
    )
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "SHARED_DIR", tmp_path / "shared"
    )
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setenv("AUTHOR", "testuser")
    monkeypatch.setenv("SIGNING_KEY_NAME", "testkey")
    _setup_checksum_creds(tmp_path, monkeypatch)
    _patch_checksum_ssh(monkeypatch)

    ready_dir = _make_ready_dir(
        tmp_path / "artifacts",
        "prod",
        {"binary-linux-amd64.tar.gz": b"archive content"},
    )

    with mock.patch.object(generate_checksums.generate_checksums, "_kinit"):
        _mock_subprocess_for_run(monkeypatch, ready_dir)
        generate_checksums.run("IPA.REDHAT.COM", "uid-123")

    assert (ready_dir / "sha256sum.txt").exists()
    content = (ready_dir / "sha256sum.txt").read_text()
    assert "binary-linux-amd64.tar.gz" in content


def test_run_uses_shared_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() prefers the shared snapshot.json over the SNAPSHOT_JSON env var."""
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "CONTENT_DIR", tmp_path / "artifacts"
    )
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "SHARED_DIR", tmp_path / "shared"
    )
    (tmp_path / "shared").mkdir()

    modified_snapshot = {"components": [{"name": "prod"}]}
    (tmp_path / "shared" / "snapshot.json").write_text(json.dumps(modified_snapshot))
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "ignored"}]}))
    monkeypatch.setenv("AUTHOR", "testuser")
    monkeypatch.setenv("SIGNING_KEY_NAME", "testkey")
    _setup_checksum_creds(tmp_path, monkeypatch)
    _patch_checksum_ssh(monkeypatch)

    ready_dir = _make_ready_dir(
        tmp_path / "artifacts",
        "prod",
        {"archive.tar.gz": b"data"},
    )

    with mock.patch.object(generate_checksums.generate_checksums, "_kinit"):
        _mock_subprocess_for_run(monkeypatch, ready_dir)
        generate_checksums.run("IPA.REDHAT.COM", "uid-123")

    assert (ready_dir / "sha256sum.txt").exists()


def test_run_raises_on_no_components(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """RuntimeError is raised when no ready_for_distribution directory exists."""
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "CONTENT_DIR", tmp_path / "artifacts"
    )
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "SHARED_DIR", tmp_path / "shared"
    )
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setenv("AUTHOR", "testuser")
    monkeypatch.setenv("SIGNING_KEY_NAME", "testkey")
    _setup_checksum_creds(tmp_path, monkeypatch)
    _patch_checksum_ssh(monkeypatch)
    # No ready_for_distribution dir created

    with (
        mock.patch.object(generate_checksums.generate_checksums, "_kinit"),
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)),
    ):
        with pytest.raises(RuntimeError, match="No archives"):
            generate_checksums.run("IPA.REDHAT.COM", "uid-123")


def test_run_raises_on_no_archives(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """RuntimeError is raised when ready_for_distribution exists but contains no archives."""
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "CONTENT_DIR", tmp_path / "artifacts"
    )
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "SHARED_DIR", tmp_path / "shared"
    )
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setenv("AUTHOR", "testuser")
    monkeypatch.setenv("SIGNING_KEY_NAME", "testkey")
    _setup_checksum_creds(tmp_path, monkeypatch)
    _patch_checksum_ssh(monkeypatch)

    # Empty ready_for_distribution dir
    ready_dir = tmp_path / "artifacts" / "prod" / "ready_for_distribution"
    ready_dir.mkdir(parents=True)

    with (
        mock.patch.object(generate_checksums.generate_checksums, "_kinit"),
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)),
    ):
        with pytest.raises(RuntimeError, match="No archives"):
            generate_checksums.run("IPA.REDHAT.COM", "uid-123")


def test_run_excludes_sha256sum_from_checksums(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """sha256sum.txt files themselves should not be checksummed."""
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "CONTENT_DIR", tmp_path / "artifacts"
    )
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "SHARED_DIR", tmp_path / "shared"
    )
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setenv("AUTHOR", "testuser")
    monkeypatch.setenv("SIGNING_KEY_NAME", "testkey")
    _setup_checksum_creds(tmp_path, monkeypatch)
    _patch_checksum_ssh(monkeypatch)

    ready_dir = _make_ready_dir(
        tmp_path / "artifacts",
        "prod",
        {"archive.tar.gz": b"data", "sha256sum.txt": b"old checksum"},
    )

    with mock.patch.object(generate_checksums.generate_checksums, "_kinit"):
        _mock_subprocess_for_run(monkeypatch, ready_dir)
        generate_checksums.run("IPA.REDHAT.COM", "uid-123")

    content = (ready_dir / "sha256sum.txt").read_text()
    assert "archive.tar.gz" in content
    # sha256sum.txt itself should not appear as a file being checksummed
    lines = [line for line in content.strip().splitlines() if line]
    assert all("sha256sum.txt" not in line.split("  ")[-1] for line in lines)


def test_run_cleans_up_remote_dir_on_signing_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Remote directory on checksum host is removed even when signing fails."""
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "CONTENT_DIR", tmp_path / "artifacts"
    )
    monkeypatch.setattr(
        generate_checksums.generate_checksums, "SHARED_DIR", tmp_path / "shared"
    )
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setenv("AUTHOR", "testuser")
    monkeypatch.setenv("SIGNING_KEY_NAME", "testkey")
    _setup_checksum_creds(tmp_path, monkeypatch)
    _patch_checksum_ssh(monkeypatch)
    _make_ready_dir(
        tmp_path / "artifacts",
        "prod",
        {"binary-linux-amd64.tar.gz": b"archive content"},
    )

    cleanup_calls = []

    def fake_run(cmd, **kwargs):
        cmd_str = " ".join(str(c) for c in cmd)
        if "rm -rf" in cmd_str:
            cleanup_calls.append(cmd)
            return mock.MagicMock(returncode=0)
        if "rpm-sign" in cmd_str and "--clearsign" in cmd_str:
            return mock.MagicMock(returncode=1)
        return mock.MagicMock(returncode=0)

    with (
        mock.patch.object(generate_checksums.generate_checksums, "_kinit"),
        mock.patch("subprocess.run", side_effect=fake_run),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            generate_checksums.run("IPA.REDHAT.COM", "uid-123")

    assert any(
        "rm -rf" in " ".join(str(c) for c in cmd) for cmd in cleanup_calls
    ), "remote cleanup rm -rf should have been called"


# ---------------------------------------------------------------------------
# _run_ssh_command
# ---------------------------------------------------------------------------


def test_run_ssh_command_succeeds_on_first_attempt() -> None:
    """Command succeeds immediately when exit code is 0."""
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)) as m:
        generate_checksums.generate_checksums._run_ssh_command(["ssh", "host", "ls"])
    m.assert_called_once()


def test_run_ssh_command_retries_on_exit_255() -> None:
    """RC 255 triggers a retry; success on second attempt passes."""
    results = iter([mock.MagicMock(returncode=255), mock.MagicMock(returncode=0)])
    with (
        mock.patch("subprocess.run", side_effect=results) as m,
        mock.patch("time.sleep"),
    ):
        generate_checksums.generate_checksums._run_ssh_command(["ssh", "host", "ls"])
    assert m.call_count == 2


def test_run_ssh_command_raises_after_max_retries_on_255() -> None:
    """RC 255 on all attempts raises _SSHConnectionError."""
    with (
        mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=255)),
        mock.patch("time.sleep"),
        pytest.raises(generate_checksums.generate_checksums._SSHConnectionError),
    ):
        generate_checksums.generate_checksums._run_ssh_command(
            ["ssh", "host", "ls"], max_attempts=3
        )


def test_run_ssh_command_does_not_retry_on_other_rc() -> None:
    """Non-255 non-zero RC raises CalledProcessError immediately, no retry."""
    with (
        mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=1)) as m,
        pytest.raises(subprocess.CalledProcessError),
    ):
        generate_checksums.generate_checksums._run_ssh_command(["ssh", "host", "false"])
    m.assert_called_once()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    """main() returns 0 and calls run() with realm and pipeline-run-uid."""
    with mock.patch.object(generate_checksums.generate_checksums, "run") as mock_run:
        rc = generate_checksums.main(
            [
                "generate_checksums.py",
                "--kerberos-realm",
                "REALM.COM",
                "--pipeline-run-uid",
                "uid",
            ]
        )
    assert rc == 0
    mock_run.assert_called_once_with("REALM.COM", "uid")


def test_main_exception_returns_error() -> None:
    """main() returns 1 when run() raises an exception."""
    with mock.patch.object(generate_checksums, "run", side_effect=RuntimeError("kinit fail")):
        rc = generate_checksums.main(
            [
                "generate_checksums.py",
                "--kerberos-realm",
                "REALM.COM",
                "--pipeline-run-uid",
                "uid",
            ]
        )
    assert rc == 1
