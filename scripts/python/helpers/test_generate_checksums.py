"""Tests for generate_checksums.py."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from unittest import mock

import pytest

import generate_checksums

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
    monkeypatch.setattr(generate_checksums, "CHECKSUM_CREDENTIALS_MOUNT", mount)
    return mount


def _make_ready_dir(base: Path, name: str, files: dict[str, bytes]) -> Path:
    d = base / name / "ready_for_distribution"
    d.mkdir(parents=True)
    for fname, content in files.items():
        (d / fname).write_bytes(content)
    return d


# ---------------------------------------------------------------------------
# _sha256sum
# ---------------------------------------------------------------------------


def test_sha256sum_correct(tmp_path: Path) -> None:
    f = tmp_path / "data.bin"
    content = b"hello world"
    f.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert generate_checksums._sha256sum(f) == expected


# ---------------------------------------------------------------------------
# _kinit
# ---------------------------------------------------------------------------


def test_kinit_calls_kinit_with_keytab(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    calls = []

    def fake_check_call(cmd, **kwargs):
        calls.append(cmd)

    with mock.patch("subprocess.check_call", side_effect=fake_check_call):
        generate_checksums._kinit("user@REALM.COM", "REALM.COM", base64.b64encode(b"fakekey"))

    assert any("kinit" in str(c) for c in calls)


def test_kinit_cleans_up_keytab(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    created_keytabs = []

    orig_write_bytes = Path.write_bytes

    def track_write(self, data):
        if str(self).endswith(".keytab"):
            created_keytabs.append(self)
        return orig_write_bytes(self, data)

    with (
        mock.patch("subprocess.check_call"),
        mock.patch.object(Path, "write_bytes", track_write),
    ):
        generate_checksums._kinit("user", "REALM", base64.b64encode(b"key"))

    # keytab file should be deleted after kinit
    for p in created_keytabs:
        assert not p.exists()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def _mock_subprocess_for_run(monkeypatch, first_ready_dir: Path) -> list:
    """Patch subprocess.check_call to simulate SSH/SCP by writing the .sig/.gpg files."""
    calls = []

    def fake_check_call(cmd, **kwargs):
        calls.append(cmd)
        cmd_list = cmd if isinstance(cmd, list) else [str(cmd)]
        cmd_str = " ".join(str(c) for c in cmd_list)
        # Simulate scp of .sig and .gpg files
        if "sha256sum.txt.sig" in cmd_str and cmd_list and cmd_list[0] == "scp":
            (first_ready_dir / "sha256sum.txt.sig").write_bytes(b"SIG")
        if "sha256sum.txt.gpg" in cmd_str and cmd_list and cmd_list[0] == "scp":
            (first_ready_dir / "sha256sum.txt.gpg").write_bytes(b"GPG")

    monkeypatch.setattr("subprocess.check_call", fake_check_call)
    return calls


def _patch_checksum_ssh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch chmod calls that target hardcoded /tmp/.ssh paths."""
    monkeypatch.setattr("pathlib.Path.chmod", lambda self, mode, **kw: None)


def test_run_generates_checksums(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(generate_checksums, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(generate_checksums, "SHARED_DIR", tmp_path / "shared")
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

    with mock.patch.object(generate_checksums, "_kinit"):
        _mock_subprocess_for_run(monkeypatch, ready_dir)
        generate_checksums.run("IPA.REDHAT.COM", "uid-123")

    assert (ready_dir / "sha256sum.txt").exists()
    content = (ready_dir / "sha256sum.txt").read_text()
    assert "binary-linux-amd64.tar.gz" in content


def test_run_uses_shared_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(generate_checksums, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(generate_checksums, "SHARED_DIR", tmp_path / "shared")
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

    with mock.patch.object(generate_checksums, "_kinit"):
        _mock_subprocess_for_run(monkeypatch, ready_dir)
        generate_checksums.run("IPA.REDHAT.COM", "uid-123")

    assert (ready_dir / "sha256sum.txt").exists()


def test_run_raises_on_no_components(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(generate_checksums, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(generate_checksums, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setenv("AUTHOR", "testuser")
    monkeypatch.setenv("SIGNING_KEY_NAME", "testkey")
    _setup_checksum_creds(tmp_path, monkeypatch)
    _patch_checksum_ssh(monkeypatch)
    # No ready_for_distribution dir created

    with (
        mock.patch.object(generate_checksums, "_kinit"),
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.check_call"),
    ):
        with pytest.raises(RuntimeError, match="No archives"):
            generate_checksums.run("IPA.REDHAT.COM", "uid-123")


def test_run_raises_on_no_archives(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(generate_checksums, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(generate_checksums, "SHARED_DIR", tmp_path / "shared")
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
        mock.patch.object(generate_checksums, "_kinit"),
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.check_call"),
    ):
        with pytest.raises(RuntimeError, match="No archives"):
            generate_checksums.run("IPA.REDHAT.COM", "uid-123")


def test_run_excludes_sha256sum_from_checksums(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """sha256sum.txt files themselves should not be checksummed."""
    monkeypatch.setattr(generate_checksums, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(generate_checksums, "SHARED_DIR", tmp_path / "shared")
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

    with mock.patch.object(generate_checksums, "_kinit"):
        _mock_subprocess_for_run(monkeypatch, ready_dir)
        generate_checksums.run("IPA.REDHAT.COM", "uid-123")

    content = (ready_dir / "sha256sum.txt").read_text()
    assert "archive.tar.gz" in content
    # sha256sum.txt itself should not appear as a file being checksummed
    lines = [line for line in content.strip().splitlines() if line]
    assert all("sha256sum.txt" not in line.split("  ")[-1] for line in lines)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    with mock.patch.object(generate_checksums, "run") as mock_run:
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
