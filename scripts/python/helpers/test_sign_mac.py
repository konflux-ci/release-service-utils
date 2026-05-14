"""Tests for sign_mac.py."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest import mock

import pytest

import sign_mac

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _setup_mounts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ssh_key = tmp_path / "mac_ssh"
    ssh_key.mkdir()
    (ssh_key / "mac_id_rsa").write_text("FAKE_KEY")
    (ssh_key / "mac_fingerprint").write_text("FAKE_FP")
    monkeypatch.setattr(sign_mac, "MAC_SSH_KEY_MOUNT", ssh_key)

    host_creds = tmp_path / "mac_host"
    host_creds.mkdir()
    (host_creds / "username").write_text("macuser")
    (host_creds / "host").write_text("mac-host.example.com")
    monkeypatch.setattr(sign_mac, "MAC_HOST_CREDS_MOUNT", host_creds)

    signing_creds = tmp_path / "mac_signing"
    signing_creds.mkdir()
    (signing_creds / "keychain_password").write_text("kpwd")
    (signing_creds / "signing_identity").write_text("My Identity")
    (signing_creds / "apple_id").write_text("dev@example.com")
    (signing_creds / "team_id").write_text("TEAMID123")
    (signing_creds / "app_specific_password").write_text("app-pwd")
    monkeypatch.setattr(sign_mac, "MAC_SIGNING_CREDS_MOUNT", signing_creds)

    quay = tmp_path / "quay"
    quay.mkdir()
    (quay / "username").write_text("quser")
    (quay / "password").write_text("qpass")
    monkeypatch.setattr(sign_mac, "QUAY_SECRET_MOUNT", quay)


# ---------------------------------------------------------------------------
# _build_signing_script
# ---------------------------------------------------------------------------


def test_build_signing_script_contains_key_commands() -> None:
    script = sign_mac._build_signing_script(
        quay_url="quay.io/org",
        quay_user="user",
        quay_pass="pass",
        component_name="prod",
        unsigned_digest="sha256:abc",
        pipeline_run_uid="uid-123",
        temp_dir="/tmp/uid-123_prod",
        binary_path="/tmp/uid-123_prod/unsigned",
        zip_path="/tmp/uid-123_prod/signed_content.zip",
        digest_file="/tmp/uid-123_prod/push_digest.txt",
        keychain_password="kpwd",
        signing_identity="My Identity",
        apple_id="dev@example.com",
        team_id="TEAMID123",
        app_specific_password="app-pwd",
    )
    assert "xcrun codesign" in script
    assert "xcrun notarytool" in script
    assert "oras pull" in script
    assert "oras push" in script
    assert "sha256:abc" in script
    assert "uid-123-mac" in script
    assert "My Identity" in script


# ---------------------------------------------------------------------------
# _ssh_opts
# ---------------------------------------------------------------------------


def test_ssh_opts_returns_list() -> None:
    opts = sign_mac._ssh_opts("/tmp/.ssh/id_rsa", "/tmp/.ssh/known_hosts")
    assert "-i" in opts
    assert "/tmp/.ssh/id_rsa" in opts
    assert "IdentitiesOnly=yes" in opts


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def _patch_ssh_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch all filesystem ops that touch the hardcoded /tmp/.ssh path."""
    monkeypatch.setattr(
        "pathlib.Path.chmod",
        lambda self, mode, **kw: None,
    )


def test_run_skips_component_without_has_mac(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(
        "SNAPSHOT_JSON",
        json.dumps(
            {"components": [{"name": "prod", "containerImage": "q.io/prod@sha256:abc"}]}
        ),
    )
    monkeypatch.setattr(sign_mac, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    # No has_mac file

    with (
        caplog.at_level(logging.INFO, logger="sign_mac"),
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run"),
    ):
        sign_mac.run("quay.io/org", "uid-123")

    assert "skipping Mac signing" in caplog.text


def test_run_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SNAPSHOT_JSON",
        json.dumps({"components": [{"name": "prod"}]}),
    )
    monkeypatch.setattr(sign_mac, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_mac").touch()
    (comp_dir / "unsigned_mac_digest.txt").write_text("sha256:unsigned")

    # signed digest written back
    signed_digest_content = "sha256:signed"

    def fake_subprocess_run(cmd, **kwargs):
        # When it's the scp that copies the digest back, create the file
        if cmd[0] == "scp" and "push_digest.txt" in " ".join(cmd):
            (comp_dir / "signed_mac_digest.txt").write_text(signed_digest_content)
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.check_call"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        sign_mac.run("quay.io/org", "uid-123")

    assert (comp_dir / "signed_mac_digest.txt").read_text() == signed_digest_content


def test_run_raises_on_ssh_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SNAPSHOT_JSON",
        json.dumps({"components": [{"name": "prod"}]}),
    )
    monkeypatch.setattr(sign_mac, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_mac").touch()
    (comp_dir / "unsigned_mac_digest.txt").write_text("sha256:unsigned")

    def fake_subprocess_run(cmd, **kwargs):
        # The first subprocess.run is the SSH signing call; fail it
        if cmd[0] == "ssh" and len(cmd) >= 4 and "bash" in cmd:
            return mock.Mock(returncode=1)
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.check_call"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        with pytest.raises(RuntimeError, match="Mac signing failed"):
            sign_mac.run("quay.io/org", "uid-123")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    with mock.patch.object(sign_mac, "run") as mock_run:
        rc = sign_mac.main(
            ["sign_mac.py", "--quay-url", "quay.io/org", "--pipeline-run-uid", "uid"]
        )
    assert rc == 0
    mock_run.assert_called_once_with("quay.io/org", "uid")


def test_main_exception_returns_error() -> None:
    with mock.patch.object(sign_mac, "run", side_effect=RuntimeError("ssh down")):
        rc = sign_mac.main(
            ["sign_mac.py", "--quay-url", "quay.io/org", "--pipeline-run-uid", "uid"]
        )
    assert rc == 1
