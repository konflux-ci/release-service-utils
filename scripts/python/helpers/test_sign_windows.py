"""Tests for sign_windows.py."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest import mock

import pytest

import sign_windows

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _setup_mounts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ssh_key = tmp_path / "win_ssh"
    ssh_key.mkdir()
    (ssh_key / "windows_id_rsa").write_text("FAKE_KEY")
    (ssh_key / "windows_fingerprint").write_text("FAKE_FP")
    monkeypatch.setattr(sign_windows, "WINDOWS_SSH_KEY_MOUNT", ssh_key)

    creds = tmp_path / "win_creds"
    creds.mkdir()
    (creds / "username").write_text("winuser")
    (creds / "port").write_text("22")
    (creds / "host").write_text("win-host.example.com")
    (creds / "cert_thumbprint").write_text("AABBCCDD")
    monkeypatch.setattr(sign_windows, "WINDOWS_CREDS_MOUNT", creds)

    quay = tmp_path / "quay"
    quay.mkdir()
    (quay / "username").write_text("quser")
    (quay / "password").write_text("qpass")
    monkeypatch.setattr(sign_windows, "QUAY_SECRET_MOUNT", quay)


# ---------------------------------------------------------------------------
# _build_batch_script
# ---------------------------------------------------------------------------


def test_build_batch_script_contains_signtool() -> None:
    """Generated batch script includes signtool sign/verify, oras pull/push, and key values."""
    script = sign_windows._build_batch_script(
        quay_url="quay.io/org",
        quay_user="user",
        quay_pass="pass",
        component_name="prod",
        unsigned_digest="sha256:unsigned",
        pipeline_run_uid="uid-123",
        windows_temp_dir="uid-123_prod",
    )
    assert "signtool sign" in script
    assert "signtool verify" in script
    assert "oras pull" in script
    assert "oras push" in script
    assert "sha256:unsigned" in script
    assert "uid-123-windows" in script
    assert "Red Hat" in script


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def _patch_ssh_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch chmod on hardcoded /tmp/.ssh paths."""
    monkeypatch.setattr(
        "pathlib.Path.chmod",
        lambda self, mode, **kw: None,
    )


def test_run_skips_component_without_has_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A component without a has_windows flag file is skipped with an INFO log message."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    # No has_windows

    with (
        caplog.at_level(logging.INFO, logger="sign_windows"),
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run"),
    ):
        sign_windows.run("quay.io/org", "uid-123")

    assert "skipping Windows signing" in caplog.text


def test_run_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Signed digest is written to signed_windows_digest.txt after a successful signing run."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_windows").touch()
    (comp_dir / "unsigned_windows_digest.txt").write_text("sha256:unsigned")

    def fake_subprocess_run(cmd, **kwargs):
        if cmd[0] == "ssh" and "Remove-Item" not in " ".join(str(a) for a in cmd):
            m = mock.Mock(returncode=0)
            m.stdout = (
                "Signing completed\n"
                "Pushed [registry] quay.io/org/signed/prod:uid-123-windows\n"
                "ArtifactType: application/vnd.unknown.artifact.v1\n"
                "Digest: sha256:signed\n"
            )
            m.stderr = ""
            return m
        return mock.Mock(returncode=0, stdout="", stderr="")

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.check_call"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        sign_windows.run("quay.io/org", "uid-123")

    digest = (comp_dir / "signed_windows_digest.txt").read_text()
    assert digest == "sha256:signed"


def test_run_raises_on_signing_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RuntimeError with 'Windows signing failed' is raised when the SSH command fails."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_windows").touch()
    (comp_dir / "unsigned_windows_digest.txt").write_text("sha256:unsigned")

    def fake_subprocess_run(cmd, **kwargs):
        # Fail the SSH signing call (not the cleanup call with Remove-Item)
        if cmd[0] == "ssh" and "Remove-Item" not in " ".join(str(a) for a in cmd):
            return mock.Mock(returncode=1)
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.check_call"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        with pytest.raises(RuntimeError, match="Windows signing failed"):
            sign_windows.run("quay.io/org", "uid-123")


def test_run_cleans_up_even_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup SSH call (Remove-Item) is always made even when signing fails."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_windows").touch()
    (comp_dir / "unsigned_windows_digest.txt").write_text("sha256:unsigned")

    ssh_calls = []

    def fake_subprocess_run(cmd, **kwargs):
        if cmd[0] == "ssh":
            ssh_calls.append(list(cmd))
        # Fail the signing SSH call but let cleanup succeed
        if cmd[0] == "ssh" and "Remove-Item" not in " ".join(str(a) for a in cmd):
            return mock.Mock(returncode=1)
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.check_call"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        with pytest.raises(RuntimeError):
            sign_windows.run("quay.io/org", "uid-123")

    # cleanup ssh call should have been made (Remove-Item call)
    cleanup_calls = [c for c in ssh_calls if any("Remove-Item" in str(a) for a in c)]
    assert len(cleanup_calls) > 0


# ---------------------------------------------------------------------------
# _run_custom_script (custom signing script path)
# ---------------------------------------------------------------------------


def test_run_custom_script_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Custom script is invoked via SSH with PowerShell env vars and digest is SCP'd back."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_windows").touch()
    (comp_dir / "unsigned_windows_digest.txt").write_text("sha256:unsigned")

    signed_digest_content = "sha256:custom-signed"
    calls: list[list[str]] = []

    def fake_subprocess_run(cmd, **kwargs):
        calls.append((list(cmd), kwargs))
        if cmd[0] == "scp" and "signed_digest_" in " ".join(cmd):
            (comp_dir / "signed_windows_digest.txt").write_text(signed_digest_content)
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        sign_windows.run(
            "quay.io/org",
            "uid-123",
            signing_script="C:/Scripts/sign.bat",
            signing_args=["--profile", "internal"],
        )

    assert (comp_dir / "signed_windows_digest.txt").read_text() == signed_digest_content

    ssh_calls = [(c, kw) for c, kw in calls if c[0] == "ssh"]
    assert len(ssh_calls) >= 1
    ssh_cmd, ssh_kwargs = ssh_calls[0]
    assert "powershell" in ssh_cmd
    assert "-Command" in ssh_cmd
    assert ssh_cmd[-1] == "-"
    stdin_script = ssh_kwargs.get("input", "")
    assert "$env:QUAY_USER=" in stdin_script
    assert "$env:QUAY_PASS=" in stdin_script
    assert "$env:UNSIGNED_REF=" in stdin_script
    assert "$env:SIGNED_REF=" in stdin_script
    assert "$env:OUTPUT_DIGEST=" in stdin_script
    assert "$env:WIN_CERT_THUMBPRINT='AABBCCDD'" in stdin_script
    assert "C:/Scripts/sign.bat" in stdin_script
    assert "'--profile'" in stdin_script
    assert "'internal'" in stdin_script


def test_run_custom_script_with_dest_quay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Separate dest quay creds are used for QUAY_DEST_USER/PASS and SIGNED_REF."""
    monkeypatch.setenv(
        "SNAPSHOT_JSON",
        json.dumps(
            {"components": [{"name": "prod", "source": {"git": {"revision": "abc123def456"}}}]}
        ),
    )
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    dest_quay = tmp_path / "dest_quay"
    dest_quay.mkdir()
    (dest_quay / "username").write_text("dest-user")
    (dest_quay / "password").write_text("dest-pass")
    monkeypatch.setattr(sign_windows, "DEST_QUAY_SECRET_MOUNT", dest_quay)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_windows").touch()
    (comp_dir / "unsigned_windows_digest.txt").write_text("sha256:unsigned")

    calls: list[list[str]] = []

    def fake_subprocess_run(cmd, **kwargs):
        calls.append((list(cmd), kwargs))
        if cmd[0] == "scp" and "signed_digest_" in " ".join(cmd):
            (comp_dir / "signed_windows_digest.txt").write_text("sha256:signed")
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        sign_windows.run(
            "quay.io/org",
            "uid-123",
            signing_script="C:/Scripts/sign.bat",
            dest_quay_url="quay.io/internal",
            origin="my-tenant",
        )

    ssh_calls = [(c, kw) for c, kw in calls if c[0] == "ssh"]
    assert len(ssh_calls) >= 1
    ssh_cmd, ssh_kwargs = ssh_calls[0]
    stdin_script = ssh_kwargs.get("input", "")
    assert "$env:QUAY_DEST_USER='dest-user'" in stdin_script
    assert "$env:QUAY_DEST_PASS='dest-pass'" in stdin_script
    assert "$env:SIGNED_REF='quay.io/internal/my-tenant/prod:" in stdin_script
    assert "$env:UNSIGNED_REF='quay.io/org/unsigned/" in stdin_script


def test_run_custom_script_dest_quay_falls_back_to_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When DEST_QUAY_SECRET_MOUNT is absent, dest creds fall back to source creds."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    # Point DEST_QUAY_SECRET_MOUNT at a non-existent directory
    monkeypatch.setattr(sign_windows, "DEST_QUAY_SECRET_MOUNT", tmp_path / "no_such_mount")

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_windows").touch()
    (comp_dir / "unsigned_windows_digest.txt").write_text("sha256:unsigned")

    calls: list[list[str]] = []

    def fake_subprocess_run(cmd, **kwargs):
        calls.append((list(cmd), kwargs))
        if cmd[0] == "scp" and "signed_digest_" in " ".join(cmd):
            (comp_dir / "signed_windows_digest.txt").write_text("sha256:signed")
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        sign_windows.run(
            "quay.io/org",
            "uid-123",
            signing_script="C:/Scripts/sign.bat",
            dest_quay_url="quay.io/internal",
        )

    ssh_calls = [(c, kw) for c, kw in calls if c[0] == "ssh"]
    assert len(ssh_calls) >= 1
    ssh_cmd, ssh_kwargs = ssh_calls[0]
    stdin_script = ssh_kwargs.get("input", "")
    # Falls back to source credentials (quser / qpass from _setup_mounts)
    assert "$env:QUAY_DEST_USER='quser'" in stdin_script
    assert "$env:QUAY_DEST_PASS='qpass'" in stdin_script


def test_run_custom_script_ssh_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RuntimeError is raised when the custom Windows script SSH command fails."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "prod"}]}))
    monkeypatch.setattr(sign_windows, "CONTENT_DIR", tmp_path)
    _setup_mounts(tmp_path, monkeypatch)
    _patch_ssh_setup(monkeypatch)

    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_windows").touch()
    (comp_dir / "unsigned_windows_digest.txt").write_text("sha256:unsigned")

    call_count = 0

    def fake_subprocess_run(cmd, **kwargs):
        nonlocal call_count
        call_count += 1
        if cmd[0] == "ssh" and call_count == 1:
            return mock.Mock(returncode=1)
        return mock.Mock(returncode=0)

    with (
        mock.patch("shutil.copy2"),
        mock.patch("subprocess.run", side_effect=fake_subprocess_run),
    ):
        with pytest.raises(RuntimeError, match="Windows signing failed"):
            sign_windows.run(
                "quay.io/org",
                "uid-123",
                signing_script="C:/Scripts/sign.bat",
            )


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


def test_parse_args_requires_quay_url() -> None:
    """SystemExit is raised when --quay-url is omitted."""
    with pytest.raises(SystemExit):
        sign_windows.parse_args(["--pipeline-run-uid", "uid"])


def test_parse_args_requires_pipeline_run_uid() -> None:
    """SystemExit is raised when --pipeline-run-uid is omitted."""
    with pytest.raises(SystemExit):
        sign_windows.parse_args(["--quay-url", "quay.io/org"])


def test_parse_args_ok() -> None:
    """Both required arguments are parsed correctly when provided."""
    args = sign_windows.parse_args(["--quay-url", "quay.io/org", "--pipeline-run-uid", "uid"])
    assert args.quay_url == "quay.io/org"
    assert args.pipeline_run_uid == "uid"


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    """main() returns 0 and passes quay URL and pipeline-run-uid to run()."""
    with mock.patch.object(sign_windows, "run") as mock_run:
        rc = sign_windows.main(
            ["sign_windows.py", "--quay-url", "quay.io/org", "--pipeline-run-uid", "uid"]
        )
    assert rc == 0
    mock_run.assert_called_once_with("quay.io/org", "uid")


def test_main_exception_returns_error() -> None:
    """main() returns 1 when run() raises an exception."""
    with mock.patch.object(sign_windows, "run", side_effect=RuntimeError("ssh fail")):
        rc = sign_windows.main(
            ["sign_windows.py", "--quay-url", "quay.io/org", "--pipeline-run-uid", "uid"]
        )
    assert rc == 1
