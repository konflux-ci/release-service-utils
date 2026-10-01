"""Tests for gssapi_ssh."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.helpers.gssapi_ssh import gssapi_ssh


def test_remote_shell_path_quotes_home_for_ssh() -> None:
    """SSH remote commands expand ``$HOME`` in a shell."""
    assert gssapi_ssh.remote_shell_path("uid", "kmods.tar.gz") == ('"$HOME"/uid/kmods.tar.gz')
    assert gssapi_ssh.remote_shell_path("uid with space") == ('"$HOME"/' + "'uid with space'")


def test_remote_scp_path_uses_tilde_and_does_not_quote() -> None:
    """SCP expands ``~`` and treats quotes as part of the filename."""
    assert gssapi_ssh.remote_scp_path("uid", "kmods.tar.gz") == "~/uid/kmods.tar.gz"
    assert "$HOME" not in gssapi_ssh.remote_scp_path("uid", "signed.tar.gz")


def test_install_known_hosts_sets_permissions(tmp_path: Path) -> None:
    """known_hosts is a copy of the fingerprint, with restricted modes."""
    fingerprint = tmp_path / "fingerprint"
    fingerprint.write_text("ssh-rsa AAAA host\n", encoding="utf-8")
    ssh_dir = tmp_path / ".ssh"

    known_hosts = gssapi_ssh.install_known_hosts(fingerprint, ssh_dir)

    assert known_hosts.read_text(encoding="utf-8") == "ssh-rsa AAAA host\n"
    assert known_hosts.stat().st_mode & 0o777 == 0o600
    assert ssh_dir.stat().st_mode & 0o777 == 0o700


def test_install_known_hosts_replaces_existing_file(tmp_path: Path) -> None:
    """A second install overwrites an existing known_hosts file."""
    fingerprint = tmp_path / "fingerprint"
    fingerprint.write_text("new-host-key\n", encoding="utf-8")
    ssh_dir = tmp_path / ".ssh"
    ssh_dir.mkdir()
    (ssh_dir / "known_hosts").write_text("old\n", encoding="utf-8")

    known_hosts = gssapi_ssh.install_known_hosts(fingerprint, ssh_dir)

    assert known_hosts.read_text(encoding="utf-8") == "new-host-key\n"


def test_ssh_options_without_identities_only(tmp_path: Path) -> None:
    """Default options enable GSSAPI and point at the known_hosts file."""
    known_hosts = tmp_path / "known_hosts"
    options = gssapi_ssh.ssh_options(known_hosts)
    assert options == [
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GSSAPIAuthentication=yes",
        "-o",
        "GSSAPIDelegateCredentials=yes",
    ]


def test_ssh_options_identities_only(tmp_path: Path) -> None:
    """identities_only adds IdentitiesOnly=yes."""
    known_hosts = tmp_path / "known_hosts"
    options = gssapi_ssh.ssh_options(known_hosts, identities_only=True)
    assert options[-2:] == ["-o", "IdentitiesOnly=yes"]


def test_run_ssh_succeeds_on_first_attempt() -> None:
    """A zero exit returns the completed process without retrying."""
    completed = subprocess.CompletedProcess(["ssh"], 0, stdout="ok", stderr="")
    with mock.patch(
        "release_service_utils.helpers.gssapi_ssh.gssapi_ssh.subprocess.run",
        return_value=completed,
    ) as run:
        result = gssapi_ssh.run_ssh(["ssh", "host", "ls"])
    run.assert_called_once_with(["ssh", "host", "ls"], input=None, text=False, check=False)
    assert result is completed


def test_run_ssh_passes_stdin_and_does_not_log_it(caplog: pytest.LogCaptureFixture) -> None:
    """Child stdin is forwarded and omitted from the retry warning."""
    secret = "sign-key-secret"
    results = iter(
        [
            subprocess.CompletedProcess(["ssh"], 255),
            subprocess.CompletedProcess(["ssh"], 0, stdout="", stderr=""),
        ]
    )
    with (
        mock.patch(
            "release_service_utils.helpers.gssapi_ssh.gssapi_ssh.subprocess.run",
            side_effect=results,
        ) as run,
        mock.patch("time.sleep"),
        caplog.at_level(logging.WARNING),
    ):
        gssapi_ssh.run_ssh(["ssh", "host", "bash", "-s"], stdin=secret)
    assert run.call_args_list[0].kwargs["input"] == secret
    assert run.call_args_list[0].kwargs["text"] is True
    assert secret not in caplog.text
    assert "exit 255" in caplog.text


def test_run_ssh_retries_on_exit_255() -> None:
    """Exit 255 is retried and a later success is returned."""
    results = iter(
        [
            subprocess.CompletedProcess(["ssh"], 255),
            subprocess.CompletedProcess(["ssh"], 0),
        ]
    )
    with (
        mock.patch(
            "release_service_utils.helpers.gssapi_ssh.gssapi_ssh.subprocess.run",
            side_effect=results,
        ) as run,
        mock.patch("time.sleep"),
    ):
        gssapi_ssh.run_ssh(["ssh", "host", "ls"])
    assert run.call_count == 2


def test_run_ssh_raises_after_max_retries_on_255() -> None:
    """Exit 255 on every attempt raises SSHConnectionError."""
    with (
        mock.patch(
            "release_service_utils.helpers.gssapi_ssh.gssapi_ssh.subprocess.run",
            return_value=subprocess.CompletedProcess(["ssh"], 255),
        ),
        mock.patch("time.sleep"),
        pytest.raises(gssapi_ssh.SSHConnectionError),
    ):
        gssapi_ssh.run_ssh(["ssh", "host", "ls"], max_attempts=3)


def test_run_ssh_does_not_retry_on_other_rc() -> None:
    """A non-255 failure raises CalledProcessError immediately."""
    with (
        mock.patch(
            "release_service_utils.helpers.gssapi_ssh.gssapi_ssh.subprocess.run",
            return_value=subprocess.CompletedProcess(["ssh"], 1),
        ) as run,
        pytest.raises(subprocess.CalledProcessError),
    ):
        gssapi_ssh.run_ssh(["ssh", "host", "false"])
    run.assert_called_once()
