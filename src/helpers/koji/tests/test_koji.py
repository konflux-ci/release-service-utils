"""Tests for the koji helper."""

from __future__ import annotations

import base64
import errno
import logging
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.helpers import koji as koji_helper
from release_service_utils.helpers import tekton
from release_service_utils.helpers.koji import koji as koji_mod

HELPER = "release_service_utils.helpers.koji.koji"


class TestInstallKojiClientConfig:
    """Test tenant koji profile installation."""

    def test_raises_when_source_missing(self, tmp_path: Path) -> None:
        """Raise ValueError when the staged config directory is missing."""
        with pytest.raises(ValueError, match="koji client config source is missing"):
            koji_helper.install_koji_client_config(tmp_path / "missing")

    def test_raises_when_source_is_file(self, tmp_path: Path) -> None:
        """Raise ValueError when the source path is a file."""
        source = tmp_path / "config"
        source.write_text("[koji]\n", encoding="utf-8")
        with pytest.raises(ValueError, match="koji client config source is missing"):
            koji_helper.install_koji_client_config(source)

    def test_empty_directory_is_allowed(self, tmp_path: Path) -> None:
        """Create ~/.koji even when the staged directory has no files."""
        source = tmp_path / "staged"
        source.mkdir()
        koji_helper.install_koji_client_config(source)
        assert (tmp_path / ".koji" / "config.d").is_dir()
        assert not (tmp_path / ".koji" / "config").exists()

    def test_copies_config_and_profiles(self, tmp_path: Path) -> None:
        """Copy the main config and each profile file into ~/.koji."""
        source = tmp_path / "staged"
        config_d = source / "config.d"
        config_d.mkdir(parents=True)
        (source / "config").write_text("[koji]\nserver=https://koji\n", encoding="utf-8")
        (config_d / "stream.conf").write_text("[stream]\n", encoding="utf-8")
        (config_d / "nested").mkdir()

        koji_helper.install_koji_client_config(source)

        dest = tmp_path / ".koji"
        assert (dest / "config").read_text(encoding="utf-8") == (
            "[koji]\nserver=https://koji\n"
        )
        assert (dest / "config.d" / "stream.conf").read_text(encoding="utf-8") == (
            "[stream]\n"
        )
        assert not (dest / "config.d" / "nested").exists()

    def test_skips_when_config_d_is_not_a_directory(self, tmp_path: Path) -> None:
        """Ignore a non-directory config.d entry."""
        source = tmp_path / "staged"
        source.mkdir()
        (source / "config").write_text("[koji]\n", encoding="utf-8")
        (source / "config.d").write_text("not-a-dir", encoding="utf-8")
        koji_helper.install_koji_client_config(source)
        dest = tmp_path / ".koji"
        assert (dest / "config").is_file()
        assert list((dest / "config.d").iterdir()) == []

    def test_follows_symlinks(self, tmp_path: Path) -> None:
        """Copy symlink targets as regular files."""
        real = tmp_path / "real"
        real.mkdir()
        (real / "koji.conf").write_text("[koji]\n", encoding="utf-8")
        (real / "brew.conf").write_text("[brew]\n", encoding="utf-8")
        source = tmp_path / "staged"
        config_d = source / "config.d"
        config_d.mkdir(parents=True)
        (source / "config").symlink_to(real / "koji.conf")
        (config_d / "brew.conf").symlink_to(real / "brew.conf")

        koji_helper.install_koji_client_config(source)

        dest = tmp_path / ".koji"
        config = dest / "config"
        profile = dest / "config.d" / "brew.conf"
        assert config.is_file() and not config.is_symlink()
        assert profile.is_file() and not profile.is_symlink()
        assert config.read_text(encoding="utf-8") == "[koji]\n"
        assert profile.read_text(encoding="utf-8") == "[brew]\n"


class TestRunKojiCmd:
    """Test koji command execution."""

    def test_runs_basic_command(self) -> None:
        """Run a basic koji command with profile and empty plugin paths."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout='{"result": true}', returncode=0)
            result = koji_helper.run_koji_cmd("myprofile", "hello")
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[:3] == ["koji", "--profile=myprofile", "--plugin-paths="]
            assert "hello" in cmd
            assert result.stdout == '{"result": true}'

    def test_runs_with_noauth(self) -> None:
        """Run a koji command with --noauth flag."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="{}", returncode=0)
            koji_helper.run_koji_cmd("myprofile", "call", "getTag", "tag1", noauth=True)
            cmd = mock_run.call_args[0][0]
            assert "--noauth" in cmd

    def test_check_false_does_not_raise(self) -> None:
        """Honor check=False so callers can inspect a non-zero result."""
        failed = subprocess.CompletedProcess(
            args=["koji"], returncode=1, stdout="", stderr="nope"
        )
        with patch("subprocess.run", return_value=failed) as mock_run:
            result = koji_helper.run_koji_cmd("koji", "hello", check=False)
        assert result.returncode == 1
        assert mock_run.call_args.kwargs["check"] is False

    def test_logs_redact_import_token(self, caplog: pytest.LogCaptureFixture) -> None:
        """Keep the import-cg token out of the command log."""
        secret = "import-secret-token"
        original = koji_mod.logger.propagate
        koji_mod.logger.propagate = True
        try:
            with (
                patch("subprocess.run") as mock_run,
                caplog.at_level(logging.INFO, logger="release"),
            ):
                mock_run.return_value = MagicMock(stdout="", returncode=0)
                koji_helper.run_koji_cmd("koji", "import-cg", f"--token={secret}", ".")
        finally:
            koji_mod.logger.propagate = original
        assert secret not in caplog.text
        assert "--token=<REDACTED>" in caplog.text
        assert f"--token={secret}" in mock_run.call_args[0][0]

    def test_logs_redact_refund_token(self, caplog: pytest.LogCaptureFixture) -> None:
        """Keep the CGRefundBuild token out of the command log."""
        secret = "refund-secret-token"
        original = koji_mod.logger.propagate
        koji_mod.logger.propagate = True
        try:
            with (
                patch("subprocess.run") as mock_run,
                caplog.at_level(logging.INFO, logger="release"),
            ):
                mock_run.return_value = MagicMock(stdout="", returncode=0)
                koji_helper.run_koji_cmd(
                    "koji",
                    "call",
                    "--json",
                    "CGRefundBuild",
                    '"konflux"',
                    "123",
                    f'"{secret}"',
                )
        finally:
            koji_mod.logger.propagate = original
        assert secret not in caplog.text
        assert '"konflux"' in caplog.text
        assert '"<REDACTED>"' in caplog.text
        assert f'"{secret}"' in mock_run.call_args[0][0]

    def test_failure_traceback_omits_token(self) -> None:
        """A failed command's exception text does not include the token."""
        secret = "traceback-secret-token"

        def _fail(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.CalledProcessError(1, cmd, stderr=f"denied {secret}")

        with patch("subprocess.run", side_effect=_fail):
            with pytest.raises(subprocess.CalledProcessError) as exc_info:
                koji_helper.run_koji_cmd("koji", "import-cg", f"--token={secret}")
        assert secret not in str(exc_info.value)
        assert secret not in (exc_info.value.stderr or "")
        assert "--token=<REDACTED>" in str(exc_info.value)

    def test_logs_redacted_stderr_on_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        """Log Koji stderr on failure without the reservation token."""
        secret = "stderr-secret-token"
        original = koji_mod.logger.propagate
        koji_mod.logger.propagate = True

        def _fail(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.CalledProcessError(
                1,
                cmd,
                stderr=f"BuildError: --token={secret} ACCESS_TOKEN=glpat-abc denied\n",
            )

        try:
            with (
                patch("subprocess.run", side_effect=_fail),
                caplog.at_level(logging.ERROR, logger="release"),
            ):
                with pytest.raises(subprocess.CalledProcessError):
                    koji_helper.run_koji_cmd("koji", "import-cg", f"--token={secret}")
        finally:
            koji_mod.logger.propagate = original
        assert "BuildError" in caplog.text
        assert secret not in caplog.text
        assert "glpat-abc" not in caplog.text
        assert "--token=<REDACTED>" in caplog.text
        assert "ACCESS_TOKEN=[REDACTED]" in caplog.text

    def test_redacts_quoted_token_in_stderr(self, caplog: pytest.LogCaptureFixture) -> None:
        """Redact a quoted CGRefundBuild token that appears in stderr."""
        secret = "quoted-secret-token"
        original = koji_mod.logger.propagate
        koji_mod.logger.propagate = True

        def _fail(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.CalledProcessError(1, cmd, stderr=f"refund failed {secret}")

        try:
            with (
                patch("subprocess.run", side_effect=_fail),
                caplog.at_level(logging.ERROR, logger="release"),
            ):
                with pytest.raises(subprocess.CalledProcessError):
                    koji_helper.run_koji_cmd(
                        "koji",
                        "call",
                        "CGRefundBuild",
                        '"konflux"',
                        "1",
                        f'"{secret}"',
                    )
        finally:
            koji_mod.logger.propagate = original
        assert secret not in caplog.text
        assert "<REDACTED>" in caplog.text

    def test_failure_without_stderr_does_not_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Skip stderr logging when Koji produces none."""

        def _fail(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.CalledProcessError(1, cmd, stderr=None)

        original = koji_mod.logger.propagate
        koji_mod.logger.propagate = True
        try:
            with (
                patch("subprocess.run", side_effect=_fail),
                caplog.at_level(logging.ERROR, logger="release"),
            ):
                with pytest.raises(subprocess.CalledProcessError):
                    koji_helper.run_koji_cmd("koji", "hello")
        finally:
            koji_mod.logger.propagate = original
        assert "Koji stderr" not in caplog.text

    def test_failure_with_whitespace_stderr_does_not_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Skip stderr logging when redacted stderr is only whitespace."""

        def _fail(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.CalledProcessError(1, cmd, stderr="   \n")

        original = koji_mod.logger.propagate
        koji_mod.logger.propagate = True
        try:
            with (
                patch("subprocess.run", side_effect=_fail),
                caplog.at_level(logging.ERROR, logger="release"),
            ):
                with pytest.raises(subprocess.CalledProcessError):
                    koji_helper.run_koji_cmd("koji", "hello")
        finally:
            koji_mod.logger.propagate = original
        assert "Koji stderr" not in caplog.text

    def test_skips_empty_token_values(self) -> None:
        """Ignore empty --token= and empty quoted arguments when collecting secrets."""
        cmd = ["koji", "--token=", '""', "hello"]
        assert koji_mod._command_secrets(cmd) == []
        assert koji_mod._redact_cmd(cmd) == [
            "koji",
            "--token=<REDACTED>",
            '"<REDACTED>"',
            "hello",
        ]


class TestTestKojiConnection:
    """Test Koji connection probing."""

    def test_success(self) -> None:
        """Connection test succeeds."""
        with patch(f"{HELPER}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(returncode=0)
            koji_helper.test_koji_connection("koji")
            mock_cmd.assert_called_once_with("koji", "hello")


class TestPrepareKeytab:
    """Test keytab preparation."""

    def test_prepares_base64_keytab(self, tmp_path: Path) -> None:
        """Prepare keytab from base64-encoded secret."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        keytab_content = b"keytab-binary-data"
        (secret_mount / "base64_keytab").write_text(
            base64.b64encode(keytab_content).decode(), encoding="utf-8"
        )

        working_dir = tmp_path / "work"
        working_dir.mkdir()

        result = koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == keytab_content

    def test_prepares_direct_keytab(self, tmp_path: Path) -> None:
        """Copy keytab directly from mount."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        keytab_content = b"direct-keytab"
        (secret_mount / "test.keytab").write_bytes(keytab_content)

        working_dir = tmp_path / "work"
        working_dir.mkdir()

        result = koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == keytab_content

    def test_keytab_created_with_private_permissions(self, tmp_path: Path) -> None:
        """Keytab file is created with mode 0600."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").write_bytes(b"keytab-content")

        working_dir = tmp_path / "work"
        working_dir.mkdir()

        result = koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        mode = result.stat().st_mode & 0o777
        assert mode == 0o600

    def test_existing_destination_permissions_are_tightened(self, tmp_path: Path) -> None:
        """Force mode 0600 when the destination file already exists."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").write_bytes(b"new-keytab")
        working_dir = tmp_path / "work"
        working_dir.mkdir()
        dest = working_dir / "test.keytab"
        dest.write_bytes(b"old")
        dest.chmod(0o644)

        result = koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == b"new-keytab"
        assert result.stat().st_mode & 0o777 == 0o600

    def test_raises_on_missing_keytab(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when keytab is missing."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(FileNotFoundError, match="Keytab file not found"):
            koji_helper.prepare_keytab(secret_mount, "missing.keytab", working_dir)

    def test_raises_when_named_keytab_is_directory(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when the named keytab path is a directory."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").mkdir()
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(FileNotFoundError, match="Keytab file not found"):
            koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)

    def test_rejects_absolute_keytab_name(self, tmp_path: Path) -> None:
        """Reject an absolute keytab name before truncating that path."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").write_bytes(b"direct-keytab")
        victim = tmp_path / "victim"
        victim.write_bytes(b"do-not-touch")
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(ValueError, match="keytab path must stay under"):
            koji_helper.prepare_keytab(secret_mount, str(victim), working_dir)
        assert victim.read_bytes() == b"do-not-touch"

    def test_rejects_traversal_keytab_name(self, tmp_path: Path) -> None:
        """Reject a keytab name that escapes the working directory."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").write_bytes(b"direct-keytab")
        victim = tmp_path / "victim"
        victim.write_bytes(b"do-not-touch")
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(ValueError, match="keytab path must stay under"):
            koji_helper.prepare_keytab(secret_mount, "../victim", working_dir)
        assert victim.read_bytes() == b"do-not-touch"

    def test_rejects_symlink_destination(self, tmp_path: Path) -> None:
        """Refuse to write through a symlink in the working directory."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").write_bytes(b"direct-keytab")
        working_dir = tmp_path / "work"
        working_dir.mkdir()
        victim = working_dir / "victim"
        victim.write_bytes(b"do-not-touch")
        (working_dir / "test.keytab").symlink_to(victim)

        with pytest.raises(ValueError, match="must not be a symlink"):
            koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert victim.read_bytes() == b"do-not-touch"

    def test_rejects_symlink_source(self, tmp_path: Path) -> None:
        """Refuse to read a keytab symlink that points outside the secret mount."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        outside = tmp_path / "outside-secret"
        outside.write_bytes(b"not-a-keytab")
        (secret_mount / "test.keytab").symlink_to(outside)
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(ValueError, match="keytab path"):
            koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert outside.read_bytes() == b"not-a-keytab"
        assert not (working_dir / "test.keytab").exists()

    def _project_secret_file(self, secret_mount: Path, name: str, payload: bytes) -> None:
        """Publish *name* the way a Kubernetes Secret volume does."""
        data_dir = secret_mount / "..2024_01_01_00_00_00.1"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / name).write_bytes(payload)
        data_link = secret_mount / "..data"
        if not data_link.exists():
            data_link.symlink_to(data_dir.name)
        key_link = secret_mount / name
        if key_link.exists() or key_link.is_symlink():
            key_link.unlink()
        key_link.symlink_to(Path("..data") / name)

    def test_reads_projected_secret_symlink(self, tmp_path: Path) -> None:
        """Read a keytab exposed through a Secret volume ..data symlink."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        content = b"projected-keytab"
        self._project_secret_file(secret_mount, "test.keytab", content)
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        result = koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == content
        assert not result.is_symlink()

    def test_reads_projected_base64_symlink(self, tmp_path: Path) -> None:
        """Read a base64 keytab exposed through a Secret volume ..data symlink."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        content = b"projected-base64"
        self._project_secret_file(
            secret_mount,
            "base64_keytab",
            base64.b64encode(content),
        )
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        result = koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == content
        assert not result.is_symlink()

    def test_rejects_symlink_base64_source(self, tmp_path: Path) -> None:
        """Refuse to read a base64_keytab symlink that points outside the mount."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        outside = tmp_path / "outside-secret"
        outside.write_text(base64.b64encode(b"leaked").decode(), encoding="utf-8")
        (secret_mount / "base64_keytab").symlink_to(outside)
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(ValueError, match="keytab path"):
            koji_helper.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert outside.read_text(encoding="utf-8") == base64.b64encode(b"leaked").decode()
        assert not (working_dir / "test.keytab").exists()

    def test_rejects_keytab_name_that_is_the_directory(self, tmp_path: Path) -> None:
        """Reject a keytab name that resolves to the working directory itself."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(ValueError, match="must name a file"):
            koji_helper.prepare_keytab(secret_mount, ".", working_dir)

    def test_read_keytab_rejects_symlink(self, tmp_path: Path) -> None:
        """Refuse to read keytab bytes through a symlink."""
        target = tmp_path / "target"
        target.write_bytes(b"secret")
        link = tmp_path / "link"
        link.symlink_to(target)
        with pytest.raises(ValueError, match="symlink"):
            koji_mod._read_keytab_bytes(link)

    def test_read_keytab_reraises_oserror(self, tmp_path: Path) -> None:
        """Re-raise a non-symlink read error."""
        with pytest.raises(FileNotFoundError):
            koji_mod._read_keytab_bytes(tmp_path / "missing.keytab")

    def test_write_keytab_rejects_symlink(self, tmp_path: Path) -> None:
        """Refuse to write keytab bytes through a symlink."""
        victim = tmp_path / "victim"
        victim.write_bytes(b"do-not-touch")
        link = tmp_path / "link"
        link.symlink_to(victim)
        with pytest.raises(ValueError, match="symlink"):
            koji_mod._write_keytab_secure(link, b"new-keytab")
        assert victim.read_bytes() == b"do-not-touch"

    def test_write_keytab_reraises_oserror(self, tmp_path: Path) -> None:
        """Re-raise a non-symlink write error."""
        with pytest.raises(FileNotFoundError):
            koji_mod._write_keytab_secure(tmp_path / "missing" / "test.keytab", b"data")

    def test_read_keytab_maps_eloop(self, tmp_path: Path) -> None:
        """Map ELOOP from os.open to a ValueError."""
        path = tmp_path / "keytab"

        def _open(*_args: object, **_kwargs: object) -> int:
            raise OSError(errno.ELOOP, "too many links")

        with patch.object(os, "open", side_effect=_open):
            with pytest.raises(ValueError, match="must not be a symlink"):
                koji_mod._read_keytab_bytes(path)

    def test_write_keytab_maps_eloop(self, tmp_path: Path) -> None:
        """Map ELOOP from os.open to a ValueError on write."""
        path = tmp_path / "keytab"

        def _open(*_args: object, **_kwargs: object) -> int:
            raise OSError(errno.ELOOP, "too many links")

        with patch.object(os, "open", side_effect=_open):
            with pytest.raises(ValueError, match="must not be a symlink"):
                koji_mod._write_keytab_secure(path, b"data")


class TestKinit:
    """Test kinit wrapper."""

    def test_kinit_success_returns_ccache_path(self, tmp_path: Path) -> None:
        """Return the ccache path after kinit succeeds."""
        keytab = tmp_path / "test.keytab"
        keytab.write_bytes(b"keytab")

        with patch(f"{HELPER}.kinit_with_retry") as mock_kinit:
            ccache_path = koji_helper.kinit("user@REALM", keytab, max_attempts=3)
            mock_kinit.assert_called_once()
            assert "KRB5CCNAME" in mock_kinit.call_args[0][2]
            assert Path(ccache_path).is_file()
        Path(ccache_path).unlink(missing_ok=True)

    def test_kinit_failure(self, tmp_path: Path) -> None:
        """Raise CheckStepError when kinit fails and remove the cache."""
        keytab = tmp_path / "test.keytab"
        keytab.write_bytes(b"keytab")
        ccache = tmp_path / "ccache"
        ccache.write_bytes(b"")
        cause = subprocess.CalledProcessError(1, "kinit")

        with (
            patch(f"{HELPER}.file_helper.make_tempfile_path", return_value=ccache),
            patch(f"{HELPER}.kinit_with_retry") as mock_kinit,
        ):
            mock_kinit.side_effect = cause
            with pytest.raises(
                tekton.CheckStepError, match="logging in with Kerberos"
            ) as exc_info:
                koji_helper.kinit("user@REALM", keytab, max_attempts=3)
        assert exc_info.value.action == "logging in with Kerberos (kinit)"
        assert exc_info.value.cause is cause
        assert exc_info.value.__cause__ is cause
        assert not ccache.exists()

    def test_kinit_removes_cache_on_other_error(self, tmp_path: Path) -> None:
        """Remove the credential cache when authentication raises a non-process error."""
        keytab = tmp_path / "test.keytab"
        keytab.write_bytes(b"keytab")
        ccache = tmp_path / "ccache"
        ccache.write_bytes(b"")

        with (
            patch(f"{HELPER}.file_helper.make_tempfile_path", return_value=ccache),
            patch(f"{HELPER}.kinit_with_retry", side_effect=OSError("kinit unavailable")),
        ):
            with pytest.raises(OSError, match="kinit unavailable"):
                koji_helper.kinit("user@REALM", keytab, max_attempts=3)
        assert not ccache.exists()

    def test_kinit_caps_retry_sleep(self, tmp_path: Path) -> None:
        """Cap each retry wait so ten attempts cannot sleep for tens of minutes."""
        keytab = tmp_path / "test.keytab"
        keytab.write_bytes(b"keytab")

        with patch(f"{HELPER}.kinit_with_retry") as mock_kinit:
            ccache_path = koji_helper.kinit("user@REALM", keytab, max_attempts=10)
        assert mock_kinit.call_args.kwargs["max_attempts"] == 10
        assert mock_kinit.call_args.kwargs["max_sleep_seconds"] == 2
        Path(ccache_path).unlink(missing_ok=True)
