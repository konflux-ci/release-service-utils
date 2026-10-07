"""Tests for ``push_rpm_to_koji``."""

from __future__ import annotations

import base64
import json
import logging
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.helpers import tekton
from release_service_utils.tasks.managed.push_rpm_to_koji import push_rpm_to_koji as task

TASK = "release_service_utils.tasks.managed.push_rpm_to_koji.push_rpm_to_koji"
_OMIT = object()  # Sentinel for omitting a field entirely
_LIST_PKGS_HEADER = (
    "Package                 Tag                   Extra Arches     Owner\n"
    "----------------------- --------------------- ---------------- ---------------\n"
)


def _snapshot(components: list[dict] | None = None) -> dict:
    """Build a minimal snapshot dictionary."""
    return {
        "componentGroup": "test-app",
        "components": components or [],
    }


def _data(
    components: list[str] | None = None,
    koji_import_draft: bool | str | None | object = _OMIT,
    koji_tags: list[str] | None = None,
) -> dict:
    """Build a minimal data dictionary."""
    result: dict = {
        "pushOptions": {
            "koji_profile": "koji",
            "koji_tags": koji_tags or [],
            "pushKeytab": {
                "name": "test.keytab",
                "principal": "test@REALM.COM",
            },
        },
    }
    # Only include koji_import_draft if not omitted
    if koji_import_draft is not _OMIT:
        result["pushOptions"]["koji_import_draft"] = koji_import_draft
    if components is not None:
        result["pushOptions"]["components"] = components
    else:
        result["mapping"] = {"components": [{"name": "comp1"}, {"name": "comp2"}]}
    return result


def _config(
    tmp_path: Path, snapshot: Path | None = None, data: Path | None = None
) -> task.KojiConfig:
    """Build a KojiConfig for tests."""
    secret_mount = tmp_path / "secret"
    secret_mount.mkdir()
    (secret_mount / "test.keytab").write_bytes(b"keytab-content")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    rpm_dir = tmp_path / "rpms"

    return task.KojiConfig(
        snapshot_path=snapshot or (tmp_path / "snapshot.json"),
        data_path=data or (tmp_path / "data.json"),
        secret_mount=secret_mount,
        data_dir=data_dir,
        rpm_download_dir=rpm_dir,
        kinit_retries=3,
    )


class TestLoadSnapshot:
    """Test snapshot loading and validation."""

    def test_loads_valid_snapshot(self, tmp_path: Path) -> None:
        """Load a valid snapshot file."""
        snap_file = tmp_path / "snap.json"
        snap_file.write_text(json.dumps(_snapshot()), encoding="utf-8")
        result = task.load_snapshot(snap_file)
        assert result["componentGroup"] == "test-app"

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when snapshot is missing."""
        with pytest.raises(FileNotFoundError, match="No valid snapshot file"):
            task.load_snapshot(tmp_path / "missing.json")


class TestLoadData:
    """Test data loading and validation."""

    def test_loads_valid_data(self, tmp_path: Path) -> None:
        """Load a valid data file."""
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps(_data()), encoding="utf-8")
        result = task.load_data(data_file)
        assert result["pushOptions"]["koji_profile"] == "koji"

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when data is missing."""
        with pytest.raises(FileNotFoundError, match="No data JSON"):
            task.load_data(tmp_path / "missing.json")


class TestParsePushOptions:
    """Test push options parsing."""

    def test_parses_with_components_list(self) -> None:
        """Parse push options with explicit components list."""
        data = _data(components=["comp-a", "comp-b"], koji_import_draft=False)
        opts = task.parse_push_options(data)
        assert opts.principal == "test@REALM.COM"
        assert opts.keytab_file == "test.keytab"
        assert opts.koji_profile == "koji"
        assert opts.release_components == ["comp-a", "comp-b"]
        assert opts.koji_import_draft is False

    def test_parses_from_mapping(self) -> None:
        """Parse push options using mapping.components when no explicit list."""
        data = _data()
        opts = task.parse_push_options(data)
        assert opts.release_components == ["comp1", "comp2"]

    def test_falls_back_to_mapping_when_components_empty(self) -> None:
        """Fall back to mapping.components when explicit list is empty."""
        data = _data(components=[])
        # _data with empty components doesn't add mapping, so add it manually
        data["mapping"] = {"components": [{"name": "mapped-comp"}]}
        opts = task.parse_push_options(data)
        assert opts.release_components == ["mapped-comp"]

    @pytest.mark.parametrize(
        ("raw_value", "expected"),
        [
            (True, True),
            (False, False),
            ("false", False),
            (_OMIT, True),  # Omitted defaults to draft (matches catalog behavior)
            ("true", True),
        ],
    )
    def test_parses_draft_flag(self, raw_value: object, expected: bool) -> None:
        """Parse koji_import_draft flag for boolean, string, and omitted values."""
        data = _data(koji_import_draft=raw_value)
        opts = task.parse_push_options(data)
        assert opts.koji_import_draft is expected

    def test_parses_koji_tags(self) -> None:
        """Parse koji_tags list."""
        data = _data(koji_tags=["tag1", "tag2"])
        opts = task.parse_push_options(data)
        assert opts.koji_tags == ["tag1", "tag2"]

    def test_replaces_non_list_koji_tags(self) -> None:
        """Replace a non-list koji_tags value with an empty list."""
        data = _data()
        data["pushOptions"]["koji_tags"] = "tag1"
        opts = task.parse_push_options(data)
        assert opts.koji_tags == []

    def test_accepts_import_push_type(self) -> None:
        """Accept the default and explicit import push type."""
        omitted = _data()
        explicit = _data()
        explicit["pushOptions"]["pushType"] = "import"
        task.parse_push_options(omitted)
        task.parse_push_options(explicit)

    def test_rejects_promote_push_type(self) -> None:
        """Reject promotion before this task imports a build."""
        data = _data()
        data["pushOptions"]["pushType"] = "promote"
        with pytest.raises(ValueError, match='pushType "promote"'):
            task.parse_push_options(data)

    def test_raises_on_missing_principal(self) -> None:
        """Raise ValueError when principal is missing."""
        data = {"pushOptions": {"pushKeytab": {"name": "test.keytab"}}}
        with pytest.raises(ValueError, match="principal and name are required"):
            task.parse_push_options(data)

    def test_raises_on_missing_profile(self) -> None:
        """Raise ValueError when koji_profile is missing."""
        data = {"pushOptions": {"pushKeytab": {"name": "test.keytab", "principal": "p@R"}}}
        with pytest.raises(ValueError, match="koji_profile is required"):
            task.parse_push_options(data)


class TestRunKojiCmd:
    """Test koji command execution."""

    def test_runs_basic_command(self) -> None:
        """Run a basic koji command."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout='{"result": true}', returncode=0)
            result = task.run_koji_cmd("myprofile", "hello")
            mock_run.assert_called_once()
            assert "--profile=myprofile" in mock_run.call_args[0][0]
            assert "hello" in mock_run.call_args[0][0]
            assert result.stdout == '{"result": true}'

    def test_runs_with_noauth(self) -> None:
        """Run a koji command with --noauth flag."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="{}", returncode=0)
            task.run_koji_cmd("myprofile", "call", "getTag", "tag1", noauth=True)
            cmd = mock_run.call_args[0][0]
            assert "--noauth" in cmd

    def test_logs_redact_import_token(self, caplog: pytest.LogCaptureFixture) -> None:
        """Keep the import-cg token out of the command log."""
        secret = "import-secret-token"
        original = task.logger.propagate
        task.logger.propagate = True
        try:
            with (
                patch("subprocess.run") as mock_run,
                caplog.at_level(logging.INFO, logger="release"),
            ):
                mock_run.return_value = MagicMock(stdout="", returncode=0)
                task.run_koji_cmd("koji", "import-cg", f"--token={secret}", ".")
        finally:
            task.logger.propagate = original
        assert secret not in caplog.text
        assert "--token=<REDACTED>" in caplog.text
        assert f"--token={secret}" in mock_run.call_args[0][0]

    def test_logs_redact_refund_token(self, caplog: pytest.LogCaptureFixture) -> None:
        """Keep the CGRefundBuild token out of the command log."""
        secret = "refund-secret-token"
        original = task.logger.propagate
        task.logger.propagate = True
        try:
            with (
                patch("subprocess.run") as mock_run,
                caplog.at_level(logging.INFO, logger="release"),
            ):
                mock_run.return_value = MagicMock(stdout="", returncode=0)
                task.run_koji_cmd(
                    "koji",
                    "call",
                    "--json",
                    "CGRefundBuild",
                    '"konflux"',
                    "123",
                    f'"{secret}"',
                )
        finally:
            task.logger.propagate = original
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
                task.run_koji_cmd("koji", "import-cg", f"--token={secret}")
        assert secret not in str(exc_info.value)
        assert secret not in (exc_info.value.stderr or "")
        assert "--token=<REDACTED>" in str(exc_info.value)

    def test_logs_redacted_stderr_on_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        """Log Koji stderr on failure without the reservation token."""
        secret = "stderr-secret-token"
        original = task.logger.propagate
        task.logger.propagate = True

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
                    task.run_koji_cmd("koji", "import-cg", f"--token={secret}")
        finally:
            task.logger.propagate = original
        assert "BuildError" in caplog.text
        assert secret not in caplog.text
        assert "glpat-abc" not in caplog.text
        assert "--token=<REDACTED>" in caplog.text
        assert "ACCESS_TOKEN=[REDACTED]" in caplog.text

    def test_redacts_quoted_token_in_stderr(self, caplog: pytest.LogCaptureFixture) -> None:
        """Redact a quoted CGRefundBuild token that appears in stderr."""
        secret = "quoted-secret-token"
        original = task.logger.propagate
        task.logger.propagate = True

        def _fail(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.CalledProcessError(1, cmd, stderr=f"refund failed {secret}")

        try:
            with (
                patch("subprocess.run", side_effect=_fail),
                caplog.at_level(logging.ERROR, logger="release"),
            ):
                with pytest.raises(subprocess.CalledProcessError):
                    task.run_koji_cmd(
                        "koji",
                        "call",
                        "CGRefundBuild",
                        '"konflux"',
                        "1",
                        f'"{secret}"',
                    )
        finally:
            task.logger.propagate = original
        assert secret not in caplog.text
        assert "<REDACTED>" in caplog.text


class TestGetTagInfo:
    """Test tag info retrieval."""

    def test_returns_tag_info(self) -> None:
        """Return parsed tag info."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(
                stdout='{"name": "tag1", "extra": {"sidetag": false}}'
            )
            result = task.get_tag_info("koji", "tag1")
            assert result["name"] == "tag1"
            assert result["extra"]["sidetag"] is False


class TestIsSidetag:
    """Test sidetag detection."""

    def test_returns_true_for_sidetag(self) -> None:
        """Return True when tag is a sidetag."""
        with patch(f"{TASK}.get_tag_info") as mock_info:
            mock_info.return_value = {"extra": {"sidetag": True}}
            assert task.is_sidetag("koji", "f38-build-side-12345") is True

    def test_returns_false_for_regular_tag(self) -> None:
        """Return False when tag is not a sidetag."""
        with patch(f"{TASK}.get_tag_info") as mock_info:
            mock_info.return_value = {"extra": {"sidetag": False}}
            assert task.is_sidetag("koji", "f38-updates-candidate") is False

    def test_returns_none_when_sidetag_omitted(self) -> None:
        """Return None when extra or sidetag is absent."""
        with patch(f"{TASK}.get_tag_info") as mock_info:
            mock_info.return_value = {}
            assert task.is_sidetag("koji", "some-tag") is None
            mock_info.return_value = {"extra": {}}
            assert task.is_sidetag("koji", "some-tag") is None

    def test_returns_false_for_explicit_false_string(self) -> None:
        """Return False when sidetag is the string false."""
        with patch(f"{TASK}.get_tag_info") as mock_info:
            mock_info.return_value = {"extra": {"sidetag": "false"}}
            assert task.is_sidetag("koji", "f38-updates-candidate") is False

    def test_returns_none_for_unexpected_sidetag_value(self) -> None:
        """Return None when sidetag is neither true nor false."""
        with patch(f"{TASK}.get_tag_info") as mock_info:
            mock_info.return_value = {"extra": {"sidetag": "yes"}}
            assert task.is_sidetag("koji", "f38-updates-candidate") is None


class TestGetDestTag:
    """Test destination tag resolution."""

    def test_returns_dest_tag(self) -> None:
        """Return the destination tag from build target."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(
                stdout='{"dest_tag_name": "f38-updates-candidate"}'
            )
            result = task.get_dest_tag("koji", "f38-updates")
            assert result == "f38-updates-candidate"

    def test_raises_on_missing_dest_tag(self) -> None:
        """Raise CheckStepError when dest_tag_name is missing."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(stdout="{}")
            with pytest.raises(
                tekton.CheckStepError, match="Failed to resolve build target"
            ) as exc_info:
                task.get_dest_tag("koji", "invalid-target")
        assert exc_info.value.action == "resolving the Koji destination tag"
        assert isinstance(exc_info.value.cause, RuntimeError)
        assert exc_info.value.__cause__ is exc_info.value.cause


class TestGetExistingBuild:
    """Test existing build checking."""

    def test_returns_build_info(self) -> None:
        """Return build info when build exists."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(stdout='{"id": 12345, "nvr": "pkg-1.0-1"}')
            result = task.get_existing_build("koji", "pkg-1.0-1")
            assert result is not None
            assert result["id"] == 12345

    def test_returns_none_when_not_found(self) -> None:
        """Return None when build does not exist."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(stdout="null")
            result = task.get_existing_build("koji", "nonexistent-1.0-1")
            assert result is None

    def test_returns_none_for_json_string_null(self) -> None:
        """Return None when Koji responds with the JSON string "null"."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(stdout='"null"')
            result = task.get_existing_build("koji", "nonexistent-1.0-1")
            assert result is None


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

        result = task.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == keytab_content

    def test_prepares_direct_keytab(self, tmp_path: Path) -> None:
        """Copy keytab directly from mount."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        keytab_content = b"direct-keytab"
        (secret_mount / "test.keytab").write_bytes(keytab_content)

        working_dir = tmp_path / "work"
        working_dir.mkdir()

        result = task.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == keytab_content

    def test_keytab_created_with_private_permissions(self, tmp_path: Path) -> None:
        """Keytab file is created with mode 0600."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").write_bytes(b"keytab-content")

        working_dir = tmp_path / "work"
        working_dir.mkdir()

        result = task.prepare_keytab(secret_mount, "test.keytab", working_dir)
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

        result = task.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert result.read_bytes() == b"new-keytab"
        assert result.stat().st_mode & 0o777 == 0o600

    def test_raises_on_missing_keytab(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when keytab is missing."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(FileNotFoundError, match="Keytab file not found"):
            task.prepare_keytab(secret_mount, "missing.keytab", working_dir)

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
            task.prepare_keytab(secret_mount, str(victim), working_dir)
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
            task.prepare_keytab(secret_mount, "../victim", working_dir)
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
            task.prepare_keytab(secret_mount, "test.keytab", working_dir)
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
            task.prepare_keytab(secret_mount, "test.keytab", working_dir)
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

        result = task.prepare_keytab(secret_mount, "test.keytab", working_dir)
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

        result = task.prepare_keytab(secret_mount, "test.keytab", working_dir)
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
            task.prepare_keytab(secret_mount, "test.keytab", working_dir)
        assert outside.read_text(encoding="utf-8") == base64.b64encode(b"leaked").decode()
        assert not (working_dir / "test.keytab").exists()

    def test_rejects_keytab_name_that_is_the_directory(self, tmp_path: Path) -> None:
        """Reject a keytab name that resolves to the working directory itself."""
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        working_dir = tmp_path / "work"
        working_dir.mkdir()

        with pytest.raises(ValueError, match="must name a file"):
            task.prepare_keytab(secret_mount, ".", working_dir)

    def test_read_keytab_rejects_symlink(self, tmp_path: Path) -> None:
        """Refuse to read keytab bytes through a symlink."""
        target = tmp_path / "target"
        target.write_bytes(b"secret")
        link = tmp_path / "link"
        link.symlink_to(target)
        with pytest.raises(ValueError, match="symlink"):
            task._read_keytab_bytes(link)

    def test_read_keytab_reraises_oserror(self, tmp_path: Path) -> None:
        """Re-raise a non-symlink read error."""
        with pytest.raises(FileNotFoundError):
            task._read_keytab_bytes(tmp_path / "missing.keytab")

    def test_write_keytab_rejects_symlink(self, tmp_path: Path) -> None:
        """Refuse to write keytab bytes through a symlink."""
        victim = tmp_path / "victim"
        victim.write_bytes(b"do-not-touch")
        link = tmp_path / "link"
        link.symlink_to(victim)
        with pytest.raises(ValueError, match="symlink"):
            task._write_keytab_secure(link, b"new-keytab")
        assert victim.read_bytes() == b"do-not-touch"

    def test_write_keytab_reraises_oserror(self, tmp_path: Path) -> None:
        """Re-raise a non-symlink write error."""
        with pytest.raises(FileNotFoundError):
            task._write_keytab_secure(tmp_path / "missing" / "test.keytab", b"data")


class TestKinit:
    """Test kinit wrapper."""

    def test_kinit_success_returns_ccache_path(self, tmp_path: Path) -> None:
        """Return the ccache path after kinit succeeds."""
        keytab = tmp_path / "test.keytab"
        keytab.write_bytes(b"keytab")

        with patch(f"{TASK}.kinit_with_retry") as mock_kinit:
            ccache_path = task.kinit("user@REALM", keytab, max_attempts=3)
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
            patch(f"{TASK}.file_helper.make_tempfile_path", return_value=ccache),
            patch(f"{TASK}.kinit_with_retry") as mock_kinit,
        ):
            mock_kinit.side_effect = cause
            with pytest.raises(
                tekton.CheckStepError, match="logging in with Kerberos"
            ) as exc_info:
                task.kinit("user@REALM", keytab, max_attempts=3)
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
            patch(f"{TASK}.file_helper.make_tempfile_path", return_value=ccache),
            patch(f"{TASK}.kinit_with_retry", side_effect=OSError("kinit unavailable")),
        ):
            with pytest.raises(OSError, match="kinit unavailable"):
                task.kinit("user@REALM", keytab, max_attempts=3)
        assert not ccache.exists()


class TestTestKojiConnection:
    """Test Koji connection testing."""

    def test_success(self) -> None:
        """Connection test succeeds."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(returncode=0)
            task.test_koji_connection("koji")
            mock_cmd.assert_called_once_with("koji", "hello")


class TestGetKojiTargetFromManifest:
    """Test Koji target extraction from manifest."""

    def test_extracts_target(self) -> None:
        """Extract koji.build-target from manifest annotations."""
        manifest = {"annotations": {"koji.build-target": "f38-updates"}}

        with (
            patch(f"{TASK}.run_cmd") as mock_run_cmd,
            patch(f"{TASK}.oras_utils.oras_manifest_fetch") as mock_fetch,
            patch(f"{TASK}.file_helper.make_tempfile_path") as mock_temp,
        ):
            mock_temp.return_value = MagicMock(write_text=MagicMock(), unlink=MagicMock())
            mock_run_cmd.return_value = MagicMock(stdout='{"auths": {}}')
            mock_fetch.return_value = json.dumps(manifest)
            result = task.get_koji_target_from_manifest("quay.io/test/image@sha256:abc")
            assert result == "f38-updates"

    def test_raises_when_missing(self) -> None:
        """Raise CheckStepError when the build-target annotation is missing."""
        manifest = {"annotations": {}}

        with (
            patch(f"{TASK}.run_cmd") as mock_run_cmd,
            patch(f"{TASK}.oras_utils.oras_manifest_fetch") as mock_fetch,
            patch(f"{TASK}.file_helper.make_tempfile_path") as mock_temp,
        ):
            mock_temp.return_value = MagicMock(write_text=MagicMock(), unlink=MagicMock())
            mock_run_cmd.return_value = MagicMock(stdout="{}")
            mock_fetch.return_value = json.dumps(manifest)
            with pytest.raises(
                tekton.CheckStepError, match="No Koji build target found"
            ) as exc_info:
                task.get_koji_target_from_manifest("quay.io/test/image@sha256:abc")
        assert exc_info.value.action == "reading the Koji build target annotation"
        assert isinstance(exc_info.value.cause, RuntimeError)
        assert exc_info.value.__cause__ is exc_info.value.cause


class TestPullComponentImage:
    """Test component image download."""

    def test_creates_dir_and_pulls(self, tmp_path: Path) -> None:
        """Create the download directory and pull the image into it."""
        download_dir = tmp_path / "out"
        with patch(f"{TASK}.oras_utils.oras_pull") as mock_pull:
            task.pull_component_image("quay.io/test/img@sha256:abc", download_dir)
        assert download_dir.is_dir()
        mock_pull.assert_called_once_with("quay.io/test/img@sha256:abc", download_dir)


class TestFindSrpm:
    """Test SRPM finding."""

    def test_finds_srpm(self, tmp_path: Path) -> None:
        """Find the source RPM in directory."""
        (tmp_path / "pkg-1.0-1.src.rpm").write_bytes(b"")
        result = task.find_srpm(tmp_path)
        assert result.name == "pkg-1.0-1.src.rpm"

    def test_raises_when_not_found(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when no SRPM exists."""
        with pytest.raises(FileNotFoundError, match="No source RPM found"):
            task.find_srpm(tmp_path)

    def test_warns_when_multiple_srpms(self, tmp_path: Path) -> None:
        """Use the first source RPM and warn when several are present."""
        first = tmp_path / "a.src.rpm"
        second = tmp_path / "b.src.rpm"
        first.write_bytes(b"")
        second.write_bytes(b"")
        result = task.find_srpm(tmp_path)
        assert result in {first, second}


class TestParseCgImport:
    """Test cg_import.json parsing."""

    def test_parses_cg_import(self, tmp_path: Path) -> None:
        """Parse a valid cg_import.json."""
        cg = {"build": {"name": "pkg", "version": "1.0", "release": "1"}}
        (tmp_path / "cg_import.json").write_text(json.dumps(cg), encoding="utf-8")
        result = task.parse_cg_import(tmp_path)
        assert result["build"]["name"] == "pkg"

    def test_raises_when_missing(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when cg_import.json is missing."""
        with pytest.raises(FileNotFoundError, match="cg_import.json not found"):
            task.parse_cg_import(tmp_path)


class TestInitKojiBuild:
    """Test Koji CG build initialization."""

    def test_initializes_build(self) -> None:
        """Initialize a CG build and return build_id and token."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(stdout='{"build_id": 123, "token": "abc-token"}')
            build_id, token = task.init_koji_build("koji", "pkg", "1.0", "1", None, draft=True)
            assert build_id == 123
            assert token == "abc-token"
            mock_cmd.assert_called_once_with(
                "koji",
                "call",
                "--json-output",
                "--json",
                "CGInitBuild",
                '"konflux"',
                json.dumps(
                    {
                        "name": "pkg",
                        "version": "1.0",
                        "release": "1",
                        "epoch": None,
                        "draft": True,
                    }
                ),
            )


class TestImportCgBuild:
    """Test CG build import."""

    def test_imports_build(self, tmp_path: Path) -> None:
        """Import a CG build successfully."""
        cg_import = tmp_path / "cg_import.json"
        cg_import.write_text('{"build": {}}', encoding="utf-8")

        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(returncode=0)
            task.import_cg_build("koji", 123, "token", cg_import, draft=True)
            assert "import-cg" in mock_cmd.call_args[0]
            assert "--draft" in mock_cmd.call_args[0]

    def test_refunds_on_failure(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Refund build on import failure without logging the token."""
        cg_import = tmp_path / "cg_import.json"
        cg_import.write_text("{}", encoding="utf-8")
        secret = "import-failure-token"
        import_error = subprocess.CalledProcessError(
            1,
            ["koji", "import-cg", f"--token={secret}"],
            stderr=f"BuildError: token {secret} was rejected",
        )
        original = task.logger.propagate
        task.logger.propagate = True
        try:
            with (
                patch(f"{TASK}.run_koji_cmd") as mock_cmd,
                caplog.at_level(logging.ERROR, logger="release"),
            ):
                mock_cmd.side_effect = [
                    import_error,
                    MagicMock(returncode=0),  # CGRefundBuild
                ]
                with pytest.raises(
                    tekton.CheckStepError, match="importing the Koji CG build"
                ) as exc_info:
                    task.import_cg_build("koji", 123, secret, cg_import, draft=False)
        finally:
            task.logger.propagate = original
        assert exc_info.value.action == "importing the Koji CG build"
        assert exc_info.value.cause is import_error
        assert exc_info.value.__cause__ is import_error
        assert mock_cmd.call_count == 2
        assert "CGRefundBuild" in str(mock_cmd.call_args_list[1])
        assert secret not in caplog.text
        assert "exit code 1" in caplog.text
        assert "BuildError" in caplog.text
        assert "<REDACTED>" in caplog.text

    def test_refunds_when_stderr_empty(self, tmp_path: Path) -> None:
        """Refund the build when import fails without a stderr diagnostic."""
        cg_import = tmp_path / "cg_import.json"
        cg_import.write_text("{}", encoding="utf-8")

        import_error = subprocess.CalledProcessError(1, ["koji", "import-cg"])
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.side_effect = [
                import_error,
                MagicMock(returncode=0),
            ]
            with pytest.raises(
                tekton.CheckStepError, match="importing the Koji CG build"
            ) as exc_info:
                task.import_cg_build("koji", 123, "token", cg_import, draft=False)
        assert exc_info.value.action == "importing the Koji CG build"
        assert exc_info.value.cause is import_error
        assert exc_info.value.__cause__ is import_error
        assert mock_cmd.call_count == 2
        assert "CGRefundBuild" in str(mock_cmd.call_args_list[1])

    def test_keeps_import_error_when_refund_fails(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Keep the import CheckStepError when the refund call also fails."""
        cg_import = tmp_path / "cg_import.json"
        cg_import.write_text("{}", encoding="utf-8")
        secret = "refund-failure-token"
        import_error = subprocess.CalledProcessError(
            1,
            ["koji", "import-cg", f"--token={secret}"],
            stderr="BuildError: import rejected",
        )
        refund_error = subprocess.CalledProcessError(
            2,
            ["koji", "call", "CGRefundBuild", f'"{secret}"'],
            stderr=f"RefundError: token {secret} was rejected",
        )
        original = task.logger.propagate
        task.logger.propagate = True
        try:
            with (
                patch(f"{TASK}.run_koji_cmd") as mock_cmd,
                caplog.at_level(logging.ERROR, logger="release"),
            ):
                mock_cmd.side_effect = [import_error, refund_error]
                with pytest.raises(
                    tekton.CheckStepError, match="importing the Koji CG build"
                ) as exc_info:
                    task.import_cg_build("koji", 123, secret, cg_import, draft=False)
        finally:
            task.logger.propagate = original
        assert exc_info.value.action == "importing the Koji CG build"
        assert exc_info.value.cause is import_error
        assert exc_info.value.__cause__ is import_error
        assert mock_cmd.call_count == 2
        assert "CGRefundBuild" in str(mock_cmd.call_args_list[1])
        assert secret not in caplog.text
        assert "Refund of build 123 failed (exit code 2)" in caplog.text
        assert "RefundError" in caplog.text
        assert "<REDACTED>" in caplog.text


class TestEnsurePackageInTag:
    """Test package listing in tag."""

    def test_adds_package_when_listing_is_empty(self) -> None:
        """Add the package when list-pkgs succeeds but lists no match."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.side_effect = [
                MagicMock(returncode=0, stdout=_LIST_PKGS_HEADER),
                MagicMock(returncode=0, stdout=""),
            ]
            task.ensure_package_in_tag("koji", "f38-updates", "mypkg", "user")
            assert mock_cmd.call_count == 2
            assert mock_cmd.call_args_list[1][0][1:] == (
                "add-pkg",
                "--force",
                "f38-updates",
                "mypkg",
                "--owner",
                "user",
            )

    def test_adds_package_when_lookup_fails(self) -> None:
        """Attempt add-pkg when list-pkgs exits nonzero."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.side_effect = [
                subprocess.CalledProcessError(1, ["koji", "list-pkgs"]),
                MagicMock(returncode=0, stdout=""),
            ]
            task.ensure_package_in_tag("koji", "f38-updates", "mypkg", "user")
            assert mock_cmd.call_count == 2
            assert mock_cmd.call_args_list[1][0][1:] == (
                "add-pkg",
                "--force",
                "f38-updates",
                "mypkg",
                "--owner",
                "user",
            )

    def test_skips_when_present(self) -> None:
        """Skip adding package when the listing includes it."""
        listed = (
            _LIST_PKGS_HEADER
            + "mypkg                   f38-updates                            user\n"
        )
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(returncode=0, stdout=listed)
            task.ensure_package_in_tag("koji", "f38-updates", "mypkg", "user")
            assert mock_cmd.call_count == 1


class TestTagBuild:
    """Test build tagging."""

    def test_tags_build(self) -> None:
        """Tag a build successfully."""
        with patch(f"{TASK}.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(returncode=0)
            task.tag_build("koji", "f38-updates", 12345)
            mock_cmd.assert_called_once()
            assert "tagBuild" in mock_cmd.call_args[0]


class TestProcessComponent:
    """Test component processing."""

    def test_skips_component_not_in_list(self, tmp_path: Path) -> None:
        """Skip component not in release list."""
        component = {"name": "excluded-comp", "containerImage": "quay.io/test/img"}
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=[],
            koji_import_draft=False,
            release_components=["other-comp"],
        )
        config = _config(tmp_path)

        # Should not raise or call any external commands
        task.process_component(component, push_opts, config, "user")

    def test_skips_component_with_missing_container_image(self, tmp_path: Path) -> None:
        """Skip component when containerImage key is absent."""
        component = {"name": "no-image-comp"}
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=[],
            koji_import_draft=False,
            release_components=["no-image-comp"],
        )
        config = _config(tmp_path)

        with patch(f"{TASK}.pull_component_image") as mock_pull:
            task.process_component(component, push_opts, config, "user")
            mock_pull.assert_not_called()

    def test_skips_component_with_blank_container_image(self, tmp_path: Path) -> None:
        """Skip component when containerImage is blank or whitespace."""
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=[],
            koji_import_draft=False,
            release_components=["blank-image-comp"],
        )
        config = _config(tmp_path)

        for blank_value in ["", "   ", None]:
            component = {"name": "blank-image-comp", "containerImage": blank_value}
            with patch(f"{TASK}.pull_component_image") as mock_pull:
                task.process_component(component, push_opts, config, "user")
                mock_pull.assert_not_called()

    def test_recreates_existing_rpm_dir(self, tmp_path: Path) -> None:
        """Remove a leftover RPM download directory before pulling."""
        component = {"name": "test-comp", "containerImage": "quay.io/test/img@sha256:abc"}
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=[],
            koji_import_draft=False,
            release_components=["test-comp"],
        )
        config = _config(tmp_path)
        config.rpm_download_dir.mkdir()
        stale = config.rpm_download_dir / "stale.txt"
        stale.write_text("old", encoding="utf-8")

        with patch(f"{TASK}.pull_component_image", side_effect=RuntimeError("stop")):
            with pytest.raises(RuntimeError, match="stop"):
                task.process_component(component, push_opts, config, "user")
        assert not stale.exists()
        assert config.rpm_download_dir.is_dir()

    def test_resolves_relative_rpm_dir_before_chdir(self, tmp_path: Path) -> None:
        """Relative rpm_download_dir is resolved to absolute before chdir."""
        import os

        # Create config with a relative path
        secret_mount = tmp_path / "secret"
        secret_mount.mkdir()
        (secret_mount / "test.keytab").write_bytes(b"keytab-content")
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Use a relative path for rpm_download_dir
        relative_rpm_dir = Path("relative_rpms")
        config = task.KojiConfig(
            snapshot_path=tmp_path / "snapshot.json",
            data_path=tmp_path / "data.json",
            secret_mount=secret_mount,
            data_dir=data_dir,
            rpm_download_dir=relative_rpm_dir,
            kinit_retries=3,
        )

        component = {"name": "test-comp", "containerImage": "quay.io/test/img@sha256:abc"}
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=[],
            koji_import_draft=False,
            release_components=["test-comp"],
        )

        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with (
                patch(f"{TASK}.pull_component_image"),
                patch(f"{TASK}.find_srpm", return_value=Path("pkg-1.0-1.src.rpm")),
                patch(
                    f"{TASK}.parse_cg_import",
                    return_value={"build": {"name": "pkg", "version": "1.0", "release": "1"}},
                ),
                patch(f"{TASK}.get_existing_build", return_value=None),
                patch(f"{TASK}.get_koji_target_from_manifest", return_value="f38-updates"),
                patch(f"{TASK}.get_dest_tag", return_value="f38-updates-candidate"),
                patch(f"{TASK}.init_koji_build", return_value=(123, "token")),
                patch(f"{TASK}.import_cg_build"),
                patch(f"{TASK}.ensure_package_in_tag"),
                patch(f"{TASK}.tag_build"),
            ):
                # Should not raise - relative path is resolved correctly
                task.process_component(component, push_opts, config, "user")
                # Verify the directory was created at the correct absolute location
                expected_abs_path = tmp_path / "relative_rpms"
                assert expected_abs_path.exists()
        finally:
            os.chdir(original_cwd)

    def test_nvr_derived_from_build_fields(self, tmp_path: Path) -> None:
        """NVR for existing-build lookup is derived from cg_import build fields."""
        component = {"name": "test-comp", "containerImage": "quay.io/test/img@sha256:abc"}
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=[],
            koji_import_draft=False,
            release_components=["test-comp"],
        )
        config = _config(tmp_path)

        cg_import_data = {
            "build": {"name": "my-package", "version": "2.0", "release": "3.el9"}
        }

        with (
            patch(f"{TASK}.pull_component_image"),
            patch(f"{TASK}.find_srpm", return_value=Path("my-package-2.0-3.el9.src.rpm")),
            patch(f"{TASK}.parse_cg_import", return_value=cg_import_data),
            patch(f"{TASK}.get_existing_build") as mock_get_existing,
            patch(f"{TASK}.get_koji_target_from_manifest", return_value="f38-updates"),
            patch(f"{TASK}.get_dest_tag", return_value="f38-updates-candidate"),
            patch(f"{TASK}.init_koji_build", return_value=(123, "token")),
            patch(f"{TASK}.import_cg_build"),
            patch(f"{TASK}.ensure_package_in_tag"),
            patch(f"{TASK}.tag_build"),
        ):
            mock_get_existing.return_value = None
            task.process_component(component, push_opts, config, "user")
            # Verify NVR is correctly formed without .src suffix
            mock_get_existing.assert_called_once_with("koji", "my-package-2.0-3.el9")

    def test_non_draft_reuses_existing_build(self, tmp_path: Path) -> None:
        """Non-draft import skips import and reuses existing build for tagging."""
        component = {"name": "test-comp", "containerImage": "quay.io/test/img@sha256:abc"}
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=["extra-tag"],
            koji_import_draft=False,
            release_components=["test-comp"],
        )
        config = _config(tmp_path)

        cg_import_data = {"build": {"name": "pkg", "version": "1.0", "release": "1"}}
        existing_build = {"id": 999, "nvr": "pkg-1.0-1"}

        with (
            patch(f"{TASK}.pull_component_image"),
            patch(f"{TASK}.find_srpm", return_value=Path("pkg-1.0-1.src.rpm")),
            patch(f"{TASK}.parse_cg_import", return_value=cg_import_data),
            patch(f"{TASK}.get_existing_build", return_value=existing_build),
            patch(f"{TASK}.get_koji_target_from_manifest", return_value="f38-updates"),
            patch(f"{TASK}.get_dest_tag", return_value="f38-updates-candidate"),
            patch(f"{TASK}.init_koji_build") as mock_init,
            patch(f"{TASK}.import_cg_build") as mock_import,
            patch(f"{TASK}.ensure_package_in_tag"),
            patch(f"{TASK}.tag_build") as mock_tag,
        ):
            task.process_component(component, push_opts, config, "user")
            # Import should be skipped
            mock_init.assert_not_called()
            mock_import.assert_not_called()
            # Existing build ID should be used for tagging
            assert mock_tag.call_count == 2  # dest tag + extra-tag
            tag_calls = [call[0] for call in mock_tag.call_args_list]
            assert all(call[2] == 999 for call in tag_calls)  # build_id=999

    @pytest.mark.parametrize(
        ("tag_info", "expected_tag"),
        [
            ({}, "f38-updates-candidate"),
            ({"extra": {}}, "f38-updates-candidate"),
            ({"extra": {"sidetag": False}}, "f38-updates-draft"),
            ({"extra": {"sidetag": True}}, "f38-updates-candidate"),
        ],
    )
    def test_draft_tag_rewrite_depends_on_explicit_sidetag(
        self, tmp_path: Path, tag_info: dict, expected_tag: str
    ) -> None:
        """Rewrite a draft tag only when Koji sets sidetag to false."""
        component = {"name": "test-comp", "containerImage": "quay.io/test/img@sha256:abc"}
        push_opts = task.PushOptions(
            principal="user@REALM",
            keytab_file="test.keytab",
            koji_profile="koji",
            koji_tags=[],
            koji_import_draft=True,
            release_components=["test-comp"],
        )
        config = _config(tmp_path)
        cg_import_data = {"build": {"name": "pkg", "version": "1.0", "release": "1"}}

        with (
            patch(f"{TASK}.pull_component_image"),
            patch(f"{TASK}.find_srpm", return_value=Path("pkg-1.0-1.src.rpm")),
            patch(f"{TASK}.parse_cg_import", return_value=cg_import_data),
            patch(f"{TASK}.get_koji_target_from_manifest", return_value="f38-updates"),
            patch(f"{TASK}.get_dest_tag", return_value="f38-updates-candidate"),
            patch(f"{TASK}.get_tag_info", return_value=tag_info),
            patch(f"{TASK}.init_koji_build", return_value=(123, "token")),
            patch(f"{TASK}.import_cg_build"),
            patch(f"{TASK}.ensure_package_in_tag") as mock_ensure,
            patch(f"{TASK}.tag_build") as mock_tag,
        ):
            task.process_component(component, push_opts, config, "user")
            assert mock_ensure.call_args[0][1] == expected_tag
            assert mock_tag.call_args[0][1] == expected_tag


class TestRun:
    """Test run workflow."""

    def test_rejects_promote_before_koji(self, tmp_path: Path) -> None:
        """Refuse a promotion request before preparing a keytab or calling Koji."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
        data = _data()
        data["pushOptions"]["pushType"] = "promote"
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps(data), encoding="utf-8")
        config = _config(tmp_path, snapshot=snapshot, data=data_file)

        with (
            patch(f"{TASK}.prepare_keytab") as mock_keytab,
            patch(f"{TASK}.run_koji_cmd") as mock_koji,
            patch(f"{TASK}.process_component") as mock_process,
        ):
            with pytest.raises(ValueError, match='pushType "promote"'):
                task.run(config)

        mock_keytab.assert_not_called()
        mock_koji.assert_not_called()
        mock_process.assert_not_called()

    def test_processes_components(self, tmp_path: Path) -> None:
        """Walk snapshot components after Koji authentication."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(
            json.dumps(_snapshot([{"name": "comp1", "containerImage": ""}])),
            encoding="utf-8",
        )
        data = tmp_path / "data.json"
        data.write_text(json.dumps(_data()), encoding="utf-8")
        config = _config(tmp_path, snapshot=snapshot, data=data)

        with (
            patch(f"{TASK}.kinit", return_value=str(tmp_path / "ccache")),
            patch(f"{TASK}.test_koji_connection"),
            patch(f"{TASK}.process_component") as mock_process,
        ):
            task.run(config)
        mock_process.assert_called_once()

    def test_cleans_up_keytab_on_success(self, tmp_path: Path) -> None:
        """Keytab file is removed after successful run."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
        data = tmp_path / "data.json"
        data.write_text(json.dumps(_data()), encoding="utf-8")

        config = _config(tmp_path, snapshot=snapshot, data=data)

        with (
            patch(f"{TASK}.kinit", return_value=str(tmp_path / "ccache")),
            patch(f"{TASK}.test_koji_connection"),
        ):
            task.run(config)

        keytab_path = config.data_dir / snapshot.parent.name / "test.keytab"
        assert not keytab_path.exists()

    def test_cleans_up_keytab_on_failure(self, tmp_path: Path) -> None:
        """Keytab file is removed even when run fails."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
        data = tmp_path / "data.json"
        data.write_text(json.dumps(_data()), encoding="utf-8")

        config = _config(tmp_path, snapshot=snapshot, data=data)

        with (
            patch(f"{TASK}.kinit", return_value=str(tmp_path / "ccache")),
            patch(
                f"{TASK}.test_koji_connection", side_effect=RuntimeError("connection failed")
            ),
        ):
            with pytest.raises(RuntimeError, match="connection failed"):
                task.run(config)

        keytab_path = config.data_dir / snapshot.parent.name / "test.keytab"
        assert not keytab_path.exists()

    def test_cleans_up_ccache_on_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Credential cache is removed after successful run."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
        data = tmp_path / "data.json"
        data.write_text(json.dumps(_data()), encoding="utf-8")

        config = _config(tmp_path, snapshot=snapshot, data=data)
        ccache_file = tmp_path / "test_ccache"
        ccache_file.write_text("ccache-content")

        with (
            patch(f"{TASK}.kinit", return_value=str(ccache_file)),
            patch(f"{TASK}.test_koji_connection"),
        ):
            task.run(config)

        assert not ccache_file.exists()

    def test_restores_original_krb5ccname(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Original KRB5CCNAME is restored after run."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
        data = tmp_path / "data.json"
        data.write_text(json.dumps(_data()), encoding="utf-8")

        config = _config(tmp_path, snapshot=snapshot, data=data)
        original_ccname = "/tmp/original_ccache"
        monkeypatch.setenv("KRB5CCNAME", original_ccname)

        with (
            patch(f"{TASK}.kinit", return_value="/tmp/new_ccache"),
            patch(f"{TASK}.test_koji_connection"),
        ):
            task.run(config)

        import os

        assert os.environ.get("KRB5CCNAME") == original_ccname

    def test_removes_krb5ccname_when_not_originally_set(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """KRB5CCNAME is removed if it was not originally set."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
        data = tmp_path / "data.json"
        data.write_text(json.dumps(_data()), encoding="utf-8")

        config = _config(tmp_path, snapshot=snapshot, data=data)
        monkeypatch.delenv("KRB5CCNAME", raising=False)

        with (
            patch(f"{TASK}.kinit", return_value="/tmp/new_ccache"),
            patch(f"{TASK}.test_koji_connection"),
        ):
            task.run(config)

        import os

        assert "KRB5CCNAME" not in os.environ


class TestMain:
    """Test main entry point."""

    def test_reads_env_and_runs(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() reads env vars and calls run()."""
        snapshot = tmp_path / "snap.json"
        snapshot.write_text(json.dumps(_snapshot()), encoding="utf-8")
        data = tmp_path / "data.json"
        data.write_text(json.dumps(_data()), encoding="utf-8")

        monkeypatch.setenv("SNAPSHOT_SPEC_FILE", str(snapshot))
        monkeypatch.setenv("DATA_FILE", str(data))
        monkeypatch.setenv("SECRET_MOUNT", str(tmp_path / "secret"))
        monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))

        with patch(f"{TASK}.run") as mock_run:
            assert task.main() == 0
        mock_run.assert_called_once()
        cfg = mock_run.call_args[0][0]
        assert cfg.snapshot_path == snapshot
        assert cfg.data_path == data

    def test_raises_on_missing_snapshot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raise CheckStepError when SNAPSHOT_SPEC_FILE is missing."""
        monkeypatch.delenv("SNAPSHOT_SPEC_FILE", raising=False)
        monkeypatch.setenv("DATA_FILE", "/some/path")
        with pytest.raises(tekton.CheckStepError, match="SNAPSHOT_SPEC_FILE") as exc_info:
            task.main()
        assert exc_info.value.action == "reading configuration"
        assert isinstance(exc_info.value.cause, ValueError)
        assert exc_info.value.__cause__ is exc_info.value.cause

    def test_raises_on_missing_data(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raise CheckStepError when DATA_FILE is missing."""
        monkeypatch.setenv("SNAPSHOT_SPEC_FILE", "/some/path")
        monkeypatch.delenv("DATA_FILE", raising=False)
        with pytest.raises(tekton.CheckStepError, match="DATA_FILE") as exc_info:
            task.main()
        assert exc_info.value.action == "reading configuration"
        assert isinstance(exc_info.value.cause, ValueError)
        assert exc_info.value.__cause__ is exc_info.value.cause

    def test_package_main_module(self) -> None:
        """Importing the package __main__ exposes main."""
        import importlib

        mod = importlib.import_module(
            "release_service_utils.tasks.managed.push_rpm_to_koji.__main__"
        )
        assert mod.main is task.main
