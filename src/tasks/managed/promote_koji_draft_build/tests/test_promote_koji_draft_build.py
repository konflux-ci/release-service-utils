"""Tests for ``promote_koji_draft_build``."""

from __future__ import annotations

import json
import logging
import os
import runpy
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.promote_koji_draft_build import (
    promote_koji_draft_build as task,
)


@pytest.fixture(autouse=True)
def _propagate_release_logger() -> None:
    """Allow caplog to capture records from the ``release`` logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


TASK = (
    "release_service_utils.tasks.managed" ".promote_koji_draft_build.promote_koji_draft_build"
)


def _snapshot(components: list[dict] | None = None) -> dict:
    """Build a minimal snapshot dictionary."""
    if components is None:
        components = [
            {
                "name": "test",
                "source": {
                    "git": {
                        "url": "https://gitlab.example.com/rpms/test",
                        "revision": "12345",
                    }
                },
            },
            {
                "name": "test-foo",
                "source": {
                    "git": {
                        "url": "https://gitlab.example.com/rpms/test-foo",
                        "revision": "12345",
                    }
                },
            },
            {
                "name": "test-bar",
                "source": {
                    "git": {
                        "url": "https://gitlab.example.com/rpms/test-bar",
                        "revision": "12345",
                    }
                },
            },
        ]
    return {
        "componentGroup": "rpms",
        "components": components,
    }


def _data(
    *,
    components: list[str] | None = None,
    mapping: list[dict] | None = None,
    principal: str = "test@test.com",
    keytab_name: str = "test.keytab",
    koji_profile: str = "koji",
) -> dict:
    """Build a minimal data dictionary."""
    result: dict = {
        "pushOptions": {
            "koji_profile": koji_profile,
            "pushKeytab": {"name": keytab_name, "principal": principal},
        }
    }
    if components is not None:
        result["pushOptions"]["components"] = components
    else:
        result["mapping"] = {
            "components": mapping or [{"name": "test-foo"}, {"name": "test-bar"}]
        }
    return result


def _write_json(path: Path, payload: dict) -> Path:
    """Write *payload* as JSON and return *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _config(
    tmp_path: Path,
    *,
    snapshot: dict | None = None,
    data: dict | None = None,
    koji_config_source: Path | None = None,
) -> task.PromoteConfig:
    """Build a PromoteConfig for tests."""
    secret_mount = tmp_path / "secret"
    secret_mount.mkdir()
    (secret_mount / "test.keytab").write_bytes(b"keytab-content")
    snapshot_path = _write_json(tmp_path / "work" / "snapshot.json", snapshot or _snapshot())
    data_path = _write_json(tmp_path / "work" / "data.json", data or _data())
    return task.PromoteConfig(
        snapshot_path=snapshot_path,
        data_path=data_path,
        secret_mount=secret_mount,
        kinit_retries=3,
        koji_config_source=koji_config_source,
    )


class TestParsePromoteOptions:
    """Test promote options parsing."""

    def test_parses_with_components_list(self, caplog: pytest.LogCaptureFixture) -> None:
        """Parse options with an explicit components list."""
        with caplog.at_level(logging.INFO, logger="release"):
            opts = task.parse_promote_options(_data(components=["comp-a", "comp-b"]))
        assert opts.principal == "test@test.com"
        assert opts.keytab_file == "test.keytab"
        assert opts.koji_profile == "koji"
        assert opts.release_components == ["comp-a", "comp-b"]
        assert "Using explicit release component list from pushOptions" in caplog.text
        assert "profile=koji" in caplog.text
        assert "principal=test@test.com" in caplog.text

    def test_parses_from_mapping(self, caplog: pytest.LogCaptureFixture) -> None:
        """Fall back to mapping.components when no explicit list is set."""
        with caplog.at_level(logging.INFO, logger="release"):
            opts = task.parse_promote_options(_data())
        assert opts.release_components == ["test-foo", "test-bar"]
        assert "Using release component list from mapping.components" in caplog.text

    def test_falls_back_to_mapping_when_components_empty(self) -> None:
        """Fall back to mapping.components when the explicit list is empty."""
        data = _data(components=[])
        data["mapping"] = {"components": [{"name": "mapped-comp"}]}
        opts = task.parse_promote_options(data)
        assert opts.release_components == ["mapped-comp"]

    def test_skips_mapping_components_without_name(self) -> None:
        """Ignore mapping entries that have no name."""
        data = _data(mapping=[{"name": "keep"}, {}, {"name": ""}])
        opts = task.parse_promote_options(data)
        assert opts.release_components == ["keep"]

    def test_raises_on_missing_principal(self) -> None:
        """Raise ValueError when principal is missing."""
        data = {"pushOptions": {"pushKeytab": {"name": "test.keytab"}}}
        with pytest.raises(ValueError, match="principal and name are required"):
            task.parse_promote_options(data)

    def test_raises_on_missing_keytab_name(self) -> None:
        """Raise ValueError when the keytab name is missing."""
        data = {"pushOptions": {"pushKeytab": {"principal": "p@R"}}}
        with pytest.raises(ValueError, match="principal and name are required"):
            task.parse_promote_options(data)

    def test_raises_on_missing_profile(self) -> None:
        """Raise ValueError when koji_profile is missing."""
        data = {"pushOptions": {"pushKeytab": {"name": "test.keytab", "principal": "p@R"}}}
        with pytest.raises(ValueError, match="koji_profile is required"):
            task.parse_promote_options(data)

    def test_raises_on_empty_data(self) -> None:
        """Raise ValueError when pushOptions is absent."""
        with pytest.raises(ValueError, match="principal and name are required"):
            task.parse_promote_options({})


class TestGitSourceUrl:
    """Test koji source URL construction."""

    def test_joins_url_and_revision(self, caplog: pytest.LogCaptureFixture) -> None:
        """Build git+url#revision from the snapshot component."""
        component = {
            "name": "foo",
            "source": {"git": {"url": "https://example.com/rpms/foo", "revision": "abc"}},
        }
        with caplog.at_level(logging.INFO, logger="release"):
            assert task.git_source_url(component) == "git+https://example.com/rpms/foo#abc"
        assert "Koji source URL for foo: git+https://example.com/rpms/foo#abc" in caplog.text


class TestLatestBuildId:
    """Test selecting the newest draft build."""

    def test_picks_latest_creation_time(self, caplog: pytest.LogCaptureFixture) -> None:
        """Return the build_id with the latest creation_time."""
        builds = [
            {"build_id": 1001, "creation_time": "2025-01-01 00:00:00.000000"},
            {"build_id": 1002, "creation_time": "2025-01-01 01:00:00.000000"},
        ]
        with caplog.at_level(logging.INFO, logger="release"):
            assert task.latest_build_id(builds, "git+url#rev") == 1002
        assert "Latest draft build for git+url#rev is 1002" in caplog.text
        assert "2 candidate(s)" in caplog.text

    def test_raises_on_empty_list(self) -> None:
        """Raise when listBuilds returned no builds."""
        with pytest.raises(RuntimeError, match="No draft Koji builds found"):
            task.latest_build_id([], "git+url#rev")

    def test_raises_on_missing_build_id(self) -> None:
        """Raise when the newest build has no build_id."""
        builds = [{"creation_time": "2025-01-01 00:00:00.000000"}]
        with pytest.raises(RuntimeError, match="has no build_id"):
            task.latest_build_id(builds, "git+url#rev")


class TestListDraftBuilds:
    """Test listing draft builds."""

    def test_parses_json_list(self, caplog: pytest.LogCaptureFixture) -> None:
        """Return the parsed listBuilds payload."""
        payload = [{"build_id": 1, "creation_time": "t"}]
        with (
            patch(f"{TASK}.koji_helper.run_koji_cmd") as mock_cmd,
            caplog.at_level(logging.INFO, logger="release"),
        ):
            mock_cmd.return_value = MagicMock(stdout=json.dumps(payload))
            result = task.list_draft_builds("koji", "test", "git+url#rev")
        assert result == payload
        assert "Listing complete draft builds for git+url#rev (user=test)" in caplog.text
        assert "Found 1 complete draft build(s) for git+url#rev" in caplog.text
        mock_cmd.assert_called_once_with(
            "koji",
            "call",
            "--json-output",
            "listBuilds",
            "userID=test",
            "state=1",
            "source=git+url#rev",
            "draft=True",
        )

    def test_raises_on_non_list(self) -> None:
        """Raise when listBuilds does not return a JSON array."""
        with patch(f"{TASK}.koji_helper.run_koji_cmd") as mock_cmd:
            mock_cmd.return_value = MagicMock(stdout="null")
            with pytest.raises(RuntimeError, match="Unexpected listBuilds result"):
                task.list_draft_builds("koji", "test", "git+url#rev")


class TestPromoteComponent:
    """Test promoting a single component."""

    def test_promotes_latest_build(self, caplog: pytest.LogCaptureFixture) -> None:
        """Look up draft builds and promote the newest one."""
        component = {
            "name": "test-foo",
            "source": {
                "git": {
                    "url": "https://gitlab.example.com/rpms/test-foo",
                    "revision": "12345",
                }
            },
        }
        builds = [
            {"build_id": 1001, "creation_time": "2025-01-01 00:00:00.000000"},
            {"build_id": 1002, "creation_time": "2025-01-01 01:00:00.000000"},
        ]
        with (
            patch(f"{TASK}.koji_helper.run_koji_cmd") as mock_cmd,
            caplog.at_level(logging.INFO, logger="release"),
        ):
            mock_cmd.side_effect = [
                MagicMock(stdout=json.dumps(builds)),
                MagicMock(returncode=0),
            ]
            task.promote_component("koji", "test", component)
        assert mock_cmd.call_count == 2
        assert mock_cmd.call_args_list[1].args == ("koji", "call", "promoteBuild", "1002")
        assert "Promoting latest draft build for component test-foo" in caplog.text
        assert "Promoting build 1002 for component test-foo" in caplog.text


class TestRun:
    """Test the promote workflow."""

    def _patch_auth(self, tmp_path: Path) -> tuple[Any, Any, Any, Any, Any]:
        """Return patches for keytab, kinit, and koji CLI."""
        keytab = tmp_path / "work" / "test.keytab"
        keytab.parent.mkdir(parents=True, exist_ok=True)
        keytab.write_bytes(b"keytab")
        ccache = tmp_path / "ccache"
        ccache.write_bytes(b"")
        return (
            patch(f"{TASK}.koji_helper.prepare_keytab", return_value=keytab),
            patch(f"{TASK}.koji_helper.kinit", return_value=str(ccache)),
            patch(f"{TASK}.koji_helper.run_koji_cmd"),
            patch(f"{TASK}.koji_helper.install_koji_client_config"),
            patch(f"{TASK}.koji_helper.test_koji_connection"),
        )

    def test_skips_components_not_in_release_list(self, tmp_path: Path) -> None:
        """Skip snapshot components that are not in the release list."""
        config = _config(tmp_path)
        keytab = config.snapshot_path.parent / "test.keytab"
        prepare, kinit, run_cmd, install, hello = self._patch_auth(tmp_path)
        with prepare, kinit, run_cmd as mock_cmd, install as mock_install, hello:
            mock_cmd.side_effect = [
                MagicMock(
                    stdout=json.dumps(
                        [{"build_id": 1002, "creation_time": "2025-01-01 01:00:00"}]
                    )
                ),
                MagicMock(returncode=0),  # promote foo
                MagicMock(
                    stdout=json.dumps(
                        [{"build_id": 2001, "creation_time": "2025-01-01 00:00:00"}]
                    )
                ),
                MagicMock(returncode=0),  # promote bar
            ]
            task.run(config)
        mock_install.assert_not_called()
        promoted = [
            call.args
            for call in mock_cmd.call_args_list
            if call.args[1:3] == ("call", "promoteBuild")
        ]
        assert promoted == [
            ("koji", "call", "promoteBuild", "1002"),
            ("koji", "call", "promoteBuild", "2001"),
        ]
        listed_sources = [
            call.args[6]
            for call in mock_cmd.call_args_list
            if len(call.args) > 6 and call.args[3] == "listBuilds"
        ]
        assert listed_sources == [
            "source=git+https://gitlab.example.com/rpms/test-foo#12345",
            "source=git+https://gitlab.example.com/rpms/test-bar#12345",
        ]
        assert not keytab.exists()

    def test_installs_koji_config_when_source_set(self, tmp_path: Path) -> None:
        """Install staged koji client config before talking to koji."""
        source = tmp_path / "koji-client-config"
        source.mkdir()
        config = _config(tmp_path, koji_config_source=source, snapshot=_snapshot([]))
        prepare, kinit, run_cmd, install, hello = self._patch_auth(tmp_path)
        with prepare, kinit, run_cmd as mock_cmd, install as mock_install, hello:
            mock_cmd.return_value = MagicMock(returncode=0)
            task.run(config)
        mock_install.assert_called_once_with(source)

    def test_falls_back_to_mapping_components(self, tmp_path: Path) -> None:
        """Use mapping.components when pushOptions.components is empty."""
        data = _data(components=[])
        data["mapping"] = {"components": [{"name": "test-foo"}]}
        config = _config(tmp_path, data=data)
        prepare, kinit, run_cmd, install, hello = self._patch_auth(tmp_path)
        with prepare, kinit, run_cmd as mock_cmd, install, hello:
            mock_cmd.side_effect = [
                MagicMock(
                    stdout=json.dumps(
                        [{"build_id": 9, "creation_time": "2025-01-01 00:00:00"}]
                    )
                ),
                MagicMock(returncode=0),
            ]
            task.run(config)
        promoted = [
            call.args[-1]
            for call in mock_cmd.call_args_list
            if call.args[1:3] == ("call", "promoteBuild")
        ]
        assert promoted == ["9"]

    def test_restores_existing_krb5ccname(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Restore KRB5CCNAME when it was already set."""
        monkeypatch.setenv("KRB5CCNAME", "/tmp/old-ccache")
        config = _config(tmp_path, snapshot=_snapshot([]))
        prepare, kinit, run_cmd, install, hello = self._patch_auth(tmp_path)
        with prepare, kinit, run_cmd as mock_cmd, install, hello:
            mock_cmd.return_value = MagicMock(returncode=0)
            task.run(config)
        assert os.environ["KRB5CCNAME"] == "/tmp/old-ccache"

    def test_clears_krb5ccname_when_unset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Remove KRB5CCNAME when the process did not have one."""
        monkeypatch.delenv("KRB5CCNAME", raising=False)
        config = _config(tmp_path, snapshot=_snapshot([]))
        prepare, kinit, run_cmd, install, hello = self._patch_auth(tmp_path)
        with prepare, kinit, run_cmd as mock_cmd, install, hello:
            mock_cmd.return_value = MagicMock(returncode=0)
            task.run(config)
        assert "KRB5CCNAME" not in os.environ

    def test_unlinks_keytab_when_kinit_fails(self, tmp_path: Path) -> None:
        """Clean up the keytab if Kerberos login fails."""
        config = _config(tmp_path)
        keytab = config.snapshot_path.parent / "test.keytab"
        keytab.write_bytes(b"keytab")
        cause = subprocess.CalledProcessError(1, "kinit")
        with (
            patch(f"{TASK}.koji_helper.prepare_keytab", return_value=keytab),
            patch(f"{TASK}.koji_helper.kinit", side_effect=cause),
        ):
            with pytest.raises(subprocess.CalledProcessError):
                task.run(config)
        assert not keytab.exists()

    def test_unknown_component_group(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Default the application name when componentGroup is absent."""
        snapshot = _snapshot([])
        snapshot.pop("componentGroup")
        config = _config(tmp_path, snapshot=snapshot)
        prepare, kinit, run_cmd, install, hello = self._patch_auth(tmp_path)
        with (
            prepare,
            kinit,
            run_cmd as mock_cmd,
            install,
            hello as mock_hello,
            caplog.at_level(logging.INFO, logger="release"),
        ):
            task.run(config)
        mock_hello.assert_called_once_with("koji")
        mock_cmd.assert_not_called()
        assert "Logging in with Kerberos as test@test.com" in caplog.text
        assert 'Start task "promote-koji-draft-build" for Application "unknown"' in caplog.text


class TestMain:
    """Test environment-variable wiring."""

    def test_reads_env_and_runs(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Build PromoteConfig from environment variables and call run()."""
        snapshot = tmp_path / "snapshot.json"
        data = tmp_path / "data.json"
        source = tmp_path / "koji-client-config"
        source.mkdir()
        monkeypatch.setenv("SNAPSHOT_SPEC_FILE", str(snapshot))
        monkeypatch.setenv("DATA_FILE", str(data))
        monkeypatch.setenv("SECRET_MOUNT", str(tmp_path / "secret"))
        monkeypatch.setenv("KINIT_RETRIES", "7")
        monkeypatch.setenv("KOJI_CONFIG_SOURCE", str(source))
        with (
            patch(f"{TASK}.run") as mock_run,
            caplog.at_level(logging.INFO, logger="release"),
        ):
            assert task.main() == 0
        assert f"snapshot={snapshot}" in caplog.text
        assert f"data={data}" in caplog.text
        config = mock_run.call_args.args[0]
        assert config.snapshot_path == snapshot
        assert config.data_path == data
        assert config.secret_mount == tmp_path / "secret"
        assert config.kinit_retries == 7
        assert config.koji_config_source == source

    def test_optional_env_defaults(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Use defaults when optional env vars are unset."""
        monkeypatch.setenv("SNAPSHOT_SPEC_FILE", str(tmp_path / "snapshot.json"))
        monkeypatch.setenv("DATA_FILE", str(tmp_path / "data.json"))
        monkeypatch.delenv("SECRET_MOUNT", raising=False)
        monkeypatch.delenv("KINIT_RETRIES", raising=False)
        monkeypatch.delenv("KOJI_CONFIG_SOURCE", raising=False)
        with patch(f"{TASK}.run") as mock_run:
            assert task.main() == 0
        config = mock_run.call_args.args[0]
        assert config.secret_mount == Path("/etc/secret")
        assert config.kinit_retries == 10
        assert config.koji_config_source is None

    def test_blank_koji_config_source_is_ignored(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Treat a whitespace-only KOJI_CONFIG_SOURCE as unset."""
        monkeypatch.setenv("SNAPSHOT_SPEC_FILE", str(tmp_path / "snapshot.json"))
        monkeypatch.setenv("DATA_FILE", str(tmp_path / "data.json"))
        monkeypatch.setenv("KOJI_CONFIG_SOURCE", "  ")
        with patch(f"{TASK}.run") as mock_run:
            assert task.main() == 0
        assert mock_run.call_args.args[0].koji_config_source is None

    def test_requires_snapshot_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Exit when SNAPSHOT_SPEC_FILE is missing."""
        monkeypatch.delenv("SNAPSHOT_SPEC_FILE", raising=False)
        monkeypatch.setenv("DATA_FILE", "/tmp/data.json")
        with pytest.raises(SystemExit) as exc_info:
            task.main()
        assert exc_info.value.code == 1

    def test_requires_data_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Exit when DATA_FILE is missing."""
        monkeypatch.setenv("SNAPSHOT_SPEC_FILE", "/tmp/snapshot.json")
        monkeypatch.delenv("DATA_FILE", raising=False)
        with pytest.raises(SystemExit) as exec_info:
            task.main()
        assert exec_info.value.code == 1


def test_dunder_main_invokes_main() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.managed.promote_koji_draft_build"
    with (
        patch(f"{TASK}.main", return_value=0) as mock_main,
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()
