"""Tests for ``push_snapshot``."""

from __future__ import annotations

import json
import subprocess
import types
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.tasks.managed.push_snapshot import push_snapshot

TASK = "release_service_utils.tasks.managed.push_snapshot.push_snapshot"


def _write_json(path: Path, data: dict | list) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _default_snapshot(components: list[dict] | None = None) -> dict:
    return {
        "componentGroup": "test-group",
        "components": components
        or [
            {
                "name": "comp1",
                "containerImage": "registry.io/image1@sha256:abc123",
                "repositories": [{"url": "prod.io/loc1", "tags": ["v1.0", "latest"]}],
                "pushSourceContainer": False,
            }
        ],
    }


def _default_data() -> dict:
    return {"mapping": {"defaults": {"pushSourceContainer": True}}}


class TestSelectOciAuth:
    """Test select-oci-auth wrapper."""

    def test_returns_stdout(self) -> None:
        """Return the raw auth JSON printed by select-oci-auth."""
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                [], 0, stdout='{"auths":{"reg.io":{"auth":"x"}}}', stderr=""
            )
            result = push_snapshot.select_oci_auth("reg.io/repo")
        assert result == '{"auths":{"reg.io":{"auth":"x"}}}'

    def test_returns_empty_braces_on_empty_output(self) -> None:
        """Fall back to an empty JSON object when there is no output."""
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            result = push_snapshot.select_oci_auth("reg.io/repo")
        assert result == "{}"


class TestCreateSourceAuthFile:
    """Test source auth file creation with key remapping."""

    def test_remaps_registry_to_repo(self, tmp_path: Path) -> None:
        """Remap the bare-registry auth key to the full repository path."""
        auth_json = '{"auths":{"registry.io":{"auth":"token123"}}}'
        with patch(f"{TASK}.select_oci_auth", return_value=auth_json):
            with patch(f"{TASK}.file.make_tempfile_path") as mock_tmp:
                tmp_file = tmp_path / "auth.json"
                mock_tmp.return_value = tmp_file
                result = push_snapshot.create_source_auth_file(
                    "registry.io/org/repo@sha256:abc"
                )

        data = json.loads(result.read_text(encoding="utf-8"))
        assert "registry.io/org/repo" in data["auths"]
        assert "registry.io" not in data["auths"]

    def test_handles_empty_auth(self, tmp_path: Path) -> None:
        """Write an empty auths object when select-oci-auth returns none."""
        with patch(f"{TASK}.select_oci_auth", return_value="{}"):
            with patch(f"{TASK}.file.make_tempfile_path") as mock_tmp:
                tmp_file = tmp_path / "auth.json"
                mock_tmp.return_value = tmp_file
                result = push_snapshot.create_source_auth_file("registry.io/repo@sha256:abc")

        data = json.loads(result.read_text(encoding="utf-8"))
        assert data == {"auths": {}}


class TestCreateDestAuthFile:
    """Test destination auth file creation."""

    def test_remaps_non_docker_registry(self, tmp_path: Path) -> None:
        """Remap a non-docker.io registry auth key to the full repo path."""
        auth_json = '{"auths":{"quay.io":{"auth":"tok"}}}'
        with patch(f"{TASK}.select_oci_auth", return_value=auth_json):
            with patch(f"{TASK}.file.make_tempfile_path") as mock_tmp:
                tmp_file = tmp_path / "auth.json"
                mock_tmp.return_value = tmp_file
                result = push_snapshot.create_dest_auth_file("quay.io/org/repo")

        data = json.loads(result.read_text(encoding="utf-8"))
        assert "quay.io/org/repo" in data["auths"]
        assert "quay.io" not in data["auths"]

    def test_keeps_docker_io_as_is(self, tmp_path: Path) -> None:
        """Keep the docker.io auth key unchanged for docker.io destinations."""
        auth_json = '{"auths":{"https://index.docker.io/v1/":{"auth":"tok"}}}'
        with patch(f"{TASK}.select_oci_auth", return_value=auth_json):
            with patch(f"{TASK}.file.make_tempfile_path") as mock_tmp:
                tmp_file = tmp_path / "auth.json"
                mock_tmp.return_value = tmp_file
                result = push_snapshot.create_dest_auth_file("docker.io/library/image")

        data = json.loads(result.read_text(encoding="utf-8"))
        assert "https://index.docker.io/v1/" in data["auths"]

    def test_leaves_auths_when_registry_key_missing(self, tmp_path: Path) -> None:
        """Keep auths unchanged when the registry hostname is not a key."""
        auth_json = '{"auths":{"other.io":{"auth":"tok"}}}'
        with patch(f"{TASK}.select_oci_auth", return_value=auth_json):
            with patch(f"{TASK}.file.make_tempfile_path") as mock_tmp:
                tmp_file = tmp_path / "auth.json"
                mock_tmp.return_value = tmp_file
                result = push_snapshot.create_dest_auth_file("quay.io/org/repo")

        data = json.loads(result.read_text(encoding="utf-8"))
        assert data["auths"] == {"other.io": {"auth": "tok"}}


class TestCreateCombinedDockerConfig:
    """Test merging source and dest auth files."""

    def test_merges_two_auth_files(self, tmp_path: Path) -> None:
        """Merge source and destination auth entries into one config."""
        src = tmp_path / "src.json"
        dst = tmp_path / "dst.json"
        src.write_text('{"auths":{"src-repo":{"auth":"a"}}}', encoding="utf-8")
        dst.write_text('{"auths":{"dst-repo":{"auth":"b"}}}', encoding="utf-8")
        config_dir = push_snapshot.create_combined_docker_config(src, dst)
        config = json.loads((config_dir / "config.json").read_text(encoding="utf-8"))
        assert "src-repo" in config["auths"]
        assert "dst-repo" in config["auths"]
        # cleanup
        (config_dir / "config.json").unlink()
        config_dir.rmdir()


class TestGetImageArchitectures:
    """Test get-image-architectures parsing."""

    def test_parses_jsonl_output(self) -> None:
        """Parse each JSON line into a platform entry."""
        output = (
            '{"platform":{"architecture":"amd64","os":"linux"}}\n'
            '{"platform":{"architecture":"arm64","os":"linux"}}\n'
        )
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                [], 0, stdout=output, stderr=""
            )
            entries = push_snapshot.get_image_architectures("reg.io/img:tag")
        assert len(entries) == 2
        assert entries[0]["platform"]["architecture"] == "amd64"
        assert entries[1]["platform"]["architecture"] == "arm64"

    def test_handles_empty_output(self) -> None:
        """Return an empty list when there is no architecture output."""
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            entries = push_snapshot.get_image_architectures("reg.io/img:tag")
        assert entries == []

    def test_skips_blank_lines(self) -> None:
        """Ignore empty lines in the JSONL architecture output."""
        output = (
            '{"platform":{"architecture":"amd64","os":"linux"}}\n'
            "\n"
            "   \n"
            '{"platform":{"architecture":"arm64","os":"linux"}}\n'
        )
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                [], 0, stdout=output, stderr=""
            )
            entries = push_snapshot.get_image_architectures("reg.io/img:tag")
        assert len(entries) == 2
        assert entries[0]["platform"]["architecture"] == "amd64"
        assert entries[1]["platform"]["architecture"] == "arm64"


class TestOrasDiscoverReferrers:
    """Test oras discover wrapper."""

    def test_returns_referrers_on_success(self, tmp_path: Path) -> None:
        """Return parsed referrers when oras discover succeeds."""
        auth_file = tmp_path / "auth.json"
        auth_file.write_text("{}", encoding="utf-8")
        discover_output = json.dumps({"referrers": [{"digest": "sha256:ref1"}]})
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                [], 0, stdout=discover_output, stderr=""
            )
            referrers = push_snapshot.oras_discover_referrers(
                "reg.io/img@sha256:abc", auth_file
            )
        assert len(referrers) == 1

    def test_raises_on_failure(self, tmp_path: Path) -> None:
        """Raise ``RuntimeError`` when oras discover exits non-zero."""
        auth_file = tmp_path / "auth.json"
        auth_file.write_text("{}", encoding="utf-8")
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                [], 1, stdout="", stderr="error"
            )
            with pytest.raises(RuntimeError, match="oras discover failed"):
                push_snapshot.oras_discover_referrers("reg.io/img@sha256:abc", auth_file)


class TestCosignCopy:
    """Test cosign copy wrapper."""

    def test_calls_cosign_with_docker_config(self, tmp_path: Path) -> None:
        """Invoke cosign copy with DOCKER_CONFIG pointed at the combined config."""
        config_dir = tmp_path / "docker"
        config_dir.mkdir()
        with patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            push_snapshot.cosign_copy("src:tag", "dst:tag", config_dir)
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs["env"]["DOCKER_CONFIG"] == str(config_dir)


class TestDiscoverArtifactsWithRetry:
    """Test _discover_artifacts_with_retry."""

    def test_returns_count_on_success(self, tmp_path: Path) -> None:
        """Return the number of discovered referrers on the first try."""
        auth_file = tmp_path / "auth.json"
        auth_file.write_text("{}", encoding="utf-8")
        with patch(
            f"{TASK}.oras_discover_referrers",
            return_value=[{"digest": "sha256:a"}, {"digest": "sha256:b"}],
        ):
            count = push_snapshot._discover_artifacts_with_retry(
                "reg.io/img@sha256:abc", auth_file, retries=2
            )
        assert count == 2

    def test_returns_zero_on_persistent_failure(self, tmp_path: Path) -> None:
        """Fall back to zero discovered artifacts after repeated failures."""
        auth_file = tmp_path / "auth.json"
        auth_file.write_text("{}", encoding="utf-8")
        with patch(
            f"{TASK}.oras_discover_referrers",
            side_effect=RuntimeError("fail"),
        ):
            count = push_snapshot._discover_artifacts_with_retry(
                "reg.io/img@sha256:abc", auth_file, retries=1
            )
        assert count == 0


class TestPushImage:
    """Test push_image function."""

    def test_skips_when_digests_match(self, tmp_path: Path) -> None:
        """Skip pushing when the destination digest already matches."""
        source_auth = tmp_path / "src_auth.json"
        source_auth.write_text("{}", encoding="utf-8")

        with patch(f"{TASK}.create_dest_auth_file") as mock_dest_auth:
            dest_file = tmp_path / "dest_auth.json"
            dest_file.write_text("{}", encoding="utf-8")
            mock_dest_auth.return_value = dest_file

            with patch(
                f"{TASK}.oras_utils.oras_resolve",
                return_value="sha256:abc123",
            ):
                result = push_snapshot.push_image(
                    push_snapshot.PushJob(
                        origin_digest="sha256:abc123",
                        name="comp1",
                        container_image="reg.io/img@sha256:abc123",
                        repository_url="prod.io/repo",
                        tag="v1.0",
                        platform="",
                        source_auth_file=source_auth,
                        retries=0,
                        copy_bundle_migrations=False,
                    )
                )

        assert result == {"name": "comp1", "url": "prod.io/repo:v1.0"}

    def test_performs_cosign_copy(self, tmp_path: Path) -> None:
        """Fall back to cosign copy when bundle migration copying is disabled."""
        source_auth = tmp_path / "src_auth.json"
        source_auth.write_text('{"auths":{}}', encoding="utf-8")

        with patch(f"{TASK}.create_dest_auth_file") as mock_dest_auth:
            dest_file = tmp_path / "dest_auth.json"
            dest_file.write_text('{"auths":{}}', encoding="utf-8")
            mock_dest_auth.return_value = dest_file

            with patch(f"{TASK}.oras_utils.oras_resolve", return_value=None):
                with patch(f"{TASK}.create_combined_docker_config") as mock_config:
                    config_dir = tmp_path / "docker"
                    config_dir.mkdir()
                    (config_dir / "config.json").write_text("{}")
                    mock_config.return_value = config_dir

                    with patch(f"{TASK}.cosign_copy") as mock_cosign:
                        result = push_snapshot.push_image(
                            push_snapshot.PushJob(
                                origin_digest="sha256:abc123",
                                name="comp1",
                                container_image="reg.io/img@sha256:abc123",
                                repository_url="prod.io/repo",
                                tag="v1.0",
                                platform="",
                                source_auth_file=source_auth,
                                retries=0,
                                copy_bundle_migrations=False,
                            )
                        )

        mock_cosign.assert_called_once_with(
            "reg.io/img@sha256:abc123", "prod.io/repo:v1.0", config_dir
        )
        assert result == {"name": "comp1", "url": "prod.io/repo:v1.0"}

    def test_uses_oras_cp_with_artifacts(self, tmp_path: Path) -> None:
        """Use oras cp -r when attached artifacts are discovered."""
        source_auth = tmp_path / "src_auth.json"
        source_auth.write_text('{"auths":{}}', encoding="utf-8")

        with patch(f"{TASK}.create_dest_auth_file") as mock_dest_auth:
            dest_file = tmp_path / "dest_auth.json"
            dest_file.write_text('{"auths":{}}', encoding="utf-8")
            mock_dest_auth.return_value = dest_file

            with patch(f"{TASK}.oras_utils.oras_resolve", return_value=None):
                with patch(f"{TASK}.create_combined_docker_config") as mock_config:
                    config_dir = tmp_path / "docker"
                    config_dir.mkdir()
                    (config_dir / "config.json").write_text("{}")
                    mock_config.return_value = config_dir

                    with patch(
                        f"{TASK}._discover_artifacts_with_retry",
                        return_value=2,
                    ):
                        with patch(f"{TASK}.oras_utils.oras_cp") as mock_oras:
                            result = push_snapshot.push_image(
                                push_snapshot.PushJob(
                                    origin_digest="sha256:abc",
                                    name="comp1",
                                    container_image="reg.io/img@sha256:abc",
                                    repository_url="prod.io/repo",
                                    tag="v1.0",
                                    platform="linux/amd64",
                                    source_auth_file=source_auth,
                                    retries=0,
                                    copy_bundle_migrations=True,
                                )
                            )

        mock_oras.assert_called_once_with(
            "reg.io/img@sha256:abc",
            "prod.io/repo:v1.0",
            from_auth=source_auth,
            to_auth=dest_file,
            recursive=True,
            platform="linux/amd64",
        )
        assert result["url"] == "prod.io/repo:v1.0"

    def test_retries_on_failure(self, tmp_path: Path) -> None:
        """Retry a failed copy and succeed on the next attempt."""
        source_auth = tmp_path / "src_auth.json"
        source_auth.write_text('{"auths":{}}', encoding="utf-8")

        with patch(f"{TASK}.create_dest_auth_file") as mock_dest_auth:
            dest_file = tmp_path / "dest_auth.json"
            dest_file.write_text('{"auths":{}}', encoding="utf-8")
            mock_dest_auth.return_value = dest_file

            with patch(f"{TASK}.oras_utils.oras_resolve", return_value=None):
                with patch(f"{TASK}.create_combined_docker_config") as mock_config:
                    config_dir = tmp_path / "docker"
                    config_dir.mkdir()
                    (config_dir / "config.json").write_text("{}")
                    mock_config.return_value = config_dir

                    with patch(
                        f"{TASK}.cosign_copy",
                        side_effect=[
                            RuntimeError("timeout"),
                            None,
                        ],
                    ):
                        result = push_snapshot.push_image(
                            push_snapshot.PushJob(
                                origin_digest="sha256:abc",
                                name="comp1",
                                container_image="reg.io/img@sha256:abc",
                                repository_url="prod.io/repo",
                                tag="v1.0",
                                platform="",
                                source_auth_file=source_auth,
                                retries=1,
                                copy_bundle_migrations=False,
                            )
                        )

        assert result["url"] == "prod.io/repo:v1.0"

    def test_raises_after_max_retries(self, tmp_path: Path) -> None:
        """Raise once the retry budget is exhausted."""
        source_auth = tmp_path / "src_auth.json"
        source_auth.write_text('{"auths":{}}', encoding="utf-8")

        with patch(f"{TASK}.create_dest_auth_file") as mock_dest_auth:
            dest_file = tmp_path / "dest_auth.json"
            dest_file.write_text('{"auths":{}}', encoding="utf-8")
            mock_dest_auth.return_value = dest_file

            with patch(f"{TASK}.oras_utils.oras_resolve", return_value=None):
                with patch(f"{TASK}.create_combined_docker_config") as mock_config:
                    config_dir = tmp_path / "docker"
                    config_dir.mkdir()
                    (config_dir / "config.json").write_text("{}")
                    mock_config.return_value = config_dir

                    with patch(
                        f"{TASK}.cosign_copy",
                        side_effect=RuntimeError("persistent failure"),
                    ):
                        with pytest.raises(RuntimeError, match="persistent"):
                            push_snapshot.push_image(
                                push_snapshot.PushJob(
                                    origin_digest="sha256:abc",
                                    name="comp1",
                                    container_image="reg.io/img@sha256:abc",
                                    repository_url="prod.io/repo",
                                    tag="v1.0",
                                    platform="",
                                    source_auth_file=source_auth,
                                    retries=1,
                                    copy_bundle_migrations=False,
                                )
                            )


class TestPushMigrationArtifact:
    """Test push_migration_artifact function."""

    def test_skips_when_already_exists(self, tmp_path: Path) -> None:
        """Skip copying when the migration artifact already exists at destination."""
        source_auth = tmp_path / "src_auth.json"
        source_auth.write_text("{}", encoding="utf-8")

        with patch(f"{TASK}.create_dest_auth_file") as mock_dest_auth:
            dest_file = tmp_path / "dest_auth.json"
            dest_file.write_text("{}", encoding="utf-8")
            mock_dest_auth.return_value = dest_file

            with patch(
                f"{TASK}.oras_utils.oras_resolve",
                return_value="sha256:mig123",
            ):
                with patch(f"{TASK}.oras_utils.oras_cp") as mock_cp:
                    push_snapshot.push_migration_artifact(
                        push_snapshot.MigrationJob(
                            source_repo="reg.io/repo",
                            migration_digest="sha256:mig123",
                            name="comp1",
                            repository_url="prod.io/dest",
                            migration_tag="migration-v1",
                            source_auth_file=source_auth,
                            retries=0,
                        )
                    )

        mock_cp.assert_not_called()

    def test_copies_migration_artifact(self, tmp_path: Path) -> None:
        """Copy the migration artifact when it is missing at destination."""
        source_auth = tmp_path / "src_auth.json"
        source_auth.write_text("{}", encoding="utf-8")

        with patch(f"{TASK}.create_dest_auth_file") as mock_dest_auth:
            dest_file = tmp_path / "dest_auth.json"
            dest_file.write_text("{}", encoding="utf-8")
            mock_dest_auth.return_value = dest_file

            with patch(f"{TASK}.oras_utils.oras_resolve", return_value=None):
                with patch(f"{TASK}.oras_utils.oras_cp") as mock_cp:
                    push_snapshot.push_migration_artifact(
                        push_snapshot.MigrationJob(
                            source_repo="reg.io/repo",
                            migration_digest="sha256:mig123",
                            name="comp1",
                            repository_url="prod.io/dest",
                            migration_tag="migration-v1",
                            source_auth_file=source_auth,
                            retries=0,
                        )
                    )

        mock_cp.assert_called_once()
        call_args = mock_cp.call_args
        assert call_args[0][0] == "reg.io/repo@sha256:mig123"
        assert call_args[0][1] == "prod.io/dest:migration-v1"


class TestValidateSnapshot:
    """Test snapshot validation."""

    def test_passes_with_valid_tags(self) -> None:
        """Accept a snapshot where every repository has tags."""
        snapshot = {
            "components": [
                {"repositories": [{"tags": ["v1.0"]}]},
            ]
        }
        push_snapshot.validate_snapshot(snapshot)

    def test_fails_on_missing_tags(self) -> None:
        """Raise ``RuntimeError`` when a repository has no tags key."""
        snapshot = {
            "components": [
                {"repositories": [{"url": "prod.io/repo"}]},
            ]
        }
        with pytest.raises(RuntimeError, match="do not contain tags"):
            push_snapshot.validate_snapshot(snapshot)

    def test_fails_on_empty_tags(self) -> None:
        """Raise ``RuntimeError`` when a repository's tags list is empty."""
        snapshot = {
            "components": [
                {"repositories": [{"tags": []}]},
            ]
        }
        with pytest.raises(RuntimeError, match="do not contain tags"):
            push_snapshot.validate_snapshot(snapshot)


@pytest.fixture
def run_mocks(tmp_path: Path) -> Generator[types.SimpleNamespace, None, None]:
    """Patch common external dependencies for ``run()`` tests.

    Tests can override any mock's ``return_value`` or ``side_effect``
    before calling ``run()``.
    """
    src_auth = tmp_path / "src_auth.json"
    src_auth.write_text("{}", encoding="utf-8")

    with (
        patch(
            f"{TASK}.create_source_auth_file",
            return_value=src_auth,
        ) as m_auth,
        patch(f"{TASK}.get_image_architectures") as m_arch,
        patch(f"{TASK}.skopeo.inspect") as m_inspect,
        patch(f"{TASK}.oras_utils.oras_resolve") as m_resolve,
        patch(f"{TASK}.push_image") as m_push,
        patch(f"{TASK}.push_migration_artifact") as m_mig,
    ):
        m_arch.return_value = [{"platform": {"architecture": "amd64", "os": "linux"}}]
        m_inspect.return_value = subprocess.CompletedProcess(
            [], 0, stdout='{"mediaType": "my_media_type"}', stderr=""
        )
        m_resolve.return_value = "sha256:origin123"
        m_push.return_value = {
            "name": "comp1",
            "url": "prod.io/loc1:v1.0",
        }
        yield types.SimpleNamespace(
            src_auth=src_auth,
            create_source_auth_file=m_auth,
            get_image_architectures=m_arch,
            inspect=m_inspect,
            resolve=m_resolve,
            push_image=m_push,
            push_migration_artifact=m_mig,
        )


class TestRun:
    """Test the run() orchestration."""

    def test_missing_snapshot_file(self, tmp_path: Path) -> None:
        """Raise ``RuntimeError`` when the snapshot file does not exist."""
        with pytest.raises(RuntimeError, match="No valid snapshot file"):
            push_snapshot.run(
                tmp_path / "missing.json",
                tmp_path / "data.json",
                tmp_path / "results",
                20,
                3,
                False,
            )

    def test_missing_data_file(self, tmp_path: Path) -> None:
        """Raise ``RuntimeError`` when the data file does not exist."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot())
        with pytest.raises(RuntimeError, match="No data JSON"):
            push_snapshot.run(
                snapshot_file,
                tmp_path / "missing.json",
                tmp_path / "results",
                20,
                3,
                False,
            )

    def test_fails_on_tagless_component(self, tmp_path: Path) -> None:
        """Raise ``RuntimeError`` when a component's repository has no tags."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(
            snapshot_file,
            {
                "components": [
                    {
                        "name": "comp1",
                        "containerImage": "reg.io/img@sha256:abc",
                        "repositories": [{"url": "prod.io/loc"}],
                    }
                ]
            },
        )
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())

        with pytest.raises(RuntimeError, match="do not contain tags"):
            push_snapshot.run(
                snapshot_file,
                data_file,
                tmp_path / "results",
                20,
                3,
                False,
            )

    def test_basic_push_flow(self, tmp_path: Path, run_mocks) -> None:
        """Push each tag of a single component and write the results file."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot())
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())
        results_dir = tmp_path / "results"

        push_snapshot.run(snapshot_file, data_file, results_dir, 5, 0, False)

        assert run_mocks.push_image.call_count == 2
        results_file = results_dir / "push-snapshot-results.json"
        assert results_file.is_file()
        results = json.loads(results_file.read_text(encoding="utf-8"))
        assert results["images"][0]["name"] == "comp1"
        assert results["images"][0]["shasum"] == "sha256:origin123"

    def test_source_container_push(self, tmp_path: Path, run_mocks) -> None:
        """Push the source container tag alongside the main and tag-source pushes."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(
            snapshot_file,
            _default_snapshot(
                [
                    {
                        "name": "comp1",
                        "containerImage": "reg.io/img@sha256:abc",
                        "repositories": [{"url": "prod.io/loc", "tags": ["v1"]}],
                        "pushSourceContainer": True,
                    }
                ]
            ),
        )
        data_file = tmp_path / "data.json"
        _write_json(data_file, {"mapping": {"defaults": {}}})
        results_dir = tmp_path / "results"

        push_snapshot.run(snapshot_file, data_file, results_dir, 5, 0, False)

        # 1 source tag push + 1 main tag + 1 tag-source = 3
        assert run_mocks.push_image.call_count == 3

    def test_source_container_not_found_raises(self, tmp_path: Path, run_mocks) -> None:
        """Raise ``RuntimeError`` when the source container digest can't be resolved."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(
            snapshot_file,
            _default_snapshot(
                [
                    {
                        "name": "comp1",
                        "containerImage": "reg.io/img@sha256:abc",
                        "repositories": [{"url": "prod.io/loc", "tags": ["v1"]}],
                        "pushSourceContainer": True,
                    }
                ]
            ),
        )
        data_file = tmp_path / "data.json"
        _write_json(data_file, {"mapping": {"defaults": {}}})

        resolve_calls = [0]

        def _resolve(ref, **kwargs):
            resolve_calls[0] += 1
            if resolve_calls[0] == 1:
                return "sha256:origin123"
            return None

        run_mocks.resolve.side_effect = _resolve

        with pytest.raises(RuntimeError, match="Source container.*not found"):
            push_snapshot.run(
                snapshot_file,
                data_file,
                tmp_path / "results",
                5,
                0,
                False,
            )

    def test_multi_arch_sets_platform(self, tmp_path: Path, run_mocks) -> None:
        """Set the job platform to the first architecture of a multi-arch image."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot())
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())
        results_dir = tmp_path / "results"

        run_mocks.get_image_architectures.return_value = [
            {"platform": {"architecture": "amd64", "os": "linux"}},
            {"platform": {"architecture": "arm64", "os": "linux"}},
        ]
        idx = "application/vnd.oci.image.index.v1+json"
        run_mocks.inspect.return_value = subprocess.CompletedProcess(
            [], 0, stdout=json.dumps({"mediaType": idx}), stderr=""
        )

        push_snapshot.run(snapshot_file, data_file, results_dir, 5, 0, False)

        for c in run_mocks.push_image.call_args_list:
            job = c[0][0]
            assert job.platform == "linux/amd64"

    def test_migration_artifacts(self, tmp_path: Path, run_mocks) -> None:
        """Push the migration artifact annotated on a component."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(
            snapshot_file,
            _default_snapshot(
                [
                    {
                        "name": "comp1",
                        "containerImage": "reg.io/img@sha256:abc",
                        "repositories": [{"url": "prod.io/loc", "tags": ["v1"]}],
                        "pushSourceContainer": False,
                        "metadata": {
                            "annotations": [
                                {
                                    "name": "dev.konflux-ci.task.migration.digest",
                                    "value": "sha256:migdigest",
                                },
                                {
                                    "name": "dev.konflux-ci.task.migration.tag",
                                    "value": "mig-tag-v1",
                                },
                            ]
                        },
                    }
                ]
            ),
        )
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())
        results_dir = tmp_path / "results"

        push_snapshot.run(snapshot_file, data_file, results_dir, 5, 0, True)

        run_mocks.push_migration_artifact.assert_called_once_with(
            push_snapshot.MigrationJob(
                source_repo="reg.io/img",
                migration_digest="sha256:migdigest",
                name="comp1",
                repository_url="prod.io/loc",
                migration_tag="mig-tag-v1",
                source_auth_file=run_mocks.src_auth,
                retries=0,
            )
        )

    def test_migration_flag_without_annotations(self, tmp_path: Path, run_mocks) -> None:
        """Skip migration push when copy_bundle_migrations=True but no annotations."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot())
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())
        results_dir = tmp_path / "results"

        push_snapshot.run(snapshot_file, data_file, results_dir, 5, 0, True)

        run_mocks.push_migration_artifact.assert_not_called()

    def test_skopeo_inspect_failure_raises(self, tmp_path: Path, run_mocks) -> None:
        """A failed skopeo inspect --raw should abort the run."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot())
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())

        run_mocks.inspect.return_value = subprocess.CompletedProcess(
            [], 1, stdout="", stderr="connection refused"
        )

        with pytest.raises(RuntimeError, match="skopeo inspect failed"):
            push_snapshot.run(snapshot_file, data_file, tmp_path / "results", 5, 0, False)

    def test_origin_digest_resolve_failure_raises(self, tmp_path: Path, run_mocks) -> None:
        """A failed oras resolve for the origin digest should abort the run."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot())
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())

        run_mocks.resolve.return_value = None

        with pytest.raises(RuntimeError, match="Failed to resolve digest"):
            push_snapshot.run(snapshot_file, data_file, tmp_path / "results", 5, 0, False)

    def test_push_failure_raises(self, tmp_path: Path, run_mocks) -> None:
        """Raise ``RuntimeError`` when a push job fails."""
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot())
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())

        run_mocks.push_image.side_effect = RuntimeError("push failed")

        with pytest.raises(RuntimeError, match="One or more jobs failed"):
            push_snapshot.run(snapshot_file, data_file, tmp_path / "results", 5, 0, False)

    def test_burst_triggers_stabilization_delay(self, tmp_path: Path, run_mocks) -> None:
        """Every BURST_SIZE-th submitted job should sleep for stabilization."""
        components = [
            {
                "name": f"comp{i}",
                "containerImage": f"registry.io/image{i}@sha256:abc123",
                "repositories": [{"url": "prod.io/loc1", "tags": ["v1.0"]}],
                "pushSourceContainer": False,
            }
            for i in range(push_snapshot.BURST_SIZE)
        ]
        snapshot_file = tmp_path / "snapshot.json"
        _write_json(snapshot_file, _default_snapshot(components))
        data_file = tmp_path / "data.json"
        _write_json(data_file, _default_data())
        results_dir = tmp_path / "results"

        with patch(f"{TASK}.time.sleep") as mock_sleep:
            push_snapshot.run(snapshot_file, data_file, results_dir, 5, 0, False)

        mock_sleep.assert_called_once_with(push_snapshot.STABILIZATION_DELAY)


class TestMain:
    """Test the CLI entry point."""

    def test_success(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Read environment variables and call run() with parsed arguments."""
        snapshot_file = tmp_path / "snapshot.json"
        data_file = tmp_path / "data.json"
        results_dir = tmp_path / "results"
        _write_json(snapshot_file, _default_snapshot())
        _write_json(data_file, _default_data())

        monkeypatch.setenv("SNAPSHOT_FILE", str(snapshot_file))
        monkeypatch.setenv("DATA_FILE", str(data_file))
        monkeypatch.setenv("RESULTS_DIR", str(results_dir))
        monkeypatch.setenv("CONCURRENT_LIMIT", "2")
        monkeypatch.setenv("RETRIES", "0")

        with patch(f"{TASK}.authentication.setup_ca_cert"):
            with patch(f"{TASK}.run") as mock_run:
                push_snapshot.main()

        mock_run.assert_called_once_with(snapshot_file, data_file, results_dir, 2, 0, False)

    def test_missing_snapshot_file_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raise ``ValueError`` when SNAPSHOT_FILE is unset."""
        monkeypatch.delenv("SNAPSHOT_FILE", raising=False)
        monkeypatch.setenv("DATA_FILE", "/some/path")
        monkeypatch.setenv("RESULTS_DIR", "/some/dir")
        with pytest.raises(ValueError, match="SNAPSHOT_FILE must be set"):
            push_snapshot.main()

    def test_missing_data_file_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raise ``ValueError`` when DATA_FILE is unset."""
        monkeypatch.setenv("SNAPSHOT_FILE", "/some/path")
        monkeypatch.delenv("DATA_FILE", raising=False)
        monkeypatch.setenv("RESULTS_DIR", "/some/dir")
        with pytest.raises(ValueError, match="DATA_FILE must be set"):
            push_snapshot.main()

    def test_missing_results_dir_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raise ``ValueError`` when RESULTS_DIR is unset."""
        monkeypatch.setenv("SNAPSHOT_FILE", "/some/path")
        monkeypatch.setenv("DATA_FILE", "/some/path")
        monkeypatch.delenv("RESULTS_DIR", raising=False)
        with pytest.raises(ValueError, match="RESULTS_DIR must be set"):
            push_snapshot.main()

    def test_copy_bundle_migrations_enabled(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Parse COPY_BUNDLE_MIGRATIONS, RETRIES and CONCURRENT_LIMIT from env."""
        monkeypatch.setenv("SNAPSHOT_FILE", str(tmp_path / "snap.json"))
        monkeypatch.setenv("DATA_FILE", str(tmp_path / "data.json"))
        monkeypatch.setenv("RESULTS_DIR", str(tmp_path / "results"))
        monkeypatch.setenv("COPY_BUNDLE_MIGRATIONS", "true")
        monkeypatch.setenv("RETRIES", "5")
        monkeypatch.setenv("CONCURRENT_LIMIT", "10")

        with patch(f"{TASK}.authentication.setup_ca_cert"):
            with patch(f"{TASK}.run") as mock_run:
                push_snapshot.main()

        call_args = mock_run.call_args[0]
        assert call_args[3] == 10  # concurrent_limit
        assert call_args[4] == 5  # retries
        assert call_args[5] is True  # copy_bundle_migrations
