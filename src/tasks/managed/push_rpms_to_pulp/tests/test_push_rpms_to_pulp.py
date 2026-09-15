"""Tests for ``push_rpms_to_pulp``."""

from __future__ import annotations

import json
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.helpers.pulp_client import PulpDigestStatus
from release_service_utils.helpers.rpm_utils import RpmNevra
from release_service_utils.tasks.managed.push_rpms_to_pulp import push_rpms_to_pulp as task

TASK = "release_service_utils.tasks.managed.push_rpms_to_pulp.push_rpms_to_pulp"


def _toml(basic: bool = True) -> str:
    """Return cli.toml contents."""
    if basic:
        return (
            "[cli]\n"
            'base_url = "https://pulp.test"\n'
            'username = "user"\n'
            'password = "pass"\n'
        )
    return (
        "[cli]\n"
        'base_url = "https://pulp.test"\n'
        'client_id = "cid"\n'
        'client_secret = "csec"\n'
    )


def _config(
    tmp_path: Path,
    *,
    snapshot: Path | None = None,
    signed: str = "",
    artifacts_dir: str = "artifacts",
    basic: bool = True,
    timeout: int = 30,
) -> task.PushConfig:
    """Build a PushConfig rooted at *tmp_path*."""
    secret = tmp_path / "cli.toml"
    secret.write_text(_toml(basic=basic), encoding="utf-8")
    files_dir = tmp_path / "rpms"
    files_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return task.PushConfig(
        snapshot_path=snapshot,
        signed_rpms_oci_artifact=signed,
        pulp_domain="mydomain",
        pulp_config_file=secret,
        default_excludes=["-debuginfo-", "-debugsource-"],
        default_architectures=["x86_64", "aarch64", "s390x", "ppc64le"],
        data_dir=data_dir,
        results_dir_path="results",
        artifacts_json_dir_path=artifacts_dir,
        files_dir=files_dir,
        pulp_upload_chunk_size="100MB",
        pulp_task_timeout=timeout,
    )


def _write_snapshot(path: Path, components: list[dict]) -> None:
    """Write a snapshot JSON file."""
    path.write_text(json.dumps({"components": components}), encoding="utf-8")


def _local_rpm(filename: str, arch: str) -> task.LocalRpm:
    """Build a LocalRpm with dummy path and checksum for unit tests."""
    return task.LocalRpm(
        filename=filename,
        path=Path(filename),
        nevra=RpmNevra(name="hello", epoch="0", version="1.0", release="1", arch=arch),
        sha256="abc",
    )


class TestBuildRpmRepoMap:
    """Test snapshot rpmsToPublish mapping."""

    def test_merges_unique_repos(self) -> None:
        """Merge target repository names per RPM filename."""
        snapshot = {
            "components": [
                {
                    "rpmsToPublish": [
                        {
                            "rpm": "hello.x86_64.rpm",
                            "targetRepos": [
                                {"repository_name": "x86_64"},
                                {"repository_name": "x86_64"},
                                "ignored",
                                {"repository_id": "no-name"},
                            ],
                        },
                        {"targetRepos": [{"repository_name": "x86_64"}]},
                    ]
                }
            ]
        }
        assert task.build_rpm_repo_map(snapshot) == {"hello.x86_64.rpm": ["x86_64"]}

    def test_empty(self) -> None:
        """Empty snapshot yields an empty map."""
        assert task.build_rpm_repo_map({}) == {}


class TestCollectLocalRpms:
    """Test RPM filtering, NEVRA parsing, and checksum collection."""

    def test_filters(self, tmp_path: Path) -> None:
        """Skip directories, non-RPMs, and excluded names."""
        (tmp_path / "logs").mkdir()
        (tmp_path / "readme.txt").write_text("x", encoding="utf-8")
        (tmp_path / "hello-1.0-1.x86_64.rpm").write_bytes(b"")
        (tmp_path / "hello-debuginfo-1.0-1.x86_64.rpm").write_bytes(b"")
        rpms = task.collect_local_rpms(tmp_path, ["-debuginfo-"])
        assert [rpm.filename for rpm in rpms] == ["hello-1.0-1.x86_64.rpm"]

    def test_parses_nevra_and_checksum(self, tmp_path: Path) -> None:
        """Parse NEVRA from the filename when the RPM header is unreadable."""
        valid = tmp_path / "hello-1.0-1.x86_64.rpm"
        valid.write_bytes(b"rpm")
        rpms = task.collect_local_rpms(tmp_path, [])
        assert [rpm.filename for rpm in rpms] == ["hello-1.0-1.x86_64.rpm"]
        assert rpms[0].nevra.arch == "x86_64"
        assert rpms[0].sha256 == task.file_helper.sha256(valid)

    def test_unparseable_nevra_raises(self, tmp_path: Path) -> None:
        """Fail when an included RPM's NEVRA cannot be inferred."""
        (tmp_path / "odd.rpm").write_bytes(b"x")
        with pytest.raises(ValueError, match="Failed to parse NEVRA"):
            task.collect_local_rpms(tmp_path, [])


class TestDetectArches:
    """Test architecture detection and default-arch expansion."""

    def test_from_parsed_rpms(self) -> None:
        """Collect unique arches from parsed binary RPMs."""
        rpms = [
            _local_rpm("hello-1.0-1.x86_64.rpm", "x86_64"),
            _local_rpm("hello-1.0-1.aarch64.rpm", "aarch64"),
            _local_rpm("hello-1.0-1.src.rpm", "src"),
            _local_rpm("hello-docs-1.0-1.noarch.rpm", "noarch"),
        ]
        arches = task.detect_arches(rpms, ["x86_64", "aarch64", "s390x"], {})
        assert arches == ["x86_64", "aarch64", "s390x"]

    def test_noarch_only_uses_defaults(self) -> None:
        """Use default architectures when only noarch RPMs are present."""
        rpms = [_local_rpm("hello-docs-1.0-1.noarch.rpm", "noarch")]
        arches = task.detect_arches(rpms, ["x86_64", "s390x"], {})
        assert arches == ["x86_64", "s390x"]

    def test_filter_uses_targeted_repos(self) -> None:
        """Filter mapping selects targeted arch repos and drops source."""
        rpms = [_local_rpm("hello-1.0-1.x86_64.rpm", "x86_64")]
        mapping = {"hello-1.0-1.x86_64.rpm": ["ppc64le", "source", "x86_64"]}
        arches = task.detect_arches(rpms, ["x86_64", "aarch64"], mapping)
        assert arches == ["ppc64le", "x86_64"]

    def test_filter_sorts_targeted_repos(self) -> None:
        """Targeted repos are unique and alphabetically sorted."""
        mapping = {
            "hello-1.0-1.x86_64.rpm": ["x86_64", "source"],
            "hello-1.0-1.ppc64le.rpm": ["ppc64le", "x86_64"],
        }
        arches = task.detect_arches([], ["aarch64"], mapping)
        assert arches == ["ppc64le", "x86_64"]

    def test_filter_ignores_filenames(self) -> None:
        """When rpmsToPublish is set, arches come from the map, not filenames."""
        rpms = [_local_rpm("hello-1.0-1.aarch64.rpm", "aarch64")]
        mapping = {"other-1.0-1.x86_64.rpm": ["ppc64le", "source"]}
        arches = task.detect_arches(rpms, ["x86_64", "aarch64"], mapping)
        assert arches == ["ppc64le"]

    def test_empty_files(self) -> None:
        """No files yields no architectures."""
        assert task.detect_arches([], ["x86_64"], {}) == []


class TestPublishRepoOrder:
    """Test which repositories are preflighted and processed."""

    def test_binary_only_omits_source(self) -> None:
        """Do not require a source repo when no source RPM will be published."""
        rpms = [_local_rpm("hello-1.0-1.x86_64.rpm", "x86_64")]
        assert task.publish_repo_order(rpms, ["x86_64"], {}) == ["x86_64"]

    def test_includes_source_when_src_rpm_present(self) -> None:
        """Append source when a source RPM is in the upload set."""
        rpms = [
            _local_rpm("hello-1.0-1.x86_64.rpm", "x86_64"),
            _local_rpm("hello-1.0-1.src.rpm", "src"),
        ]
        assert task.publish_repo_order(rpms, ["x86_64"], {}) == ["x86_64", "source"]

    def test_filter_omits_untargeted_source(self) -> None:
        """A source RPM on disk is ignored when the filter does not target source."""
        rpms = [
            _local_rpm("hello-1.0-1.x86_64.rpm", "x86_64"),
            _local_rpm("hello-1.0-1.src.rpm", "src"),
        ]
        mapping = {"hello-1.0-1.x86_64.rpm": ["x86_64"]}
        assert task.publish_repo_order(rpms, ["x86_64"], mapping) == ["x86_64"]

    def test_filter_includes_targeted_source(self) -> None:
        """Keep source when the filter maps a source RPM to the source repo."""
        rpms = [_local_rpm("hello-1.0-1.src.rpm", "src")]
        mapping = {"hello-1.0-1.src.rpm": ["source"]}
        assert task.publish_repo_order(rpms, [], mapping) == ["source"]

    def test_empty(self) -> None:
        """No RPMs yields no repositories to preflight."""
        assert task.publish_repo_order([], [], {}) == []


class TestContentUrl:
    """Test pulp-content URL construction."""

    def test_uses_first_letter_of_name(self) -> None:
        """Package URLs use the lowercased first letter of the RPM name."""
        url = task.content_url(
            "https://pulp.test", "mydomain", "x86_64", "Hello-1.rpm", "Hello"
        )
        assert url.endswith("/Packages/h/Hello-1.rpm")


class TestIsTargeted:
    """Test rpm-to-repo filter matching."""

    def test_unfiltered(self) -> None:
        """Empty map targets every repo."""
        assert task._is_targeted({}, "pkg.rpm", "x86_64")

    def test_filtered_hit(self) -> None:
        """Listed repo is targeted."""
        assert task._is_targeted({"pkg.rpm": ["x86_64"]}, "pkg.rpm", "x86_64")

    def test_filtered_miss(self) -> None:
        """Unlisted repo is not targeted."""
        assert not task._is_targeted({"pkg.rpm": ["aarch64"]}, "pkg.rpm", "x86_64")


class TestPlacementFor:
    """Test which RPMs belong in which repository."""

    def test_arch_specific(self) -> None:
        """Binary RPMs map to the matching arch repo."""
        rpm = _local_rpm("hello-1.0-1.x86_64.rpm", "x86_64")
        assert task._placement_for(rpm, "x86_64") == task.RpmPlacement(
            repo="x86_64", result_arch="x86_64", artifact_arch="x86_64"
        )

    def test_arch_mismatch(self) -> None:
        """Binary RPMs are not placed in a different arch repo."""
        rpm = _local_rpm("hello-1.0-1.x86_64.rpm", "x86_64")
        assert task._placement_for(rpm, "aarch64") is None

    def test_noarch_in_arch_repo(self) -> None:
        """Noarch RPMs use result_arch and artifact_arch noarch."""
        rpm = _local_rpm("hello-docs-1.0-1.noarch.rpm", "noarch")
        assert task._placement_for(rpm, "x86_64") == task.RpmPlacement(
            repo="x86_64", result_arch="noarch", artifact_arch="noarch"
        )

    def test_source(self) -> None:
        """Source RPMs map only to the source repo."""
        src = _local_rpm("hello-1.0-1.src.rpm", "src")
        binary = _local_rpm("hello-1.0-1.x86_64.rpm", "x86_64")
        noarch = _local_rpm("hello-docs-1.0-1.noarch.rpm", "noarch")
        assert task._placement_for(src, "source") == task.RpmPlacement(
            repo="source", result_arch="src", artifact_arch="source"
        )
        assert task._placement_for(src, "x86_64") is None
        assert task._placement_for(binary, "source") is None
        assert task._placement_for(noarch, "source") is None

    def test_uses_nevra_arch_not_filename(self) -> None:
        """Header architecture wins when it disagrees with the filename suffix."""
        rpm = _local_rpm("hello-1.0-1.x86_64.rpm", "aarch64")
        assert task._placement_for(rpm, "aarch64") == task.RpmPlacement(
            repo="aarch64", result_arch="aarch64", artifact_arch="aarch64"
        )
        assert task._placement_for(rpm, "x86_64") is None


class TestExtractFromSigned:
    """Test signed OCI artifact extraction."""

    def test_pulls_without_archive(self, tmp_path: Path) -> None:
        """Oras pull is used and missing signed-rpms is ignored."""
        with patch(f"{TASK}.oras_utils.oras_pull") as mock_pull:
            task.extract_from_signed("oci:quay.io/a@sha256:1", tmp_path)
        mock_pull.assert_called_once_with("quay.io/a@sha256:1", tmp_path)

    def test_extracts_archive(self, tmp_path: Path) -> None:
        """Unpack signed-rpms and remove the archive."""
        inner = tmp_path / "payload"
        inner.mkdir()
        rpm = inner / "hello-1.0-1.x86_64.rpm"
        rpm.write_bytes(b"rpm")
        archive = tmp_path / "signed-rpms"
        with tarfile.open(archive, "w:gz") as tf:
            tf.add(rpm, arcname="hello-1.0-1.x86_64.rpm")

        def fake_pull(_ref: str, dest: Path) -> None:
            dest.mkdir(parents=True, exist_ok=True)

        with patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull):
            task.extract_from_signed("quay.io/a@sha256:1", tmp_path)
        assert not archive.exists()
        assert (tmp_path / "hello-1.0-1.x86_64.rpm").is_file()


class TestExtractFromSnapshot:
    """Test snapshot image pulls."""

    def test_pulls_each_image(self, tmp_path: Path) -> None:
        """Pull present images, skip missing/blank containerImage, return the rpm map."""
        snap = tmp_path / "snap.json"
        _write_snapshot(
            snap,
            [
                {
                    "containerImage": "quay.io/a",
                    "rpmsToPublish": [
                        {
                            "rpm": "hello-1.0-1.x86_64.rpm",
                            "targetRepos": [{"repository_name": "x86_64"}],
                        }
                    ],
                },
                {},
                {"containerImage": None},
                {"containerImage": "  "},
            ],
        )
        with patch(f"{TASK}.oras_utils.oras_pull") as mock_pull:
            mapping = task.extract_from_snapshot(snap, tmp_path / "out")
        assert mapping == {"hello-1.0-1.x86_64.rpm": ["x86_64"]}
        mock_pull.assert_called_once_with("quay.io/a", tmp_path / "out")


def _happy_files(files_dir: Path) -> None:
    """Create the catalog happy-path RPM set."""
    names = [
        "hello-2.12.1-6.fc44.aarch64.rpm",
        "hello-2.12.1-6.fc44.ppc64le.rpm",
        "hello-2.12.1-6.fc44.s390x.rpm",
        "hello-2.12.1-6.fc44.src.rpm",
        "hello-2.12.1-6.fc44.x86_64.rpm",
        "hello-docs-2.12.1-6.fc44.noarch.rpm",
        "hello-debuginfo-2.12.1-6.fc44.x86_64.rpm",
        "notes.txt",
    ]
    for name in names:
        (files_dir / name).write_bytes(b"")
    (files_dir / "logs").mkdir()


class TestRun:
    """Test the full push workflow."""

    def test_happy_path(self, tmp_path: Path) -> None:
        """Upload arch, noarch, and source RPMs and write both JSON files."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [{"containerImage": "quay.io/test/happypath"}])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.NOT_FOUND
        client.upload_rpm.return_value = "/href/1"

        def fake_pull(_image: str, dest: Path) -> None:
            _happy_files(dest)

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client) as mock_from_config,
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        ):
            task.run(cfg)

        mock_from_config.assert_called_once()
        results = json.loads(
            (cfg.data_dir / "results" / "push-rpms-to-pulp-results.json").read_text()
        )
        artifacts = json.loads((cfg.data_dir / "artifacts" / "artifacts.json").read_text())
        uploaded = [call.args[0].name for call in client.upload_rpm.call_args_list]
        assert uploaded == [
            "hello-2.12.1-6.fc44.aarch64.rpm",
            "hello-2.12.1-6.fc44.ppc64le.rpm",
            "hello-2.12.1-6.fc44.s390x.rpm",
            "hello-2.12.1-6.fc44.src.rpm",
            "hello-2.12.1-6.fc44.x86_64.rpm",
            "hello-docs-2.12.1-6.fc44.noarch.rpm",
        ]
        assert len(results["rpmfiles"]) == 9
        assert len([row for row in results["rpmfiles"] if row["arch"] == "noarch"]) == 4
        assert {row["pulprepo"] for row in results["rpmfiles"] if row["arch"] == "noarch"} == {
            "mydomain/aarch64",
            "mydomain/ppc64le",
            "mydomain/s390x",
            "mydomain/x86_64",
        }
        assert len(artifacts["artifacts"]) == 6
        assert "source" in artifacts["distributions"]
        noarch_url = artifacts["artifacts"]["hello-docs-2.12.1-6.fc44.noarch.rpm"]["url"]
        assert "/aarch64/Packages/" in noarch_url
        assert [call.args[0] for call in client.add_content.call_args_list] == [
            "aarch64",
            "ppc64le",
            "s390x",
            "x86_64",
            "source",
        ]
        assert client.check_digest.call_args.kwargs["fallback_to_latest"] is False
        client.ensure_domain_exists.assert_called_once()
        client.ensure_repos_exist.assert_called_once_with(
            ["aarch64", "ppc64le", "s390x", "x86_64", "source"]
        )
        assert all(call.args[1] == "100MB" for call in client.upload_rpm.call_args_list)
        copied_configs = [call.args[2] for call in client.upload_rpm.call_args_list]
        assert copied_configs
        assert len(set(copied_configs)) == 1
        copied = copied_configs[0]
        assert copied != cfg.pulp_config_file
        assert not copied.exists()

    def test_signed_artifact_oauth_and_results_dir_fallback(self, tmp_path: Path) -> None:
        """Signed artifact path, oauth log, and artifacts.json in the results dir."""
        cfg = _config(
            tmp_path, signed="oci:quay.io/signed@sha256:abc", artifacts_dir="", basic=False
        )
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.NOT_FOUND
        client.upload_rpm.return_value = "/href/1"

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-2.12.1-6.fc44.x86_64.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        ):
            task.run(cfg)

        artifacts = json.loads((cfg.data_dir / "results" / "artifacts.json").read_text())
        assert "hello-2.12.1-6.fc44.x86_64.rpm" in artifacts["artifacts"]
        client.ensure_repos_exist.assert_called_once_with(["x86_64"])

    def test_copies_pulp_config_to_writable_temp(self, tmp_path: Path) -> None:
        """Copy cli.toml to a temp file for the pulp CLI, then delete it."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [{"containerImage": "quay.io/a"}])
        cfg = _config(tmp_path, snapshot=snap)
        source_bytes = cfg.pulp_config_file.read_bytes()
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.NOT_FOUND
        client.upload_rpm.return_value = "/href/1"
        created: list[Path] = []
        real_make = task.file_helper.make_tempfile_path

        def tracking_make(prefix: str, data: bytes | None = None) -> Path:
            assert prefix == "pulp-cli-"
            assert data == source_bytes
            path = real_make(prefix, data)
            created.append(path)
            return path

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-1.0-1.x86_64.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
            patch(f"{TASK}.file_helper.make_tempfile_path", side_effect=tracking_make),
        ):
            task.run(cfg)

        assert created == [client.upload_rpm.call_args.args[2]]
        assert created[0] != cfg.pulp_config_file
        assert not created[0].exists()

    def test_writable_config_removed_on_failure(self, tmp_path: Path) -> None:
        """Delete the temp Pulp config even when the push fails."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        client.ensure_domain_exists.side_effect = RuntimeError("no domain")
        created: list[Path] = []
        real_make = task.file_helper.make_tempfile_path

        def tracking_make(prefix: str, data: bytes | None = None) -> Path:
            path = real_make(prefix, data)
            created.append(path)
            return path

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.file_helper.make_tempfile_path", side_effect=tracking_make),
            pytest.raises(RuntimeError, match="no domain"),
        ):
            task.run(cfg)

        assert created
        assert all(not path.exists() for path in created)

    def test_missing_snapshot_raises(self, tmp_path: Path) -> None:
        """A provided snapshot path that does not exist raises FileNotFoundError."""
        cfg = _config(tmp_path, snapshot=tmp_path / "missing.json")
        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=MagicMock()),
            pytest.raises(FileNotFoundError),
        ):
            task.run(cfg)

    def test_none_snapshot_raises(self, tmp_path: Path) -> None:
        """Raise when both the signed artifact and snapshot path are absent."""
        cfg = _config(tmp_path, snapshot=None)
        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=MagicMock()),
            pytest.raises(RuntimeError, match="SNAPSHOT_PATH must be provided"),
        ):
            task.run(cfg)

    def test_empty_components_writes_source_distribution(self, tmp_path: Path) -> None:
        """An empty snapshot still writes results and a source distribution."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull") as mock_pull,
        ):
            task.run(cfg)
        mock_pull.assert_not_called()
        artifacts = json.loads((cfg.data_dir / "artifacts" / "artifacts.json").read_text())
        assert artifacts["artifacts"] == {}
        assert "source" in artifacts["distributions"]
        client.ensure_repos_exist.assert_called_once_with([])
        client.add_content.assert_not_called()

    def test_source_only_uses_default_first_arch(self, tmp_path: Path) -> None:
        """Source-only uploads still record a source artifact and distribution."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [{"containerImage": "quay.io/a"}])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.NOT_FOUND
        client.upload_rpm.return_value = "/href/src"

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-2.12.1-6.fc44.src.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        ):
            task.run(cfg)

        artifacts = json.loads((cfg.data_dir / "artifacts" / "artifacts.json").read_text())
        assert "hello-2.12.1-6.fc44.src.rpm" in artifacts["artifacts"]
        assert artifacts["artifacts"]["hello-2.12.1-6.fc44.src.rpm"]["labels"]["arch"] == (
            "source"
        )
        client.ensure_repos_exist.assert_called_once_with(["source"])
        client.add_content.assert_called_once_with("source", ["/href/src"], 30)

    def test_digest_match_skips_upload(self, tmp_path: Path) -> None:
        """Matching digests skip upload and still populate artifacts.json."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [{"containerImage": "quay.io/a"}])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.MATCH

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-2.12.1-6.fc44.x86_64.rpm").write_bytes(b"")
            (dest / "hello-docs-2.12.1-6.fc44.noarch.rpm").write_bytes(b"")
            (dest / "hello-2.12.1-6.fc44.src.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        ):
            task.run(cfg)

        client.upload_rpm.assert_not_called()
        artifacts = json.loads((cfg.data_dir / "artifacts" / "artifacts.json").read_text())
        assert "hello-2.12.1-6.fc44.x86_64.rpm" in artifacts["artifacts"]
        assert "hello-docs-2.12.1-6.fc44.noarch.rpm" in artifacts["artifacts"]
        assert "hello-2.12.1-6.fc44.src.rpm" in artifacts["artifacts"]
        client.add_content.assert_not_called()

    def test_digest_mismatch_raises(self, tmp_path: Path) -> None:
        """A digest mismatch fails the task."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [{"containerImage": "quay.io/a"}])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.MISMATCH

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-2.12.1-6.fc44.x86_64.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
            pytest.raises(RuntimeError, match="different digest"),
        ):
            task.run(cfg)

    def test_filter_skips_untargeted(self, tmp_path: Path) -> None:
        """Only RPMs listed in rpmsToPublish are uploaded."""
        snap = tmp_path / "snap.json"
        _write_snapshot(
            snap,
            [
                {
                    "containerImage": "quay.io/a",
                    "rpmsToPublish": [
                        {
                            "rpm": "hello-2.12.1-6.fc44.x86_64.rpm",
                            "targetRepos": [{"repository_name": "x86_64"}],
                        }
                    ],
                }
            ],
        )
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.NOT_FOUND
        client.upload_rpm.return_value = "/href/1"

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-2.12.1-6.fc44.x86_64.rpm").write_bytes(b"")
            (dest / "other-1.0-1.x86_64.rpm").write_bytes(b"")
            (dest / "hello-2.12.1-6.fc44.src.rpm").write_bytes(b"")
            (dest / "hello-docs-2.12.1-6.fc44.noarch.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        ):
            task.run(cfg)

        uploaded = [call.args[0].name for call in client.upload_rpm.call_args_list]
        assert uploaded == ["hello-2.12.1-6.fc44.x86_64.rpm"]
        client.ensure_repos_exist.assert_called_once_with(["x86_64"])

    def test_noarch_partial_match_uploads_once(self, tmp_path: Path) -> None:
        """Upload noarch once and add it only to repos missing a matching digest."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [{"containerImage": "quay.io/a"}])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()

        def fake_digest(repo: str, *_args: object, **_kwargs: object) -> PulpDigestStatus:
            if repo == "x86_64":
                return PulpDigestStatus.MATCH
            return PulpDigestStatus.NOT_FOUND

        client.check_digest.side_effect = fake_digest
        client.upload_rpm.return_value = "/href/noarch"

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-docs-2.12.1-6.fc44.noarch.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        ):
            task.run(cfg)

        client.upload_rpm.assert_called_once()
        added_repos = [call.args[0] for call in client.add_content.call_args_list]
        assert added_repos == ["aarch64", "s390x", "ppc64le"]
        results = json.loads(
            (cfg.data_dir / "results" / "push-rpms-to-pulp-results.json").read_text()
        )
        assert {row["pulprepo"] for row in results["rpmfiles"]} == {
            "mydomain/aarch64",
            "mydomain/s390x",
            "mydomain/ppc64le",
        }
        artifacts = json.loads((cfg.data_dir / "artifacts" / "artifacts.json").read_text())
        assert "hello-docs-2.12.1-6.fc44.noarch.rpm" in artifacts["artifacts"]
        client.ensure_repos_exist.assert_called_once_with(
            ["x86_64", "aarch64", "s390x", "ppc64le"]
        )

    def test_source_mismatch_raises(self, tmp_path: Path) -> None:
        """Source RPM digest mismatch fails the task."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [{"containerImage": "quay.io/a"}])
        cfg = _config(tmp_path, snapshot=snap)
        client = MagicMock()
        client.check_digest.return_value = PulpDigestStatus.MISMATCH

        def fake_pull(_image: str, dest: Path) -> None:
            (dest / "hello-2.12.1-6.fc44.src.rpm").write_bytes(b"")

        with (
            patch(f"{TASK}.PulpClient.from_config", return_value=client),
            patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
            pytest.raises(RuntimeError, match="different digest"),
        ):
            task.run(cfg)


class TestMain:
    """Test Tekton env wiring."""

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() reads env and calls run()."""
        secret = tmp_path / "cli.toml"
        secret.write_text(_toml(), encoding="utf-8")
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [])
        monkeypatch.setenv("PULP_DOMAIN", "mydomain")
        monkeypatch.setenv("RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("SNAPSHOT_PATH", str(snap))
        monkeypatch.setenv("PULP_CONFIG_FILE", str(secret))
        monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("FILES_DIR", str(tmp_path / "rpms"))
        monkeypatch.setenv("ARTIFACTS_JSON_DIR_PATH", "artifacts")
        monkeypatch.setenv("SIGNED_RPMS_OCI_ARTIFACT", "")
        monkeypatch.setenv("PULP_TASK_TIMEOUT", "30")
        with patch(f"{TASK}.run") as mock_run:
            assert task.main() == 0
        mock_run.assert_called_once()
        cfg = mock_run.call_args.args[0]
        assert cfg.pulp_domain == "mydomain"
        assert cfg.snapshot_path == snap
        assert cfg.pulp_task_timeout == 30

    def test_rejects_non_positive_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Non-positive PULP_TASK_TIMEOUT raises ValueError."""
        monkeypatch.setenv("PULP_TASK_TIMEOUT", "0")
        monkeypatch.setenv("PULP_DOMAIN", "mydomain")
        monkeypatch.setenv("RESULTS_DIR_PATH", "results")
        with pytest.raises(ValueError, match="positive integer"):
            task.main()

    def test_empty_snapshot_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Blank SNAPSHOT_PATH becomes None."""
        secret = tmp_path / "cli.toml"
        secret.write_text(_toml(), encoding="utf-8")
        monkeypatch.setenv("PULP_DOMAIN", "mydomain")
        monkeypatch.setenv("RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("SNAPSHOT_PATH", "   ")
        monkeypatch.setenv("PULP_CONFIG_FILE", str(secret))
        monkeypatch.setenv("SIGNED_RPMS_OCI_ARTIFACT", "oci:quay.io/a")
        with patch(f"{TASK}.run") as mock_run:
            assert task.main() == 0
        assert mock_run.call_args.args[0].snapshot_path is None
        assert mock_run.call_args.args[0].signed_rpms_oci_artifact == "oci:quay.io/a"

    def test_defaults(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Optional env vars fall back to task defaults."""
        monkeypatch.setenv("PULP_DOMAIN", "mydomain")
        monkeypatch.setenv("RESULTS_DIR_PATH", "results")
        monkeypatch.delenv("PULP_TASK_TIMEOUT", raising=False)
        monkeypatch.delenv("SNAPSHOT_PATH", raising=False)
        monkeypatch.delenv("SIGNED_RPMS_OCI_ARTIFACT", raising=False)
        monkeypatch.delenv("DEFAULT_EXCLUDES", raising=False)
        monkeypatch.delenv("DEFAULT_ARCHITECTURES", raising=False)
        monkeypatch.delenv("ARTIFACTS_JSON_DIR_PATH", raising=False)
        monkeypatch.delenv("PULP_UPLOAD_CHUNK_SIZE", raising=False)
        monkeypatch.delenv("PULP_CONFIG_FILE", raising=False)
        monkeypatch.delenv("DATA_DIR", raising=False)
        monkeypatch.delenv("FILES_DIR", raising=False)
        with patch(f"{TASK}.run") as mock_run:
            assert task.main() == 0
        cfg = mock_run.call_args.args[0]
        assert cfg.pulp_task_timeout == task.DEFAULT_TASK_TIMEOUT
        assert cfg.snapshot_path is None
        assert cfg.files_dir == task.DEFAULT_FILES_DIR
        assert cfg.pulp_config_file == task.DEFAULT_PULP_SECRET
        assert cfg.pulp_upload_chunk_size == task.DEFAULT_CHUNK_SIZE

    def test_invalid_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Non-integer PULP_TASK_TIMEOUT raises ValueError."""
        monkeypatch.setenv("PULP_TASK_TIMEOUT", "abc")
        monkeypatch.setenv("PULP_DOMAIN", "mydomain")
        monkeypatch.setenv("RESULTS_DIR_PATH", "results")
        with pytest.raises(ValueError):
            task.main()

    def test_package_main_module(self) -> None:
        """Importing the package ``__main__`` exposes ``main``."""
        import importlib

        mod = importlib.import_module(
            "release_service_utils.tasks.managed.push_rpms_to_pulp.__main__"
        )
        assert mod.main is task.main
