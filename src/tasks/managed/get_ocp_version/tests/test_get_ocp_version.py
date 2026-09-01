"""Test OCP version collection and validation across FBC fragment components."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from release_service_utils.tasks.managed.get_ocp_version import get_ocp_version

TASK = "release_service_utils.tasks.managed.get_ocp_version.get_ocp_version"


def _snapshot(components: list[dict]) -> dict:
    """Build a minimal snapshot dict."""
    return {"application": "test", "components": components}


def _component(image: str) -> dict:
    """Build a component dict with a containerImage."""
    return {"name": "comp", "containerImage": image}


def _completed(
    stdout: str = "", returncode: int = 0, stderr: str = ""
) -> subprocess.CompletedProcess:
    """Build a subprocess.CompletedProcess like skopeo.inspect() returns."""
    return subprocess.CompletedProcess(
        args=["skopeo", "inspect"], returncode=returncode, stdout=stdout, stderr=stderr
    )


def _manifest(media_type: str, base_name: str | None = None) -> str:
    """Build a raw skopeo inspect manifest JSON string."""
    manifest: dict = {"mediaType": media_type}
    if base_name is not None:
        manifest["annotations"] = {"org.opencontainers.image.base.name": base_name}
    return json.dumps(manifest)


class TestBaseNameTag:
    """Test _base_name_tag extraction of the version tag from annotations."""

    def test_extracts_tag_after_last_colon(self) -> None:
        """Tag after the last colon in the base.name annotation is returned."""
        manifest = {
            "annotations": {
                "org.opencontainers.image.base.name": (
                    "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.12"
                )
            }
        }
        assert get_ocp_version._base_name_tag(manifest) == "v4.12"

    def test_missing_annotations_returns_empty(self) -> None:
        """Manifest without annotations returns an empty string."""
        assert get_ocp_version._base_name_tag({}) == ""

    def test_missing_base_name_returns_empty(self) -> None:
        """Manifest with other annotations but no base.name returns empty."""
        assert get_ocp_version._base_name_tag({"annotations": {"other": "value"}}) == ""

    def test_empty_base_name_returns_empty(self) -> None:
        """Empty base.name annotation value returns an empty string."""
        manifest = {"annotations": {"org.opencontainers.image.base.name": ""}}
        assert get_ocp_version._base_name_tag(manifest) == ""


class TestResolveOcpVersion:
    """Test resolve_ocp_version, including single-arch and multi-arch paths."""

    def test_single_arch_returns_tag_directly(self) -> None:
        """Single-arch manifest resolves the version without a second inspect."""
        with patch(
            f"{TASK}.skopeo.inspect",
            return_value=_completed(
                stdout=_manifest(
                    "application/vnd.oci.image.manifest.v1+json",
                    "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.12",
                )
            ),
        ) as mock_inspect:
            version = get_ocp_version.resolve_ocp_version(
                "quay.io/fbc/test-fbc-component@sha256:manifest"
            )
        assert version == "v4.12"
        mock_inspect.assert_called_once_with(
            "quay.io/fbc/test-fbc-component@sha256:manifest", raw=True, check=True
        )

    def test_oci_index_resolves_via_get_image_architectures(self) -> None:
        """OCI image index is resolved to the first platform digest, then re-inspected."""
        arch_output = (
            json.dumps({"digest": "sha256:manifest", "platform": {"architecture": "amd64"}})
            + "\n"
            + json.dumps(
                {"digest": "sha256:manifest", "platform": {"architecture": "ppc64le"}}
            )
        )
        with (
            patch(
                f"{TASK}.skopeo.inspect",
                side_effect=[
                    _completed(
                        stdout=json.dumps(
                            {"mediaType": "application/vnd.oci.image.index.v1+json"}
                        )
                    ),
                    _completed(
                        stdout=_manifest(
                            "application/vnd.oci.image.manifest.v1+json",
                            "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.12",
                        )
                    ),
                ],
            ) as mock_inspect,
            patch(
                f"{TASK}.run_cmd_text",
                return_value=arch_output,
            ) as mock_run_cmd,
        ):
            version = get_ocp_version.resolve_ocp_version(
                "quay.io/fbc/multi-arch@sha256:index"
            )
        assert version == "v4.12"
        mock_run_cmd.assert_called_once_with(
            ["get-image-architectures", "quay.io/fbc/multi-arch@sha256:index"]
        )
        assert mock_inspect.call_args_list[1].args == (
            "quay.io/fbc/multi-arch@sha256:manifest",
        )
        assert mock_inspect.call_args_list[1].kwargs == {"raw": True, "check": True}

    def test_docker_v2s2_manifest_list_resolves(self) -> None:
        """Docker v2 schema 2 manifest-list media type also triggers multi-arch resolution."""
        arch_output = json.dumps(
            {"digest": "sha256:dockerv2s2manifest", "platform": {"architecture": "amd64"}}
        )
        with (
            patch(
                f"{TASK}.skopeo.inspect",
                side_effect=[
                    _completed(
                        stdout=json.dumps(
                            {
                                "mediaType": (
                                    "application/vnd.docker.distribution.manifest.list.v2+json"
                                )
                            }
                        )
                    ),
                    _completed(
                        stdout=_manifest(
                            "application/vnd.docker.distribution.manifest.v2+json",
                            "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.15",
                        )
                    ),
                ],
            ),
            patch(f"{TASK}.run_cmd_text", return_value=arch_output),
        ):
            version = get_ocp_version.resolve_ocp_version(
                "quay.io/hacbs-release-tests/test-ocp-version/test-fbc-component-docker-v2s2"
                "@sha256:dockerv2s2index"
            )
        assert version == "v4.15"

    def test_skopeo_failure_propagates(self) -> None:
        """A failing skopeo inspect (check=True) raises CalledProcessError."""
        with patch(
            f"{TASK}.skopeo.inspect",
            side_effect=subprocess.CalledProcessError(1, "skopeo"),
        ):
            with pytest.raises(subprocess.CalledProcessError):
                get_ocp_version.resolve_ocp_version("reg.io/img@sha256:missing")


class TestValidateOcpVersions:
    """Test validate_ocp_versions across multiple components."""

    def test_single_component_happy_path(self) -> None:
        """A single valid component returns its version."""
        snapshot = _snapshot([_component("quay.io/fbc/test-fbc-component@sha256:manifest")])
        with patch(f"{TASK}.resolve_ocp_version", return_value="v4.12"):
            assert get_ocp_version.validate_ocp_versions(snapshot) == "v4.12"

    def test_matching_versions_across_components(self) -> None:
        """Multiple components reporting the same version validate successfully."""
        snapshot = _snapshot(
            [
                _component("quay.io/fbc/comp-a@sha256:a"),
                _component("quay.io/fbc/comp-b@sha256:b"),
            ]
        )
        with patch(f"{TASK}.resolve_ocp_version", return_value="v4.14"):
            assert get_ocp_version.validate_ocp_versions(snapshot) == "v4.14"

    def test_version_mismatch_raises(self) -> None:
        """A version mismatch between components raises ValueError."""
        snapshot = _snapshot(
            [
                _component("quay.io/fbc/comp-a@sha256:a"),
                _component("quay.io/fbc/comp-b@sha256:b"),
            ]
        )
        with patch(
            f"{TASK}.resolve_ocp_version",
            side_effect=["v4.12", "v4.13"],
        ):
            with pytest.raises(ValueError, match="OCP version mismatch"):
                get_ocp_version.validate_ocp_versions(snapshot)

    def test_invalid_version_format_raises(self) -> None:
        """A malformed version string raises ValueError."""
        snapshot = _snapshot([_component("quay.io/fbc/comp-a@sha256:a")])
        with patch(f"{TASK}.resolve_ocp_version", return_value="not-a-version"):
            with pytest.raises(ValueError, match="Invalid OCP version format"):
                get_ocp_version.validate_ocp_versions(snapshot)

    def test_empty_components_raises(self) -> None:
        """An empty components list raises ValueError."""
        with pytest.raises(ValueError, match="No components found"):
            get_ocp_version.validate_ocp_versions(_snapshot([]))

    def test_missing_components_key_raises(self) -> None:
        """A snapshot without a components key raises ValueError."""
        with pytest.raises(ValueError, match="No components found"):
            get_ocp_version.validate_ocp_versions({"application": "test"})

    def test_missing_container_image_raises(self) -> None:
        """A component without a containerImage raises KeyError."""
        snapshot = _snapshot([{"name": "comp"}])
        with pytest.raises(KeyError, match="containerImage"):
            get_ocp_version.validate_ocp_versions(snapshot)


class TestRun:
    """Test the run() orchestration: file I/O plus validation."""

    def test_writes_version_to_result_file(self, tmp_path: Path) -> None:
        """run() writes the validated version to the result path, no trailing newline."""
        snapshot_path = tmp_path / "snapshot.json"
        snapshot_path.write_text(
            json.dumps(_snapshot([_component("quay.io/fbc/comp-a@sha256:a")])),
            encoding="utf-8",
        )
        result_path = tmp_path / "stored-version"
        with patch(f"{TASK}.validate_ocp_versions", return_value="v4.12"):
            get_ocp_version.run(snapshot_path, result_path)
        assert result_path.read_text(encoding="utf-8") == "v4.12"

    def test_missing_snapshot_raises(self, tmp_path: Path) -> None:
        """A missing snapshot file raises naturally (FileNotFoundError)."""
        with pytest.raises(FileNotFoundError):
            get_ocp_version.run(tmp_path / "missing.json", tmp_path / "stored-version")

    def test_validation_error_propagates(self, tmp_path: Path) -> None:
        """A ValueError from validate_ocp_versions propagates out of run()."""
        snapshot_path = tmp_path / "snapshot.json"
        snapshot_path.write_text(json.dumps(_snapshot([])), encoding="utf-8")
        with pytest.raises(ValueError, match="No components found"):
            get_ocp_version.run(snapshot_path, tmp_path / "stored-version")


class TestParseArgs:
    """Test CLI argument parsing."""

    def test_snapshot_path_required(self) -> None:
        """--snapshot-path is required."""
        with pytest.raises(SystemExit):
            get_ocp_version._parse_args([])

    def test_parses_snapshot_path(self) -> None:
        """--snapshot-path is correctly parsed."""
        args = get_ocp_version._parse_args(["--snapshot-path", "/data/snapshot.json"])
        assert args.snapshot_path == "/data/snapshot.json"


class TestMain:
    """Test the main() entry point."""

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return 0 on successful run."""
        snapshot_path = tmp_path / "snapshot.json"
        snapshot_path.write_text(json.dumps(_snapshot([])), encoding="utf-8")
        result_path = tmp_path / "stored-version"
        monkeypatch.setenv("RESULT_STORED_VERSION", str(result_path))
        with patch(f"{TASK}.run") as mock_run:
            assert get_ocp_version.main(["--snapshot-path", str(snapshot_path)]) == 0
        mock_run.assert_called_once_with(snapshot_path, result_path)

    def test_missing_env_var_exits(self) -> None:
        """SystemExit when RESULT_STORED_VERSION is not set."""
        with pytest.raises(SystemExit):
            get_ocp_version.main(["--snapshot-path", "/tmp/snapshot.json"])

    def test_value_error_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ValueError from run() propagates out of main()."""
        result_path = tmp_path / "stored-version"
        monkeypatch.setenv("RESULT_STORED_VERSION", str(result_path))
        with patch(
            f"{TASK}.run",
            side_effect=ValueError("No components found in snapshot"),
        ):
            with pytest.raises(ValueError, match="No components found"):
                get_ocp_version.main(["--snapshot-path", "/tmp/snapshot.json"])
