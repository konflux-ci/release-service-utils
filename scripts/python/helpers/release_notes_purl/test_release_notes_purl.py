"""Tests for `release_notes_purl`."""

from __future__ import annotations

import json
import tarfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from unittest import mock

import pytest
from release_notes_purl import release_notes_purl as rnp_module


def _write_data(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path*."""
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _mock_checksum_pull(checksum_map: list[dict[str, Any]]):
    """Return a fake oras pull that materializes checksum_map as a gz tarball."""

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        cwd = kwargs.get("cwd")
        assert cwd is not None
        json_path = Path(cwd) / "checksum_map.json"
        json_path.write_text(json.dumps(checksum_map), encoding="utf-8")
        archive_path = Path(cwd) / "checksum_map"
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(json_path, arcname="checksum_map.json")
        json_path.unlink()
        return mock.Mock(stdout="", returncode=0)

    return fake_run_cmd


def _mock_checksum_pull_plain_json(checksum_map: list[dict[str, Any]]):
    """Return a fake oras pull that leaves checksum_map.json in the pull directory."""

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        cwd = kwargs.get("cwd")
        assert cwd is not None
        json_path = Path(cwd) / "checksum_map.json"
        json_path.write_text(json.dumps(checksum_map), encoding="utf-8")
        return mock.Mock(stdout="", returncode=0)

    return fake_run_cmd


@contextmanager
def _patch_oci_update(checksum_map: list[dict[str, Any]]) -> Iterator[None]:
    """Patch OCI pull and docker setup for update_artifact_purls integration tests."""
    with (
        mock.patch.object(
            rnp_module.subprocess_cmd,
            "run_cmd",
            side_effect=_mock_checksum_pull(checksum_map),
        ),
        mock.patch.object(rnp_module.authentication, "setup_docker_config"),
    ):
        yield


def _generic_mapping_data(
    *,
    component_name: str = "app",
    content_type: str = "generic",
    component_extra: dict[str, Any] | None = None,
    artifact_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build minimal data.json for a single-component generic release."""
    component: dict[str, Any] = {
        "name": component_name,
        "contentType": content_type,
        "staged": {"version": "1.0"},
        "files": [{"arch": "x86_64", "os": "linux", "source": "app.tgz"}],
    }
    if component_extra:
        component.update(component_extra)
    artifact: dict[str, Any] = {
        "component": component_name,
        "architecture": "x86_64",
        "os": "linux",
        "purl": "placeholder",
    }
    if artifact_extra:
        artifact.update(artifact_extra)
    return {
        "cdn": {"env": "production"},
        "mapping": {"components": [component]},
        "releaseNotes": {"content": {"artifacts": [artifact]}},
    }


def test_artifact_rows_skips_invalid_shapes() -> None:
    """Return only dict rows from a well-formed artifacts list."""
    release_notes = {
        "content": {
            "artifacts": [
                {"component": "app"},
                "not-a-dict",
                {"component": "other"},
            ],
        },
    }
    rows = rnp_module._artifact_rows(release_notes)
    assert rows == [{"component": "app"}, {"component": "other"}]


def test_artifact_rows_returns_empty_when_content_missing() -> None:
    """Missing or non-dict content yields an empty artifact list."""
    assert rnp_module._artifact_rows({}) == []
    assert rnp_module._artifact_rows({"content": []}) == []


def test_artifact_rows_returns_empty_when_artifacts_not_list() -> None:
    """Non-list artifacts yields an empty artifact list."""
    assert rnp_module._artifact_rows({"content": {"artifacts": "bad"}}) == []


def test_all_artifacts_have_purls_requires_non_placeholder_values() -> None:
    """Treat empty lists, placeholders, and missing PURLs as not fully populated."""
    assert rnp_module._all_artifacts_have_purls([]) is False
    assert (
        rnp_module._all_artifacts_have_purls(
            [{"purl": "placeholder"}],
        )
        is False
    )
    assert (
        rnp_module._all_artifacts_have_purls(
            [{"purl": "pkg:generic/app@1.0"}],
        )
        is True
    )


def test_build_purl_url_encodes_query_parameters() -> None:
    """Build a pkg:generic PURL with URL-encoded filename, checksum, and download_url."""
    purl = rnp_module._build_purl(
        component_name="app",
        version_name="1.0",
        filename_basename="app.tgz",
        checksum="sha256:abc",
        download_url="https://developers.redhat.com/products",
    )
    assert purl.startswith("pkg:generic/app@1.0?")
    assert "filename=app.tgz" in purl
    assert "checksum=sha256%3Aabc" in purl
    assert "download_url=https%3A%2F%2Fdevelopers.redhat.com%2Fproducts" in purl


def test_checksum_for_file_returns_match_or_empty_string() -> None:
    """Look up checksums by component and basename; warn and return empty when missing."""
    checksum_map = [{"component": "app", "files": {"app.tgz": "sha256:abc"}}]
    assert (
        rnp_module._checksum_for_file(
            checksum_map,
            component_name="app",
            filename_basename="app.tgz",
        )
        == "sha256:abc"
    )
    with mock.patch.object(rnp_module.logger, "warning") as warning:
        missing = rnp_module._checksum_for_file(
            checksum_map,
            component_name="app",
            filename_basename="missing.tgz",
        )
    assert missing == ""
    warning.assert_called_once()


def test_checksum_for_file_skips_malformed_checksum_map_rows() -> None:
    """Ignore non-dict rows, other components, and rows whose files value is not a dict."""
    checksum_map: list[Any] = [
        "skip",
        {"component": "other", "files": {"other.tgz": "sha256:1"}},
        {"component": "app", "files": "not-a-dict"},
        {"component": "app", "files": {"app.tgz": "sha256:abc"}},
    ]
    assert (
        rnp_module._checksum_for_file(
            checksum_map,
            component_name="app",
            filename_basename="app.tgz",
        )
        == "sha256:abc"
    )


def test_load_checksum_map_reads_plain_json_file() -> None:
    """Accept a pull directory that already contains checksum_map.json."""
    checksum_map = [{"component": "app", "files": {"app.tgz": "sha256:abc"}}]
    with mock.patch.object(
        rnp_module.subprocess_cmd,
        "run_cmd",
        side_effect=_mock_checksum_pull_plain_json(checksum_map),
    ):
        loaded = rnp_module.load_checksum_map("oci:checksum")
    assert loaded == checksum_map


def test_load_checksum_map_extracts_gzip_tarball() -> None:
    """Unpack checksum_map tarball artifacts produced by build_checksum_map."""
    checksum_map = [{"component": "app", "files": {"app.tgz": "sha256:abc"}}]
    with mock.patch.object(
        rnp_module.subprocess_cmd,
        "run_cmd",
        side_effect=_mock_checksum_pull(checksum_map),
    ):
        loaded = rnp_module.load_checksum_map("oci:checksum")
    assert loaded == checksum_map


def test_load_checksum_map_raises_when_json_missing() -> None:
    """Fail when oras pull does not materialize checksum_map.json."""

    def empty_pull(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        return mock.Mock(stdout="", returncode=0)

    with (
        mock.patch.object(rnp_module.subprocess_cmd, "run_cmd", side_effect=empty_pull),
        pytest.raises(FileNotFoundError, match="checksum_map.json not found"),
    ):
        rnp_module.load_checksum_map("oci:checksum")


def test_load_checksum_map_raises_when_manifest_empty() -> None:
    """Fail when checksum_map.json decodes to an empty list."""

    def empty_manifest_pull(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        cwd = kwargs.get("cwd")
        assert cwd is not None
        (Path(cwd) / "checksum_map.json").write_text("[]", encoding="utf-8")
        return mock.Mock(stdout="", returncode=0)

    with (
        mock.patch.object(
            rnp_module.subprocess_cmd,
            "run_cmd",
            side_effect=empty_manifest_pull,
        ),
        pytest.raises(ValueError, match="checksum map is empty or invalid JSON"),
    ):
        rnp_module.load_checksum_map("oci:checksum")


def test_merge_updated_artifacts_replaces_rows_by_component_arch_os() -> None:
    """Updated PURLs replace placeholder rows without duplicating arch/os entries."""
    data = {
        "releaseNotes": {
            "content": {
                "artifacts": [
                    {
                        "component": "app",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                    {
                        "component": "app",
                        "architecture": "x86_64",
                        "os": "windows",
                        "purl": "placeholder",
                    },
                ],
            },
        },
    }
    updated_entries = [
        {
            "component": "app",
            "architecture": "x86_64",
            "os": "linux",
            "purl": "pkg:generic/app@1.0?filename=linux.tgz",
        },
    ]
    rnp_module._merge_updated_artifacts(
        data,
        updated_entries=updated_entries,
        updated_disk_entries=[],
    )
    artifacts = data["releaseNotes"]["content"]["artifacts"]
    linux = next(row for row in artifacts if row["os"] == "linux")
    windows = next(row for row in artifacts if row["os"] == "windows")
    assert "linux.tgz" in linux["purl"]
    assert windows["purl"] == "placeholder"


def test_merge_updated_artifacts_normalizes_invalid_content_and_artifact_rows() -> None:
    """Repair non-dict content, non-list artifacts, and skip invalid artifact rows."""
    data: dict[str, Any] = {
        "releaseNotes": {
            "content": "not-a-dict",
        },
    }
    updated_entries = [
        {
            "component": "app",
            "architecture": "x86_64",
            "os": "linux",
            "purl": "pkg:generic/app@1.0",
        },
    ]
    rnp_module._merge_updated_artifacts(
        data,
        updated_entries=updated_entries,
        updated_disk_entries=[],
    )
    content = data["releaseNotes"]["content"]
    assert isinstance(content, dict)
    assert content["artifacts"] == updated_entries

    data = {
        "releaseNotes": {
            "content": {
                "artifacts": [
                    "skip",
                    {
                        "component": "app",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                ],
            },
        },
    }
    rnp_module._merge_updated_artifacts(
        data,
        updated_entries=updated_entries,
        updated_disk_entries=[],
    )
    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 1
    assert artifacts[0]["purl"] == "pkg:generic/app@1.0"


def test_updated_binary_or_generic_entries_uses_content_gateway_version_and_url() -> None:
    """CGW components use productVersionName and the Developer Portal download base."""
    data = {
        "releaseNotes": {
            "content": {
                "artifacts": [
                    {
                        "component": "helm",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                ],
            },
        },
    }
    component = {
        "name": "helm",
        "contentGateway": {"productVersionName": "1.5.0"},
        "files": [{"arch": "x86_64", "os": "linux", "source": "helm-linux.tgz"}],
    }
    checksum_map = [{"component": "helm", "files": {"helm-linux.tgz": "sha256:abc"}}]
    updated = rnp_module._updated_binary_or_generic_entries(
        data,
        component,
        checksum_map=checksum_map,
        cgw_base_url="https://developers.redhat.com/products",
        cdn_base_url="https://access.redhat.com/downloads",
    )
    purl = updated[0]["purl"]
    assert "pkg:generic/helm@1.5.0" in purl
    assert "download_url=https%3A%2F%2Fdevelopers.redhat.com%2Fproducts" in purl


def test_updated_binary_or_generic_entries_returns_empty_when_version_missing() -> None:
    """Skip a component that has neither contentGateway nor staged version metadata."""
    data = {"releaseNotes": {"content": {"artifacts": [{"component": "app"}]}}}
    component = {"name": "app", "files": []}
    updated = rnp_module._updated_binary_or_generic_entries(
        data,
        component,
        checksum_map=[],
        cgw_base_url="https://developers.redhat.com/products",
        cdn_base_url="https://access.redhat.com/downloads",
    )
    assert updated == []


def test_updated_disk_image_entries_requires_staged_files() -> None:
    """Fail when a disk-image has artifact rows but no snapshot staged.files[]."""
    data = {
        "releaseNotes": {
            "content": {
                "artifacts": [
                    {
                        "component": "iso",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                ],
            },
        },
    }
    component = {"name": "iso", "staged": {"version": "1.0"}}
    with pytest.raises(ValueError, match="no staged.files"):
        rnp_module._updated_disk_image_entries(
            data,
            component,
            staged_files=[],
            checksum_map=[],
            cgw_base_url="https://developers.redhat.com/products",
            cdn_base_url="https://access.redhat.com/downloads",
        )


@pytest.mark.parametrize("filename", ["", "null", None])
def test_updated_disk_image_entries_requires_filename(filename: str | None) -> None:
    """Fail when a staged file has a missing, empty, or literal null filename."""
    data = {
        "releaseNotes": {
            "content": {
                "artifacts": [
                    {
                        "component": "iso",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                ],
            },
        },
    }
    component = {"name": "iso", "staged": {"version": "1.0"}}
    with pytest.raises(ValueError, match="filename is required"):
        rnp_module._updated_disk_image_entries(
            data,
            component,
            staged_files=[{"filename": filename}],
            checksum_map=[],
            cgw_base_url="https://developers.redhat.com/products",
            cdn_base_url="https://access.redhat.com/downloads",
        )


def test_updated_disk_image_entries_uses_cdn_when_content_gateway_empty() -> None:
    """Disk-image PURLs use the CDN base when contentGateway is null or empty."""
    data = {
        "releaseNotes": {
            "content": {
                "artifacts": [
                    {
                        "component": "iso",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                ],
            },
        },
    }
    component = {
        "name": "iso",
        "contentGateway": None,
        "staged": {"version": "1.5"},
    }
    updated = rnp_module._updated_disk_image_entries(
        data,
        component,
        staged_files=[{"filename": "product-1.5-x86_64.iso.gz"}],
        checksum_map=[
            {"component": "iso", "files": {"product-1.5-x86_64.iso.gz": "sha256:iso"}},
        ],
        cgw_base_url="https://developers.redhat.com/products",
        cdn_base_url="https://access.redhat.com/downloads",
    )
    assert "download_url=https%3A%2F%2Faccess.redhat.com%2Fdownloads" in updated[0]["purl"]

    component["contentGateway"] = {}
    updated_empty = rnp_module._updated_disk_image_entries(
        data,
        component,
        staged_files=[{"filename": "product-1.5-x86_64.iso.gz"}],
        checksum_map=[
            {"component": "iso", "files": {"product-1.5-x86_64.iso.gz": "sha256:iso"}},
        ],
        cgw_base_url="https://developers.redhat.com/products",
        cdn_base_url="https://access.redhat.com/downloads",
    )
    assert (
        "download_url=https%3A%2F%2Faccess.redhat.com%2Fdownloads" in updated_empty[0]["purl"]
    )


def test_update_artifact_purls_skips_marketplace_release(tmp_path: Path) -> None:
    """Skip PURL updates when cloudMarketplacesSecret is set."""
    data = {
        "mapping": {"cloudMarketplacesSecret": "secret", "components": []},
        "releaseNotes": {"content": {"artifacts": []}},
    }
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    rnp_module.update_artifact_purls(
        data_file,
        checksum_map_param="oci:checksum",
    )
    assert json.loads(data_file.read_text(encoding="utf-8")) == data


def test_update_artifact_purls_skips_github_release(tmp_path: Path) -> None:
    """Skip PURL updates for GitHub-only releases."""
    data = {"github": {}, "releaseNotes": {"content": {"artifacts": []}}}
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    rnp_module.update_artifact_purls(
        data_file,
        checksum_map_param="oci:checksum",
    )
    assert "github" in json.loads(data_file.read_text(encoding="utf-8"))


def test_update_artifact_purls_skips_unsupported_content_type(tmp_path: Path) -> None:
    """Skip PURL updates for content types outside binary/generic/disk-image."""
    data = {
        "mapping": {"components": [{"name": "img", "contentType": "image"}]},
        "releaseNotes": {"content": {"artifacts": []}},
    }
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    rnp_module.update_artifact_purls(
        data_file,
        checksum_map_param="oci:checksum",
    )
    assert json.loads(data_file.read_text(encoding="utf-8")) == data


def test_update_artifact_purls_skips_when_no_content_type_and_not_github(
    tmp_path: Path,
) -> None:
    """Skip when mapping has no artifact content type and this is not a github release."""
    data = {
        "mapping": {"components": []},
        "releaseNotes": {"content": {"artifacts": []}},
    }
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    rnp_module.update_artifact_purls(
        data_file,
        checksum_map_param="oci:checksum",
    )
    assert json.loads(data_file.read_text(encoding="utf-8")) == data


def test_update_artifact_purls_skips_when_all_purls_populated(tmp_path: Path) -> None:
    """Skip when every artifact row already has a non-placeholder PURL."""
    data = {
        "mapping": {"components": [{"name": "app", "contentType": "generic"}]},
        "releaseNotes": {
            "content": {
                "artifacts": [
                    {
                        "component": "app",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "pkg:generic/app@1.0?filename=app.tgz",
                    },
                ],
            },
        },
    }
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    with _patch_oci_update([{"component": "app", "files": {}}]):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
        )
    assert json.loads(data_file.read_text(encoding="utf-8")) == data


def test_update_artifact_purls_requires_checksum_map(tmp_path: Path) -> None:
    """Fail when checksum_map is missing for binary/generic/disk-image content."""
    data = _generic_mapping_data()
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    with pytest.raises(ValueError, match="checksum map is required"):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="",
        )


def test_update_artifact_purls_updates_binary_artifact_purl(tmp_path: Path) -> None:
    """Populate PURLs for generic/binary artifacts from checksum_map."""
    data = _generic_mapping_data()
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    checksum_map = [{"component": "app", "files": {"app.tgz": "sha256:abc"}}]
    with _patch_oci_update(checksum_map):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
        )

    purl = json.loads(data_file.read_text(encoding="utf-8"))["releaseNotes"]["content"][
        "artifacts"
    ][0]["purl"]
    assert "pkg:generic/app@1.0" in purl
    assert "filename=app.tgz" in purl
    assert "download_url=https%3A%2F%2Faccess.redhat.com%2Fdownloads" in purl


def test_update_artifact_purls_updates_disk_image_artifact_purl(tmp_path: Path) -> None:
    """Populate PURLs for disk-image rows from snapshot staged.files[]."""
    data = _generic_mapping_data(
        component_name="iso-image",
        content_type="disk-image",
        component_extra={"staged": {"version": "1.5"}},
        artifact_extra={"component": "iso-image"},
    )
    del data["mapping"]["components"][0]["files"]
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    snapshot_file = tmp_path / "snapshot.json"
    _write_data(
        snapshot_file,
        {
            "components": [
                {
                    "name": "iso-image",
                    "staged": {
                        "files": [{"filename": "product-1.5-x86_64.iso.gz"}],
                    },
                },
            ],
        },
    )
    checksum_map = [
        {"component": "iso-image", "files": {"product-1.5-x86_64.iso.gz": "sha256:iso"}},
    ]
    with _patch_oci_update(checksum_map):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
            snapshot_path=snapshot_file,
        )

    artifact = json.loads(data_file.read_text(encoding="utf-8"))["releaseNotes"]["content"][
        "artifacts"
    ][0]
    assert "pkg:generic/iso-image@1.5" in artifact["purl"]
    assert "filename=product-1.5-x86_64.iso.gz" in artifact["purl"]
    assert artifact["architecture"] == "x86_64"
    assert artifact["os"] == "linux"


def test_update_artifact_purls_expands_disk_image_per_staged_file(tmp_path: Path) -> None:
    """Expand one artifact row per staged file when multiple share the same arch."""
    data = _generic_mapping_data(
        component_name="bootc",
        content_type="disk-image",
        component_extra={"staged": {"version": "1.5"}},
        artifact_extra={"component": "bootc"},
    )
    del data["mapping"]["components"][0]["files"]
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    snapshot_file = tmp_path / "snapshot.json"
    _write_data(
        snapshot_file,
        {
            "components": [
                {
                    "name": "bootc",
                    "staged": {
                        "files": [
                            {"filename": "product-1.5-x86_64.iso.gz"},
                            {"filename": "product-1.5-x86_64.qcow2"},
                        ],
                    },
                },
            ],
        },
    )
    checksum_map = [
        {
            "component": "bootc",
            "files": {
                "product-1.5-x86_64.iso.gz": "sha256:iso",
                "product-1.5-x86_64.qcow2": "sha256:qcow",
            },
        },
    ]
    with _patch_oci_update(checksum_map):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
            snapshot_path=snapshot_file,
        )

    artifacts = json.loads(data_file.read_text(encoding="utf-8"))["releaseNotes"]["content"][
        "artifacts"
    ]
    assert len(artifacts) == 2
    filenames = {row["purl"].split("filename=")[1].split("&")[0] for row in artifacts}
    assert filenames == {"product-1.5-x86_64.iso.gz", "product-1.5-x86_64.qcow2"}
    assert all(row["architecture"] == "x86_64" for row in artifacts)


def test_update_artifact_purls_mixed_content_types(tmp_path: Path) -> None:
    """Update later PURL components even when the first mapping component is image."""
    data = {
        "cdn": {"env": "production"},
        "mapping": {
            "components": [
                {
                    "name": "container",
                    "contentType": "image",
                },
                {
                    "name": "cli",
                    "contentType": "binary",
                    "staged": {"version": "1.0"},
                    "files": [{"arch": "x86_64", "os": "linux", "source": "cli.tgz"}],
                },
                {
                    "name": "iso-image",
                    "contentType": "disk-image",
                    "staged": {"version": "1.5"},
                },
            ],
        },
        "releaseNotes": {
            "content": {
                "artifacts": [
                    {
                        "component": "cli",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                    {
                        "component": "iso-image",
                        "architecture": "x86_64",
                        "os": "linux",
                        "purl": "placeholder",
                    },
                ],
            },
        },
    }
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    snapshot_file = tmp_path / "snapshot.json"
    _write_data(
        snapshot_file,
        {
            "components": [
                {
                    "name": "iso-image",
                    "staged": {
                        "files": [{"filename": "product-1.5-x86_64.iso.gz"}],
                    },
                },
            ],
        },
    )
    checksum_map = [
        {"component": "cli", "files": {"cli.tgz": "sha256:cli"}},
        {"component": "iso-image", "files": {"product-1.5-x86_64.iso.gz": "sha256:iso"}},
    ]
    with _patch_oci_update(checksum_map):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
            snapshot_path=snapshot_file,
        )

    artifacts = json.loads(data_file.read_text(encoding="utf-8"))["releaseNotes"]["content"][
        "artifacts"
    ]
    by_component = {row["component"]: row for row in artifacts}
    assert "filename=cli.tgz" in by_component["cli"]["purl"]
    assert "filename=product-1.5-x86_64.iso.gz" in by_component["iso-image"]["purl"]


def test_update_artifact_purls_uses_windows_zip_for_checksum_lookup(tmp_path: Path) -> None:
    """Windows rows look up checksums using the .zip name after compress-artifacts."""
    data = _generic_mapping_data(
        component_extra={
            "files": [
                {"arch": "x86_64", "os": "windows", "source": "app-windows-amd64.tar.gz"},
            ],
        },
        artifact_extra={"os": "windows"},
    )
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    checksum_map = [
        {"component": "app", "files": {"app-windows-amd64.zip": "sha256:win"}},
    ]
    with _patch_oci_update(checksum_map):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
        )

    purl = json.loads(data_file.read_text(encoding="utf-8"))["releaseNotes"]["content"][
        "artifacts"
    ][0]["purl"]
    assert "filename=app-windows-amd64.zip" in purl
    assert "checksum=sha256%3Awin" in purl


def test_update_artifact_purls_leaves_file_unchanged_when_nothing_updated(
    tmp_path: Path,
) -> None:
    """Return without writing data.json when no component produces updated rows."""
    data = {
        "mapping": {
            "components": [
                {
                    "name": "app",
                    "contentType": "generic",
                    "staged": {"version": "1.0"},
                    "files": [],
                },
            ],
        },
        "releaseNotes": {"content": {"artifacts": []}},
    }
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    original_text = data_file.read_text(encoding="utf-8")
    with _patch_oci_update([{"component": "unused", "files": {}}]):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
        )
    assert data_file.read_text(encoding="utf-8") == original_text


def test_update_artifact_purls_skips_malformed_mapping_components(tmp_path: Path) -> None:
    """Ignore non-list components and non-dict component entries during iteration."""
    data = _generic_mapping_data()
    data["mapping"]["components"] = [
        "skip",
        {
            "name": "app",
            "contentType": "generic",
            "staged": {"version": "1.0"},
            "files": [{"arch": "x86_64", "os": "linux", "source": "app.tgz"}],
        },
    ]
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    checksum_map = [{"component": "app", "files": {"app.tgz": "sha256:abc"}}]
    with _patch_oci_update(checksum_map):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
        )

    purl = json.loads(data_file.read_text(encoding="utf-8"))["releaseNotes"]["content"][
        "artifacts"
    ][0]["purl"]
    assert "pkg:generic/app@1.0" in purl


def test_update_artifact_purls_treats_non_list_components_as_empty(tmp_path: Path) -> None:
    """When mapping.components is not a list, no PURL content type is detected."""
    data = _generic_mapping_data()
    data["mapping"]["components"] = "not-a-list"
    data["releaseNotes"]["content"]["artifacts"] = []
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    original_text = data_file.read_text(encoding="utf-8")
    rnp_module.update_artifact_purls(
        data_file,
        checksum_map_param="oci:checksum",
    )
    assert data_file.read_text(encoding="utf-8") == original_text


def test_component_content_type_prefers_content_gateway() -> None:
    """Prefer contentGateway.contentType over the top-level contentType field."""
    component = {
        "contentType": "image",
        "contentGateway": {"contentType": "disk-image"},
    }
    assert rnp_module._component_content_type(component) == "disk-image"


def test_staged_files_by_component_edge_cases() -> None:
    """Skip invalid snapshot rows and treat missing staged.files as an empty list."""
    assert rnp_module._staged_files_by_component({"components": "not-a-list"}) == {}
    assert rnp_module._staged_files_by_component(
        {
            "components": [
                "skip-me",
                {"name": ""},
                {"name": None},
                {"name": "no-staged"},
                {"name": "bad-files", "staged": {"files": "not-a-list"}},
                {
                    "name": "ok",
                    "staged": {
                        "files": [
                            {"filename": "a.iso"},
                            "skip-row",
                        ],
                    },
                },
            ],
        },
    ) == {
        "no-staged": [],
        "bad-files": [],
        "ok": [{"filename": "a.iso"}],
    }


def test_updated_disk_image_entries_returns_empty_when_version_missing() -> None:
    """Skip disk-image components that have neither CGW nor staged version metadata."""
    data = {
        "releaseNotes": {
            "content": {
                "artifacts": [{"component": "iso", "purl": "placeholder"}],
            },
        },
    }
    updated = rnp_module._updated_disk_image_entries(
        data,
        {"name": "iso"},
        staged_files=[{"filename": "product-x86_64.iso.gz"}],
        checksum_map=[],
        cgw_base_url="https://developers.redhat.com/products",
        cdn_base_url="https://access.redhat.com/downloads",
    )
    assert updated == []


def test_updated_disk_image_entries_returns_empty_when_no_matching_artifacts() -> None:
    """Skip disk-image components with no matching releaseNotes artifact rows."""
    data = {"releaseNotes": {"content": {"artifacts": []}}}
    updated = rnp_module._updated_disk_image_entries(
        data,
        {"name": "iso", "staged": {"version": "1.0"}},
        staged_files=[{"filename": "product-x86_64.iso.gz"}],
        checksum_map=[],
        cgw_base_url="https://developers.redhat.com/products",
        cdn_base_url="https://access.redhat.com/downloads",
    )
    assert updated == []


def test_update_artifact_purls_non_list_components_after_purl_gate(
    tmp_path: Path,
) -> None:
    """Treat mapping.components as empty when it is not a list after the PURL gate."""
    data = _generic_mapping_data()
    data["mapping"]["components"] = "not-a-list"
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    original_text = data_file.read_text(encoding="utf-8")
    with (
        mock.patch.object(rnp_module, "_first_purl_content_type", return_value="generic"),
        _patch_oci_update([{"component": "unused", "files": {}}]),
    ):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
        )
    assert data_file.read_text(encoding="utf-8") == original_text


def test_update_artifact_purls_uses_content_gateway_content_type(tmp_path: Path) -> None:
    """Detect PURL content types nested under contentGateway.contentType."""
    data = _generic_mapping_data(
        component_name="iso-image",
        content_type="image",
        component_extra={
            "contentGateway": {"contentType": "disk-image", "productVersionName": "1.5"},
            "staged": {"version": "ignored"},
        },
        artifact_extra={"component": "iso-image"},
    )
    del data["mapping"]["components"][0]["files"]
    data_file = tmp_path / "data.json"
    _write_data(data_file, data)
    snapshot_file = tmp_path / "snapshot.json"
    _write_data(
        snapshot_file,
        {
            "components": [
                {
                    "name": "iso-image",
                    "staged": {
                        "files": [{"filename": "product-1.5-x86_64.iso.gz"}],
                    },
                },
            ],
        },
    )
    checksum_map = [
        {"component": "iso-image", "files": {"product-1.5-x86_64.iso.gz": "sha256:iso"}},
    ]
    with _patch_oci_update(checksum_map):
        rnp_module.update_artifact_purls(
            data_file,
            checksum_map_param="oci:checksum",
            snapshot_path=snapshot_file,
        )

    artifact = json.loads(data_file.read_text(encoding="utf-8"))["releaseNotes"]["content"][
        "artifacts"
    ][0]
    assert "pkg:generic/iso-image@1.5" in artifact["purl"]
    assert "filename=product-1.5-x86_64.iso.gz" in artifact["purl"]
