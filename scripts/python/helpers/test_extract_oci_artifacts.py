"""Tests for extract_oci_artifacts.py."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest import mock

import pytest

import extract_artifacts
import extract_oci_artifacts

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

OCI_MANIFEST = {
    "layers": [
        {
            "digest": "sha256:aabbcc",
            "annotations": {"org.opencontainers.image.title": "app-bundle.tar.gz"},
        },
        {
            "digest": "sha256:ddeeff",
            "annotations": {"org.opencontainers.image.title": "metadata.tar.gz"},
        },
    ]
}

SNAPSHOT_ONE = {
    "components": [
        {
            "name": "mycomp",
            "containerImage": "quay.io/org/mycomp@sha256:abc",
            "staged": {
                "files": [
                    {
                        "source": "app-bundle.tar.gz",
                        "filename": "app-bundle.tar.gz",
                        "os": "darwin",
                        "arch": "arm64",
                    }
                ]
            },
        }
    ]
}

SNAPSHOT_NO_FILES = {
    "components": [{"name": "operator", "containerImage": "quay.io/org/op@sha256:abc"}]
}


# ---------------------------------------------------------------------------
# _get_platform_overrides
# ---------------------------------------------------------------------------


def test_get_platform_overrides_from_staged_files() -> None:
    """Platform flags are built from the first staged file entry with os and arch."""
    component = {"staged": {"files": [{"os": "darwin", "arch": "arm64"}]}}
    flags = extract_oci_artifacts._get_platform_overrides(component)
    assert flags == ["--override-os", "darwin", "--override-arch", "arm64"]


def test_get_platform_overrides_from_files() -> None:
    """Platform flags are built from the first files entry with os and arch."""
    component = {"files": [{"os": "windows", "arch": "amd64"}]}
    flags = extract_oci_artifacts._get_platform_overrides(component)
    assert flags == ["--override-os", "windows", "--override-arch", "amd64"]


def test_get_platform_overrides_empty_when_no_os_arch() -> None:
    """Empty list returned when no entries have both os and arch."""
    component = {"files": [{"source": "binary.tar.gz"}]}
    assert extract_oci_artifacts._get_platform_overrides(component) == []


def test_get_platform_overrides_empty_when_no_files() -> None:
    """Empty list returned for a component with no file entries."""
    assert extract_oci_artifacts._get_platform_overrides({}) == []


# ---------------------------------------------------------------------------
# _get_wanted_filenames
# ---------------------------------------------------------------------------


def test_get_wanted_filenames_from_source() -> None:
    """Basenames of source paths are collected."""
    component = {"files": [{"source": "releases/darwin/arm64/app-bundle.tar.gz"}]}
    wanted = extract_oci_artifacts._get_wanted_filenames(component)
    assert wanted == {"app-bundle.tar.gz"}


def test_get_wanted_filenames_from_filename_field() -> None:
    """Explicit filename fields are also collected."""
    component = {"files": [{"source": "foo/bar.tar.gz", "filename": "renamed.tar.gz"}]}
    wanted = extract_oci_artifacts._get_wanted_filenames(component)
    assert "bar.tar.gz" in wanted
    assert "renamed.tar.gz" in wanted


def test_get_wanted_filenames_deduplicates() -> None:
    """Duplicate filenames from source and staged.files are deduplicated."""
    component = {
        "files": [{"source": "app.tar.gz"}],
        "staged": {"files": [{"source": "app.tar.gz"}]},
    }
    assert extract_oci_artifacts._get_wanted_filenames(component) == {"app.tar.gz"}


def test_get_wanted_filenames_empty_for_no_files() -> None:
    """Empty set returned for a component with no file entries."""
    assert extract_oci_artifacts._get_wanted_filenames({}) == set()


# ---------------------------------------------------------------------------
# _extract_oci_component
# ---------------------------------------------------------------------------


def test_extract_oci_component_copies_matching_blob(tmp_path: Path) -> None:
    """The blob matching the wanted layer title is copied to destination."""
    blob = tmp_path / "aabbcc"
    blob.write_bytes(b"fake-bundle")
    destination = tmp_path / "dest"
    destination.mkdir()

    component = {"name": "mycomp", "files": [{"source": "app-bundle.tar.gz"}]}
    extract_oci_artifacts._extract_oci_component(
        component, OCI_MANIFEST, tmp_path, destination
    )

    assert (destination / "app-bundle.tar.gz").read_bytes() == b"fake-bundle"


def test_extract_oci_component_raises_on_missing_blob(tmp_path: Path) -> None:
    """RuntimeError raised when the blob file for a matched layer is absent."""
    destination = tmp_path / "dest"
    destination.mkdir()
    component = {"name": "mycomp", "files": [{"source": "app-bundle.tar.gz"}]}
    with pytest.raises(RuntimeError, match="not found on disk"):
        extract_oci_artifacts._extract_oci_component(
            component, OCI_MANIFEST, tmp_path, destination
        )


def test_extract_oci_component_raises_on_missing_layer(tmp_path: Path) -> None:
    """RuntimeError raised when a wanted file has no matching layer title."""
    (tmp_path / "aabbcc").write_bytes(b"data")
    destination = tmp_path / "dest"
    destination.mkdir()
    component = {"name": "mycomp", "files": [{"source": "nonexistent.tar.gz"}]}
    with pytest.raises(RuntimeError, match="missing layers"):
        extract_oci_artifacts._extract_oci_component(
            component, OCI_MANIFEST, tmp_path, destination
        )


def test_extract_oci_component_skips_unmatched_layers(tmp_path: Path) -> None:
    """Layers whose title is not in the wanted set are ignored."""
    (tmp_path / "aabbcc").write_bytes(b"app-data")
    (tmp_path / "ddeeff").write_bytes(b"meta-data")
    destination = tmp_path / "dest"
    destination.mkdir()
    # only want app-bundle, not metadata
    component = {"name": "mycomp", "files": [{"source": "app-bundle.tar.gz"}]}
    extract_oci_artifacts._extract_oci_component(
        component, OCI_MANIFEST, tmp_path, destination
    )
    assert (destination / "app-bundle.tar.gz").exists()
    assert not (destination / "metadata.tar.gz").exists()


# ---------------------------------------------------------------------------
# process_component
# ---------------------------------------------------------------------------


def test_process_component_skips_no_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Components with no files or staged.files are skipped."""
    monkeypatch.setattr(extract_oci_artifacts, "CONTENT_DIR", tmp_path)
    with caplog.at_level(logging.INFO, logger="extract_oci_artifacts"):
        extract_oci_artifacts.process_component({"name": "op"})
    assert "Skipping" in caplog.text


def test_process_component_raises_on_missing_container_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ValueError raised when component has files but no containerImage."""
    monkeypatch.setattr(extract_oci_artifacts, "CONTENT_DIR", tmp_path)
    with pytest.raises(ValueError, match="containerImage"):
        extract_oci_artifacts.process_component(
            {"name": "p", "files": [{"source": "f.tar.gz"}]}
        )


def test_process_component_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Blob is copied to destination when manifest matches."""
    monkeypatch.setattr(extract_oci_artifacts, "CONTENT_DIR", tmp_path)

    component = {
        "name": "mycomp",
        "containerImage": "quay.io/org/mycomp@sha256:abc",
        "staged": {
            "files": [{"source": "app-bundle.tar.gz", "os": "darwin", "arch": "arm64"}]
        },
    }

    def fake_check_output(cmd, **kwargs):
        return b'{"auths":{}}'

    def fake_check_call(cmd, **kwargs):
        if cmd[0] == "skopeo":
            dest = next(a for a in cmd if a.startswith("dir:")).removeprefix("dir:")
            dest_path = Path(dest)
            (dest_path / "aabbcc").write_bytes(b"bundle-data")
            manifest = {
                "layers": [
                    {
                        "digest": "sha256:aabbcc",
                        "annotations": {"org.opencontainers.image.title": "app-bundle.tar.gz"},
                    }
                ]
            }
            (dest_path / "manifest.json").write_text(json.dumps(manifest))

    with (
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
        mock.patch("subprocess.check_call", side_effect=fake_check_call),
    ):
        extract_oci_artifacts.process_component(component)

    assert (tmp_path / "mycomp" / "app-bundle.tar.gz").read_bytes() == b"bundle-data"


# ---------------------------------------------------------------------------
# run / main
# ---------------------------------------------------------------------------


def test_run_skips_no_files_component(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() skips components with no files."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_NO_FILES))
    monkeypatch.setattr(extract_oci_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(extract_artifacts, "REDHAT_WORKLOADS_TOKEN_MOUNT", tmp_path / "tok")
    (tmp_path / "tok").mkdir()
    (tmp_path / "tok" / ".dockerconfigjson").write_text('{"auths":{}}')
    with mock.patch("pathlib.Path.home", return_value=tmp_path / "home"):
        (tmp_path / "home").mkdir()
        extract_oci_artifacts.run(3)
    assert not (tmp_path / "artifacts" / "operator").is_dir()


def test_run_propagates_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exceptions from process_component propagate out of run()."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_ONE))
    monkeypatch.setattr(extract_oci_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(extract_artifacts, "REDHAT_WORKLOADS_TOKEN_MOUNT", tmp_path / "tok")
    (tmp_path / "tok").mkdir()
    (tmp_path / "tok" / ".dockerconfigjson").write_text('{"auths":{}}')
    with (
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch.object(
            extract_oci_artifacts, "process_component", side_effect=RuntimeError("pull failed")
        ),
    ):
        (tmp_path / "home").mkdir()
        with pytest.raises(RuntimeError, match="pull failed"):
            extract_oci_artifacts.run(3)


def test_main_success() -> None:
    """main() returns 0 and passes concurrent-limit to run()."""
    with mock.patch.object(extract_oci_artifacts, "run") as mock_run:
        rc = extract_oci_artifacts.main(
            ["extract_oci_artifacts.py", "--concurrent-limit", "2"]
        )
    assert rc == 0
    mock_run.assert_called_once_with(2)


def test_main_exception_returns_error() -> None:
    """main() returns 1 when run() raises."""
    with mock.patch.object(extract_oci_artifacts, "run", side_effect=RuntimeError("boom")):
        rc = extract_oci_artifacts.main(["extract_oci_artifacts.py"])
    assert rc == 1
