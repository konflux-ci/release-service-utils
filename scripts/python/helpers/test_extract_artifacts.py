"""Tests for extract_artifacts.py."""

from __future__ import annotations

import io
import json
import logging
import tarfile
from pathlib import Path
from unittest import mock

import pytest

import extract_artifacts

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

SNAPSHOT_ONE = {
    "components": [
        {
            "name": "testproduct",
            "containerImage": "quay.io/org/test@sha256:abc",
            "files": [
                {
                    "source": "/releases/binary-linux-amd64.tar.gz",
                    "os": "linux",
                    "arch": "amd64",
                },
                {
                    "source": "/releases/binary-darwin-amd64.tar.gz",
                    "os": "darwin",
                    "arch": "amd64",
                },
                {
                    "source": "/releases/binary-windows-amd64.tar.gz",
                    "os": "windows",
                    "arch": "amd64",
                },
            ],
        }
    ]
}

SNAPSHOT_NO_FILES = {
    "components": [
        {
            "name": "operator",
            "containerImage": "quay.io/org/operator@sha256:abc",
        }
    ]
}

SNAPSHOT_STAGED = {
    "components": [
        {
            "name": "testproduct",
            "containerImage": "quay.io/org/test@sha256:abc",
            "staged": {
                "destination": "dest",
                "version": "1.0",
                "files": [
                    {
                        "source": "/releases/binary-linux-amd64.tar.gz",
                        "os": "linux",
                        "arch": "amd64",
                    },
                ],
            },
        }
    ]
}


def _setup_token_mount(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "token"
    mount.mkdir()
    (mount / ".dockerconfigjson").write_text('{"auths":{}}')
    monkeypatch.setenv("REDHAT_WORKLOADS_TOKEN_MOUNT", str(mount))
    return mount


# ---------------------------------------------------------------------------
# _get_source_paths
# ---------------------------------------------------------------------------


def test_get_source_paths_files_array() -> None:
    """Source paths and parent directories are extracted from the files array."""
    component = {
        "files": [
            {"source": "/releases/binary-linux-amd64.tar.gz"},
            {"source": "/releases/binary-darwin-amd64.tar.gz"},
        ]
    }
    wanted, dirs = extract_artifacts._get_source_paths(component)
    assert "releases/binary-linux-amd64.tar.gz" in wanted
    assert "releases/binary-darwin-amd64.tar.gz" in wanted
    assert "releases" in dirs


def test_get_source_paths_staged_files() -> None:
    """Source paths are extracted from the staged.files array."""
    component = {
        "staged": {
            "files": [
                {"source": "/releases/binary-linux-amd64.tar.gz"},
            ]
        }
    }
    wanted, dirs = extract_artifacts._get_source_paths(component)
    assert "releases/binary-linux-amd64.tar.gz" in wanted


def test_get_source_paths_both_arrays_deduplicates() -> None:
    """Duplicate paths present in both files and staged.files are deduplicated."""
    component = {
        "files": [{"source": "/releases/binary-linux-amd64.tar.gz"}],
        "staged": {"files": [{"source": "/releases/binary-linux-amd64.tar.gz"}]},
    }
    wanted, dirs = extract_artifacts._get_source_paths(component)
    assert wanted.count("releases/binary-linux-amd64.tar.gz") == 1


def test_get_source_paths_default_dir_when_no_parent() -> None:
    """A source with no parent directory falls back to the default 'releases' directory."""
    component = {"files": [{"source": "binary.tar.gz"}]}
    _, dirs = extract_artifacts._get_source_paths(component)
    assert "releases" in dirs


def test_get_source_paths_no_source_skipped() -> None:
    """File entries without a source key are silently skipped."""
    component = {"files": [{"os": "linux"}]}
    wanted, _ = extract_artifacts._get_source_paths(component)
    assert wanted == []


# ---------------------------------------------------------------------------
# create_os_flag_files
# ---------------------------------------------------------------------------


def _make_component_dir(base: Path, name: str) -> Path:
    d = base / name
    d.mkdir(parents=True)
    return d


def test_create_os_flag_files_darwin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A darwin OS file creates has_mac flag and no other OS flags."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    _make_component_dir(tmp_path, "prod")
    snapshot = {
        "components": [
            {
                "name": "prod",
                "files": [{"source": "/releases/bin-darwin-amd64.tar.gz", "os": "darwin"}],
            }
        ]
    }
    extract_artifacts.create_os_flag_files(snapshot)
    assert (tmp_path / "prod" / "has_mac").exists()
    assert not (tmp_path / "prod" / "has_windows").exists()
    assert not (tmp_path / "prod" / "has_linux").exists()


def test_create_os_flag_files_windows_by_source_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 'windows' substring in the source path creates the has_windows flag."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    _make_component_dir(tmp_path, "prod")
    snapshot = {
        "components": [
            {
                "name": "prod",
                "files": [{"source": "/releases/binary-windows-amd64.tar.gz"}],
            }
        ]
    }
    extract_artifacts.create_os_flag_files(snapshot)
    assert (tmp_path / "prod" / "has_windows").exists()


def test_create_os_flag_files_linux(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A linux OS file entry creates the has_linux flag file."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    _make_component_dir(tmp_path, "prod")
    snapshot = {
        "components": [
            {
                "name": "prod",
                "files": [{"os": "linux", "source": "/releases/binary-linux.tar.gz"}],
            }
        ]
    }
    extract_artifacts.create_os_flag_files(snapshot)
    assert (tmp_path / "prod" / "has_linux").exists()


def test_create_os_flag_files_from_staged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OS flags are created from staged.files entries just like regular files."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    _make_component_dir(tmp_path, "prod")
    snapshot = {
        "components": [
            {
                "name": "prod",
                "staged": {
                    "files": [{"os": "darwin", "source": "/releases/bin-darwin.tar.gz"}]
                },
            }
        ]
    }
    extract_artifacts.create_os_flag_files(snapshot)
    assert (tmp_path / "prod" / "has_mac").exists()


def test_create_os_flag_files_skips_missing_component_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Components whose directory does not exist are silently skipped."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    # No directory created for "missing"
    snapshot = {"components": [{"name": "missing", "files": [{"os": "linux"}]}]}
    extract_artifacts.create_os_flag_files(snapshot)  # should not raise


def test_validate_disk_image_components_non_linux_raises() -> None:
    """A disk-image component with a non-linux OS entry raises RuntimeError."""
    components = [
        {
            "name": "diskimg",
            "contentGateway": {"contentType": "disk-image"},
            "files": [
                {"source": "/releases/disk.qcow2", "os": "darwin", "arch": "amd64"},
            ],
        }
    ]
    with pytest.raises(RuntimeError) as exc_info:
        extract_artifacts._validate_disk_image_components(components)
    assert "disk-image" in str(exc_info.value)
    assert "darwin" in str(exc_info.value)


# ---------------------------------------------------------------------------
# _normalize_tar_member_name / _safe_extract_layer
# ---------------------------------------------------------------------------


def test_normalize_tar_member_name_strips_prefixes() -> None:
    """Leading slashes and ./ segments are stripped for wanted-path matching."""
    assert extract_artifacts._normalize_tar_member_name("/releases/bin") == "releases/bin"
    assert extract_artifacts._normalize_tar_member_name("./releases/bin") == "releases/bin"
    assert extract_artifacts._normalize_tar_member_name("././releases/bin") == "releases/bin"


def _add_regular_file(tf: tarfile.TarFile, arcname: str, content: bytes) -> None:
    """Add a regular file member to an open tarfile."""
    info = tarfile.TarInfo(name=arcname)
    info.size = len(content)
    tf.addfile(info, io.BytesIO(content))


def _add_symlink(tf: tarfile.TarFile, arcname: str, linkname: str) -> None:
    """Add a symlink member to an open tarfile."""
    info = tarfile.TarInfo(name=arcname)
    info.type = tarfile.SYMTYPE
    info.linkname = linkname
    tf.addfile(info)


def test_safe_extract_layer_skips_noise_symlink(tmp_path: Path) -> None:
    """Sibling symlinks under the extract dir are skipped; wanted regular files extract."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        _add_regular_file(tf, "releases/binary.tar.gz", b"payload")
        _add_symlink(tf, "releases/noise-link", "../etc/passwd")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        found = extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer.tar")

    assert found is True
    assert (extract_dir / "releases" / "binary.tar.gz").read_bytes() == b"payload"
    assert not (extract_dir / "releases" / "noise-link").exists()


def test_safe_extract_layer_skips_wanted_path_symlink(tmp_path: Path) -> None:
    """A symlink at a wanted path is skipped; final state is checked later."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        _add_symlink(tf, "releases/binary.tar.gz", "/etc/passwd")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        found = extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer.tar")

    assert found is True
    assert not (extract_dir / "releases" / "binary.tar.gz").exists()


def test_safe_extract_layer_skips_wanted_path_hardlink(tmp_path: Path) -> None:
    """A hardlink at a wanted path is skipped like any other special entry."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        info = tarfile.TarInfo(name="releases/binary.tar.gz")
        info.type = tarfile.LNKTYPE
        info.linkname = "releases/other"
        tf.addfile(info)

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        found = extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer.tar")

    assert found is True
    assert not (extract_dir / "releases" / "binary.tar.gz").exists()


def test_safe_extract_layer_rejects_path_traversal(tmp_path: Path) -> None:
    """Members that resolve outside the extract dir are rejected."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        _add_regular_file(tf, "releases/../../escape.txt", b"nope")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        with pytest.raises(RuntimeError, match="unsafe path"):
            extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer.tar")


def test_safe_extract_layer_skips_parent_symlink_extracts_child(
    tmp_path: Path,
) -> None:
    """A symlink at a parent path is skipped; a wanted child regular file extracts."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        _add_symlink(tf, "assets/downloads/cli", "/somewhere/else")
        _add_regular_file(tf, "assets/downloads/cli/roxctl", b"roxctl-bin")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        found = extract_artifacts._safe_extract_layer(
            tf, "assets/downloads/cli", extract_dir, "layer.tar"
        )

    assert found is True
    assert (extract_dir / "assets/downloads/cli/roxctl").read_bytes() == b"roxctl-bin"
    # Parent symlink was not materialized
    cli_path = extract_dir / "assets" / "downloads" / "cli"
    assert not cli_path.is_symlink()


def test_safe_extract_layer_matches_dot_slash_member_names(tmp_path: Path) -> None:
    """Members named with a ./ prefix still match image_path after normalization."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        _add_symlink(tf, "./releases/noise", "other")
        _add_regular_file(tf, "./releases/binary.tar.gz", b"ok")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        found = extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer.tar")

    assert found is True
    assert (extract_dir / "releases" / "binary.tar.gz").read_bytes() == b"ok"


def test_safe_extract_layer_multilayer_symlink_then_regular_file(
    tmp_path: Path,
) -> None:
    """Base-layer symlink at a path is skipped; a later regular file wins."""
    layer1 = tmp_path / "layer1.tar"
    with tarfile.open(str(layer1), "w") as tf:
        _add_symlink(tf, "releases/binary.tar.gz", "/etc/passwd")

    layer2 = tmp_path / "layer2.tar"
    with tarfile.open(str(layer2), "w") as tf:
        _add_regular_file(tf, "releases/binary.tar.gz", b"final")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer1)) as tf:
        extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer1.tar")
    with tarfile.open(str(layer2)) as tf:
        extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer2.tar")

    assert (extract_dir / "releases" / "binary.tar.gz").read_bytes() == b"final"
    assert not (extract_dir / "releases" / "binary.tar.gz").is_symlink()


def test_safe_extract_layer_multilayer_symlink_overwrite_preserves_file(
    tmp_path: Path,
) -> None:
    """A later symlink at a path is skipped; an earlier regular file remains."""
    layer1 = tmp_path / "layer1.tar"
    with tarfile.open(str(layer1), "w") as tf:
        _add_regular_file(tf, "releases/binary.tar.gz", b"v1")

    layer2 = tmp_path / "layer2.tar"
    with tarfile.open(str(layer2), "w") as tf:
        _add_symlink(tf, "releases/binary.tar.gz", "/etc/passwd")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer1)) as tf:
        extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer1.tar")
    with tarfile.open(str(layer2)) as tf:
        extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer2.tar")

    assert (extract_dir / "releases" / "binary.tar.gz").read_bytes() == b"v1"
    assert not (extract_dir / "releases" / "binary.tar.gz").is_symlink()


def test_safe_extract_layer_ignores_members_outside_image_path(tmp_path: Path) -> None:
    """Tar members outside image_path are skipped without extraction."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        _add_regular_file(tf, "usr/bin/unrelated", b"skip-me")
        _add_regular_file(tf, "releases/binary.tar.gz", b"wanted")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        found = extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer.tar")

    assert found is True
    assert (extract_dir / "releases" / "binary.tar.gz").read_bytes() == b"wanted"
    assert not (extract_dir / "usr").exists()


def test_safe_extract_layer_extracts_absolute_member_names(tmp_path: Path) -> None:
    """Absolute tar member paths are normalized and extracted under target_dir."""
    layer = tmp_path / "layer.tar"
    with tarfile.open(str(layer), "w") as tf:
        _add_regular_file(tf, "/releases/binary.tar.gz", b"abs-payload")

    extract_dir = tmp_path / "out"
    extract_dir.mkdir()
    with tarfile.open(str(layer)) as tf:
        found = extract_artifacts._safe_extract_layer(tf, "releases", extract_dir, "layer.tar")

    assert found is True
    assert (extract_dir / "releases" / "binary.tar.gz").read_bytes() == b"abs-payload"


# ---------------------------------------------------------------------------
# process_component
# ---------------------------------------------------------------------------


def test_process_component_skips_no_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Components with no files or staged.files are skipped with an INFO log."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    with caplog.at_level(logging.INFO, logger="extract_artifacts"):
        extract_artifacts.process_component({"name": "op"})
    assert "Skipping" in caplog.text


def test_process_component_missing_containerimage_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ValueError is raised when the component has files but no containerImage."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    with pytest.raises(ValueError, match="containerImage"):
        extract_artifacts.process_component(
            {"name": "p", "files": [{"source": "/r/f.tar.gz"}]}
        )


def test_process_component_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Artifacts declared in files are extracted from the container image into CONTENT_DIR."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)

    component = {
        "name": "prod",
        "containerImage": "quay.io/org/prod@sha256:abc",
        "files": [{"source": "/releases/binary.tar.gz"}],
    }

    # Create a fake container dir with a manifest and a layer containing our file
    import shutil
    import tempfile

    tmp_layer_dir = Path(tempfile.mkdtemp())
    try:
        layer_file = tmp_layer_dir / "abc123"
        releases_dir = tmp_layer_dir / "releases"
        releases_dir.mkdir()
        (releases_dir / "binary.tar.gz").write_bytes(b"fake-binary")
        with tarfile.open(str(layer_file), "w") as tf:
            tf.add(str(releases_dir / "binary.tar.gz"), arcname="releases/binary.tar.gz")

        manifest = {
            "layers": [{"digest": "sha256:abc123"}],
        }

        def fake_select_oci_auth(pullspec):
            return b'{"auths":{}}'

        def fake_subprocess_check_output(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return b'{"auths":{}}'
            raise ValueError(f"unexpected command: {cmd}")

        def fake_subprocess_check_call(cmd, **kwargs):
            if cmd[0] == "skopeo":
                # Populate the temp dir with layer + manifest
                dest_dir_flag = cmd.index(next(a for a in cmd if a.startswith("dir:")))
                dest_path = cmd[dest_dir_flag].removeprefix("dir:")
                dest = Path(dest_path)
                shutil.copy2(str(layer_file), str(dest / "abc123"))
                (dest / "manifest.json").write_text(json.dumps(manifest))
                return
            if cmd[0] == "tar" and "-xzvf" in cmd:
                # Simulate extraction by creating the expected file in cwd
                cwd = Path(kwargs.get("cwd", "."))
                (cwd / "releases").mkdir(parents=True, exist_ok=True)
                (cwd / "releases" / "binary.tar.gz").write_bytes(b"fake-binary")
                return
            if cmd[0] == "tar":
                return
            raise ValueError(f"unexpected command: {cmd}")

        with (
            mock.patch("subprocess.check_output", side_effect=fake_subprocess_check_output),
            mock.patch("subprocess.check_call", side_effect=fake_subprocess_check_call),
            mock.patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = mock.Mock(stdout="releases/binary.tar.gz\n", returncode=0)
            extract_artifacts.process_component(component)

        assert (tmp_path / "prod").is_dir()
    finally:
        shutil.rmtree(str(tmp_layer_dir), ignore_errors=True)


def test_process_component_raises_when_file_missing_from_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A file declared in the RPA but absent from all layers must raise RuntimeError."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)

    component = {
        "name": "prod",
        "containerImage": "quay.io/org/prod@sha256:abc",
        "files": [{"source": "/releases/missing-binary.tar.gz"}],
    }

    import shutil
    import tempfile

    tmp_layer_dir = Path(tempfile.mkdtemp())
    try:
        layer_file = tmp_layer_dir / "abc123"
        with tarfile.open(str(layer_file), "w"):
            pass  # empty layer — file is not present

        manifest = {"layers": [{"digest": "sha256:abc123"}]}

        def fake_subprocess_check_output(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return b'{"auths":{}}'
            raise ValueError(f"unexpected command: {cmd}")

        def fake_subprocess_check_call(cmd, **kwargs):
            if cmd[0] == "skopeo":
                dest_dir_flag = cmd.index(next(a for a in cmd if a.startswith("dir:")))
                dest_path = cmd[dest_dir_flag].removeprefix("dir:")
                dest = Path(dest_path)
                shutil.copy2(str(layer_file), str(dest / "abc123"))
                (dest / "manifest.json").write_text(json.dumps(manifest))
                return
            if cmd[0] == "tar":
                return
            raise ValueError(f"unexpected command: {cmd}")

        with (
            mock.patch("subprocess.check_output", side_effect=fake_subprocess_check_output),
            mock.patch("subprocess.check_call", side_effect=fake_subprocess_check_call),
            mock.patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = mock.Mock(stdout="", returncode=0)
            with pytest.raises(RuntimeError, match="releases/missing-binary.tar.gz"):
                extract_artifacts.process_component(component)
    finally:
        shutil.rmtree(str(tmp_layer_dir), ignore_errors=True)


def test_process_component_skips_missing_layer_blob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Layer digests listed in the manifest but absent on disk are skipped."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)

    component = {
        "name": "prod",
        "containerImage": "quay.io/org/prod@sha256:abc",
        "files": [{"source": "/releases/binary.tar.gz"}],
    }

    import shutil
    import tempfile

    tmp_layer_dir = Path(tempfile.mkdtemp())
    try:
        layer_file = tmp_layer_dir / "present"
        with tarfile.open(str(layer_file), "w") as tf:
            _add_regular_file(tf, "releases/binary.tar.gz", b"from-present-layer")

        # missingdigest is listed but never written to the skopeo dest dir
        manifest = {
            "layers": [
                {"digest": "sha256:missingdigest"},
                {"digest": "sha256:present"},
            ]
        }

        def fake_subprocess_check_output(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return b'{"auths":{}}'
            raise ValueError(f"unexpected command: {cmd}")

        def fake_subprocess_check_call(cmd, **kwargs):
            if cmd[0] == "skopeo":
                dest_dir_flag = cmd.index(next(a for a in cmd if a.startswith("dir:")))
                dest_path = cmd[dest_dir_flag].removeprefix("dir:")
                dest = Path(dest_path)
                shutil.copy2(str(layer_file), str(dest / "present"))
                (dest / "manifest.json").write_text(json.dumps(manifest))
                return
            raise ValueError(f"unexpected command: {cmd}")

        with (
            mock.patch("subprocess.check_output", side_effect=fake_subprocess_check_output),
            mock.patch("subprocess.check_call", side_effect=fake_subprocess_check_call),
        ):
            extract_artifacts.process_component(component)

        assert (tmp_path / "prod" / "binary.tar.gz").read_bytes() == b"from-present-layer"
    finally:
        shutil.rmtree(str(tmp_layer_dir), ignore_errors=True)


def test_process_component_rejects_symlink_at_wanted_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defense in depth: a symlink at the wanted path after extract must fail."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)

    component = {
        "name": "prod",
        "containerImage": "quay.io/org/prod@sha256:abc",
        "files": [{"source": "/releases/binary.tar.gz"}],
    }

    import shutil
    import tempfile

    tmp_layer_dir = Path(tempfile.mkdtemp())
    try:
        layer_file = tmp_layer_dir / "abc123"
        with tarfile.open(str(layer_file), "w"):
            pass

        manifest = {"layers": [{"digest": "sha256:abc123"}]}

        def fake_subprocess_check_output(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return b'{"auths":{}}'
            raise ValueError(f"unexpected command: {cmd}")

        def fake_subprocess_check_call(cmd, **kwargs):
            if cmd[0] == "skopeo":
                dest_dir_flag = cmd.index(next(a for a in cmd if a.startswith("dir:")))
                dest_path = cmd[dest_dir_flag].removeprefix("dir:")
                dest = Path(dest_path)
                shutil.copy2(str(layer_file), str(dest / "abc123"))
                (dest / "manifest.json").write_text(json.dumps(manifest))
                return
            raise ValueError(f"unexpected command: {cmd}")

        def plant_wanted_symlink(tf, image_path, target_dir, layer_name):
            """Simulate extract leaving a symlink at the wanted path."""
            link_path = target_dir / "releases/binary.tar.gz"
            link_path.parent.mkdir(parents=True, exist_ok=True)
            link_path.symlink_to("/etc/passwd")
            return True

        with (
            mock.patch("subprocess.check_output", side_effect=fake_subprocess_check_output),
            mock.patch("subprocess.check_call", side_effect=fake_subprocess_check_call),
            mock.patch.object(
                extract_artifacts, "_safe_extract_layer", side_effect=plant_wanted_symlink
            ),
        ):
            with pytest.raises(RuntimeError, match="resolved to a symlink"):
                extract_artifacts.process_component(component)
    finally:
        shutil.rmtree(str(tmp_layer_dir), ignore_errors=True)


# ---------------------------------------------------------------------------
# _extract_from_oras
# ---------------------------------------------------------------------------


def test_extract_from_oras_copies_blobs(tmp_path: Path) -> None:
    """Blobs matching wanted filenames are copied directly to destination."""
    blob1 = tmp_path / "aabbcc"
    blob1.write_bytes(b"qcow2-content")
    blob2 = tmp_path / "ddeeff"
    blob2.write_bytes(b"iso-content")

    manifest = {
        "layers": [
            {
                "digest": "sha256:aabbcc",
                "annotations": {"org.opencontainers.image.title": "disk.qcow2"},
            },
            {
                "digest": "sha256:ddeeff",
                "annotations": {"org.opencontainers.image.title": "install.iso.gz"},
            },
        ]
    }

    dest = tmp_path / "out"
    dest.mkdir()
    extract_artifacts._extract_from_oras(
        manifest,
        tmp_path,
        ["releases/disk.qcow2", "releases/install.iso.gz"],
        dest,
        "mycomp",
    )

    assert (dest / "disk.qcow2").read_bytes() == b"qcow2-content"
    assert (dest / "install.iso.gz").read_bytes() == b"iso-content"


def test_extract_from_oras_raises_when_title_missing(tmp_path: Path) -> None:
    """RuntimeError is raised when a wanted file has no matching ORAS blob."""
    manifest = {
        "layers": [
            {
                "digest": "sha256:aabbcc",
                "annotations": {"org.opencontainers.image.title": "disk.qcow2"},
            },
        ]
    }

    blob = tmp_path / "aabbcc"
    blob.write_bytes(b"data")
    dest = tmp_path / "out"
    dest.mkdir()

    with pytest.raises(RuntimeError, match="install.iso.gz"):
        extract_artifacts._extract_from_oras(
            manifest, tmp_path, ["releases/install.iso.gz"], dest, "mycomp"
        )


# ---------------------------------------------------------------------------
# setup_docker_config
# ---------------------------------------------------------------------------


def test_setup_docker_config_strips_noise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Docker config from the mount is written to ~/.docker/config.json."""
    mount = tmp_path / "mount"
    mount.mkdir()
    (mount / ".dockerconfigjson").write_text('{"auths":{}}')
    monkeypatch.setattr(extract_artifacts, "REDHAT_WORKLOADS_TOKEN_MOUNT", mount)
    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        extract_artifacts.setup_docker_config()
    assert (home / ".docker" / "config.json").exists()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_skips_no_files_component(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() does not create an artifact directory for a component with no files."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_NO_FILES))
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(extract_artifacts, "REDHAT_WORKLOADS_TOKEN_MOUNT", tmp_path / "tok")
    (tmp_path / "tok").mkdir()
    (tmp_path / "tok" / ".dockerconfigjson").write_text('{"auths":{}}')
    with mock.patch("pathlib.Path.home", return_value=tmp_path / "home"):
        (tmp_path / "home").mkdir()
        extract_artifacts.run(3)
    # no artifacts directory created for skipped component
    assert not (tmp_path / "artifacts" / "operator").is_dir()


def test_run_propagates_component_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exceptions raised by process_component propagate out of run()."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_ONE))
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(extract_artifacts, "REDHAT_WORKLOADS_TOKEN_MOUNT", tmp_path / "tok")
    (tmp_path / "tok").mkdir()
    (tmp_path / "tok" / ".dockerconfigjson").write_text('{"auths":{}}')
    with (
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch.object(
            extract_artifacts, "process_component", side_effect=RuntimeError("skopeo fail")
        ),
    ):
        (tmp_path / "home").mkdir()
        with pytest.raises(RuntimeError, match="skopeo fail"):
            extract_artifacts.run(3)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    """main() returns 0 and passes the concurrent-limit argument to run()."""
    with mock.patch.object(extract_artifacts, "run") as mock_run:
        rc = extract_artifacts.main(["extract_artifacts.py", "--concurrent-limit", "2"])
    assert rc == 0
    mock_run.assert_called_once_with(2)


def test_main_exception_returns_error() -> None:
    """main() returns 1 when run() raises an exception."""
    with mock.patch.object(extract_artifacts, "run", side_effect=RuntimeError("boom")):
        rc = extract_artifacts.main(["extract_artifacts.py"])
    assert rc == 1
