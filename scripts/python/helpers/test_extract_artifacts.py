"""Tests for extract_artifacts.py."""

from __future__ import annotations

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
# _create_os_flag_files
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
    extract_artifacts._create_os_flag_files(snapshot)
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
    extract_artifacts._create_os_flag_files(snapshot)
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
    extract_artifacts._create_os_flag_files(snapshot)
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
    extract_artifacts._create_os_flag_files(snapshot)
    assert (tmp_path / "prod" / "has_mac").exists()


def test_create_os_flag_files_skips_missing_component_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Components whose directory does not exist are silently skipped."""
    monkeypatch.setattr(extract_artifacts, "CONTENT_DIR", tmp_path)
    # No directory created for "missing"
    snapshot = {"components": [{"name": "missing", "files": [{"os": "linux"}]}]}
    extract_artifacts._create_os_flag_files(snapshot)  # should not raise


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


# ---------------------------------------------------------------------------
# _setup_docker_config
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
        extract_artifacts._setup_docker_config()
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
