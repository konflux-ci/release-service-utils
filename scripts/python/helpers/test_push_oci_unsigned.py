"""Tests for push_oci_unsigned.py."""

from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path
from unittest import mock

import pytest

import push_oci_unsigned

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

SNAPSHOT = {
    "components": [
        {
            "name": "testproduct",
            "staged": {
                "files": [
                    {"source": "app-bundle.tar.gz", "os": "darwin", "arch": "arm64"},
                    {"source": "unpacked-app.tar.gz", "os": "windows", "arch": "amd64"},
                ]
            },
        }
    ]
}

SNAPSHOT_NO_FILES = {"components": [{"name": "operator"}]}


def _make_tar(path: Path, files: dict[str, bytes]) -> None:
    with tarfile.open(str(path), "w:gz") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def _make_quay_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "quay"
    mount.mkdir()
    (mount / "username").write_text("quser")
    (mount / "password").write_text("qpass")
    monkeypatch.setattr(push_oci_unsigned, "QUAY_SECRET_MOUNT", mount)
    return mount


# ---------------------------------------------------------------------------
# _stage_file_entries — darwin and windows: move as-is
# ---------------------------------------------------------------------------


def test_stage_file_entries_darwin_moves_as_is(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """macOS archives are moved intact (not extracted) to preserve .app symlinks."""
    monkeypatch.setattr(push_oci_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    unsigned_dir = comp_dir / "unsigned"
    archive = comp_dir / "app-bundle.tar.gz"
    _make_tar(archive, {"mybinary": b"data"})

    push_oci_unsigned._stage_file_entries(
        [{"source": "app-bundle.tar.gz", "os": "darwin", "arch": "arm64"}],
        comp_dir,
        unsigned_dir,
    )
    assert (unsigned_dir / "macos" / "arm64" / "app-bundle.tar.gz").exists()
    assert not archive.exists()
    assert not (unsigned_dir / "macos" / "arm64" / "mybinary").exists()


def test_stage_file_entries_windows_moves_as_is(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows archives are moved intact for the same reason as macOS."""
    monkeypatch.setattr(push_oci_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    unsigned_dir = comp_dir / "unsigned"
    archive = comp_dir / "unpacked-app.tar.gz"
    _make_tar(archive, {"app.exe": b"exe"})

    push_oci_unsigned._stage_file_entries(
        [{"source": "unpacked-app.tar.gz", "os": "windows", "arch": "amd64"}],
        comp_dir,
        unsigned_dir,
    )
    assert (unsigned_dir / "windows" / "amd64" / "unpacked-app.tar.gz").exists()
    assert not archive.exists()


def test_stage_file_entries_linux_extracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Linux archives are still extracted (no symlink concerns)."""
    monkeypatch.setattr(push_oci_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    unsigned_dir = comp_dir / "unsigned"
    archive = comp_dir / "binary-linux-amd64.tar.gz"
    _make_tar(archive, {"mybinary": b"data"})

    push_oci_unsigned._stage_file_entries(
        [{"source": "binary-linux-amd64.tar.gz", "os": "linux", "arch": "amd64"}],
        comp_dir,
        unsigned_dir,
    )
    assert (comp_dir / "linux" / "amd64" / "mybinary").exists()
    assert not archive.exists()


def test_stage_file_entries_warns_on_missing_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A warning is logged when the archive file does not exist."""
    monkeypatch.setattr(push_oci_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    unsigned_dir = comp_dir / "unsigned"

    with caplog.at_level("WARNING", logger="push_oci_unsigned"):
        push_oci_unsigned._stage_file_entries(
            [{"source": "missing.tar.gz", "os": "darwin", "arch": "arm64"}],
            comp_dir,
            unsigned_dir,
        )
    assert "Archive not found" in caplog.text


def test_stage_file_entries_skips_incomplete_entries(tmp_path: Path) -> None:
    """Entries missing source, os, or arch are silently skipped."""
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    unsigned_dir = comp_dir / "unsigned"
    push_oci_unsigned._stage_file_entries(
        [{"source": "app.tar.gz"}, {"os": "darwin"}, {}],
        comp_dir,
        unsigned_dir,
    )
    assert not unsigned_dir.exists()


def test_stage_file_entries_skips_unknown_os(tmp_path: Path) -> None:
    """Unknown OS values (not darwin/windows/linux) are silently skipped."""
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    archive = comp_dir / "app.tar.gz"
    _make_tar(archive, {"f": b"x"})
    unsigned_dir = comp_dir / "unsigned"
    push_oci_unsigned._stage_file_entries(
        [{"source": "app.tar.gz", "os": "freebsd", "arch": "amd64"}],
        comp_dir,
        unsigned_dir,
    )
    assert not unsigned_dir.exists()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_skips_no_files_component(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Components with no files or staged.files are skipped."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_NO_FILES))
    monkeypatch.setattr(push_oci_unsigned, "CONTENT_DIR", tmp_path / "artifacts")
    _make_quay_secret(tmp_path, monkeypatch)

    with mock.patch("oras_utils.oras_login"), mock.patch("oras_utils.oras_push") as mock_push:
        push_oci_unsigned.run("quay.io/org", "uid-123")
    mock_push.assert_not_called()


def test_run_moves_and_pushes_mac_and_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run() moves mac/windows archives as-is and pushes them to Quay."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT))
    content_dir = tmp_path / "artifacts"
    monkeypatch.setattr(push_oci_unsigned, "CONTENT_DIR", content_dir)
    _make_quay_secret(tmp_path, monkeypatch)

    comp_dir = content_dir / "testproduct"
    comp_dir.mkdir(parents=True)
    (comp_dir / "has_mac").touch()
    (comp_dir / "has_windows").touch()
    _make_tar(comp_dir / "app-bundle.tar.gz", {"binary": b"mac"})
    _make_tar(comp_dir / "unpacked-app.tar.gz", {"app.exe": b"win"})

    digests = {"mac": "sha256:aaa", "windows": "sha256:bbb"}

    def fake_push(tag, directory, subdirectory, component_name):
        return digests["mac"] if "mac" in tag else digests["windows"]

    with (
        mock.patch("oras_utils.oras_login"),
        mock.patch("oras_utils.oras_push", side_effect=fake_push),
    ):
        push_oci_unsigned.run("quay.io/org", "uid-123")

    assert (comp_dir / "unsigned_mac_digest.txt").read_text() == "sha256:aaa"
    assert (comp_dir / "unsigned_windows_digest.txt").read_text() == "sha256:bbb"
    assert (comp_dir / "unsigned" / "macos" / "arm64" / "app-bundle.tar.gz").exists()
    assert (comp_dir / "unsigned" / "windows" / "amd64" / "unpacked-app.tar.gz").exists()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    """main() returns 0 and calls run with parsed args."""
    with mock.patch.object(push_oci_unsigned, "run") as mock_run:
        rc = push_oci_unsigned.main(
            [
                "push_oci_unsigned.py",
                "--quay-url",
                "quay.io/org",
                "--pipeline-run-uid",
                "uid-1",
            ]
        )
    assert rc == 0
    mock_run.assert_called_once_with("quay.io/org", "uid-1")


def test_main_exception_returns_error() -> None:
    """main() returns 1 when run() raises."""
    with mock.patch.object(push_oci_unsigned, "run", side_effect=RuntimeError("boom")):
        rc = push_oci_unsigned.main(
            [
                "push_oci_unsigned.py",
                "--quay-url",
                "quay.io/org",
                "--pipeline-run-uid",
                "uid-1",
            ]
        )
    assert rc == 1
