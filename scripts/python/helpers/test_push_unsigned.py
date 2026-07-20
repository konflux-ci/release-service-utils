"""Tests for push_unsigned.py."""

from __future__ import annotations

import json
import logging
import tarfile
from pathlib import Path
from unittest import mock

import pytest

import oras_utils
import push_unsigned

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

SNAPSHOT = {
    "components": [
        {
            "name": "testproduct",
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


def _make_quay_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "quay"
    mount.mkdir()
    (mount / "username").write_text("quser")
    (mount / "password").write_text("qpass")
    monkeypatch.setattr(push_unsigned, "QUAY_SECRET_MOUNT", mount)
    return mount


def _make_component_dir(
    base: Path,
    name: str,
    has_mac: bool = False,
    has_windows: bool = False,
    has_linux: bool = False,
) -> Path:
    d = base / name
    d.mkdir(parents=True)
    if has_mac:
        (d / "has_mac").touch()
    if has_windows:
        (d / "has_windows").touch()
    if has_linux:
        (d / "has_linux").touch()
    return d


# ---------------------------------------------------------------------------
# is_supplementary_file
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        "README",
        "readme",
        "Readme",
        "README.md",
        "readme.md",
        "README.MD",
        "README.txt",
        "readme.TXT",
        "LICENSE",
        "license",
        "LICENSE.md",
        "LICENSE.txt",
        "LICENSE.TXT",
        "LICENSE.MD",
        "CHANGELOG",
        "changelog",
        "CHANGELOG.md",
        "changelog.txt",
        "CHANGELOG.TXT",
        "Changelog.MD",
        "/some/path/README.md",
        "amd64/LICENSE",
    ],
)
def test_is_supplementary_file_true(filename: str) -> None:
    """Parametrized filenames that should be identified as supplementary."""
    assert push_unsigned.is_supplementary_file(Path(filename))


@pytest.mark.parametrize(
    "filename",
    [
        "NOTES.TXT",
        "RELEASE_NOTES.TXT",
        "INSTALL.md",
        "CONTRIBUTING.md",
        "setup.cfg",
        "Makefile",
        "config.json",
        "binary-name",
        "README.rst",
        "README.html",
        "LICENSE.html",
        "CHANGELOG.rst",
        "readme.doc",
        "LICENSE-MIT",
        "README-dev.md",
        "my-binary.exe",
    ],
)
def test_is_supplementary_file_false(filename: str) -> None:
    """Parametrized filenames that should NOT be identified as supplementary."""
    assert not push_unsigned.is_supplementary_file(Path(filename))


# ---------------------------------------------------------------------------
# move_supplementary_out
# ---------------------------------------------------------------------------


def test_move_supplementary_out_moves_supplementary(tmp_path: Path) -> None:
    """Supplementary files are moved to the hold dir while non-supplementary files remain."""
    src = tmp_path / "unsigned" / "macos" / "amd64"
    hold = tmp_path / "supplementary" / "macos"
    src.mkdir(parents=True)

    (src / "my-binary").write_bytes(b"binary")
    (src / "README.md").write_text("readme")
    (src / "LICENSE").write_text("license")
    (src / "CHANGELOG.txt").write_text("changelog")

    push_unsigned.move_supplementary_out(src, hold)

    assert (src / "my-binary").exists()
    assert not (src / "README.md").exists()
    assert not (src / "LICENSE").exists()
    assert not (src / "CHANGELOG.txt").exists()
    assert (hold / "README.md").exists()
    assert (hold / "LICENSE").exists()
    assert (hold / "CHANGELOG.txt").exists()


def test_move_supplementary_out_missing_src_is_noop(tmp_path: Path) -> None:
    """A missing source directory is handled silently without raising."""
    push_unsigned.move_supplementary_out(tmp_path / "nonexistent", tmp_path / "hold")


def test_move_supplementary_out_preserves_subdirs(tmp_path: Path) -> None:
    """Supplementary files in subdirectories are moved preserving the sub-path structure."""
    src = tmp_path / "src"
    hold = tmp_path / "hold"
    sub = src / "arm64"
    sub.mkdir(parents=True)
    (sub / "readme.TXT").write_text("r")
    (sub / "my-binary").write_bytes(b"b")

    push_unsigned.move_supplementary_out(src, hold)

    assert not (src / "arm64" / "readme.TXT").exists()
    assert (src / "arm64" / "my-binary").exists()
    assert (hold / "arm64" / "readme.TXT").exists()


# ---------------------------------------------------------------------------
# _unpack_file_entries
# ---------------------------------------------------------------------------


def _make_tar(path: Path, files: dict[str, bytes]) -> None:
    """Create a tar.gz at *path* containing the given filenames→contents."""
    with tarfile.open(str(path), "w:gz") as tf:
        for name, content in files.items():
            import io

            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))


def test_unpack_file_entries_linux(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Linux tarball is extracted into the linux/<arch> directory."""
    monkeypatch.setattr(push_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    archive = comp_dir / "binary-linux-amd64.tar.gz"
    _make_tar(archive, {"mybinary": b"data"})

    push_unsigned._unpack_file_entries(
        [{"source": "/releases/binary-linux-amd64.tar.gz", "os": "linux", "arch": "amd64"}],
        comp_dir,
        comp_dir / "unsigned",
    )
    assert (comp_dir / "linux" / "amd64" / "mybinary").exists()
    assert not archive.exists()


def test_unpack_file_entries_darwin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Darwin tarball is extracted into the unsigned/macos/<arch> directory."""
    monkeypatch.setattr(push_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    unsigned_dir = comp_dir / "unsigned"
    archive = comp_dir / "binary-darwin-amd64.tar.gz"
    _make_tar(archive, {"mybinary": b"data"})

    push_unsigned._unpack_file_entries(
        [{"source": "/releases/binary-darwin-amd64.tar.gz", "os": "darwin", "arch": "amd64"}],
        comp_dir,
        unsigned_dir,
    )
    assert (unsigned_dir / "macos" / "amd64" / "mybinary").exists()
    assert not archive.exists()


def test_unpack_file_entries_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Windows tarball is extracted into the unsigned/windows/<arch> directory."""
    monkeypatch.setattr(push_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    unsigned_dir = comp_dir / "unsigned"
    archive = comp_dir / "binary-windows-amd64.tar.gz"
    _make_tar(archive, {"mybinary.exe": b"data"})

    push_unsigned._unpack_file_entries(
        [
            {
                "source": "/releases/binary-windows-amd64.tar.gz",
                "os": "windows",
                "arch": "amd64",
            }
        ],
        comp_dir,
        unsigned_dir,
    )
    assert (unsigned_dir / "windows" / "amd64" / "mybinary.exe").exists()
    assert not archive.exists()


def test_unpack_file_entries_missing_archive_is_warned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A warning is logged when the archive file referenced in an entry does not exist."""
    monkeypatch.setattr(push_unsigned, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()

    with caplog.at_level(logging.WARNING, logger="push_unsigned"):
        push_unsigned._unpack_file_entries(
            [{"source": "/releases/missing.tar.gz", "os": "linux", "arch": "amd64"}],
            comp_dir,
            comp_dir / "unsigned",
        )
    assert "Archive not found" in caplog.text


def test_unpack_file_entries_skips_missing_fields(tmp_path: Path) -> None:
    """File entries without source, os, or arch fields are silently skipped."""
    push_unsigned._unpack_file_entries(
        [{"os": "linux"}],
        tmp_path,
        tmp_path / "unsigned",
    )
    # no crash


def test_unpack_file_entries_qcow2_passthrough(tmp_path: Path) -> None:
    """A qcow2 disk image is moved directly to the OS/arch dir without tar extraction."""
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    disk_image = comp_dir / "rhel-10.0-x86_64-kvm.qcow2"
    disk_image.write_bytes(b"qcow2 raw content")

    push_unsigned._unpack_file_entries(
        [{"source": "/releases/rhel-10.0-x86_64-kvm.qcow2", "os": "linux", "arch": "x86_64"}],
        comp_dir,
        comp_dir / "unsigned",
    )
    dest = comp_dir / "linux" / "x86_64" / "rhel-10.0-x86_64-kvm.qcow2"
    assert dest.exists()
    assert dest.read_bytes() == b"qcow2 raw content"
    assert not disk_image.exists()


def test_unpack_file_entries_iso_passthrough(tmp_path: Path) -> None:
    """An iso disk image is moved directly to the OS/arch dir without tar extraction."""
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    disk_image = comp_dir / "rhel-10.0-x86_64-boot.iso"
    disk_image.write_bytes(b"iso raw content")

    push_unsigned._unpack_file_entries(
        [{"source": "/releases/rhel-10.0-x86_64-boot.iso", "os": "linux", "arch": "x86_64"}],
        comp_dir,
        comp_dir / "unsigned",
    )
    dest = comp_dir / "linux" / "x86_64" / "rhel-10.0-x86_64-boot.iso"
    assert dest.exists()
    assert dest.read_bytes() == b"iso raw content"
    assert not disk_image.exists()


def test_unpack_file_entries_iso_gz_passthrough(tmp_path: Path) -> None:
    """A .iso.gz disk image is moved directly via extension without tar extraction."""
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    disk_image = comp_dir / "install.iso.gz"
    disk_image.write_bytes(b"iso.gz content")

    push_unsigned._unpack_file_entries(
        [{"source": "/releases/install.iso.gz", "os": "linux", "arch": "x86_64"}],
        comp_dir,
        comp_dir / "unsigned",
    )
    dest = comp_dir / "linux" / "x86_64" / "install.iso.gz"
    assert dest.exists()
    assert dest.read_bytes() == b"iso.gz content"
    assert not disk_image.exists()


def test_unpack_file_entries_tar_gz_passthrough_via_content_type(tmp_path: Path) -> None:
    """A .tar.gz GCP disk image is moved directly when is_disk_image_component=True."""
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    disk_image = comp_dir / "image.tar.gz"
    disk_image.write_bytes(b"gcp disk image content")

    push_unsigned._unpack_file_entries(
        [{"source": "/releases/image.tar.gz", "os": "linux", "arch": "x86_64"}],
        comp_dir,
        comp_dir / "unsigned",
        is_disk_image_component=True,
    )
    dest = comp_dir / "linux" / "x86_64" / "image.tar.gz"
    assert dest.exists()
    assert dest.read_bytes() == b"gcp disk image content"
    assert not disk_image.exists()


def test_unpack_file_entries_rejects_path_traversal(tmp_path: Path) -> None:
    """RuntimeError is raised when a tar entry contains an unsafe path traversal sequence."""
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    archive = comp_dir / "binary-linux-amd64.tar.gz"

    with tarfile.open(str(archive), "w:gz") as tf:
        import io

        payload = b"oops"
        info = tarfile.TarInfo(name="../../escape.txt")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))

    with pytest.raises(RuntimeError, match="unsafe path"):
        push_unsigned._unpack_file_entries(
            [
                {
                    "source": "/releases/binary-linux-amd64.tar.gz",
                    "os": "linux",
                    "arch": "amd64",
                }
            ],
            comp_dir,
            comp_dir / "unsigned",
        )


# ---------------------------------------------------------------------------
# oras_utils.oras_push
# ---------------------------------------------------------------------------


def test_oras_push_returns_digest(tmp_path: Path) -> None:
    """oras_push returns the sha256 digest parsed from the oras push output."""
    with mock.patch("subprocess.check_output", return_value="Digest: sha256:abc123\n"):
        digest = oras_utils.oras_push("quay.io/org/prod:tag", tmp_path, "macos", "prod")
    assert digest == "sha256:abc123"


def test_oras_push_raises_on_missing_digest(tmp_path: Path) -> None:
    """RuntimeError is raised when oras push output contains no Digest line."""
    with mock.patch("subprocess.check_output", return_value="no digest here"):
        with pytest.raises(RuntimeError, match="digest"):
            oras_utils.oras_push("quay.io/org/prod:tag", tmp_path, "macos", "prod")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_skips_no_files_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A component with no files or staged.files entries is skipped with a log message."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "op"}]}))
    monkeypatch.setattr(push_unsigned, "CONTENT_DIR", tmp_path)
    _make_quay_secret(tmp_path, monkeypatch)

    with (
        caplog.at_level(logging.INFO, logger="push_unsigned"),
        mock.patch("oras_utils.oras_login"),
    ):
        push_unsigned.run("quay.io/org", "uid-123")

    assert "Skipping" in caplog.text


def test_run_pushes_mac_and_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two oras push calls are made for a component with both mac and windows artifacts."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT))
    monkeypatch.setattr(push_unsigned, "CONTENT_DIR", tmp_path / "artifacts")
    _make_quay_secret(tmp_path, monkeypatch)

    comp_dir = _make_component_dir(
        tmp_path / "artifacts", "testproduct", has_mac=True, has_windows=True, has_linux=True
    )
    # Create fake archives
    for fname, os_name, arch in [
        ("binary-darwin-amd64.tar.gz", "darwin", "amd64"),
        ("binary-windows-amd64.tar.gz", "windows", "amd64"),
        ("binary-linux-amd64.tar.gz", "linux", "amd64"),
    ]:
        archive = comp_dir / fname
        _make_tar(archive, {"binary": b"data"})

    oras_calls = []

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "oras" and cmd[1] == "push":
            oras_calls.append(cmd)
            return "Digest: sha256:fakedigest\n"
        raise ValueError(f"unexpected: {cmd}")

    with (
        mock.patch("oras_utils.oras_login"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        push_unsigned.run("quay.io/org", "uid-123")

    assert len(oras_calls) == 2  # mac + windows
    assert (comp_dir / "unsigned_mac_digest.txt").read_text() == "sha256:fakedigest"
    assert (comp_dir / "unsigned_windows_digest.txt").read_text() == "sha256:fakedigest"


def test_run_no_mac_or_windows_skips_pushes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No oras push is made when the component has neither has_mac nor has_windows flags."""
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT))
    monkeypatch.setattr(push_unsigned, "CONTENT_DIR", tmp_path / "artifacts")
    _make_quay_secret(tmp_path, monkeypatch)

    comp_dir = _make_component_dir(tmp_path / "artifacts", "testproduct", has_linux=True)
    _make_tar(comp_dir / "binary-linux-amd64.tar.gz", {"binary": b"data"})
    # No has_mac or has_windows flag files

    oras_push_calls = []

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "oras" and cmd[1] == "push":
            oras_push_calls.append(cmd)
            return "Digest: sha256:x\n"
        return b""

    with (
        mock.patch("oras_utils.oras_login"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        push_unsigned.run("quay.io/org", "uid-123")

    assert oras_push_calls == []


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    """main() returns 0 and passes quay URL and pipeline-run-uid to run()."""
    with mock.patch.object(push_unsigned, "run") as mock_run:
        rc = push_unsigned.main(
            ["push_unsigned.py", "--quay-url", "quay.io/org", "--pipeline-run-uid", "uid"]
        )
    assert rc == 0
    mock_run.assert_called_once_with("quay.io/org", "uid")


def test_main_exception_returns_error() -> None:
    """main() returns 1 when run() raises an exception."""
    with mock.patch.object(push_unsigned, "run", side_effect=RuntimeError("oras fail")):
        rc = push_unsigned.main(
            ["push_unsigned.py", "--quay-url", "quay.io/org", "--pipeline-run-uid", "uid"]
        )
    assert rc == 1
