"""Tests for compress_artifacts.py."""

from __future__ import annotations

import json
import tarfile
import zipfile
from pathlib import Path
from unittest import mock

import pytest

import compress_artifacts

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

COMPONENT_LINUX = {
    "name": "testproduct",
    "files": [
        {"source": "/releases/binary-linux-amd64.tar.gz", "os": "linux", "arch": "amd64"},
    ],
}

COMPONENT_DARWIN = {
    "name": "testproduct",
    "files": [
        {"source": "/releases/binary-darwin-amd64.tar.gz", "os": "darwin", "arch": "amd64"},
    ],
}

COMPONENT_WINDOWS = {
    "name": "testproduct",
    "files": [
        {"source": "/releases/binary-windows-amd64.tar.gz", "os": "windows", "arch": "amd64"},
    ],
}


def _setup_quay_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    quay = tmp_path / "quay"
    quay.mkdir()
    (quay / "username").write_text("quser")
    (quay / "password").write_text("qpass")
    monkeypatch.setattr(compress_artifacts, "QUAY_SECRET_MOUNT", quay)


def _make_arch_dir(base: Path, os_name: str, arch: str, binary_name: str = "mybinary") -> Path:
    d = base / os_name / arch
    d.mkdir(parents=True)
    (d / binary_name).write_bytes(b"binary content")
    return d


# ---------------------------------------------------------------------------
# _windows_filename
# ---------------------------------------------------------------------------


def test_windows_filename_tar_gz() -> None:
    assert compress_artifacts._windows_filename("binary-amd64.tar.gz") == "binary-amd64.zip"


def test_windows_filename_tar() -> None:
    assert compress_artifacts._windows_filename("binary-amd64.tar") == "binary-amd64.zip"


def test_windows_filename_already_zip() -> None:
    assert compress_artifacts._windows_filename("binary-amd64.zip") == "binary-amd64.zip"


def test_windows_filename_exe() -> None:
    assert compress_artifacts._windows_filename("binary.exe") == "binary.exe"


# ---------------------------------------------------------------------------
# _process_file_entry
# ---------------------------------------------------------------------------


def test_process_file_entry_linux(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    ready_dir = comp_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True)

    # Linux: component_dir / "linux" / arch
    _make_arch_dir(comp_dir, "linux", "amd64")

    result = compress_artifacts._process_file_entry(
        {"source": "/releases/binary-linux-amd64.tar.gz", "os": "linux", "arch": "amd64"},
        "files",
        comp_dir,
        ready_dir,
    )
    archive = ready_dir / "binary-linux-amd64.tar.gz"
    assert archive.exists()
    assert tarfile.is_tarfile(str(archive))
    assert result == "/releases/binary-linux-amd64.tar.gz"


def test_process_file_entry_darwin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    ready_dir = comp_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True)

    # Darwin: component_dir / "signed" / "macos" / arch
    _make_arch_dir(comp_dir / "signed", "macos", "amd64")

    result = compress_artifacts._process_file_entry(
        {"source": "/releases/binary-darwin-amd64.tar.gz", "os": "darwin", "arch": "amd64"},
        "files",
        comp_dir,
        ready_dir,
    )
    assert (ready_dir / "binary-darwin-amd64.tar.gz").exists()
    assert result == "/releases/binary-darwin-amd64.tar.gz"


def test_process_file_entry_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    ready_dir = comp_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True)

    # Windows: component_dir / "signed" / "windows" / arch
    _make_arch_dir(comp_dir / "signed", "windows", "amd64", "mybinary.exe")

    result = compress_artifacts._process_file_entry(
        {"source": "/releases/binary-windows-amd64.tar.gz", "os": "windows", "arch": "amd64"},
        "files",
        comp_dir,
        ready_dir,
    )
    zip_file = ready_dir / "binary-windows-amd64.zip"
    assert zip_file.exists()
    assert zipfile.is_zipfile(str(zip_file))
    # source is updated to .zip
    assert result is not None and result.endswith(".zip")


def test_process_file_entry_missing_arch_dir_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    ready_dir = comp_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True)
    # No arch directory created

    with pytest.raises(RuntimeError, match="not found"):
        compress_artifacts._process_file_entry(
            {"source": "/releases/binary-linux-amd64.tar.gz", "os": "linux", "arch": "amd64"},
            "files",
            comp_dir,
            ready_dir,
        )


def test_process_file_entry_empty_arch_dir_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    ready_dir = comp_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True)
    (comp_dir / "linux" / "amd64").mkdir(parents=True)  # empty dir

    with pytest.raises(RuntimeError, match="empty or not found"):
        compress_artifacts._process_file_entry(
            {"source": "/releases/binary-linux-amd64.tar.gz", "os": "linux", "arch": "amd64"},
            "files",
            comp_dir,
            ready_dir,
        )


def test_process_file_entry_missing_source_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    ready_dir = comp_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True)

    with pytest.raises(RuntimeError, match="Missing source"):
        compress_artifacts._process_file_entry(
            {"os": "linux", "arch": "amd64"}, "files", comp_dir, ready_dir
        )


def test_process_file_entry_unknown_os_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    ready_dir = comp_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True)

    with pytest.raises(RuntimeError, match="Unknown OS"):
        compress_artifacts._process_file_entry(
            {"source": "/releases/binary.tar.gz", "os": "solaris", "arch": "sparc"},
            "files",
            comp_dir,
            ready_dir,
        )


# ---------------------------------------------------------------------------
# _pull_signed_content
# ---------------------------------------------------------------------------


def test_pull_signed_content_skips_when_no_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()

    with mock.patch("subprocess.check_call") as mock_cc:
        compress_artifacts._pull_signed_content("quay.io/org", "prod", comp_dir)

    mock_cc.assert_not_called()


def test_pull_signed_content_pulls_mac_and_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    comp_dir = tmp_path / "prod"
    comp_dir.mkdir()
    (comp_dir / "has_mac").touch()
    (comp_dir / "has_windows").touch()
    (comp_dir / "signed_mac_digest.txt").write_text("sha256:mac")
    (comp_dir / "signed_windows_digest.txt").write_text("sha256:win ")

    calls = []

    def fake_check_call(cmd, **kwargs):
        calls.append(cmd[0:3])

    with mock.patch("subprocess.check_call", side_effect=fake_check_call):
        compress_artifacts._pull_signed_content("quay.io/org", "prod", comp_dir)

    assert len(calls) == 2
    assert all(c[0] == "oras" for c in calls)


# ---------------------------------------------------------------------------
# _restore_supplementary
# ---------------------------------------------------------------------------


def test_restore_supplementary_restores_files(tmp_path: Path) -> None:
    comp_dir = tmp_path / "prod"
    supp = comp_dir / "supplementary"
    signed = comp_dir / "signed"

    for os_name in ("macos", "windows"):
        (supp / os_name / "amd64").mkdir(parents=True)
        (supp / os_name / "amd64" / "README.md").write_text("readme")
        (signed / os_name / "amd64").mkdir(parents=True)
        (signed / os_name / "amd64" / "binary").write_bytes(b"bin")

    compress_artifacts._restore_supplementary(comp_dir)

    assert (signed / "macos" / "amd64" / "README.md").exists()
    assert (signed / "windows" / "amd64" / "README.md").exists()
    assert not (supp / "macos" / "amd64" / "README.md").exists()


# ---------------------------------------------------------------------------
# compress_component
# ---------------------------------------------------------------------------


def test_compress_component_linux(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    # component name is "testproduct" → CONTENT_DIR / "testproduct"
    comp_dir = tmp_path / "testproduct"
    _make_arch_dir(comp_dir, "linux", "amd64")

    updated = compress_artifacts.compress_component(
        COMPONENT_LINUX, {"components": [COMPONENT_LINUX]}
    )
    assert (comp_dir / "ready_for_distribution" / "binary-linux-amd64.tar.gz").exists()
    assert updated["files"][0]["source"] == "/releases/binary-linux-amd64.tar.gz"


def test_compress_component_windows_updates_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    comp_dir = tmp_path / "testproduct"
    _make_arch_dir(comp_dir / "signed", "windows", "amd64", "binary.exe")

    updated = compress_artifacts.compress_component(
        COMPONENT_WINDOWS, {"components": [COMPONENT_WINDOWS]}
    )
    assert (comp_dir / "ready_for_distribution" / "binary-windows-amd64.zip").exists()
    assert updated["files"][0]["source"].endswith(".zip")


def test_compress_component_staged_files_processed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path)
    component = {
        "name": "prod",
        "staged": {
            "files": [
                {
                    "source": "/releases/binary-linux-amd64.tar.gz",
                    "os": "linux",
                    "arch": "amd64",
                }
            ]
        },
    }
    comp_dir = tmp_path / "prod"
    _make_arch_dir(comp_dir, "linux", "amd64")

    compress_artifacts.compress_component(component, {"components": [component]})
    assert (comp_dir / "ready_for_distribution" / "binary-linux-amd64.tar.gz").exists()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_saves_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compress_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(compress_artifacts, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [COMPONENT_LINUX]}))
    _setup_quay_secret(tmp_path, monkeypatch)

    # CONTENT_DIR = tmp_path / "artifacts"; component name = "testproduct"
    comp_dir = tmp_path / "artifacts" / "testproduct"
    _make_arch_dir(comp_dir, "linux", "amd64")

    with mock.patch("subprocess.check_call"):
        compress_artifacts.run("quay.io/org")

    snap = json.loads((tmp_path / "shared" / "snapshot.json").read_text())
    assert "components" in snap


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    with mock.patch.object(compress_artifacts, "run") as mock_run:
        rc = compress_artifacts.main(["compress_artifacts.py", "--quay-url", "quay.io/org"])
    assert rc == 0
    mock_run.assert_called_once_with("quay.io/org")


def test_main_exception_returns_error() -> None:
    with mock.patch.object(compress_artifacts, "run", side_effect=RuntimeError("oras fail")):
        rc = compress_artifacts.main(["compress_artifacts.py", "--quay-url", "quay.io/org"])
    assert rc == 1
