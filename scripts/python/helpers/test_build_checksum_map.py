"""Tests for build_checksum_map.py."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from unittest import mock

import pytest

import build_checksum_map

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

SNAPSHOT = {
    "components": [
        {
            "name": "testproduct",
            "staged": {
                "destination": "testproduct-amd64",
                "version": "1.3",
            },
        }
    ]
}


def _setup_dockerconfig(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "dockerconfig"
    mount.mkdir()
    (mount / ".dockerconfigjson").write_text('{"auths":{}}')
    monkeypatch.setattr(build_checksum_map, "TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT", mount)
    return mount


def _make_ready_dir(base: Path, name: str, files: dict[str, bytes]) -> Path:
    d = base / name / "ready_for_distribution"
    d.mkdir(parents=True)
    for fname, content in files.items():
        (d / fname).write_bytes(content)
    return d


# ---------------------------------------------------------------------------
# _sha256
# ---------------------------------------------------------------------------


def test_sha256_correct(tmp_path: Path) -> None:
    f = tmp_path / "data.bin"
    content = b"hello world"
    f.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert build_checksum_map._sha256(f) == expected


def test_sha256_empty_file(tmp_path: Path) -> None:
    f = tmp_path / "empty"
    f.write_bytes(b"")
    expected = hashlib.sha256(b"").hexdigest()
    assert build_checksum_map._sha256(f) == expected


# ---------------------------------------------------------------------------
# _setup_docker_config
# ---------------------------------------------------------------------------


def test_setup_docker_config_copies_to_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount = tmp_path / "dockerconfig"
    mount.mkdir()
    (mount / ".dockerconfigjson").write_text('{"auths":{}}')
    monkeypatch.setattr(build_checksum_map, "TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT", mount)

    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        build_checksum_map._setup_docker_config()

    assert (home / ".docker" / "config.json").exists()
    assert json.loads((home / ".docker" / "config.json").read_text()) == {"auths": {}}


def test_setup_docker_config_skips_missing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount = tmp_path / "empty_mount"
    mount.mkdir()
    # No .dockerconfigjson file
    monkeypatch.setattr(build_checksum_map, "TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT", mount)

    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        build_checksum_map._setup_docker_config()

    assert not (home / ".docker" / "config.json").exists()


def test_setup_docker_config_skips_empty_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount = tmp_path / "empty_dockerconfig"
    mount.mkdir()
    (mount / ".dockerconfigjson").write_bytes(b"")
    monkeypatch.setattr(build_checksum_map, "TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT", mount)

    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        build_checksum_map._setup_docker_config()

    assert not (home / ".docker" / "config.json").exists()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_writes_oci_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(build_checksum_map, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(build_checksum_map, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "snapshot.json").write_text(json.dumps(SNAPSHOT))
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": []}))  # ignored
    _setup_dockerconfig(tmp_path, monkeypatch)

    _make_ready_dir(
        tmp_path / "artifacts",
        "testproduct",
        {"archive.tar.gz": b"data"},
    )

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "select-oci-auth":
            return b'{"auths":{}}'
        if cmd[0] == "oras":
            return "Digest: sha256:" + "a" * 64 + "\n"
        return b""

    with (
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        (tmp_path / "home").mkdir()
        oci_result = build_checksum_map.run()

    assert build_checksum_map.OCI_STORE in oci_result
    assert "sha256:" in oci_result


def test_run_uses_shared_snapshot_over_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run() should prefer snapshot.json from shared dir over SNAPSHOT_JSON env."""
    monkeypatch.setattr(build_checksum_map, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(build_checksum_map, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "snapshot.json").write_text(json.dumps(SNAPSHOT))
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps({"components": [{"name": "IGNORED"}]}))
    _setup_dockerconfig(tmp_path, monkeypatch)

    _make_ready_dir(
        tmp_path / "artifacts",
        "testproduct",
        {"archive.tar.gz": b"data"},
    )

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "select-oci-auth":
            return b'{"auths":{}}'
        if cmd[0] == "oras":
            return "Digest: sha256:" + "b" * 64 + "\n"
        return b""

    with (
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        (tmp_path / "home").mkdir()
        result = build_checksum_map.run()

    assert "sha256:" in result


def test_run_uses_env_snapshot_when_no_shared_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(build_checksum_map, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(build_checksum_map, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    # No snapshot.json in shared dir
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT))
    _setup_dockerconfig(tmp_path, monkeypatch)

    _make_ready_dir(
        tmp_path / "artifacts",
        "testproduct",
        {"archive.tar.gz": b"data"},
    )

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "select-oci-auth":
            return b'{"auths":{}}'
        if cmd[0] == "oras":
            return "Digest: sha256:" + "c" * 64 + "\n"
        return b""

    with (
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        (tmp_path / "home").mkdir()
        result = build_checksum_map.run()

    assert "sha256:" in result


def test_run_raises_on_missing_oras_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(build_checksum_map, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(build_checksum_map, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "snapshot.json").write_text(json.dumps(SNAPSHOT))
    _setup_dockerconfig(tmp_path, monkeypatch)

    _make_ready_dir(
        tmp_path / "artifacts",
        "testproduct",
        {"archive.tar.gz": b"data"},
    )

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "select-oci-auth":
            return b'{"auths":{}}'
        if cmd[0] == "oras":
            return "no digest here\n"
        return b""

    with (
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        (tmp_path / "home").mkdir()
        with pytest.raises(RuntimeError, match="digest"):
            build_checksum_map.run()


def test_run_skips_missing_ready_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(build_checksum_map, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(build_checksum_map, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "snapshot.json").write_text(
        json.dumps({"components": [{"name": "missing"}]})
    )
    _setup_dockerconfig(tmp_path, monkeypatch)

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "select-oci-auth":
            return b'{"auths":{}}'
        if cmd[0] == "oras":
            return "Digest: sha256:" + "d" * 64 + "\n"
        return b""

    with (
        caplog.at_level(logging.WARNING, logger="build_checksum_map"),
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        (tmp_path / "home").mkdir()
        result = build_checksum_map.run()

    assert "sha256:" in result
    assert "not found" in caplog.text


def test_run_excludes_sha256sum_from_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(build_checksum_map, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(build_checksum_map, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "snapshot.json").write_text(json.dumps(SNAPSHOT))
    _setup_dockerconfig(tmp_path, monkeypatch)

    _make_ready_dir(
        tmp_path / "artifacts",
        "testproduct",
        {
            "archive.tar.gz": b"data",
            "sha256sum.txt": b"checksums",
            "sha256sum.txt.sig": b"sig",
        },
    )

    captured_manifests = []

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "select-oci-auth":
            return b'{"auths":{}}'
        if cmd[0] == "oras":
            # Read the checksum_map.json from the cwd
            cwd = kwargs.get("cwd", ".")
            manifest_file = Path(cwd) / "checksum_map.json"
            if manifest_file.exists():
                captured_manifests.append(json.loads(manifest_file.read_text()))
            return "Digest: sha256:" + "e" * 64 + "\n"
        return b""

    with (
        mock.patch("pathlib.Path.home", return_value=tmp_path / "home"),
        mock.patch("subprocess.check_output", side_effect=fake_check_output),
    ):
        (tmp_path / "home").mkdir()
        build_checksum_map.run()

    assert len(captured_manifests) == 1
    manifest = captured_manifests[0]
    assert len(manifest) == 1
    files = manifest[0]["files"]
    assert "archive.tar.gz" in files
    assert "sha256sum.txt" not in files
    assert "sha256sum.txt.sig" not in files


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    ref = f"{build_checksum_map.OCI_STORE}@sha256:abc123"
    with mock.patch.object(build_checksum_map, "run", return_value=ref):
        rc = build_checksum_map.main(["build_checksum_map.py"])
    assert rc == 0


def test_main_exception_returns_error() -> None:
    with mock.patch.object(build_checksum_map, "run", side_effect=RuntimeError("oras fail")):
        rc = build_checksum_map.main(["build_checksum_map.py"])
    assert rc == 1
