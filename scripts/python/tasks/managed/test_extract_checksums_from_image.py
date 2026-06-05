"""Tests for ``extract_checksums_from_image``."""

from __future__ import annotations

import io
import json
import subprocess
import tarfile
from pathlib import Path
from unittest import mock

import extract_checksums_from_image as ecfi
import pytest

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_snapshot(path: Path, components: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"components": components}), encoding="utf-8")


def _write_data(path: Path, component_names: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mapping = {"mapping": {"components": [{"name": n} for n in component_names]}}
    path.write_text(json.dumps(mapping), encoding="utf-8")


def _make_layer_tar(dest: Path, base_dir: str, files: dict[str, str]) -> str:
    """Create a gzip tar at *dest* containing files under *base_dir*.

    Returns the sha256 digest string (``sha256:<hex>``).
    """
    import hashlib

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, content in files.items():
            info = tarfile.TarInfo(name=f"{base_dir}/{name}")
            data = content.encode("utf-8")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    raw = buf.getvalue()
    digest = hashlib.sha256(raw).hexdigest()
    dest.mkdir(parents=True, exist_ok=True)
    (dest / digest).write_bytes(raw)
    return f"sha256:{digest}"


def _make_empty_layer_tar(dest_dir: Path) -> str:
    """Create a tar with no relevant entries. Returns digest string."""
    import hashlib

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="unrelated/file.txt")
        data = b"nope"
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
    raw = buf.getvalue()
    digest = hashlib.sha256(raw).hexdigest()
    (dest_dir / digest).write_bytes(raw)
    return f"sha256:{digest}"


def _write_manifest(image_dir: Path, digests: list[str]) -> None:
    manifest = {"layers": [{"digest": d} for d in digests]}
    (image_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def _fake_copy_image(image_dir: Path, base_dir: str, files: dict[str, str]):
    """Return a copy_image callable that populates *image_dir* with a layer."""
    digest = _make_layer_tar(image_dir, base_dir, files)
    _write_manifest(image_dir, [digest])

    def _copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
        import shutil

        dest_path = Path(dest.removeprefix("dir:"))
        for item in image_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, dest_path)
        return subprocess.CompletedProcess(
            args=["skopeo", "copy"], returncode=0, stdout="", stderr=""
        )

    return _copy


def _stub_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
    """Copy stub that writes a minimal valid layer with a SHA256SUMS file."""
    dest_path = Path(dest.removeprefix("dir:"))
    digest = _make_layer_tar(dest_path, "releases", {"SHA256SUMS": "stub"})
    _write_manifest(dest_path, [digest])
    return subprocess.CompletedProcess(
        args=["skopeo", "copy"], returncode=0, stdout="", stderr=""
    )


# ---------------------------------------------------------------------------
# load_snapshot
# ---------------------------------------------------------------------------


def test_load_snapshot_valid(tmp_path: Path) -> None:
    """Valid JSON file is parsed and returned."""
    p = tmp_path / "snapshot.json"
    expected = {"components": [{"name": "c1"}]}
    p.write_text(json.dumps(expected), encoding="utf-8")

    assert ecfi.load_snapshot(p) == expected


def test_load_snapshot_missing_file(tmp_path: Path) -> None:
    """Missing file raises ValueError."""
    with pytest.raises(ValueError, match="No valid snapshot file"):
        ecfi.load_snapshot(tmp_path / "nope.json")


def test_load_snapshot_invalid_json(tmp_path: Path) -> None:
    """Malformed JSON raises ValueError (JSONDecodeError is a subclass)."""
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")

    with pytest.raises(ValueError):
        ecfi.load_snapshot(p)


# ---------------------------------------------------------------------------
# load_components
# ---------------------------------------------------------------------------


def test_load_components_file_missing(tmp_path: Path) -> None:
    """Missing data file returns empty list (no filtering)."""
    assert ecfi.load_components(tmp_path / "nope.json") == []


def test_load_components_no_mapping_key(tmp_path: Path) -> None:
    """Data file without ``mapping.components`` returns empty list."""
    p = tmp_path / "data.json"
    p.write_text("{}", encoding="utf-8")

    assert ecfi.load_components(p) == []


def test_load_components_empty_components(tmp_path: Path) -> None:
    """Empty components array returns empty list."""
    p = tmp_path / "data.json"
    p.write_text(json.dumps({"mapping": {"components": []}}), encoding="utf-8")

    assert ecfi.load_components(p) == []


def test_load_components_extracts_names(tmp_path: Path) -> None:
    """Component names are extracted in order."""
    p = tmp_path / "data.json"
    _write_data(p, ["comp-a", "comp-b"])

    assert ecfi.load_components(p) == ["comp-a", "comp-b"]


def test_load_components_missing_name_raises(tmp_path: Path) -> None:
    """Component entry without ``name`` raises ValueError."""
    p = tmp_path / "data.json"
    p.write_text(
        json.dumps({"mapping": {"components": [{"repo": "x"}]}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing 'name' field"):
        ecfi.load_components(p)


# ---------------------------------------------------------------------------
# extract_binaries_from_layers
# ---------------------------------------------------------------------------


def test_extract_binaries_from_layers_matching(tmp_path: Path) -> None:
    """Layers containing the target path are extracted."""
    digest = _make_layer_tar(
        tmp_path, "releases", {"binary.zip": "data", "SHA256SUMS": "abc 123"}
    )
    _write_manifest(tmp_path, [digest])

    ecfi.extract_binaries_from_layers(tmp_path, "releases")

    assert (tmp_path / "releases" / "binary.zip").exists()
    assert (tmp_path / "releases" / "SHA256SUMS").exists()


def test_extract_binaries_from_layers_skips_non_matching(
    tmp_path: Path,
) -> None:
    """Layers without the target path are skipped."""
    digest = _make_empty_layer_tar(tmp_path)
    _write_manifest(tmp_path, [digest])

    ecfi.extract_binaries_from_layers(tmp_path, "releases")

    assert not (tmp_path / "releases").exists()


def test_extract_binaries_from_layers_mixed(tmp_path: Path) -> None:
    """Only matching layers are extracted when mixed with non-matching."""
    d1 = _make_layer_tar(tmp_path, "releases", {"file.bin": "content"})
    d2 = _make_empty_layer_tar(tmp_path)
    _write_manifest(tmp_path, [d1, d2])

    ecfi.extract_binaries_from_layers(tmp_path, "releases")

    assert (tmp_path / "releases" / "file.bin").exists()
    assert not (tmp_path / "unrelated").exists()


# ---------------------------------------------------------------------------
# copy_to_binaries
# ---------------------------------------------------------------------------


def test_copy_to_binaries(tmp_path: Path) -> None:
    """All files from source are copied to destination."""
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    (src / "a.txt").write_text("aaa", encoding="utf-8")
    (src / "b.txt").write_text("bbb", encoding="utf-8")

    ecfi.copy_to_binaries(src, dst)

    assert (dst / "a.txt").read_text(encoding="utf-8") == "aaa"
    assert (dst / "b.txt").read_text(encoding="utf-8") == "bbb"


# ---------------------------------------------------------------------------
# remove_non_checksum_files
# ---------------------------------------------------------------------------


def test_remove_non_checksum_files_keeps_sha256sums(tmp_path: Path) -> None:
    """Files ending with SHA256SUMS are kept."""
    (tmp_path / "linux-amd64-SHA256SUMS").write_text("hash", encoding="utf-8")
    (tmp_path / "SHA256SUMS").write_text("hash2", encoding="utf-8")

    ecfi.remove_non_checksum_files(tmp_path)

    assert (tmp_path / "linux-amd64-SHA256SUMS").exists()
    assert (tmp_path / "SHA256SUMS").exists()


def test_remove_non_checksum_files_removes_binaries(tmp_path: Path) -> None:
    """Non-checksum files are deleted."""
    (tmp_path / "binary.zip").write_text("bin", encoding="utf-8")
    (tmp_path / "app.tar.gz").write_text("tar", encoding="utf-8")
    (tmp_path / "linux-SHA256SUMS").write_text("hash", encoding="utf-8")

    ecfi.remove_non_checksum_files(tmp_path)

    assert not (tmp_path / "binary.zip").exists()
    assert not (tmp_path / "app.tar.gz").exists()
    assert (tmp_path / "linux-SHA256SUMS").exists()


def test_remove_non_checksum_files_empty_dir(tmp_path: Path) -> None:
    """Empty directory is a no-op."""
    ecfi.remove_non_checksum_files(tmp_path)


# ---------------------------------------------------------------------------
# extract_checksums
# ---------------------------------------------------------------------------


def _setup_extract(
    tmp_path: Path,
    components: list[dict],
    data_names: list[str] | None = None,
) -> tuple[Path, Path, Path, str]:
    """Set up directories and files for extract_checksums tests.

    Returns (snapshot_path, data_path, data_dir, snapshot_rel_path).
    """
    data_dir = tmp_path / "workdir"
    uid = "uid123"
    snapshot_rel_path = f"{uid}/snapshot.json"
    snapshot_path = data_dir / snapshot_rel_path
    data_path = data_dir / uid / "data.json"

    _write_snapshot(snapshot_path, components)
    if data_names is not None:
        _write_data(data_path, data_names)

    return snapshot_path, data_path, data_dir, snapshot_rel_path


def test_extract_checksums_single_component(tmp_path: Path) -> None:
    """Single component: image downloaded, checksums extracted, binaries removed."""
    snapshot_path, data_path, data_dir, rel = _setup_extract(
        tmp_path,
        [{"name": "c1", "containerImage": "registry.io/img:v1"}],
    )

    image_staging = tmp_path / "staging"
    image_staging.mkdir()
    copy_fn = _fake_copy_image(
        image_staging,
        "releases",
        {"app.zip": "binary", "app-SHA256SUMS": "deadbeef  app.zip"},
    )

    result = ecfi.extract_checksums(
        snapshot_path, data_path, data_dir, "releases", rel, copy_image=copy_fn
    )

    assert result == "uid123/binaries"
    binaries = data_dir / result
    assert (binaries / "app-SHA256SUMS").exists()
    assert not (binaries / "app.zip").exists()


def test_extract_checksums_multiple_components(tmp_path: Path) -> None:
    """All components are processed when no filtering is applied."""
    components = [
        {"name": f"c{i}", "containerImage": f"registry.io/img{i}:v1"} for i in range(3)
    ]
    snapshot_path, data_path, data_dir, rel = _setup_extract(tmp_path, components)

    call_count = 0

    def _counting_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
        nonlocal call_count
        call_count += 1
        return _stub_copy(source, dest)

    ecfi.extract_checksums(
        snapshot_path,
        data_path,
        data_dir,
        "releases",
        rel,
        copy_image=_counting_copy,
    )

    assert call_count == 3


def test_extract_checksums_component_filtering(tmp_path: Path) -> None:
    """Only desired components from data file are processed."""
    components = [
        {"name": "c1", "containerImage": "registry.io/img1:v1"},
        {"name": "c2", "containerImage": "registry.io/img2:v1"},
        {"name": "c3", "containerImage": "registry.io/img3:v1"},
    ]
    snapshot_path, data_path, data_dir, rel = _setup_extract(
        tmp_path, components, data_names=["c1", "c3"]
    )

    processed: list[str] = []

    def _tracking_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
        processed.append(source)
        return _stub_copy(source, dest)

    ecfi.extract_checksums(
        snapshot_path,
        data_path,
        data_dir,
        "releases",
        rel,
        copy_image=_tracking_copy,
    )

    assert len(processed) == 2
    assert any("img1" in s for s in processed)
    assert any("img3" in s for s in processed)
    assert not any("img2" in s for s in processed)


def test_extract_checksums_empty_image_url_raises(tmp_path: Path) -> None:
    """Component with empty containerImage raises ValueError."""
    snapshot_path, data_path, data_dir, rel = _setup_extract(
        tmp_path,
        [{"name": "c1", "containerImage": ""}],
    )

    with pytest.raises(ValueError, match="Unable to get image url"):
        ecfi.extract_checksums(snapshot_path, data_path, data_dir, "releases", rel)


def test_extract_checksums_missing_image_url_raises(tmp_path: Path) -> None:
    """Component without containerImage key raises ValueError."""
    snapshot_path, data_path, data_dir, rel = _setup_extract(
        tmp_path,
        [{"name": "c1"}],
    )

    with pytest.raises(ValueError, match="Unable to get image url"):
        ecfi.extract_checksums(snapshot_path, data_path, data_dir, "releases", rel)


def test_extract_checksums_null_components(tmp_path: Path) -> None:
    """Snapshot with ``components: null`` is treated as no components."""
    data_dir = tmp_path / "workdir"
    rel = "uid/snapshot.json"
    snapshot_path = data_dir / rel
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps({"components": None}), encoding="utf-8")

    result = ecfi.extract_checksums(
        snapshot_path, data_dir / "data.json", data_dir, "releases", rel
    )

    assert result == "uid/binaries"


def test_extract_checksums_missing_snapshot_raises(tmp_path: Path) -> None:
    """Missing snapshot file raises ValueError."""
    data_dir = tmp_path / "workdir"
    data_dir.mkdir()

    with pytest.raises(ValueError, match="No valid snapshot file"):
        ecfi.extract_checksums(
            data_dir / "nope.json",
            data_dir / "data.json",
            data_dir,
            "releases",
            "uid/nope.json",
        )


def test_extract_checksums_missing_binaries_path_raises(tmp_path: Path) -> None:
    """Image without the expected binaries directory raises ValueError."""
    snapshot_path, data_path, data_dir, rel = _setup_extract(
        tmp_path,
        [{"name": "c1", "containerImage": "registry.io/img:v1"}],
    )

    def _copy_without_binaries(source: str, dest: str) -> subprocess.CompletedProcess[str]:
        dest_path = Path(dest.removeprefix("dir:"))
        digest = _make_empty_layer_tar(dest_path)
        _write_manifest(dest_path, [digest])
        return subprocess.CompletedProcess(
            args=["skopeo", "copy"], returncode=0, stdout="", stderr=""
        )

    with pytest.raises(ValueError, match="does not contain the 'releases' directory"):
        ecfi.extract_checksums(
            snapshot_path,
            data_path,
            data_dir,
            "releases",
            rel,
            copy_image=_copy_without_binaries,
        )


def test_extract_checksums_skopeo_failure_raises(tmp_path: Path) -> None:
    """Non-zero skopeo exit code raises CalledProcessError."""
    snapshot_path, data_path, data_dir, rel = _setup_extract(
        tmp_path,
        [{"name": "c1", "containerImage": "registry.io/img:v1"}],
    )

    def _failing_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["skopeo", "copy"],
            returncode=1,
            stdout="",
            stderr="unauthorized",
        )

    with pytest.raises(subprocess.CalledProcessError):
        ecfi.extract_checksums(
            snapshot_path,
            data_path,
            data_dir,
            "releases",
            rel,
            copy_image=_failing_copy,
        )


def test_extract_checksums_returns_correct_relative_path(
    tmp_path: Path,
) -> None:
    """Returned path is ``{snapshot_parent}/binaries``."""
    snapshot_path, data_path, data_dir, _ = _setup_extract(
        tmp_path,
        [{"name": "c1", "containerImage": "registry.io/img:v1"}],
    )
    rel = "deep/nested/snapshot.json"
    _write_snapshot(data_dir / rel, [{"name": "c1", "containerImage": "r/i:1"}])

    def _noop_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
        return _stub_copy(source, dest)

    result = ecfi.extract_checksums(
        data_dir / rel,
        data_dir / "data.json",
        data_dir,
        "releases",
        rel,
        copy_image=_noop_copy,
    )

    assert result == "deep/nested/binaries"


def test_extract_checksums_temp_dir_cleaned_on_failure(
    tmp_path: Path,
) -> None:
    """Temporary directories are cleaned up even when extraction fails."""
    snapshot_path, data_path, data_dir, rel = _setup_extract(
        tmp_path,
        [{"name": "c1", "containerImage": "registry.io/img:v1"}],
    )

    created_dirs: list[Path] = []
    original_mkdtemp = ecfi.tempfile.mkdtemp

    def _tracking_mkdtemp() -> str:
        d = original_mkdtemp()
        created_dirs.append(Path(d))
        return d

    def _failing_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="fail")

    with mock.patch.object(ecfi.tempfile, "mkdtemp", _tracking_mkdtemp):
        with pytest.raises(subprocess.CalledProcessError):
            ecfi.extract_checksums(
                snapshot_path,
                data_path,
                data_dir,
                "releases",
                rel,
                copy_image=_failing_copy,
            )

    assert created_dirs
    for d in created_dirs:
        assert not d.exists()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_writes_result(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Successful run writes relative binaries path to result file."""
    result_file = tmp_path / "result"
    monkeypatch.setenv("RESULT_BINARIES_PATH", str(result_file))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "workdir"))
    monkeypatch.setenv("SNAPSHOT_PATH", "uid/snapshot.json")
    monkeypatch.setenv("DATA_PATH", "")

    with mock.patch.object(ecfi, "extract_checksums", return_value="uid/binaries"):
        rc = ecfi.main()

    assert rc == 0
    assert result_file.read_text(encoding="utf-8") == "uid/binaries"


def test_main_missing_result_env_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing RESULT_BINARIES_PATH causes SystemExit."""
    monkeypatch.delenv("RESULT_BINARIES_PATH", raising=False)
    with pytest.raises(SystemExit):
        ecfi.main()


def test_main_missing_data_dir_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Missing DATA_DIR causes SystemExit."""
    monkeypatch.setenv("RESULT_BINARIES_PATH", str(tmp_path / "r"))
    monkeypatch.delenv("DATA_DIR", raising=False)
    with pytest.raises(SystemExit):
        ecfi.main()


def test_main_extract_error_exits_with_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exceptions from extract_checksums become SystemExit with PROG prefix."""
    monkeypatch.setenv("RESULT_BINARIES_PATH", str(tmp_path / "r"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SNAPSHOT_PATH", "uid/snapshot.json")

    with mock.patch.object(ecfi, "extract_checksums", side_effect=ValueError("boom")):
        with pytest.raises(SystemExit, match="extract_checksums_from_image.py: boom"):
            ecfi.main()
