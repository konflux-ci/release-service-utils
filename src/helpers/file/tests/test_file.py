"""Tests for the `file` helper module."""

from __future__ import annotations

import base64
import gzip
import io
import json
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

from release_service_utils.helpers import file


def test_path_from_env_variable_uses_set_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-empty env value (after trim) is returned as a `Path`."""
    p = tmp_path / "m"
    monkeypatch.setenv("MOUNT", str(p))
    assert file.path_from_env_variable("MOUNT", "/d/e/f") == p


def test_path_from_env_variable_strips_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Surrounding whitespace on the env value is removed before path construction."""
    p = tmp_path / "m"
    monkeypatch.setenv("MOUNT", f"  {p}  ")
    assert file.path_from_env_variable("MOUNT", "/d") == p


def test_path_from_env_variable_uses_default_when_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unset or all-whitespace *name* yields *default* (`Path` or str)."""
    default = str(tmp_path / "default")
    monkeypatch.delenv("MOUNT", raising=False)
    assert file.path_from_env_variable("MOUNT", default) == tmp_path / "default"
    monkeypatch.setenv("MOUNT", "   ")
    assert file.path_from_env_variable("MOUNT", default) == tmp_path / "default"


def test_path_from_env_variable_path_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """*default* may be a `Path` object, returned unchanged when the env is unset."""
    d = tmp_path / "d"
    monkeypatch.delenv("MOUNTX", raising=False)
    assert file.path_from_env_variable("MOUNTX", d) == d


def test_load_json_dict(tmp_path: Path) -> None:
    """A JSON object file is parsed and returned as a dict."""
    path = tmp_path / "data.json"
    path.write_text(json.dumps({"a": 1}), encoding="utf-8")
    assert file.load_json_dict(path) == {"a": 1}


def test_load_json_dict_rejects_non_object(tmp_path: Path) -> None:
    """A JSON array (non-object root) raises `TypeError`."""
    path = tmp_path / "data.json"
    path.write_text("[1]", encoding="utf-8")
    with pytest.raises(TypeError, match="object"):
        file.load_json_dict(path)


def test_resolve_path_under_base_relative_file(tmp_path: Path) -> None:
    """A normal relative path resolves under *base*."""
    target = tmp_path / "uid" / "charon.env"
    target.parent.mkdir(parents=True)
    assert file.resolve_path_under_base(tmp_path, "uid/charon.env") == target.resolve()


def test_resolve_path_under_base_rejects_absolute(tmp_path: Path) -> None:
    """Absolute paths are rejected even if they exist."""
    with pytest.raises(ValueError, match="must be relative"):
        file.resolve_path_under_base(tmp_path, "/etc/passwd")


def test_resolve_path_under_base_rejects_traversal(tmp_path: Path) -> None:
    """``..`` segments that escape *base* are rejected."""
    with pytest.raises(ValueError, match="must stay under"):
        file.resolve_path_under_base(tmp_path, "../outside")


def test_resolve_path_under_base_rejects_blank(tmp_path: Path) -> None:
    """Blank relative paths are rejected."""
    with pytest.raises(ValueError, match="must be relative"):
        file.resolve_path_under_base(tmp_path, "   ")


def test_contained_regular_files_returns_nested_matches(tmp_path: Path) -> None:
    """Return regular files under *root* that match *pattern*."""
    nested = tmp_path / "nested"
    nested.mkdir()
    wheel = nested / "pkg.whl"
    other = tmp_path / "notes.txt"
    wheel.write_bytes(b"wheel")
    other.write_text("skip", encoding="utf-8")
    (tmp_path / "dir.whl").mkdir()

    assert file.contained_regular_files(tmp_path, "*.whl") == [wheel]


def test_contained_regular_files_rejects_outside_symlink(tmp_path: Path) -> None:
    """Reject a symbolic link whose target is outside *root*."""
    outside = tmp_path / "outside" / "secret.whl"
    source = tmp_path / "source"
    source.mkdir()
    outside.parent.mkdir()
    outside.write_bytes(b"secret")
    (source / "pkg.whl").symlink_to(outside)

    with pytest.raises(ValueError, match="source path must stay under"):
        file.contained_regular_files(source, "*.whl")


def test_contained_regular_files_rejects_in_tree_symlink(tmp_path: Path) -> None:
    """Reject a symbolic link even when its target stays under *root*."""
    target = tmp_path / "real.whl"
    target.write_bytes(b"wheel")
    (tmp_path / "alias.whl").symlink_to(target)

    with pytest.raises(ValueError, match="source path must stay under"):
        file.contained_regular_files(tmp_path, "*.whl")


def _write_dir(path: Path, name: str, text: str) -> Path:
    """Create *path* with one file and return that file."""
    path.mkdir()
    child = path / name
    child.write_text(text, encoding="utf-8")
    return child


def test_swap_directory_returns_backup_and_leaves_source_at_dest(tmp_path: Path) -> None:
    """Move *dest* aside and leave the caller owning that backup."""
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    _write_dir(source, "new.txt", "new")
    _write_dir(dest, "old.txt", "old")

    backup = file.swap_directory(source, dest)

    assert backup is not None
    assert (dest / "new.txt").read_text(encoding="utf-8") == "new"
    assert (backup / "old.txt").read_text(encoding="utf-8") == "old"
    assert not source.exists()
    shutil.rmtree(backup)


def test_swap_directory_returns_none_when_dest_is_missing(tmp_path: Path) -> None:
    """Replace a missing *dest* without creating a backup."""
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    _write_dir(source, "new.txt", "new")

    assert file.swap_directory(source, dest) is None
    assert (dest / "new.txt").read_text(encoding="utf-8") == "new"


def test_swap_directory_rejects_symlink_or_file_dest(tmp_path: Path) -> None:
    """Reject a destination that is not a real directory."""
    source = tmp_path / "source"
    _write_dir(source, "new.txt", "new")
    linked = tmp_path / "linked"
    linked.symlink_to(tmp_path / "other")
    regular = tmp_path / "regular"
    regular.write_text("file", encoding="utf-8")

    with pytest.raises(ValueError, match="destination must be a directory"):
        file.swap_directory(source, linked)
    with pytest.raises(ValueError, match="destination must be a directory"):
        file.swap_directory(source, regular)


def test_swap_directory_restores_dest_when_source_rename_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Put the previous *dest* back when moving *source* into place fails."""
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    _write_dir(source, "new.txt", "new")
    old = _write_dir(dest, "old.txt", "old")
    real_rename = Path.rename

    def wrapped(self: Path, target: str | Path) -> Path:
        if Path(self) == source and Path(target) == dest:
            raise OSError("swap failed")
        return real_rename(self, target)

    monkeypatch.setattr(Path, "rename", wrapped)
    with pytest.raises(OSError, match="swap failed"):
        file.swap_directory(source, dest)

    assert dest.is_dir()
    assert old.read_text(encoding="utf-8") == "old"
    assert source.is_dir()
    assert list(tmp_path.glob(".dest-outgoing-*")) == []


def test_replace_directory_deletes_previous_dest(tmp_path: Path) -> None:
    """Replace *dest* and remove the backup after a successful swap."""
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    _write_dir(source, "new.txt", "new")
    _write_dir(dest, "old.txt", "old")

    file.replace_directory(source, dest)

    assert (dest / "new.txt").read_text(encoding="utf-8") == "new"
    assert not source.exists()
    assert list(tmp_path.glob(".dest-outgoing-*")) == []


def test_restore_directory_puts_backup_back(tmp_path: Path) -> None:
    """Remove the published *dest* and restore the caller-owned backup."""
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    _write_dir(source, "new.txt", "new")
    _write_dir(dest, "old.txt", "old")
    backup = file.swap_directory(source, dest)

    file.restore_directory(backup, dest)

    assert (dest / "old.txt").read_text(encoding="utf-8") == "old"
    assert backup is None or not backup.exists()


def test_restore_directory_removes_dest_when_backup_is_missing(tmp_path: Path) -> None:
    """Delete a newly published *dest* when there was no previous directory."""
    dest = tmp_path / "dest"
    _write_dir(dest, "new.txt", "new")

    file.restore_directory(None, dest)

    assert not dest.exists()


def test_make_tempfile_path_empty_file() -> None:
    """A `None` payload leaves the created file with zero length."""
    p = file.make_tempfile_path("t-", None)
    try:
        assert p.read_bytes() == b""
    finally:
        p.unlink(missing_ok=True)


def test_make_tempfile_path_with_bytes() -> None:
    """If `data` is set, the file on disk has exactly those bytes."""
    p = file.make_tempfile_path("t-", b"hello")
    try:
        assert p.read_bytes() == b"hello"
    finally:
        p.unlink(missing_ok=True)


def test_encode_json_gzip_b64_roundtrip() -> None:
    """Compact JSON survives gzip+base64 encoding."""
    payload = [{"repository": "foo", "cves": {"fixed": {"CVE-1": {}}}}]
    encoded = file.encode_json_gzip_b64(payload)
    decoded = json.loads(gzip.decompress(base64.standard_b64decode(encoded)))
    assert decoded == payload


def test_encode_json_gzip_b64_uses_compact_separators() -> None:
    """Encoded JSON does not include spaces after separators."""
    encoded = file.encode_json_gzip_b64({"a": 1, "b": [2]})
    raw = gzip.decompress(base64.standard_b64decode(encoded)).decode("utf-8")
    assert raw == '{"a":1,"b":[2]}'


def test_read_bounded_returns_bytes() -> None:
    """Read a stream that stays within *max_bytes*."""
    handle = io.BytesIO(b"hello")
    assert file.read_bounded(handle, max_bytes=16) == b"hello"


def test_read_bounded_rejects_oversized_output() -> None:
    """Stop reading once output exceeds *max_bytes*."""
    handle = io.BytesIO(b"x" * 50)
    with pytest.raises(ValueError, match="read data exceeds 10 bytes"):
        file.read_bounded(handle, max_bytes=10)


def test_decompress_gzip_bounded_roundtrip() -> None:
    """Valid gzip input decompresses to the original bytes."""
    raw = b'[{"repository": "foo"}]'
    compressed = gzip.compress(raw)
    assert file.decompress_gzip_bounded(compressed, max_bytes=1024) == raw


def test_decompress_gzip_bounded_rejects_oversized_output() -> None:
    """Decompression stops once output exceeds *max_bytes*."""
    raw = b"x" * 5000
    compressed = gzip.compress(raw)
    with pytest.raises(ValueError, match="gzip bomb"):
        file.decompress_gzip_bounded(compressed, max_bytes=1000)


def test_is_gzip_or_tar_archive_posix_tar() -> None:
    """POSIX tar archives are recognized."""

    def fake_file_cmd(
        cmd: Sequence[str | Path],
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            [str(x) for x in cmd],
            0,
            stdout="POSIX tar archive\n",
            stderr="",
        )

    assert file.is_gzip_or_tar_archive(Path("/tmp/archive.tar"), file_cmd=fake_file_cmd)


def test_is_gzip_or_tar_archive_rejects_other_types() -> None:
    """Non-archive ``file -b`` output returns False."""

    def fake_file_cmd(
        cmd: Sequence[str | Path],
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            [str(x) for x in cmd],
            0,
            stdout="ASCII text\n",
            stderr="",
        )

    assert not file.is_gzip_or_tar_archive(Path("/tmp/readme.txt"), file_cmd=fake_file_cmd)
