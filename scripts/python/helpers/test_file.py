"""Tests for the `file` helper module."""

from __future__ import annotations

import json
import gzip
from pathlib import Path

import file
import pytest


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
