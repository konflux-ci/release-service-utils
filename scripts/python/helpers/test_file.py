"""Tests for the ``file`` helper module."""

from __future__ import annotations

from pathlib import Path

import file
import pytest


def test_path_from_env_variable_uses_set_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-empty env value (after trim) is returned as a ``Path``."""
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
    """Unset or all-whitespace *name* yields *default* (``Path`` or str)."""
    default = str(tmp_path / "default")
    monkeypatch.delenv("MOUNT", raising=False)
    assert file.path_from_env_variable("MOUNT", default) == tmp_path / "default"
    monkeypatch.setenv("MOUNT", "   ")
    assert file.path_from_env_variable("MOUNT", default) == tmp_path / "default"


def test_path_from_env_variable_path_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """*default* may be a ``Path`` object, returned unchanged when the env is unset."""
    d = tmp_path / "d"
    monkeypatch.delenv("MOUNTX", raising=False)
    assert file.path_from_env_variable("MOUNTX", d) == d


def test_make_tempfile_path_empty_file() -> None:
    """A ``None`` payload leaves the created file with zero length."""
    p = file.make_tempfile_path("t-", None)
    try:
        assert p.read_bytes() == b""
    finally:
        p.unlink(missing_ok=True)


def test_make_tempfile_path_with_bytes() -> None:
    """If ``data`` is set, the file on disk has exactly those bytes."""
    p = file.make_tempfile_path("t-", b"hello")
    try:
        assert p.read_bytes() == b"hello"
    finally:
        p.unlink(missing_ok=True)
