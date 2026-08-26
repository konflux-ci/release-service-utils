"""Tests for validate_single_component."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import validate_single_component


def _write_snapshot(path: Path, components: Any) -> None:
    """Write a minimal snapshot JSON file with the given components value."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"application": "app", "components": components}) + "\n",
        encoding="utf-8",
    )


def test_run_allows_zero_components(tmp_path: Path) -> None:
    """Zero components is allowed (matches the original length check)."""
    snapshot_path = Path("snapshot.json")
    _write_snapshot(tmp_path / snapshot_path, [])
    validate_single_component.run(data_dir=tmp_path, snapshot_path=snapshot_path)


def test_run_allows_missing_components_key(tmp_path: Path) -> None:
    """A missing components key is treated as an empty list."""
    snapshot_path = Path("snapshot.json")
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / snapshot_path).write_text('{"application": "app"}\n', encoding="utf-8")
    validate_single_component.run(data_dir=tmp_path, snapshot_path=snapshot_path)


def test_run_allows_one_component(tmp_path: Path) -> None:
    """A single component passes validation."""
    snapshot_path = Path("snapshot.json")
    _write_snapshot(tmp_path / snapshot_path, [{"name": "comp"}])
    validate_single_component.run(data_dir=tmp_path, snapshot_path=snapshot_path)


def test_run_rejects_non_list_components(tmp_path: Path) -> None:
    """Reject snapshots whose components value is not a JSON array."""
    snapshot_path = Path("snapshot.json")
    _write_snapshot(tmp_path / snapshot_path, {"name": "x"})
    with pytest.raises(TypeError, match="must be a JSON array"):
        validate_single_component.run(data_dir=tmp_path, snapshot_path=snapshot_path)


def test_run_rejects_multiple_components(tmp_path: Path) -> None:
    """More than one component fails with the catalog error message."""
    snapshot_path = Path("snapshot.json")
    _write_snapshot(tmp_path / snapshot_path, [{"name": "a"}, {"name": "b"}])
    with pytest.raises(
        ValueError,
        match="found 2 components, only one component per application is supported",
    ):
        validate_single_component.run(data_dir=tmp_path, snapshot_path=snapshot_path)


def test_main_reads_env_and_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() wires PARAM_* env vars into run()."""
    snapshot_path = Path("snapshot.json")
    _write_snapshot(tmp_path / snapshot_path, [{"name": "comp"}])
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", str(snapshot_path))
    assert validate_single_component.main() == 0


def test_main_propagates_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() raises validation failures."""
    snapshot_path = Path("snapshot.json")
    _write_snapshot(tmp_path / snapshot_path, [{"name": "a"}, {"name": "b"}])
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", str(snapshot_path))
    with pytest.raises(ValueError, match="found 2 components"):
        validate_single_component.main()
