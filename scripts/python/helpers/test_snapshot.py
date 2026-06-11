"""Tests for `snapshot` helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import snapshot


def test_first_component_missing_components(tmp_path: Path) -> None:
    """Reject snapshots with no `components` list."""
    path = tmp_path / "snapshot.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="no components"):
        snapshot.first_component(path)


def test_first_component_invalid_component(tmp_path: Path) -> None:
    """Reject when the first component is not a JSON object."""
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"components": ["bad"]}), encoding="utf-8")
    with pytest.raises(TypeError, match="component\\[0\\] must be an object"):
        snapshot.first_component(path)


def test_first_component_invalid_source(tmp_path: Path) -> None:
    """Reject when `component[0].source` is not a JSON object."""
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"components": [{}]}), encoding="utf-8")
    with pytest.raises(TypeError, match="source must be an object"):
        snapshot.first_component(path)


def test_first_component_invalid_git(tmp_path: Path) -> None:
    """Reject when `component[0].source.git` is not a JSON object."""
    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps({"components": [{"source": {"git": "bad"}}]}),
        encoding="utf-8",
    )
    with pytest.raises(TypeError, match="source.git must be an object"):
        snapshot.first_component(path)


def test_first_component(tmp_path: Path) -> None:
    """Return revision, origin repo URL, and container image from the first component."""
    snap = {
        "components": [
            {
                "containerImage": "quay.io/org/img@sha256:abc",
                "source": {
                    "git": {
                        "revision": "deadbeef" * 5,
                        "url": "https://github.com/org/repo.git",
                    }
                },
            }
        ]
    }
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snap), encoding="utf-8")
    out = snapshot.first_component(path)
    assert out["revision"] == "deadbeef" * 5
    assert out["origin_repo"] == "https://github.com/org/repo"
    assert "quay.io" in out["container_image"]
