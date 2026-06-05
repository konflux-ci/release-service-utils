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


def test_default_push_source_container_defaults_true() -> None:
    """Default pushSourceContainer to true when mapping defaults are absent."""
    assert snapshot.default_push_source_container({}) is True


def test_default_push_source_container_honors_false() -> None:
    """Respect mapping.defaults.pushSourceContainer when explicitly false."""
    data = {"mapping": {"defaults": {"pushSourceContainer": False}}}
    assert snapshot.default_push_source_container(data) is False


def test_default_push_source_container_null_value_defaults_true() -> None:
    """Default pushSourceContainer to true when the key is absent in defaults."""
    assert snapshot.default_push_source_container(
        {"mapping": {"defaults": {}}},
    )


def test_default_push_source_container_without_defaults_mapping() -> None:
    """Default pushSourceContainer to true when mapping.defaults is missing."""
    assert snapshot.default_push_source_container(
        {"mapping": {"defaults": "not-a-mapping"}},
    )


def test_component_push_source_container_true_and_default_paths() -> None:
    """Enable source container when component or mapping default says so."""
    assert snapshot.component_push_source_container(
        {"pushSourceContainer": True},
        False,
    )
    assert snapshot.component_push_source_container({}, True)


def test_component_push_source_container_explicit_false() -> None:
    """Do not enable source container when component sets false."""
    assert not snapshot.component_push_source_container(
        {"pushSourceContainer": False},
        True,
    )
