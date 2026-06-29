"""Tests for collect_registry_token_secret."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from collect_registry_token_secret import (
    collect_registry_token_secret,
    is_secret_required,
    main,
    run,
)


def test_is_secret_required_when_defaults_public() -> None:
    """defaults.public=true requires a registry secret."""
    data = {"mapping": {"defaults": {"public": True}, "components": []}}
    assert is_secret_required(data) is True


def test_is_secret_required_when_component_public() -> None:
    """Any component with public=true requires a registry secret."""
    data = {
        "mapping": {
            "defaults": {},
            "components": [{"name": "a"}, {"name": "b", "public": True}],
        }
    }
    assert is_secret_required(data) is True


def test_is_secret_required_false_when_nothing_public() -> None:
    """No secret is required when nothing is marked public."""
    data = {
        "mapping": {
            "defaults": {},
            "components": [{"name": "a"}, {"name": "b", "public": False}],
        }
    }
    assert is_secret_required(data) is False


def test_collect_returns_secret_when_required() -> None:
    """Return mapping.registrySecret when public repos need a token."""
    data = {
        "mapping": {
            "defaults": {},
            "components": [{"name": "c", "public": True}],
            "registrySecret": "mysecret",
        }
    }
    assert collect_registry_token_secret(data) == "mysecret"


def test_collect_returns_empty_when_not_required() -> None:
    """Return an empty string when no repos are public."""
    data = {
        "mapping": {
            "defaults": {},
            "components": [{"name": "c"}],
            "registrySecret": "mysecret",
        }
    }
    assert collect_registry_token_secret(data) == ""


def test_collect_raises_when_secret_missing() -> None:
    """Raise when public=true but mapping.registrySecret is absent."""
    data = {
        "mapping": {
            "defaults": {"public": True},
            "components": [],
        }
    }
    with pytest.raises(ValueError, match="Registry secret missing"):
        collect_registry_token_secret(data)


def test_run_raises_when_data_file_missing(tmp_path: Path) -> None:
    """Raise FileNotFoundError when the data file does not exist."""
    with pytest.raises(FileNotFoundError, match="No valid data file"):
        run(tmp_path / "missing.json")


def test_run_reads_file(tmp_path: Path) -> None:
    """Load JSON from disk and return the registry secret name."""
    data_file = tmp_path / "data.json"
    data_file.write_text(
        json.dumps(
            {
                "mapping": {
                    "components": [{"name": "c", "public": True}],
                    "defaults": {},
                    "registrySecret": "token-secret",
                }
            }
        ),
        encoding="utf-8",
    )
    assert run(data_file) == "token-secret"


def test_main_writes_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write the secret name to the Tekton result file."""
    data_dir = tmp_path / "release"
    data_dir.mkdir()
    (data_dir / "data.json").write_text(
        json.dumps(
            {
                "mapping": {
                    "components": [{"name": "c", "public": True}],
                    "defaults": {},
                    "registrySecret": "mysecret",
                }
            }
        ),
        encoding="utf-8",
    )
    result_file = tmp_path / "registrySecret"
    monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("RESULT_REGISTRY_SECRET", str(result_file))

    assert main() == 0
    assert result_file.read_text(encoding="utf-8") == "mysecret"


def test_main_writes_empty_when_not_required(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Write an empty result when no secret is required."""
    data_dir = tmp_path / "release"
    data_dir.mkdir()
    (data_dir / "data.json").write_text(
        json.dumps({"mapping": {"components": [{"name": "c"}], "defaults": {}}}),
        encoding="utf-8",
    )
    result_file = tmp_path / "registrySecret"
    monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("RESULT_REGISTRY_SECRET", str(result_file))

    assert main() == 0
    assert result_file.read_text(encoding="utf-8") == ""


def test_main_raises_when_secret_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Propagate ValueError when registrySecret is missing but required."""
    data_dir = tmp_path / "release"
    data_dir.mkdir()
    (data_dir / "data.json").write_text(
        json.dumps(
            {
                "mapping": {
                    "components": [],
                    "defaults": {"public": True},
                }
            }
        ),
        encoding="utf-8",
    )
    result_file = tmp_path / "registrySecret"
    monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("RESULT_REGISTRY_SECRET", str(result_file))

    with pytest.raises(ValueError, match="Registry secret missing"):
        main()

    assert not result_file.exists()
