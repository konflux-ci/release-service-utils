"""Tests for collect_registry_token_secret."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from release_service_utils.tasks.managed.collect_registry_token_secret import (
    collect_registry_token_secret,
    is_secret_required,
    main,
    run,
)


def test_is_secret_required_when_defaults_public() -> None:
    """defaults.public=true requires a registry secret, even with no components."""
    data = {"mapping": {"defaults": {"public": True}, "components": []}}
    assert is_secret_required(data) is True


def test_is_secret_required_default_public_short_circuits_component_overrides() -> None:
    """A true mapping default requires a secret regardless of component overrides."""
    data = {
        "mapping": {
            "defaults": {"public": True},
            "components": [{"name": "a", "public": False}],
        }
    }
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


def test_is_secret_required_when_component_public_string() -> None:
    """Component with public='true' (string) also requires a registry secret."""
    data = {
        "mapping": {
            "defaults": {},
            "components": [{"name": "a", "public": "true"}],
        }
    }
    assert is_secret_required(data) is True


def test_is_secret_required_skips_non_dict_components() -> None:
    """Non-dict entries in the components list are skipped gracefully."""
    data = {
        "mapping": {
            "defaults": {},
            "components": ["not-a-dict", None, {"name": "a"}],
        }
    }
    assert is_secret_required(data) is False


def test_is_secret_required_with_empty_mapping() -> None:
    """Empty mapping dict returns False."""
    assert is_secret_required({"mapping": {}}) is False


def test_is_secret_required_with_null_mapping() -> None:
    """Missing mapping key returns False."""
    assert is_secret_required({}) is False


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
    """Raise KeyError when public=true but mapping.registrySecret is absent."""
    data = {
        "mapping": {
            "defaults": {"public": True},
            "components": [],
        }
    }
    with pytest.raises(KeyError):
        collect_registry_token_secret(data)


def test_collect_strips_secret_whitespace() -> None:
    """Leading and trailing whitespace is stripped from the secret name."""
    data = {
        "mapping": {
            "defaults": {},
            "components": [{"name": "c", "public": True}],
            "registrySecret": "  mysecret  ",
        }
    }
    assert collect_registry_token_secret(data) == "mysecret"


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
    """Propagate KeyError when registrySecret is missing but required."""
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

    with pytest.raises(KeyError):
        main()

    assert not result_file.exists()
