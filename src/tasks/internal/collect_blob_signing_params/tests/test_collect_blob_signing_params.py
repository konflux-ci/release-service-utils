"""Tests for collect_blob_signing_params task."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from release_service_utils.helpers import kubectl
from release_service_utils.tasks.internal import collect_blob_signing_params

TASK = "release_service_utils.tasks.internal.collect_blob_signing_params"


def _full_configmap_data() -> dict[str, str]:
    """Return a complete set of blob-signing ConfigMap data keys."""
    return {
        "PYXIS_URL": "https://pyxis.stage.engineering.redhat.com",
        "SIG_KEY_ID": "4096R/37036783 SHA-256",
        "SIG_KEY_NAME": "redhate2etesting",
        "PYXIS_SSL_CERT_SECRET_NAME": "hacbs-signing-pipeline-certs",
        "UMB_CLIENT_NAME": "hacbs-signing-pipeline-nonprod",
        "UMB_LISTEN_TOPIC": "VirtualTopic.eng.robosignatory.hacbs.sign",
        "UMB_PUBLISH_TOPIC": "VirtualTopic.eng.hacbs-signing-pipeline.hacbs.sign",
        "UMB_URL": "umb.stage.api.redhat.com",
        "UMB_SSL_CERT_SECRET_NAME": "hacbs-signing-pipeline-certs",
        "PYXIS_SSL_CERT_FILE_NAME": "hacbs-signing-pipeline.pem",
        "PYXIS_SSL_KEY_FILE_NAME": "hacbs-signing-pipeline.key",
        "UMB_SSL_CERT_FILE_NAME": "hacbs-signing-pipeline.pem",
        "UMB_SSL_KEY_FILE_NAME": "hacbs-signing-pipeline.key",
    }


def _mock_configmap(data: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a mock ConfigMap object with the given data."""
    if data is None:
        data = _full_configmap_data()
    return {"data": data}


def _make_result_paths(tmp_path: Path) -> dict[str, Path]:
    """Create a dictionary mapping result keys to file paths."""
    return {key: tmp_path / key for key in collect_blob_signing_params.RESULT_KEYS}


def _set_result_env_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Set environment variables for all result paths."""
    for key in collect_blob_signing_params.RESULT_KEYS:
        monkeypatch.setenv(f"RESULT_{key.upper()}", str(tmp_path / key))


def test_extract_blob_signing_params_full_configmap() -> None:
    """Extract all required parameters from a complete ConfigMap."""
    params = collect_blob_signing_params.extract_blob_signing_params(_mock_configmap())

    assert params["pyxis_url"] == "https://pyxis.stage.engineering.redhat.com"
    assert params["sig_key_id"] == "4096R/37036783 SHA-256"
    assert params["sig_key_name"] == "redhate2etesting"
    assert params["pyxis_ssl_cert_secret_name"] == "hacbs-signing-pipeline-certs"
    assert params["umb_client_name"] == "hacbs-signing-pipeline-nonprod"
    assert params["umb_listen_topic"] == "VirtualTopic.eng.robosignatory.hacbs.sign"
    assert params["umb_publish_topic"] == "VirtualTopic.eng.hacbs-signing-pipeline.hacbs.sign"
    assert params["umb_url"] == "umb.stage.api.redhat.com"
    assert params["umb_ssl_cert_secret_name"] == "hacbs-signing-pipeline-certs"


def test_extract_blob_signing_params_ignores_unused_keys() -> None:
    """Extra ConfigMap keys that the task does not consume are ignored."""
    params = collect_blob_signing_params.extract_blob_signing_params(_mock_configmap())
    assert "PYXIS_SSL_CERT_FILE_NAME" not in params
    assert set(params) == set(collect_blob_signing_params.RESULT_KEYS)


def test_extract_blob_signing_params_missing_data_key() -> None:
    """A ConfigMap without a data key raises KeyError."""
    with pytest.raises(KeyError, match="data"):
        collect_blob_signing_params.extract_blob_signing_params({})


def test_extract_blob_signing_params_data_not_mapping() -> None:
    """A ConfigMap whose data is not a mapping raises TypeError."""
    with pytest.raises(TypeError):
        collect_blob_signing_params.extract_blob_signing_params({"data": "not-a-dict"})


def test_extract_blob_signing_params_missing_key() -> None:
    """A missing required ConfigMap key raises KeyError."""
    data = _full_configmap_data()
    del data["SIG_KEY_NAME"]
    with pytest.raises(KeyError, match="SIG_KEY_NAME"):
        collect_blob_signing_params.extract_blob_signing_params(_mock_configmap(data))


def test_extract_blob_signing_params_empty_key() -> None:
    """An empty required ConfigMap key raises ValueError."""
    data = _full_configmap_data()
    data["UMB_URL"] = ""
    with pytest.raises(ValueError, match="UMB_URL"):
        collect_blob_signing_params.extract_blob_signing_params(_mock_configmap(data))


def test_extract_blob_signing_params_whitespace_only_key() -> None:
    """A whitespace-only required ConfigMap key raises ValueError."""
    data = _full_configmap_data()
    data["PYXIS_URL"] = "   "
    with pytest.raises(ValueError, match="PYXIS_URL"):
        collect_blob_signing_params.extract_blob_signing_params(_mock_configmap(data))


def test_extract_blob_signing_params_strips_whitespace() -> None:
    """Surrounding whitespace is stripped from ConfigMap values."""
    data = _full_configmap_data()
    data["SIG_KEY_NAME"] = "  redhate2etesting  "
    params = collect_blob_signing_params.extract_blob_signing_params(_mock_configmap(data))
    assert params["sig_key_name"] == "redhate2etesting"


def test_collect_blob_signing_params_with_configmap(tmp_path: Path) -> None:
    """Collect blob-signing params when ConfigMap is available."""
    result_paths = _make_result_paths(tmp_path)
    with patch(
        f"{TASK}.collect_blob_signing_params.kubectl.get_configmap",
        return_value=_mock_configmap(),
    ) as mock_get:
        params = collect_blob_signing_params.collect_blob_signing_params(
            config_map_name="hacbs-signing-pipeline-config-example",
            result_paths=result_paths,
        )

    mock_get.assert_called_once_with("hacbs-signing-pipeline-config-example")
    assert params["sig_key_name"] == "redhate2etesting"
    for key in collect_blob_signing_params.RESULT_KEYS:
        assert (tmp_path / key).exists()
    assert (tmp_path / "sig_key_name").read_text() == "redhate2etesting"
    assert (tmp_path / "pyxis_url").read_text() == "https://pyxis.stage.engineering.redhat.com"


def test_collect_blob_signing_params_missing_configmap(tmp_path: Path) -> None:
    """A missing ConfigMap is not treated as empty params."""
    result_paths = _make_result_paths(tmp_path)
    with patch(
        f"{TASK}.collect_blob_signing_params.kubectl.get_configmap",
        side_effect=kubectl.ConfigMapNotFoundError("ConfigMap not found"),
    ):
        with pytest.raises(kubectl.ConfigMapNotFoundError, match="ConfigMap not found"):
            collect_blob_signing_params.collect_blob_signing_params(
                config_map_name="missing-cm",
                result_paths=result_paths,
            )

    assert not (tmp_path / "sig_key_name").exists()


def test_collect_blob_signing_params_other_kubectl_failure(tmp_path: Path) -> None:
    """A non-NotFound kubectl failure propagates."""
    result_paths = _make_result_paths(tmp_path)
    with patch(
        f"{TASK}.collect_blob_signing_params.kubectl.get_configmap",
        side_effect=RuntimeError("Unable to connect to the server: dial tcp: timeout"),
    ):
        with pytest.raises(RuntimeError, match="Unable to connect"):
            collect_blob_signing_params.collect_blob_signing_params(
                config_map_name="hacbs-signing-pipeline-config-example",
                result_paths=result_paths,
            )


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Main returns 0 on success."""
    _set_result_env_vars(tmp_path, monkeypatch)
    monkeypatch.setenv("PARAM_CONFIG_MAP_NAME", "hacbs-signing-pipeline-config-example")
    with patch(
        f"{TASK}.collect_blob_signing_params.kubectl.get_configmap",
        return_value=_mock_configmap(),
    ) as mock_get:
        result = collect_blob_signing_params.main()

    assert result == 0
    mock_get.assert_called_once_with("hacbs-signing-pipeline-config-example")
    assert (tmp_path / "sig_key_name").read_text() == "redhate2etesting"
    assert (tmp_path / "umb_url").read_text() == "umb.stage.api.redhat.com"


def test_main_missing_param_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing PARAM_CONFIG_MAP_NAME fails fast via tekton.require_env."""
    monkeypatch.delenv("PARAM_CONFIG_MAP_NAME", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        collect_blob_signing_params.main()
    assert exc_info.value.code == 1


def test_main_missing_result_env_vars_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing RESULT_* env vars fail fast via tekton.result_paths_from_env."""
    monkeypatch.setenv("PARAM_CONFIG_MAP_NAME", "hacbs-signing-pipeline-config-example")
    with pytest.raises(SystemExit) as exc_info:
        collect_blob_signing_params.main()
    assert exc_info.value.code == 1


def test_package_main_module() -> None:
    """Importing the package ``__main__`` exposes ``main``."""
    mod = importlib.import_module(f"{TASK}.__main__")
    assert mod.main is collect_blob_signing_params.main
