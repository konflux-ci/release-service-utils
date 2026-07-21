"""Tests for collect_signing_params task."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import collect_signing_params


def _mock_configmap(data: dict[str, str]) -> dict[str, Any]:
    """Return a mock ConfigMap object with the given data."""
    return {"data": data}


def _full_signing_params() -> dict[str, str]:
    """Return a complete set of signing parameters as they appear in ConfigMap."""
    return {
        "enableKeylessSigning": "true",
        "defaultOIDCIssuer": "https://oidc.example.com",
        "rekorExternalUrl": "https://rekor-external.example.com",
        "rekorInternalUrl": "https://rekor-internal.example.com",
        "fulcioExternalUrl": "https://fulcio-external.example.com",
        "fulcioInternalUrl": "https://fulcio-internal.example.com",
        "tufExternalUrl": "https://tuf-external.example.com",
        "tufInternalUrl": "https://tuf-internal.example.com",
        "buildIdentityRegexp": "^https://kubernetes.io/.*",
        "tektonChainsIdentity": "URI:https://kubernetes.io/test/serviceaccounts/default",
    }


def test_get_empty_signing_params_returns_false_for_keyless() -> None:
    """Empty params have enableKeylessSigning set to 'false'."""
    params = collect_signing_params.get_empty_signing_params()
    assert params["enableKeylessSigning"] == "false"


def test_get_empty_signing_params_all_keys_present() -> None:
    """All result keys are present in empty params."""
    params = collect_signing_params.get_empty_signing_params()
    for key in collect_signing_params.RESULT_KEYS:
        assert key in params


def test_get_empty_signing_params_values_empty_except_keyless() -> None:
    """All values except enableKeylessSigning are empty strings."""
    params = collect_signing_params.get_empty_signing_params()
    for key, value in params.items():
        if key == "enableKeylessSigning":
            assert value == "false"
        else:
            assert value == ""


def test_extract_signing_params_full_configmap() -> None:
    """Extract all parameters from a complete ConfigMap."""
    configmap = _mock_configmap(_full_signing_params())
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["enableKeylessSigning"] == "true"
    assert params["defaultOIDCIssuer"] == "https://oidc.example.com"
    assert params["rekorExternalUrl"] == "https://rekor-external.example.com"
    assert params["rekorUrl"] == "https://rekor-internal.example.com"
    assert params["fulcioExternalUrl"] == "https://fulcio-external.example.com"
    assert params["fulcioUrl"] == "https://fulcio-internal.example.com"
    assert params["tufExternalUrl"] == "https://tuf-external.example.com"
    assert params["tufUrl"] == "https://tuf-internal.example.com"
    assert params["buildIdentityRegexp"] == "^https://kubernetes.io/.*"
    expected_identity = "URI:https://kubernetes.io/test/serviceaccounts/default"
    assert params["tektonChainsIdentity"] == expected_identity


def test_extract_signing_params_internal_to_result_mapping() -> None:
    """Internal URLs are mapped to the non-suffixed result keys."""
    configmap = _mock_configmap(
        {
            "rekorInternalUrl": "https://rekor-internal.example.com",
            "fulcioInternalUrl": "https://fulcio-internal.example.com",
            "tufInternalUrl": "https://tuf-internal.example.com",
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["rekorUrl"] == "https://rekor-internal.example.com"
    assert params["fulcioUrl"] == "https://fulcio-internal.example.com"
    assert params["tufUrl"] == "https://tuf-internal.example.com"


def test_extract_signing_params_external_fallback_when_internal_missing() -> None:
    """External URLs fallback to non-suffixed result keys when internal is missing."""
    configmap = _mock_configmap(
        {
            "rekorExternalUrl": "https://rekor-external.example.com",
            "fulcioExternalUrl": "https://fulcio-external.example.com",
            "tufExternalUrl": "https://tuf-external.example.com",
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["rekorExternalUrl"] == "https://rekor-external.example.com"
    assert params["rekorUrl"] == "https://rekor-external.example.com"
    assert params["fulcioExternalUrl"] == "https://fulcio-external.example.com"
    assert params["fulcioUrl"] == "https://fulcio-external.example.com"
    assert params["tufExternalUrl"] == "https://tuf-external.example.com"
    assert params["tufUrl"] == "https://tuf-external.example.com"


def test_extract_signing_params_internal_preferred_over_external() -> None:
    """Internal URLs are preferred over external URLs for non-suffixed result keys."""
    configmap = _mock_configmap(
        {
            "rekorInternalUrl": "https://rekor-internal.example.com",
            "rekorExternalUrl": "https://rekor-external.example.com",
            "fulcioInternalUrl": "https://fulcio-internal.example.com",
            "fulcioExternalUrl": "https://fulcio-external.example.com",
            "tufInternalUrl": "https://tuf-internal.example.com",
            "tufExternalUrl": "https://tuf-external.example.com",
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["rekorUrl"] == "https://rekor-internal.example.com"
    assert params["rekorExternalUrl"] == "https://rekor-external.example.com"
    assert params["fulcioUrl"] == "https://fulcio-internal.example.com"
    assert params["fulcioExternalUrl"] == "https://fulcio-external.example.com"
    assert params["tufUrl"] == "https://tuf-internal.example.com"
    assert params["tufExternalUrl"] == "https://tuf-external.example.com"


def test_extract_signing_params_empty_configmap_data() -> None:
    """Missing ConfigMap data defaults enableKeylessSigning to 'false'."""
    configmap = _mock_configmap({})
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["enableKeylessSigning"] == "false"


def test_extract_signing_params_missing_data_key() -> None:
    """ConfigMap without 'data' key defaults enableKeylessSigning to 'false'."""
    configmap: dict[str, Any] = {}
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["enableKeylessSigning"] == "false"


def test_extract_signing_params_partial_configmap() -> None:
    """Partial ConfigMap data extracts available values, defaults others."""
    configmap = _mock_configmap(
        {
            "enableKeylessSigning": "true",
            "defaultOIDCIssuer": "https://oidc.example.com",
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["enableKeylessSigning"] == "true"
    assert params["defaultOIDCIssuer"] == "https://oidc.example.com"
    assert params["rekorUrl"] == ""
    assert params["fulcioUrl"] == ""


def test_write_result_files_creates_all_files(tmp_path: Path) -> None:
    """All result files are created in the results directory."""
    params = {
        "enableKeylessSigning": "true",
        "defaultOIDCIssuer": "https://oidc.example.com",
        "rekorExternalUrl": "",
        "rekorUrl": "",
        "fulcioExternalUrl": "",
        "fulcioUrl": "",
        "tufExternalUrl": "",
        "tufUrl": "",
        "buildIdentityRegexp": "",
        "tektonChainsIdentity": "",
    }
    collect_signing_params.write_result_files(tmp_path, params)

    for key in collect_signing_params.RESULT_KEYS:
        assert (tmp_path / key).exists()


def test_write_result_files_correct_content(tmp_path: Path) -> None:
    """Result files contain the correct values."""
    params = {
        "enableKeylessSigning": "true",
        "defaultOIDCIssuer": "https://oidc.example.com",
        "rekorExternalUrl": "https://rekor.example.com",
        "rekorUrl": "https://rekor-internal.example.com",
        "fulcioExternalUrl": "https://fulcio.example.com",
        "fulcioUrl": "https://fulcio-internal.example.com",
        "tufExternalUrl": "https://tuf.example.com",
        "tufUrl": "https://tuf-internal.example.com",
        "buildIdentityRegexp": "^https://kubernetes.io/.*",
        "tektonChainsIdentity": "URI:https://kubernetes.io/test",
    }
    collect_signing_params.write_result_files(tmp_path, params)

    assert (tmp_path / "enableKeylessSigning").read_text() == "true"
    assert (tmp_path / "defaultOIDCIssuer").read_text() == "https://oidc.example.com"
    assert (tmp_path / "rekorUrl").read_text() == "https://rekor-internal.example.com"


def test_collect_signing_params_with_configmap(tmp_path: Path) -> None:
    """Collect signing params when ConfigMap is available."""
    from unittest.mock import patch

    mock_configmap = _mock_configmap(_full_signing_params())
    with patch(
        "collect_signing_params.kubectl.get_configmap", return_value=mock_configmap
    ) as m:
        params = collect_signing_params.collect_signing_params(
            config_map_name="cluster-config",
            config_map_namespace="konflux-info",
            results_dir=tmp_path,
        )

        m.assert_called_once_with("cluster-config", namespace="konflux-info")
    assert params["enableKeylessSigning"] == "true"
    assert (tmp_path / "enableKeylessSigning").read_text() == "true"


def test_collect_signing_params_missing_configmap(tmp_path: Path) -> None:
    """Collect empty signing params when ConfigMap is not found."""
    from unittest.mock import patch

    with patch(
        "collect_signing_params.kubectl.get_configmap",
        side_effect=RuntimeError("ConfigMap not found"),
    ):
        params = collect_signing_params.collect_signing_params(
            config_map_name="cluster-config",
            config_map_namespace="konflux-info",
            results_dir=tmp_path,
        )

    assert params["enableKeylessSigning"] == "false"
    assert params["defaultOIDCIssuer"] == ""
    assert (tmp_path / "enableKeylessSigning").read_text() == "false"
    assert (tmp_path / "defaultOIDCIssuer").read_text() == ""


def test_collect_signing_params_custom_configmap_name(tmp_path: Path) -> None:
    """Collect signing params from a custom ConfigMap name."""
    from unittest.mock import patch

    with patch(
        "collect_signing_params.kubectl.get_configmap", return_value=_mock_configmap({})
    ) as m:
        collect_signing_params.collect_signing_params(
            config_map_name="custom-config",
            config_map_namespace="custom-ns",
            results_dir=tmp_path,
        )

        m.assert_called_once_with("custom-config", namespace="custom-ns")


def test_parse_args_required_results_dir() -> None:
    """Parse args with all required arguments."""
    args = collect_signing_params.parse_args(["--results-dir", "/tmp/results"])

    assert args.results_dir == "/tmp/results"
    assert args.config_map_name == "cluster-config"
    assert args.config_map_namespace == "konflux-info"


def test_parse_args_custom_configmap() -> None:
    """Parse args with custom ConfigMap name and namespace."""
    args = collect_signing_params.parse_args(
        [
            "--results-dir",
            "/tmp/results",
            "--config-map-name",
            "my-config",
            "--config-map-namespace",
            "my-namespace",
        ]
    )

    assert args.results_dir == "/tmp/results"
    assert args.config_map_name == "my-config"
    assert args.config_map_namespace == "my-namespace"


def test_parse_args_missing_results_dir_exits() -> None:
    """Exit when --results-dir is missing."""
    with pytest.raises(SystemExit) as exc_info:
        collect_signing_params.parse_args([])
    assert exc_info.value.code == 1


def test_parse_args_help_exits() -> None:
    """Exit when --help is provided."""
    with pytest.raises(SystemExit) as exc_info:
        collect_signing_params.parse_args(["--help"])
    assert exc_info.value.code == 1


def test_main_success(tmp_path: Path) -> None:
    """Main returns 0 on success."""
    from unittest.mock import patch

    mock_configmap = _mock_configmap(_full_signing_params())
    with patch("collect_signing_params.kubectl.get_configmap", return_value=mock_configmap):
        result = collect_signing_params.main(["--results-dir", str(tmp_path)])

    assert result == 0
    assert (tmp_path / "enableKeylessSigning").read_text() == "true"


def test_main_missing_configmap_succeeds(tmp_path: Path) -> None:
    """Main returns 0 even when ConfigMap is missing."""
    from unittest.mock import patch

    with patch(
        "collect_signing_params.kubectl.get_configmap",
        side_effect=RuntimeError("Not found"),
    ):
        result = collect_signing_params.main(["--results-dir", str(tmp_path)])

    assert result == 0
    assert (tmp_path / "enableKeylessSigning").read_text() == "false"


def test_module_main_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Executing the module as __main__ calls main()."""
    import runpy
    from unittest.mock import patch

    mock_configmap = _mock_configmap(_full_signing_params())
    monkeypatch.setattr(
        "sys.argv", ["collect_signing_params.py", "--results-dir", str(tmp_path)]
    )

    with patch("collect_signing_params.kubectl.get_configmap", return_value=mock_configmap):
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("collect_signing_params", run_name="__main__")

    assert exc_info.value.code == 0


# --- Additional edge case tests ---


def test_extract_signing_params_non_string_values() -> None:
    """Convert non-string values (int, bool) to strings."""
    configmap = _mock_configmap(
        {
            "enableKeylessSigning": True,  # type: ignore[dict-item]
            "defaultOIDCIssuer": 12345,  # type: ignore[dict-item]
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["enableKeylessSigning"] == "True"
    assert params["defaultOIDCIssuer"] == "12345"


def test_extract_signing_params_special_characters() -> None:
    """Handle special characters and Unicode in values."""
    configmap = _mock_configmap(
        {
            "enableKeylessSigning": "true",
            "defaultOIDCIssuer": "https://oidc.example.com/path?param=value&other=1",
            "buildIdentityRegexp": r"^https://kubernetes\.io/namespaces/[^/]+/.*$",
            "tektonChainsIdentity": "URI:https://example.com/unicode/日本語",
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["defaultOIDCIssuer"] == "https://oidc.example.com/path?param=value&other=1"
    assert params["buildIdentityRegexp"] == r"^https://kubernetes\.io/namespaces/[^/]+/.*$"
    assert params["tektonChainsIdentity"] == "URI:https://example.com/unicode/日本語"


def test_extract_signing_params_explicit_empty_string_keyless() -> None:
    """Empty string for enableKeylessSigning defaults to 'false'."""
    configmap = _mock_configmap(
        {
            "enableKeylessSigning": "",
            "defaultOIDCIssuer": "https://oidc.example.com",
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["enableKeylessSigning"] == "false"


def test_extract_signing_params_whitespace_only_keyless() -> None:
    """Whitespace-only enableKeylessSigning is treated as falsy and defaults to 'false'."""
    configmap = _mock_configmap(
        {
            "enableKeylessSigning": "   ",
        }
    )
    params = collect_signing_params.extract_signing_params_from_configmap(configmap)

    assert params["enableKeylessSigning"] == "   "


def test_write_result_files_unicode_content(tmp_path: Path) -> None:
    """Write Unicode content to result files correctly."""
    params = {
        "enableKeylessSigning": "true",
        "defaultOIDCIssuer": "https://example.com/日本語",
        "rekorExternalUrl": "",
        "rekorUrl": "",
        "fulcioExternalUrl": "",
        "fulcioUrl": "",
        "tufExternalUrl": "",
        "tufUrl": "",
        "buildIdentityRegexp": "",
        "tektonChainsIdentity": "",
    }
    collect_signing_params.write_result_files(tmp_path, params)

    content = (tmp_path / "defaultOIDCIssuer").read_text(encoding="utf-8")
    assert content == "https://example.com/日本語"


def test_write_result_files_missing_key_in_params(tmp_path: Path) -> None:
    """Missing keys in params result in empty files."""
    params = {"enableKeylessSigning": "true"}
    collect_signing_params.write_result_files(tmp_path, params)

    assert (tmp_path / "enableKeylessSigning").read_text() == "true"
    assert (tmp_path / "defaultOIDCIssuer").read_text() == ""
    assert (tmp_path / "rekorUrl").read_text() == ""


def test_collect_signing_params_returns_all_keys(tmp_path: Path) -> None:
    """Verify collect_signing_params returns dict with all RESULT_KEYS."""
    from unittest.mock import patch

    with patch(
        "collect_signing_params.kubectl.get_configmap", return_value=_mock_configmap({})
    ):
        params = collect_signing_params.collect_signing_params(
            config_map_name="cluster-config",
            config_map_namespace="konflux-info",
            results_dir=tmp_path,
        )

    for key in collect_signing_params.RESULT_KEYS:
        assert key in params, f"Missing key '{key}' in returned params"


def test_parse_args_empty_results_dir_exits() -> None:
    """Exit when --results-dir is provided but empty."""
    with pytest.raises(SystemExit) as exc_info:
        collect_signing_params.parse_args(["--results-dir", ""])
    assert exc_info.value.code == 1


def test_parse_args_whitespace_results_dir_exits() -> None:
    """Exit when --results-dir is whitespace only."""
    with pytest.raises(SystemExit) as exc_info:
        collect_signing_params.parse_args(["--results-dir", "   "])
    assert exc_info.value.code == 1
