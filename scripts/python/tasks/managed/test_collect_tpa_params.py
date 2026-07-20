"""Tests for collect_tpa_params task logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import collect_tpa_params


def _write_data(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")


def _make_result_paths(tmp_path: Path) -> dict[str, Path]:
    """Create result paths dictionary for testing."""
    result_dir = tmp_path / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    return {
        "atlasApiUrl": result_dir / "atlasApiUrl",
        "ssoTokenUrl": result_dir / "ssoTokenUrl",
        "secretName": result_dir / "secretName",
        "retryAWSSecretName": result_dir / "retryAWSSecretName",
        "retryS3Bucket": result_dir / "retryS3Bucket",
    }


def _mock_configmap(data: dict[str, str]) -> MagicMock:
    """Create a mock get_configmap function that returns the given data."""

    def mock_get_configmap(name: str, *, namespace: str | None = None) -> dict[str, Any]:
        return {"data": data}

    return mock_get_configmap


def _mock_configmap_failure() -> MagicMock:
    """Create a mock get_configmap function that raises RuntimeError."""

    def mock_get_configmap(name: str, *, namespace: str | None = None) -> dict[str, Any]:
        raise RuntimeError(f"Failed to retrieve ConfigMap '{name}'")

    return mock_get_configmap


class TestTryTsfConfig:
    """Tests for try_tsf_config function."""

    def test_returns_params_when_configmap_has_tsf_config(self) -> None:
        """Return TPAParams when ConfigMap contains valid TSF configuration."""
        mock_cm = _mock_configmap(
            {
                "trustifyServerExternalUrl": "https://trustify.example.com",
                "trustifyOIDCIssuerUrl": "https://sso.example.com/realms/test",
            }
        )
        result = collect_tpa_params.try_tsf_config(
            "cluster-config", "konflux-info", get_configmap=mock_cm, sleep_fn=lambda _: None
        )

        assert result is not None
        assert result.atlas_api_url == "https://trustify.example.com"
        assert result.sso_token_url == (
            "https://sso.example.com/realms/test/protocol/openid-connect/token"
        )
        assert result.secret_name == "release-sso-secret"
        assert result.retry_aws_secret_name == "secret-not-present"
        assert result.retry_s3_bucket == ""

    def test_returns_none_when_configmap_missing(self) -> None:
        """Return None when ConfigMap cannot be retrieved after retries."""
        result = collect_tpa_params.try_tsf_config(
            "cluster-config",
            "konflux-info",
            get_configmap=_mock_configmap_failure(),
            sleep_fn=lambda _: None,
        )
        assert result is None

    def test_retries_on_configmap_failure(self) -> None:
        """Retry kubectl call up to 3 times before giving up."""
        call_count = 0

        def mock_get_configmap_fails_twice(
            name: str, *, namespace: str | None = None
        ) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError(f"Transient failure {call_count}")
            return {
                "data": {
                    "trustifyServerExternalUrl": "https://trustify.example.com",
                    "trustifyOIDCIssuerUrl": "https://sso.example.com/realms/test",
                }
            }

        result = collect_tpa_params.try_tsf_config(
            "cluster-config",
            "konflux-info",
            get_configmap=mock_get_configmap_fails_twice,
            sleep_fn=lambda _: None,
        )

        assert result is not None
        assert call_count == 3
        assert result.atlas_api_url == "https://trustify.example.com"

    def test_returns_none_when_configmap_incomplete(self) -> None:
        """Return None when ConfigMap is missing required fields."""
        mock_cm = _mock_configmap(
            {"trustifyServerExternalUrl": "https://trustify.example.com"}
        )
        result = collect_tpa_params.try_tsf_config(
            "cluster-config", "konflux-info", get_configmap=mock_cm, sleep_fn=lambda _: None
        )
        assert result is None

    def test_returns_none_when_configmap_empty(self) -> None:
        """Return None when ConfigMap has empty data."""
        mock_cm = _mock_configmap({})
        result = collect_tpa_params.try_tsf_config(
            "cluster-config", "konflux-info", get_configmap=mock_cm, sleep_fn=lambda _: None
        )
        assert result is None

    def test_returns_none_when_configmap_has_only_issuer_url(self) -> None:
        """Return None when ConfigMap has only trustifyOIDCIssuerUrl."""
        mock_cm = _mock_configmap(
            {"trustifyOIDCIssuerUrl": "https://sso.example.com/realms/test"}
        )
        result = collect_tpa_params.try_tsf_config(
            "cluster-config", "konflux-info", get_configmap=mock_cm, sleep_fn=lambda _: None
        )
        assert result is None

    def test_returns_none_when_configmap_has_empty_string_values(self) -> None:
        """Return None when ConfigMap has empty string values for trustify fields."""
        mock_cm = _mock_configmap(
            {
                "trustifyServerExternalUrl": "",
                "trustifyOIDCIssuerUrl": "https://sso.example.com/realms/test",
            }
        )
        result = collect_tpa_params.try_tsf_config(
            "cluster-config", "konflux-info", get_configmap=mock_cm, sleep_fn=lambda _: None
        )
        assert result is None


class TestGetTpaConfig:
    """Tests for get_tpa_config function."""

    def test_returns_atlas_config(self) -> None:
        """Return atlas configuration when present."""
        data = {"atlas": {"server": "stage"}}
        assert collect_tpa_params.get_tpa_config(data) == {"server": "stage"}

    def test_returns_tpa_config(self) -> None:
        """Return tpa configuration when atlas is not present."""
        data = {"tpa": {"server": "production"}}
        assert collect_tpa_params.get_tpa_config(data) == {"server": "production"}

    def test_prefers_atlas_over_tpa(self) -> None:
        """Prefer atlas over tpa when both are present."""
        data = {"atlas": {"server": "stage"}, "tpa": {"server": "production"}}
        assert collect_tpa_params.get_tpa_config(data) == {"server": "stage"}

    def test_returns_empty_dict_when_neither_present(self) -> None:
        """Return empty dict when neither atlas nor tpa is present."""
        data = {"other": "data"}
        assert collect_tpa_params.get_tpa_config(data) == {}


class TestParamsFromDataFile:
    """Tests for params_from_data_file function."""

    def test_stage_config_with_defaults(self, tmp_path: Path) -> None:
        """Return stage configuration with default secret names."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "stage"}})

        result = collect_tpa_params.params_from_data_file(data_dir, "data.json")

        assert result is not None
        assert result.atlas_api_url == "https://atlas.release.stage.devshift.net"
        assert result.sso_token_url == (
            "https://auth.stage.redhat.com/auth/realms/EmployeeIDP/"
            "protocol/openid-connect/token"
        )
        assert result.secret_name == "atlas-staging-sso-secret"
        assert result.retry_aws_secret_name == "atlas-retry-s3-staging-secret"
        assert (
            result.retry_s3_bucket
            == "mpp-e1-preprod-sbom-29093454-2ea7-4fd0-b4cf-dc69a7529ee0"
        )

    def test_production_config_with_defaults(self, tmp_path: Path) -> None:
        """Return production configuration with default secret names."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "production"}})

        result = collect_tpa_params.params_from_data_file(data_dir, "data.json")

        assert result is not None
        assert result.atlas_api_url == "https://atlas.release.devshift.net"
        assert result.sso_token_url == (
            "https://auth.redhat.com/auth/realms/EmployeeIDP/" "protocol/openid-connect/token"
        )
        assert result.secret_name == "atlas-prod-sso-secret"
        assert result.retry_aws_secret_name == "atlas-retry-s3-production-secret"
        assert (
            result.retry_s3_bucket == "mpp-e1-prod-sbom-e02138d3-5c5c-4d90-a38f-6c54f658604d"
        )

    def test_stage_config_with_custom_secrets(self, tmp_path: Path) -> None:
        """Return stage configuration with custom secret names from data file."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(
            data_file,
            {
                "tpa": {
                    "server": "stage",
                    "atlas-sso-secret-name": "custom-sso-secret",
                    "atlas-retry-aws-secret-name": "custom-aws-secret",
                }
            },
        )

        result = collect_tpa_params.params_from_data_file(data_dir, "data.json")

        assert result is not None
        assert result.secret_name == "custom-sso-secret"
        assert result.retry_aws_secret_name == "custom-aws-secret"

    def test_production_config_with_custom_secrets(self, tmp_path: Path) -> None:
        """Return production configuration with custom secret names from data file."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(
            data_file,
            {
                "atlas": {
                    "server": "production",
                    "atlas-sso-secret-name": "prod-custom-secret",
                    "atlas-retry-aws-secret-name": "prod-custom-aws",
                }
            },
        )

        result = collect_tpa_params.params_from_data_file(data_dir, "data.json")

        assert result is not None
        assert result.secret_name == "prod-custom-secret"
        assert result.retry_aws_secret_name == "prod-custom-aws"

    def test_missing_data_file_raises(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when data file does not exist."""
        with pytest.raises(FileNotFoundError):
            collect_tpa_params.params_from_data_file(tmp_path, "missing.json")

    def test_missing_server_raises_when_fail_on_missing(self, tmp_path: Path) -> None:
        """Raise ValueError when server is missing and fail_on_missing is True."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {})

        with pytest.raises(ValueError, match="server value is missing"):
            collect_tpa_params.params_from_data_file(
                data_dir, "data.json", fail_on_missing=True
            )

    def test_invalid_server_raises_when_fail_on_missing(self, tmp_path: Path) -> None:
        """Raise ValueError when server value is invalid and fail_on_missing is True."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "invalid"}})

        with pytest.raises(ValueError, match="Unknown.*server value 'invalid'"):
            collect_tpa_params.params_from_data_file(
                data_dir, "data.json", fail_on_missing=True
            )

    def test_missing_server_returns_none_when_not_fail_on_missing(
        self, tmp_path: Path
    ) -> None:
        """Return None when server is missing and fail_on_missing is False."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {})

        result = collect_tpa_params.params_from_data_file(
            data_dir, "data.json", fail_on_missing=False
        )
        assert result is None

    def test_invalid_server_returns_none_when_not_fail_on_missing(
        self, tmp_path: Path
    ) -> None:
        """Return None when server is invalid and fail_on_missing is False."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "invalid"}})

        result = collect_tpa_params.params_from_data_file(
            data_dir, "data.json", fail_on_missing=False
        )
        assert result is None

    def test_empty_string_server_raises_when_fail_on_missing(self, tmp_path: Path) -> None:
        """Raise ValueError when server is empty string and fail_on_missing is True."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": ""}})

        with pytest.raises(ValueError, match="Unknown.*server value"):
            collect_tpa_params.params_from_data_file(
                data_dir, "data.json", fail_on_missing=True
            )

    def test_invalid_json_raises_json_error(self, tmp_path: Path) -> None:
        """Raise JSONDecodeError when data file contains invalid JSON."""
        import json

        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        data_file = data_dir / "data.json"
        data_file.write_text("not valid json {", encoding="utf-8")

        with pytest.raises(json.JSONDecodeError):
            collect_tpa_params.params_from_data_file(data_dir, "data.json")

    def test_non_dict_json_raises_type_error(self, tmp_path: Path) -> None:
        """Raise TypeError when data file contains non-object JSON."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        data_file = data_dir / "data.json"
        data_file.write_text('["array", "not", "object"]', encoding="utf-8")

        with pytest.raises(TypeError, match="JSON root must be an object"):
            collect_tpa_params.params_from_data_file(data_dir, "data.json")

    def test_nested_data_path(self, tmp_path: Path) -> None:
        """Handle data file in nested subdirectory."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "nested" / "subdir" / "data.json"
        _write_data(data_file, {"tpa": {"server": "stage"}})

        result = collect_tpa_params.params_from_data_file(data_dir, "nested/subdir/data.json")

        assert result is not None
        assert result.atlas_api_url == "https://atlas.release.stage.devshift.net"


class TestWriteResults:
    """Tests for write_results function."""

    def test_writes_all_params(self, tmp_path: Path) -> None:
        """Write all parameters to result files."""
        result_paths = _make_result_paths(tmp_path)
        params = collect_tpa_params.TPAParams(
            atlas_api_url="https://api.example.com",
            sso_token_url="https://sso.example.com/token",
            secret_name="my-secret",
            retry_aws_secret_name="aws-secret",
            retry_s3_bucket="my-bucket",
        )

        collect_tpa_params.write_results(params, result_paths)

        assert result_paths["atlasApiUrl"].read_text() == "https://api.example.com"
        assert result_paths["ssoTokenUrl"].read_text() == "https://sso.example.com/token"
        assert result_paths["secretName"].read_text() == "my-secret"
        assert result_paths["retryAWSSecretName"].read_text() == "aws-secret"
        assert result_paths["retryS3Bucket"].read_text() == "my-bucket"

    def test_writes_empty_strings_when_params_is_none(self, tmp_path: Path) -> None:
        """Write empty strings when params is None."""
        result_paths = _make_result_paths(tmp_path)

        collect_tpa_params.write_results(None, result_paths)

        assert result_paths["atlasApiUrl"].read_text() == ""
        assert result_paths["ssoTokenUrl"].read_text() == ""
        assert result_paths["secretName"].read_text() == ""
        assert result_paths["retryAWSSecretName"].read_text() == ""
        assert result_paths["retryS3Bucket"].read_text() == ""


class TestRunCollectTpaParams:
    """Tests for run_collect_tpa_params function."""

    def test_uses_tsf_config_when_available(self, tmp_path: Path) -> None:
        """Use TSF configuration when cluster ConfigMap is available."""
        result_paths = _make_result_paths(tmp_path)
        mock_cm = _mock_configmap(
            {
                "trustifyServerExternalUrl": "https://trustify.example.com",
                "trustifyOIDCIssuerUrl": "https://sso.example.com/realms/test",
            }
        )

        collect_tpa_params.run_collect_tpa_params(
            data_dir=tmp_path,
            data_path="",
            configmap_name="cluster-config",
            configmap_namespace="konflux-info",
            fail_on_missing=True,
            result_paths=result_paths,
            get_configmap=mock_cm,
            sleep_fn=lambda _: None,
        )

        assert result_paths["atlasApiUrl"].read_text() == "https://trustify.example.com"
        assert result_paths["secretName"].read_text() == "release-sso-secret"

    def test_falls_back_to_data_file_when_configmap_fails(self, tmp_path: Path) -> None:
        """Fall back to data file when ConfigMap is not available."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "stage"}})
        result_paths = _make_result_paths(tmp_path)

        collect_tpa_params.run_collect_tpa_params(
            data_dir=data_dir,
            data_path="data.json",
            configmap_name="cluster-config",
            configmap_namespace="konflux-info",
            fail_on_missing=True,
            result_paths=result_paths,
            get_configmap=_mock_configmap_failure(),
            sleep_fn=lambda _: None,
        )

        assert result_paths["atlasApiUrl"].read_text() == (
            "https://atlas.release.stage.devshift.net"
        )
        assert result_paths["secretName"].read_text() == "atlas-staging-sso-secret"

    def test_raises_when_no_data_path_and_fail_on_missing(self, tmp_path: Path) -> None:
        """Raise ValueError when no data path and configmap fails with fail_on_missing."""
        result_paths = _make_result_paths(tmp_path)

        with pytest.raises(ValueError, match="No dataPath provided"):
            collect_tpa_params.run_collect_tpa_params(
                data_dir=tmp_path,
                data_path="",
                configmap_name="cluster-config",
                configmap_namespace="konflux-info",
                fail_on_missing=True,
                result_paths=result_paths,
                get_configmap=_mock_configmap_failure(),
                sleep_fn=lambda _: None,
            )

    def test_writes_empty_when_no_data_path_and_not_fail_on_missing(
        self, tmp_path: Path
    ) -> None:
        """Write empty results when no data path and fail_on_missing is False."""
        result_paths = _make_result_paths(tmp_path)

        collect_tpa_params.run_collect_tpa_params(
            data_dir=tmp_path,
            data_path="",
            configmap_name="cluster-config",
            configmap_namespace="konflux-info",
            fail_on_missing=False,
            result_paths=result_paths,
            get_configmap=_mock_configmap_failure(),
            sleep_fn=lambda _: None,
        )

        assert result_paths["atlasApiUrl"].read_text() == ""
        assert result_paths["secretName"].read_text() == ""

    def test_production_config_from_data_file(self, tmp_path: Path) -> None:
        """Use production configuration from data file."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "production"}})
        result_paths = _make_result_paths(tmp_path)

        collect_tpa_params.run_collect_tpa_params(
            data_dir=data_dir,
            data_path="data.json",
            configmap_name="cluster-config",
            configmap_namespace="konflux-info",
            fail_on_missing=True,
            result_paths=result_paths,
            get_configmap=_mock_configmap_failure(),
            sleep_fn=lambda _: None,
        )

        assert result_paths["atlasApiUrl"].read_text() == "https://atlas.release.devshift.net"
        assert result_paths["secretName"].read_text() == "atlas-prod-sso-secret"

    def test_writes_empty_when_data_file_invalid_server_not_fail_on_missing(
        self, tmp_path: Path
    ) -> None:
        """Write empty results when data file has invalid server."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "invalid"}})
        result_paths = _make_result_paths(tmp_path)

        collect_tpa_params.run_collect_tpa_params(
            data_dir=data_dir,
            data_path="data.json",
            configmap_name="cluster-config",
            configmap_namespace="konflux-info",
            fail_on_missing=False,
            result_paths=result_paths,
            get_configmap=_mock_configmap_failure(),
            sleep_fn=lambda _: None,
        )

        assert result_paths["atlasApiUrl"].read_text() == ""
        assert result_paths["secretName"].read_text() == ""

    def test_tsf_config_overrides_data_file(self, tmp_path: Path) -> None:
        """TSF config from ConfigMap takes precedence over data file."""
        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "production"}})
        result_paths = _make_result_paths(tmp_path)
        mock_cm = _mock_configmap(
            {
                "trustifyServerExternalUrl": "https://trustify.example.com",
                "trustifyOIDCIssuerUrl": "https://sso.example.com/realms/test",
            }
        )

        collect_tpa_params.run_collect_tpa_params(
            data_dir=data_dir,
            data_path="data.json",
            configmap_name="cluster-config",
            configmap_namespace="konflux-info",
            fail_on_missing=True,
            result_paths=result_paths,
            get_configmap=mock_cm,
            sleep_fn=lambda _: None,
        )

        assert result_paths["atlasApiUrl"].read_text() == "https://trustify.example.com"
        assert result_paths["secretName"].read_text() == "release-sso-secret"


class TestParseArgs:
    """Tests for parse_args function."""

    def test_required_args(self) -> None:
        """Parse required arguments."""
        args = collect_tpa_params.parse_args(["--data-dir", "/tmp/data"])
        assert args.data_dir == "/tmp/data"
        assert args.data_path == ""
        assert args.configmap_name == "cluster-config"
        assert args.configmap_namespace == "konflux-info"
        assert args.fail_on_missing is True

    def test_all_args(self) -> None:
        """Parse all arguments."""
        args = collect_tpa_params.parse_args(
            [
                "--data-dir",
                "/tmp/data",
                "--data-path",
                "subdir/data.json",
                "--configmap-name",
                "my-config",
                "--configmap-namespace",
                "my-ns",
                "--fail-on-missing",
                "false",
            ]
        )
        assert args.data_dir == "/tmp/data"
        assert args.data_path == "subdir/data.json"
        assert args.configmap_name == "my-config"
        assert args.configmap_namespace == "my-ns"
        assert args.fail_on_missing is False

    def test_help_flag_exits(self) -> None:
        """Exit with usage when help flag is provided."""
        with pytest.raises(SystemExit) as exc_info:
            collect_tpa_params.parse_args(["--data-dir", "/tmp", "--help"])
        assert exc_info.value.code == 1


class TestMain:
    """Tests for main function."""

    def test_main_with_tsf_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Main exits 0 when TSF config is available."""
        from unittest.mock import patch

        result_paths = _make_result_paths(tmp_path)
        monkeypatch.setenv("RESULT_ATLASAPIURL", str(result_paths["atlasApiUrl"]))
        monkeypatch.setenv("RESULT_SSOTOKENURL", str(result_paths["ssoTokenUrl"]))
        monkeypatch.setenv("RESULT_SECRETNAME", str(result_paths["secretName"]))
        monkeypatch.setenv(
            "RESULT_RETRYAWSSECRETNAME", str(result_paths["retryAWSSecretName"])
        )
        monkeypatch.setenv("RESULT_RETRYS3BUCKET", str(result_paths["retryS3Bucket"]))

        mock_cm = _mock_configmap(
            {
                "trustifyServerExternalUrl": "https://trustify.example.com",
                "trustifyOIDCIssuerUrl": "https://sso.example.com/realms/test",
            }
        )

        with patch("collect_tpa_params.kubectl.get_configmap", mock_cm):
            result = collect_tpa_params.main(["--data-dir", str(tmp_path)])

        assert result == 0
        assert result_paths["atlasApiUrl"].read_text() == "https://trustify.example.com"

    def test_main_with_data_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Main exits 0 when using data file config."""
        from unittest.mock import patch

        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "stage"}})

        result_paths = _make_result_paths(tmp_path)
        monkeypatch.setenv("RESULT_ATLASAPIURL", str(result_paths["atlasApiUrl"]))
        monkeypatch.setenv("RESULT_SSOTOKENURL", str(result_paths["ssoTokenUrl"]))
        monkeypatch.setenv("RESULT_SECRETNAME", str(result_paths["secretName"]))
        monkeypatch.setenv(
            "RESULT_RETRYAWSSECRETNAME", str(result_paths["retryAWSSecretName"])
        )
        monkeypatch.setenv("RESULT_RETRYS3BUCKET", str(result_paths["retryS3Bucket"]))

        with patch("collect_tpa_params.kubectl.get_configmap", _mock_configmap_failure()):
            result = collect_tpa_params.main(
                [
                    "--data-dir",
                    str(data_dir),
                    "--data-path",
                    "data.json",
                ]
            )

        assert result == 0
        assert result_paths["atlasApiUrl"].read_text() == (
            "https://atlas.release.stage.devshift.net"
        )

    def test_main_missing_result_env_vars_exits(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Main exits 1 when result env vars are missing."""
        monkeypatch.delenv("RESULT_ATLASAPIURL", raising=False)

        with pytest.raises(SystemExit) as exc_info:
            collect_tpa_params.main(["--data-dir", str(tmp_path)])

        assert exc_info.value.code == 1

    def test_main_invalid_server_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Main raises ValueError when server value is invalid."""
        from unittest.mock import patch

        data_dir = tmp_path / "data"
        data_file = data_dir / "data.json"
        _write_data(data_file, {"atlas": {"server": "invalid"}})

        result_paths = _make_result_paths(tmp_path)
        monkeypatch.setenv("RESULT_ATLASAPIURL", str(result_paths["atlasApiUrl"]))
        monkeypatch.setenv("RESULT_SSOTOKENURL", str(result_paths["ssoTokenUrl"]))
        monkeypatch.setenv("RESULT_SECRETNAME", str(result_paths["secretName"]))
        monkeypatch.setenv(
            "RESULT_RETRYAWSSECRETNAME", str(result_paths["retryAWSSecretName"])
        )
        monkeypatch.setenv("RESULT_RETRYS3BUCKET", str(result_paths["retryS3Bucket"]))

        with patch("collect_tpa_params.kubectl.get_configmap", _mock_configmap_failure()):
            with pytest.raises(ValueError, match="Unknown.*server value"):
                collect_tpa_params.main(
                    [
                        "--data-dir",
                        str(data_dir),
                        "--data-path",
                        "data.json",
                    ]
                )


class TestModuleMain:
    """Tests for module main guard."""

    def test_module_main_guard(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Executing the module as `__main__` propagates failures from main()."""
        import runpy

        monkeypatch.delenv("RESULT_ATLASAPIURL", raising=False)
        monkeypatch.setattr("sys.argv", ["collect_tpa_params", "--data-dir", str(tmp_path)])

        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("collect_tpa_params", run_name="__main__")

        assert exc_info.value.code == 1
