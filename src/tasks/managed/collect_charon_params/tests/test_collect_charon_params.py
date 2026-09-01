"""Tests for ``collect_charon_params``."""

from __future__ import annotations

import json
from pathlib import Path

from release_service_utils.tasks.managed.collect_charon_params import (
    collect_charon_params as _collect_charon_params,
)
from release_service_utils.tasks.managed import collect_charon_params
import sys as _sys
import collect_charon_params.collect_charon_params  # ensure submodule is loaded

import pytest

_collect_charon_params_mod = _sys.modules["collect_charon_params.collect_charon_params"]


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _default_data(
    *,
    environment: str = "dev",
    release: str = "ga",
    package_type: str | None = None,
    sign_key: str | None = None,
    sign_ca_secret: str | None = None,
    config: object = "charon-config",
) -> dict:
    signing: dict = {}
    if sign_key is not None:
        signing["signKey"] = sign_key
    if sign_ca_secret is not None:
        signing["signCASecret"] = sign_ca_secret

    charon: dict = {
        "environment": environment,
        "release": release,
        "awsSecret": "charon-aws-credentials",
        "config": config,
    }
    if package_type is not None:
        charon["packageType"] = package_type
    if signing:
        charon["signing"] = signing

    return {
        "releaseNotes": {
            "product_name": "test",
            "product_version": "0.0.1",
        },
        "charon": charon,
    }


def _default_snapshot(
    images: list[str] | None = None,
) -> dict:
    if images is None:
        images = ["quay.io/test/test.zip@sha256:02b0c8aadf2b7c69"]
    return {
        "application": "test",
        "components": [
            {"containerImage": img, "name": f"comp-{i}"} for i, img in enumerate(images)
        ],
    }


def _default_release() -> dict:
    return {"status": {"attribution": {"author": "testuser"}}}


def _setup_files(
    tmp_path: Path,
    data: dict | None = None,
    snapshot: dict | None = None,
    release: dict | None = None,
    subdir: str = "uid",
) -> tuple[Path, str, str, str]:
    """Create work directory with data, snapshot, and release files.

    Return (work_dir, data_json_path, snapshot_path, release_path).
    """
    work_dir = tmp_path / "workdir"
    work_dir.mkdir()
    sub = work_dir / subdir
    sub.mkdir(parents=True, exist_ok=True)

    _write_json(sub / "data.json", data or _default_data())
    _write_json(
        sub / "snapshot.json",
        snapshot or _default_snapshot(),
    )
    _write_json(
        sub / "release.json",
        release or _default_release(),
    )

    return (
        work_dir,
        f"{subdir}/data.json",
        f"{subdir}/snapshot.json",
        f"{subdir}/release.json",
    )


def _make_result_paths(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path]:
    """Create result file paths for Tekton results."""
    results = tmp_path / "results"
    results.mkdir(exist_ok=True)
    return (
        results / "charonParamFilePath",
        results / "charonConfigFilePath",
        results / "charonAWSSecret",
        results / "charonSignCASecret",
    )


# --- collect_charon_params ---


class TestCollectCharonParams:
    """Test extraction of charon parameters from JSON dicts."""

    def test_happy_path_all_fields(self) -> None:
        """Extract all fields including optional signing keys."""
        data = _default_data(
            sign_key="testkey",
            sign_ca_secret="radas-sa-secret",
        )
        snapshot = _default_snapshot()
        release = _default_release()

        params = _collect_charon_params(data, snapshot, release)

        assert params.target == "dev-maven-ga"
        assert params.product_name == "test"
        assert params.product_version == "0.0.1"
        assert params.sign_key == "testkey"
        assert params.oci_registry == "quay.io/test/test.zip@sha256:02b0c8aadf2b7c69"
        assert params.aws_secret == "charon-aws-credentials"
        assert params.sign_ca_secret == "radas-sa-secret"
        assert params.author == "testuser"
        assert params.config == "charon-config"

    def test_default_package_type(self) -> None:
        """Default packageType to 'maven' when not specified."""
        data = _default_data()
        params = _collect_charon_params(data, _default_snapshot(), _default_release())
        assert params.target == "dev-maven-ga"

    def test_custom_package_type(self) -> None:
        """Use custom packageType when specified."""
        data = _default_data(package_type="npm")
        params = _collect_charon_params(data, _default_snapshot(), _default_release())
        assert params.target == "dev-npm-ga"

    def test_no_signing_section(self) -> None:
        """Empty sign_key and sign_ca_secret without signing."""
        data = _default_data()
        params = _collect_charon_params(data, _default_snapshot(), _default_release())
        assert params.sign_key == ""
        assert params.sign_ca_secret == ""

    def test_signing_null(self) -> None:
        """Handle signing explicitly set to null."""
        data = _default_data()
        data["charon"]["signing"] = None
        params = _collect_charon_params(data, _default_snapshot(), _default_release())
        assert params.sign_key == ""
        assert params.sign_ca_secret == ""

    def test_multiple_components(self) -> None:
        """Join multiple container images with percent separator."""
        snapshot = _default_snapshot(images=["img1@sha256:aaa", "img2@sha256:bbb"])
        params = _collect_charon_params(
            _default_data(),
            snapshot,
            _default_release(),
        )
        assert params.oci_registry == "img1@sha256:aaa%img2@sha256:bbb"

    def test_no_components(self) -> None:
        """Empty oci_registry when snapshot has no components."""
        snapshot = {"components": []}
        params = _collect_charon_params(_default_data(), snapshot, _default_release())
        assert params.oci_registry == ""

    def test_missing_components_key(self) -> None:
        """Empty oci_registry when snapshot has no components key."""
        snapshot: dict = {}
        params = _collect_charon_params(_default_data(), snapshot, _default_release())
        assert params.oci_registry == ""

    def test_missing_environment(self) -> None:
        """Raise on missing charon.environment."""
        data = _default_data()
        del data["charon"]["environment"]
        with pytest.raises(KeyError, match="environment"):
            _collect_charon_params(data, _default_snapshot(), _default_release())

    def test_missing_release(self) -> None:
        """Raise on missing charon.release."""
        data = _default_data()
        del data["charon"]["release"]
        with pytest.raises(KeyError, match="release"):
            _collect_charon_params(data, _default_snapshot(), _default_release())

    def test_missing_aws_secret(self) -> None:
        """Raise on missing charon.awsSecret."""
        data = _default_data()
        del data["charon"]["awsSecret"]
        with pytest.raises(KeyError, match="awsSecret"):
            _collect_charon_params(data, _default_snapshot(), _default_release())

    def test_missing_product_name(self) -> None:
        """Raise on missing releaseNotes.product_name."""
        data = _default_data()
        del data["releaseNotes"]["product_name"]
        with pytest.raises(KeyError, match="product_name"):
            _collect_charon_params(data, _default_snapshot(), _default_release())

    def test_missing_product_version(self) -> None:
        """Raise on missing releaseNotes.product_version."""
        data = _default_data()
        del data["releaseNotes"]["product_version"]
        with pytest.raises(KeyError, match="product_version"):
            _collect_charon_params(data, _default_snapshot(), _default_release())

    def test_missing_author(self) -> None:
        """Raise on missing release status.attribution.author."""
        release = {"status": {"attribution": {}}}
        with pytest.raises(KeyError, match="author"):
            _collect_charon_params(
                _default_data(),
                _default_snapshot(),
                release,
            )

    def test_missing_charon_section(self) -> None:
        """Raise on missing top-level charon key."""
        data = {"releaseNotes": {"product_name": "x"}}
        with pytest.raises(KeyError, match="charon"):
            _collect_charon_params(data, _default_snapshot(), _default_release())

    def test_missing_config(self) -> None:
        """Raise on missing charon.config."""
        data = _default_data()
        del data["charon"]["config"]
        with pytest.raises(KeyError, match="config"):
            _collect_charon_params(data, _default_snapshot(), _default_release())

    def test_missing_container_image(self) -> None:
        """Raise on component missing containerImage key."""
        snapshot = {"components": [{"name": "broken-component"}]}
        with pytest.raises(KeyError, match="containerImage|missing the"):
            _collect_charon_params(_default_data(), snapshot, _default_release())


# --- write_charon_env ---


class TestWriteCharonEnv:
    """Test the env file writer."""

    def test_with_sign_key(self, tmp_path: Path) -> None:
        """Include CHARON_SIGN_KEY when sign_key is non-empty."""
        env_path = tmp_path / "charon.env"
        params = collect_charon_params.CharonParams(
            target="dev-maven-ga",
            product_name="test",
            product_version="0.0.1",
            sign_key="testkey",
            oci_registry="img@sha256:abc",
            aws_secret="aws-sec",
            sign_ca_secret="ca-sec",
            author="testuser",
            config="cfg",
        )
        collect_charon_params.write_charon_env(env_path, params)
        content = env_path.read_text(encoding="utf-8")
        assert "export CHARON_TARGET=dev-maven-ga\n" in content
        assert "export CHARON_PRODUCT_NAME=test\n" in content
        assert "export CHARON_PRODUCT_VERSION=0.0.1\n" in content
        assert "export CHARON_SIGN_KEY=testkey\n" in content
        assert "export CHARON_OCI_REGISTRY=img@sha256:abc\n" in content
        assert "export CHARON_AUTHOR=testuser\n" in content

    def test_special_chars_quoted(self, tmp_path: Path) -> None:
        """Values with special characters are shell-escaped."""
        env_path = tmp_path / "charon.env"
        params = collect_charon_params.CharonParams(
            target="dev-maven-ga",
            product_name="my product",
            product_version="1.0 beta",
            sign_key="key with $pecial",
            oci_registry="img@sha256:abc",
            aws_secret="aws-sec",
            sign_ca_secret="ca-sec",
            author="user name",
            config="cfg",
        )
        collect_charon_params.write_charon_env(env_path, params)
        content = env_path.read_text(encoding="utf-8")
        assert "export CHARON_PRODUCT_NAME='my product'\n" in content
        assert "export CHARON_PRODUCT_VERSION='1.0 beta'\n" in content
        assert "export CHARON_SIGN_KEY='key with $pecial'\n" in content
        assert "export CHARON_AUTHOR='user name'\n" in content

    def test_without_sign_key(self, tmp_path: Path) -> None:
        """Omit CHARON_SIGN_KEY when sign_key is empty."""
        env_path = tmp_path / "charon.env"
        params = collect_charon_params.CharonParams(
            target="dev-maven-ga",
            product_name="test",
            product_version="0.0.1",
            sign_key="",
            oci_registry="img@sha256:abc",
            aws_secret="aws-sec",
            sign_ca_secret="",
            author="testuser",
            config="cfg",
        )
        collect_charon_params.write_charon_env(env_path, params)
        content = env_path.read_text(encoding="utf-8")
        assert "CHARON_SIGN_KEY" not in content


# --- write_charon_config ---


class TestWriteCharonConfig:
    """Test the config file writer."""

    def test_string_config(self, tmp_path: Path) -> None:
        """Write string config as-is with trailing newline."""
        cfg_path = tmp_path / "charon-config.yaml"
        collect_charon_params.write_charon_config(cfg_path, "charon-config")
        assert cfg_path.read_text(encoding="utf-8") == "charon-config\n"

    def test_dict_config(self, tmp_path: Path) -> None:
        """Serialise non-string config as JSON."""
        cfg_path = tmp_path / "charon-config.yaml"
        config_obj = {"key": "value", "num": 42}
        collect_charon_params.write_charon_config(cfg_path, config_obj)
        content = cfg_path.read_text(encoding="utf-8")
        assert json.loads(content.strip()) == config_obj


# --- run ---


class TestRun:
    """Test the end-to-end run() orchestration."""

    def test_full_happy_path(self, tmp_path: Path) -> None:
        """Run writes env, config, and all result files."""
        work_dir, djp, sp, rp = _setup_files(
            tmp_path,
            data=_default_data(
                sign_key="testkey",
                sign_ca_secret="radas-sa-secret",
            ),
        )
        r_param, r_cfg, r_aws, r_ca = _make_result_paths(tmp_path)

        collect_charon_params.run(
            work_dir=work_dir,
            data_json_path=djp,
            snapshot_path=sp,
            release_path=rp,
            result_charon_param_file_path=r_param,
            result_charon_config_file_path=r_cfg,
            result_charon_aws_secret=r_aws,
            result_charon_sign_ca_secret=r_ca,
        )

        env_rel = r_param.read_text(encoding="utf-8")
        assert env_rel == "uid/charon.env"

        cfg_rel = r_cfg.read_text(encoding="utf-8")
        assert cfg_rel == "uid/charon-config.yaml"

        assert r_aws.read_text(encoding="utf-8") == "charon-aws-credentials"
        assert r_ca.read_text(encoding="utf-8") == "radas-sa-secret"

        env_file = work_dir / env_rel
        assert env_file.is_file()
        env_content = env_file.read_text(encoding="utf-8")
        assert "CHARON_TARGET=dev-maven-ga" in env_content
        assert "CHARON_SIGN_KEY=testkey" in env_content
        assert "CHARON_AUTHOR=testuser" in env_content

        cfg_file = work_dir / cfg_rel
        assert cfg_file.is_file()
        assert cfg_file.read_text(encoding="utf-8").strip() == "charon-config"

    def test_missing_data_file(self, tmp_path: Path) -> None:
        """Raise when data file does not exist."""
        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        r_param, r_cfg, r_aws, r_ca = _make_result_paths(tmp_path)

        with pytest.raises(FileNotFoundError):
            collect_charon_params.run(
                work_dir=work_dir,
                data_json_path="missing/data.json",
                snapshot_path="missing/snapshot.json",
                release_path="missing/release.json",
                result_charon_param_file_path=r_param,
                result_charon_config_file_path=r_cfg,
                result_charon_aws_secret=r_aws,
                result_charon_sign_ca_secret=r_ca,
            )

    def test_empty_sign_ca_secret(self, tmp_path: Path) -> None:
        """Write empty string for sign_ca_secret result."""
        work_dir, djp, sp, rp = _setup_files(tmp_path)
        r_param, r_cfg, r_aws, r_ca = _make_result_paths(tmp_path)

        collect_charon_params.run(
            work_dir=work_dir,
            data_json_path=djp,
            snapshot_path=sp,
            release_path=rp,
            result_charon_param_file_path=r_param,
            result_charon_config_file_path=r_cfg,
            result_charon_aws_secret=r_aws,
            result_charon_sign_ca_secret=r_ca,
        )

        assert r_ca.read_text(encoding="utf-8") == ""


# --- _parse_args ---


class TestParseArgs:
    """Test CLI argument parsing."""

    def test_all_args(self) -> None:
        """Parse all required arguments."""
        args = _collect_charon_params_mod._parse_args(
            [
                "--work-dir",
                "/work",
                "--data-json-path",
                "uid/data.json",
                "--snapshot-path",
                "uid/snapshot.json",
                "--release-path",
                "uid/release.json",
            ]
        )
        assert args.work_dir == "/work"
        assert args.data_json_path == "uid/data.json"
        assert args.snapshot_path == "uid/snapshot.json"
        assert args.release_path == "uid/release.json"

    def test_missing_required_arg(self) -> None:
        """Exit on missing required argument."""
        with pytest.raises(SystemExit):
            _collect_charon_params_mod._parse_args([])


# --- main ---


class TestMain:
    """Test the CLI entry point."""

    @staticmethod
    def _set_result_env(
        monkeypatch: pytest.MonkeyPatch,
        r_param: Path,
        r_cfg: Path,
        r_aws: Path,
        r_ca: Path,
    ) -> None:
        monkeypatch.setenv("RESULT_CHARON_PARAM_FILE_PATH", str(r_param))
        monkeypatch.setenv("RESULT_CHARON_CONFIG_FILE_PATH", str(r_cfg))
        monkeypatch.setenv("RESULT_CHARON_AWS_SECRET", str(r_aws))
        monkeypatch.setenv("RESULT_CHARON_SIGN_CA_SECRET", str(r_ca))

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return 0 on successful run."""
        work_dir, djp, sp, rp = _setup_files(
            tmp_path,
            data=_default_data(sign_key="k", sign_ca_secret="ca"),
        )
        r_param, r_cfg, r_aws, r_ca = _make_result_paths(tmp_path)
        self._set_result_env(monkeypatch, r_param, r_cfg, r_aws, r_ca)

        rc = collect_charon_params.main(
            [
                "--work-dir",
                str(work_dir),
                "--data-json-path",
                djp,
                "--snapshot-path",
                sp,
                "--release-path",
                rp,
            ]
        )
        assert rc == 0

    def test_failure_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Errors from run() propagate as exceptions."""
        r_param, r_cfg, r_aws, r_ca = _make_result_paths(tmp_path)
        self._set_result_env(monkeypatch, r_param, r_cfg, r_aws, r_ca)

        with pytest.raises(FileNotFoundError):
            collect_charon_params.main(
                [
                    "--work-dir",
                    str(tmp_path),
                    "--data-json-path",
                    "no/data.json",
                    "--snapshot-path",
                    "no/snap.json",
                    "--release-path",
                    "no/rel.json",
                ]
            )

    def test_missing_result_env_vars(self, tmp_path: Path) -> None:
        """Exit when Tekton result env vars are not set."""
        work_dir, djp, sp, rp = _setup_files(tmp_path)
        with pytest.raises(SystemExit):
            collect_charon_params.main(
                [
                    "--work-dir",
                    str(work_dir),
                    "--data-json-path",
                    djp,
                    "--snapshot-path",
                    sp,
                    "--release-path",
                    rp,
                ]
            )
