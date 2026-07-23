"""Tests for ``charon_env``."""

from __future__ import annotations

from pathlib import Path

import charon_env
import pytest


def test_load_charon_env_parses_dotenv_lines(tmp_path: Path) -> None:
    """Dotenv lines are parsed into a key/value mapping."""
    env_file = tmp_path / "charon.env"
    env_file.write_text(
        "\nCHARON_OCI_REGISTRY=repo@sha256:abcdef0123456789\n"
        "CHARON_TARGET=dev-npm-ga\n"
        'CHARON_PRODUCT_NAME="Test Product"\n',
        encoding="utf-8",
    )
    env = charon_env.load_charon_env(env_file)
    assert env["CHARON_OCI_REGISTRY"] == "repo@sha256:abcdef0123456789"
    assert env["CHARON_TARGET"] == "dev-npm-ga"
    assert env["CHARON_PRODUCT_NAME"] == "Test Product"


def test_load_charon_env_missing_file(tmp_path: Path) -> None:
    """Missing env files raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        charon_env.load_charon_env(tmp_path / "missing.env")


def test_load_charon_env_rejects_invalid_utf8(tmp_path: Path) -> None:
    """Invalid UTF-8 bytes fail instead of being silently replaced."""
    env_file = tmp_path / "charon.env"
    env_file.write_bytes(b"CHARON_TARGET=dev\xff\n")
    with pytest.raises(UnicodeDecodeError):
        charon_env.load_charon_env(env_file)


def test_split_oci_registries() -> None:
    """Registry references are split on percent signs."""
    value = "quay.io/a@sha256:111111%quay.io/b@sha256:222222"
    assert charon_env.split_oci_registries(value) == [
        "quay.io/a@sha256:111111",
        "quay.io/b@sha256:222222",
    ]


def test_short_sha256_prefix() -> None:
    """Short hash uses the first six digest characters."""
    assert charon_env.short_sha256_prefix("repo@sha256:0b15aad24f1b847") == "0b15aa"


def test_source_repo() -> None:
    """Source repo strips the digest suffix."""
    assert charon_env.source_repo("quay.io/org/app@sha256:abc") == "quay.io/org/app"


def test_load_charon_env_skips_malformed_lines(tmp_path: Path) -> None:
    """Lines without ``=`` are ignored."""
    env_file = tmp_path / "charon.env"
    env_file.write_text("CHARON_TARGET=dev\nnot-a-variable\n", encoding="utf-8")
    env = charon_env.load_charon_env(env_file)
    assert env == {"CHARON_TARGET": "dev"}


def test_short_sha256_prefix_requires_digest() -> None:
    """Registry references without a digest raise ValueError."""
    with pytest.raises(ValueError, match="@sha256:"):
        charon_env.short_sha256_prefix("quay.io/org/app:latest")


def test_split_oci_registries_ignores_empty_segments() -> None:
    """Trailing or duplicate ``%`` separators yield no empty entries."""
    assert charon_env.split_oci_registries("quay.io/a@sha256:111111%") == [
        "quay.io/a@sha256:111111",
    ]
    assert charon_env.split_oci_registries("%quay.io/a@sha256:111111%") == [
        "quay.io/a@sha256:111111",
    ]


def test_load_charon_env_skips_comments_and_blank_lines(tmp_path: Path) -> None:
    """Comments and blank lines are ignored."""
    env_file = tmp_path / "charon.env"
    env_file.write_text(
        "# comment\n\nCHARON_TARGET=dev\n# CHARON_FOO=bar\n",
        encoding="utf-8",
    )
    assert charon_env.load_charon_env(env_file) == {"CHARON_TARGET": "dev"}


def test_load_charon_env_strips_single_quotes(tmp_path: Path) -> None:
    """Single-quoted values are unquoted."""
    env_file = tmp_path / "charon.env"
    env_file.write_text("CHARON_PRODUCT_NAME='Test Product'\n", encoding="utf-8")
    assert charon_env.load_charon_env(env_file)["CHARON_PRODUCT_NAME"] == "Test Product"


def test_require_env_keys_raises_for_missing_key() -> None:
    """Missing required keys raise ValueError."""
    with pytest.raises(ValueError, match="missing required charon env variable"):
        charon_env.require_env_keys({"CHARON_TARGET": "dev"}, "CHARON_PRODUCT_NAME")


def test_require_oci_registries(tmp_path: Path) -> None:
    """Non-empty registry lists are returned from parsed env data."""
    env_file = tmp_path / "charon.env"
    env_file.write_text(
        "CHARON_OCI_REGISTRY=quay.io/a@sha256:111111%quay.io/b@sha256:222222\n",
        encoding="utf-8",
    )
    env = charon_env.load_charon_env(env_file)
    assert charon_env.require_oci_registries(env) == [
        "quay.io/a@sha256:111111",
        "quay.io/b@sha256:222222",
    ]


def test_require_oci_registries_missing_key() -> None:
    """Missing ``CHARON_OCI_REGISTRY`` raises ValueError."""
    with pytest.raises(ValueError, match="CHARON_OCI_REGISTRY is required"):
        charon_env.require_oci_registries({})


def test_require_oci_registries_empty_value() -> None:
    """Blank ``CHARON_OCI_REGISTRY`` values raise ValueError."""
    with pytest.raises(ValueError, match="at least one registry"):
        charon_env.require_oci_registries({"CHARON_OCI_REGISTRY": ""})


def test_nrrc_work_dir_default() -> None:
    """Default NRRC work directory uses the image writable root."""
    assert charon_env.nrrc_work_dir() == Path("/var/workdir/nrrc")


def test_nrrc_work_dir_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``WORK_DIR`` overrides the default NRRC staging directory."""
    custom = tmp_path / "staging"
    monkeypatch.setenv("WORK_DIR", str(custom))
    assert charon_env.nrrc_work_dir() == custom


def test_mrrc_work_dir_default() -> None:
    """Default MRRC work directory uses the volume mount root."""
    assert charon_env.mrrc_work_dir() == Path("/var/workdir/mrrc")


def test_mrrc_work_dir_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``WORK_DIR`` overrides the default MRRC staging directory."""
    custom = tmp_path / "staging"
    monkeypatch.setenv("WORK_DIR", str(custom))
    assert charon_env.mrrc_work_dir() == custom


def test_charon_config_path_uses_explicit_home(tmp_path: Path) -> None:
    """An explicit *home* overrides ``Path.home()``."""
    assert charon_env.charon_config_path(home=tmp_path) == tmp_path / ".charon" / "charon.yaml"


def test_install_charon_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Charon config is copied into ``$HOME/.charon/charon.yaml``."""
    monkeypatch.setenv("HOME", str(tmp_path))
    config_path = charon_env.charon_config_path()
    source = tmp_path / "input.yaml"
    source.write_text("charon-config\n", encoding="utf-8")
    dest = charon_env.install_charon_config(source)
    assert dest == config_path
    assert config_path.read_text(encoding="utf-8") == "charon-config\n"


def test_install_charon_config_missing_source(tmp_path: Path) -> None:
    """Missing config sources raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="charon config file not found"):
        charon_env.install_charon_config(tmp_path / "missing.yaml")
