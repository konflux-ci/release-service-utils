"""Tests for publish_to_mrrc_prepare_repo."""

from __future__ import annotations

import subprocess
from pathlib import Path

import publish_to_mrrc_prepare_repo
import pytest


def _write_charon_env(path: Path, registry: str) -> None:
    path.write_text(
        f"CHARON_OCI_REGISTRY={registry}\n",
        encoding="utf-8",
    )


def _set_prepare_repo_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    charon_param_file_path: str = "charon.env",
    work_dir: str | None = None,
) -> None:
    """Set env vars for main()."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("CHARON_PARAM_FILE_PATH", charon_param_file_path)
    if work_dir is not None:
        monkeypatch.setenv("WORK_DIR", work_dir)


def test_prepare_repo_downloads_into_short_hash_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Registry is pulled into a subdir named by its sha256 prefix."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")
    work_dir = tmp_path / "mrrc"

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        assert pull_spec == "quay.io/test/app@sha256:0b15aad24f1b847"
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "maven-repo.zip").write_bytes(b"fake-zip")

    monkeypatch.setattr(publish_to_mrrc_prepare_repo.oras_utils, "oras_pull", fake_oras_pull)
    publish_to_mrrc_prepare_repo.prepare_repo(
        charon_param_file=env_file,
        work_dir=work_dir,
    )
    subdir = work_dir / "0b15aa"
    assert subdir.is_dir()
    assert (subdir / "maven-repo.zip").is_file()


def test_prepare_repo_multiple_registries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Multiple %-separated registries each get their own subdir."""
    env_file = tmp_path / "charon.env"
    env_file.write_text(
        "CHARON_OCI_REGISTRY="
        "quay.io/a@sha256:0b15aad24f1b847%"
        "quay.io/b@sha256:e400bc4b4398295\n",
        encoding="utf-8",
    )
    work_dir = tmp_path / "mrrc"
    pulls: list[str] = []

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        pulls.append(pull_spec)
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "maven-repo.zip").write_bytes(b"fake-zip")

    monkeypatch.setattr(publish_to_mrrc_prepare_repo.oras_utils, "oras_pull", fake_oras_pull)
    publish_to_mrrc_prepare_repo.prepare_repo(
        charon_param_file=env_file,
        work_dir=work_dir,
    )
    assert pulls == [
        "quay.io/a@sha256:0b15aad24f1b847",
        "quay.io/b@sha256:e400bc4b4398295",
    ]
    assert (work_dir / "0b15aa").is_dir()
    assert (work_dir / "e400bc").is_dir()


def test_prepare_repo_requires_charon_oci_registry(tmp_path: Path) -> None:
    """Missing CHARON_OCI_REGISTRY raises ValueError."""
    env_file = tmp_path / "charon.env"
    env_file.write_text("CHARON_TARGET=dev\n", encoding="utf-8")
    with pytest.raises(ValueError, match="CHARON_OCI_REGISTRY"):
        publish_to_mrrc_prepare_repo.prepare_repo(
            charon_param_file=env_file,
            work_dir=tmp_path / "mrrc",
        )


def test_prepare_repo_empty_oci_registry(tmp_path: Path) -> None:
    """Empty CHARON_OCI_REGISTRY raises ValueError."""
    env_file = tmp_path / "charon.env"
    env_file.write_text("CHARON_OCI_REGISTRY=\n", encoding="utf-8")
    with pytest.raises(ValueError, match="at least one registry"):
        publish_to_mrrc_prepare_repo.prepare_repo(
            charon_param_file=env_file,
            work_dir=tmp_path / "mrrc",
        )


def test_prepare_repo_oras_pull_failure_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pull failure from oras propagates."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")

    def fail_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del pull_spec, download_dir, kwargs
        raise subprocess.CalledProcessError(1, ["oras", "pull"])

    monkeypatch.setattr(publish_to_mrrc_prepare_repo.oras_utils, "oras_pull", fail_oras_pull)
    with pytest.raises(subprocess.CalledProcessError):
        publish_to_mrrc_prepare_repo.prepare_repo(
            charon_param_file=env_file,
            work_dir=tmp_path / "mrrc",
        )


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() returns 0 on success."""
    _set_prepare_repo_env(
        monkeypatch,
        tmp_path,
        work_dir=str(tmp_path / "mrrc"),
    )
    monkeypatch.setattr(
        publish_to_mrrc_prepare_repo,
        "prepare_repo",
        lambda **_: None,
    )
    assert publish_to_mrrc_prepare_repo.main() == 0


def test_main_missing_data_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing DATA_DIR exits with code 1."""
    monkeypatch.delenv("DATA_DIR", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        publish_to_mrrc_prepare_repo.main()
    assert exc_info.value.code == 1


def test_main_missing_charon_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing charon env file raises FileNotFoundError."""
    _set_prepare_repo_env(
        monkeypatch,
        tmp_path,
        charon_param_file_path="missing.env",
        work_dir=str(tmp_path / "mrrc"),
    )
    with pytest.raises(FileNotFoundError, match="charon env file not found"):
        publish_to_mrrc_prepare_repo.main()
