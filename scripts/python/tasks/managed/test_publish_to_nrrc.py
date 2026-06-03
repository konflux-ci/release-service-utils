"""Tests for ``publish_to_nrrc``."""

from __future__ import annotations

import logging
import subprocess
import shutil
import tarfile
import runpy
import sys
from collections.abc import Sequence
from pathlib import Path

import publish_to_nrrc
import pytest


@pytest.fixture
def release_log_caplog(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Allow caplog to capture records from the ``release`` logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield caplog
    release_logger.propagate = False


def _write_charon_env(path: Path, registry: str) -> None:
    path.write_text(
        f"CHARON_OCI_REGISTRY={registry}\n",
        encoding="utf-8",
    )


def _make_gzip_tar(path: Path) -> None:
    package_dir = path.parent / "package"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "package.json").write_text("{}", encoding="utf-8")
    with tarfile.open(path, "w:gz") as tar:
        tar.add(package_dir, arcname="package")
    shutil.rmtree(package_dir)


def _patch_file_type_cmd(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stdout: str = "gzip compressed data\n",
) -> None:
    """Mock ``file -b`` used by ``file.is_gzip_or_tar_archive``."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        *,
        check: bool = True,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del check, kwargs
        argv = [str(x) for x in cmd]
        if argv[:2] == ["file", "-b"]:
            return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")
        raise AssertionError(f"unexpected command: {argv}")

    monkeypatch.setattr(publish_to_nrrc.subprocess_cmd, "run_cmd", fake_run_cmd)


def test_prepare_repo_downloads_and_collects_archives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each registry is pulled and archive files are moved into shared/."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")
    work_dir = tmp_path / "nrrc"
    subdir = work_dir / "0b15aa"

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        assert pull_spec == "quay.io/test/app@sha256:0b15aad24f1b847"
        download_dir.mkdir(parents=True, exist_ok=True)
        _make_gzip_tar(download_dir / "test.tgz")

    monkeypatch.setattr(publish_to_nrrc.oras_utils, "oras_pull", fake_oras_pull)
    _patch_file_type_cmd(monkeypatch)
    publish_to_nrrc.prepare_repo(
        charon_param_file=env_file,
        work_dir=work_dir,
    )
    shared = work_dir / "shared"
    moved = shared / "0b15aa_test.tgz"
    assert moved.is_file()
    assert not (subdir / "test.tgz").exists()


def test_prepare_repo_skips_existing_shared_file(
    tmp_path: Path,
    release_log_caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate archive names in shared/ produce a warning and are skipped."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")
    work_dir = tmp_path / "nrrc"
    shared = work_dir / "shared"
    shared.mkdir(parents=True)
    existing = shared / "0b15aa_test.tgz"
    existing.write_text("keep", encoding="utf-8")

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del pull_spec, kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "test.tgz").write_text("new", encoding="utf-8")

    monkeypatch.setattr(publish_to_nrrc.oras_utils, "oras_pull", fake_oras_pull)
    _patch_file_type_cmd(monkeypatch)
    with release_log_caplog.at_level(logging.WARNING, logger="release"):
        publish_to_nrrc.prepare_repo(
            charon_param_file=env_file,
            work_dir=work_dir,
        )
    assert existing.read_text(encoding="utf-8") == "keep"
    assert "already exists" in release_log_caplog.text


def _set_prepare_repo_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    charon_param_file_path: str = "charon.env",
    work_dir: str | None = None,
) -> None:
    """Set Tekton env vars used by ``publish_to_nrrc.main``."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("CHARON_PARAM_FILE_PATH", charon_param_file_path)
    if work_dir is not None:
        monkeypatch.setenv("WORK_DIR", work_dir)


def test_main_missing_charon_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing charon env files raise FileNotFoundError."""
    _set_prepare_repo_env(
        monkeypatch,
        tmp_path,
        charon_param_file_path="missing.env",
        work_dir=str(tmp_path / "nrrc"),
    )
    with pytest.raises(FileNotFoundError, match="charon env file not found"):
        publish_to_nrrc.main()


def test_main_missing_data_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing ``DATA_DIR`` exits before prepare logic runs."""
    monkeypatch.delenv("DATA_DIR", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        publish_to_nrrc.main()
    assert exc_info.value.code == 1


def test_main_rejects_absolute_charon_param_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Absolute charon param paths are rejected before file access."""
    _set_prepare_repo_env(monkeypatch, tmp_path, charon_param_file_path="/etc/passwd")
    with pytest.raises(ValueError, match="must be relative"):
        publish_to_nrrc.main()


def test_prepare_repo_multiple_registries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each ``%``-separated registry is pulled into shared/."""
    env_file = tmp_path / "charon.env"
    env_file.write_text(
        "CHARON_OCI_REGISTRY="
        "quay.io/a@sha256:0b15aad24f1b847%"
        "quay.io/b@sha256:e400bc4b4398295\n",
        encoding="utf-8",
    )
    work_dir = tmp_path / "nrrc"
    pulls: list[str] = []

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        pulls.append(pull_spec)
        download_dir.mkdir(parents=True, exist_ok=True)
        _make_gzip_tar(download_dir / "test.tgz")

    monkeypatch.setattr(publish_to_nrrc.oras_utils, "oras_pull", fake_oras_pull)
    _patch_file_type_cmd(monkeypatch)
    publish_to_nrrc.prepare_repo(
        charon_param_file=env_file,
        work_dir=work_dir,
    )
    assert pulls == [
        "quay.io/a@sha256:0b15aad24f1b847",
        "quay.io/b@sha256:e400bc4b4398295",
    ]
    shared = work_dir / "shared"
    assert (shared / "0b15aa_test.tgz").is_file()
    assert (shared / "e400bc_test.tgz").is_file()


def test_prepare_repo_requires_charon_oci_registry(tmp_path: Path) -> None:
    """Missing ``CHARON_OCI_REGISTRY`` raises ValueError."""
    env_file = tmp_path / "charon.env"
    env_file.write_text("CHARON_TARGET=dev\n", encoding="utf-8")
    with pytest.raises(ValueError, match="CHARON_OCI_REGISTRY"):
        publish_to_nrrc.prepare_repo(
            charon_param_file=env_file,
            work_dir=tmp_path / "nrrc",
        )


def test_prepare_repo_skips_non_archive_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non gzip/tar files are left in the pull directory."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")
    work_dir = tmp_path / "nrrc"
    subdir = work_dir / "0b15aa"

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del pull_spec, kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "notes.txt").write_text("skip me", encoding="utf-8")

    monkeypatch.setattr(publish_to_nrrc.oras_utils, "oras_pull", fake_oras_pull)
    _patch_file_type_cmd(monkeypatch, stdout="ASCII text\n")
    publish_to_nrrc.prepare_repo(
        charon_param_file=env_file,
        work_dir=work_dir,
    )
    assert (subdir / "notes.txt").is_file()
    assert list((work_dir / "shared").iterdir()) == []


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy-path Tekton env invocation exits with status 0."""
    _set_prepare_repo_env(
        monkeypatch,
        tmp_path,
        work_dir=str(tmp_path / "nrrc"),
    )
    monkeypatch.setattr(
        publish_to_nrrc,
        "prepare_repo",
        lambda **_: None,
    )
    assert publish_to_nrrc.main() == 0


def test_prepare_repo_empty_oci_registry(tmp_path: Path) -> None:
    """An empty ``CHARON_OCI_REGISTRY`` value raises ValueError."""
    env_file = tmp_path / "charon.env"
    env_file.write_text("CHARON_OCI_REGISTRY=\n", encoding="utf-8")
    with pytest.raises(ValueError, match="at least one registry"):
        publish_to_nrrc.prepare_repo(
            charon_param_file=env_file,
            work_dir=tmp_path / "nrrc",
        )


def test_prepare_repo_skips_subdirectory_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Directory entries from ``rglob`` are skipped; files are still collected."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")
    work_dir = tmp_path / "nrrc"
    subdir = work_dir / "0b15aa"

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del pull_spec, kwargs
        (download_dir / "nested").mkdir(parents=True)
        _make_gzip_tar(download_dir / "test.tgz")

    monkeypatch.setattr(publish_to_nrrc.oras_utils, "oras_pull", fake_oras_pull)
    _patch_file_type_cmd(monkeypatch)
    publish_to_nrrc.prepare_repo(
        charon_param_file=env_file,
        work_dir=work_dir,
    )
    assert (work_dir / "shared" / "0b15aa_test.tgz").is_file()
    assert (subdir / "nested").is_dir()


def test_prepare_repo_oras_pull_failure_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``oras_pull`` failures propagate from ``prepare_repo``."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")

    def fail_oras_pull(pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del pull_spec, download_dir, kwargs
        raise subprocess.CalledProcessError(1, ["oras", "pull"])

    monkeypatch.setattr(publish_to_nrrc.oras_utils, "oras_pull", fail_oras_pull)
    with pytest.raises(subprocess.CalledProcessError):
        publish_to_nrrc.prepare_repo(
            charon_param_file=env_file,
            work_dir=tmp_path / "nrrc",
        )


def test_main_oras_failure_exits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``CalledProcessError`` from prepare logic propagates from ``main()``."""
    env_file = tmp_path / "charon.env"
    _write_charon_env(env_file, "quay.io/test/app@sha256:0b15aad24f1b847")
    _set_prepare_repo_env(
        monkeypatch,
        tmp_path,
        work_dir=str(tmp_path / "nrrc"),
    )

    def fail_prepare(**kwargs: object) -> None:
        del kwargs
        raise subprocess.CalledProcessError(1, ["oras", "pull"])

    monkeypatch.setattr(publish_to_nrrc, "prepare_repo", fail_prepare)
    with pytest.raises(subprocess.CalledProcessError):
        publish_to_nrrc.main()


def test_main_rejects_traversal_charon_param_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Param paths that escape ``DATA_DIR`` are rejected before file access."""
    _set_prepare_repo_env(monkeypatch, tmp_path, charon_param_file_path="../outside.env")
    with pytest.raises(ValueError, match="must stay under"):
        publish_to_nrrc.main()


def test_module_main_guard_raises_system_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Executing the file as ``__main__`` runs the bottom ``raise SystemExit(main())``."""
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.setattr(sys, "argv", ["publish_to_nrrc.py"])
    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(
            str(Path(publish_to_nrrc.__file__)),
            run_name="__main__",
        )
    assert exc_info.value.code == 1
