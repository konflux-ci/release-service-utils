"""Tests for collect_gh_params."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import collect_gh_params


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _make_dirs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    binaries = data_dir / "binaries"
    binaries.mkdir()
    results = tmp_path / "results"
    results.mkdir()
    return data_dir, binaries, results, tmp_path


def _default_data() -> dict:
    return {"github": {"githubSecret": "my-secret"}}


def _default_snapshot() -> dict:
    return {"components": [{"source": {"git": {"url": "https://github.com/org/repo"}}}]}


def _setup_happy_path(
    tmp_path: Path,
    *,
    data: dict | None = None,
    snapshot: dict | None = None,
    sha_filename: str = "project_v1.0.0_SHA256SUMS",
) -> dict[str, Path]:
    data_dir, binaries, results, _ = _make_dirs(tmp_path)

    data_file = data_dir / "data.json"
    _write_json(data_file, data or _default_data())

    snapshot_file = data_dir / "snapshot.json"
    _write_json(snapshot_file, snapshot or _default_snapshot())

    (binaries / sha_filename).touch()

    return {
        "data_file": data_file,
        "snapshot_file": snapshot_file,
        "binaries_path": binaries,
        "result_repository": results / "repository",
        "result_release_version": results / "release_version",
        "result_github_secret": results / "github_secret",
    }


# --- collect_params happy path ---


def test_collect_params_happy_path(tmp_path: Path) -> None:
    """Writes correct values to all three result files."""
    paths = _setup_happy_path(tmp_path)
    rc = collect_gh_params.collect_params(**paths)

    assert rc == 0
    assert paths["result_github_secret"].read_text() == "my-secret"
    assert paths["result_repository"].read_text() == "https://github.com/org/repo"
    assert paths["result_release_version"].read_text() == "v1.0.0"


# --- collect_params error cases ---


def test_collect_params_missing_github_key(tmp_path: Path) -> None:
    """KeyError when 'github' key is absent from data."""
    paths = _setup_happy_path(tmp_path, data={"other": "value"})

    with pytest.raises(KeyError, match="github"):
        collect_gh_params.collect_params(**paths)


def test_collect_params_empty_github_secret(tmp_path: Path) -> None:
    """RuntimeError when githubSecret is an empty string."""
    paths = _setup_happy_path(tmp_path, data={"github": {"githubSecret": ""}})

    with pytest.raises(RuntimeError, match="No valid secret"):
        collect_gh_params.collect_params(**paths)


def test_collect_params_missing_github_secret_key(tmp_path: Path) -> None:
    """KeyError when 'githubSecret' key is absent."""
    paths = _setup_happy_path(tmp_path, data={"github": {"other": "val"}})

    with pytest.raises(KeyError, match="githubSecret"):
        collect_gh_params.collect_params(**paths)


def test_collect_params_no_sha256sums_file(tmp_path: Path) -> None:
    """StopIteration when no *_SHA256SUMS file exists."""
    paths = _setup_happy_path(tmp_path)
    sha_file = next(paths["binaries_path"].glob("*_SHA256SUMS"))
    sha_file.unlink()

    with pytest.raises(StopIteration):
        collect_gh_params.collect_params(**paths)


def test_collect_params_empty_components(tmp_path: Path) -> None:
    """IndexError when components list is empty."""
    paths = _setup_happy_path(tmp_path, snapshot={"components": []})

    with pytest.raises(IndexError):
        collect_gh_params.collect_params(**paths)


def test_collect_params_missing_components_key(tmp_path: Path) -> None:
    """KeyError when 'components' key is absent from snapshot."""
    paths = _setup_happy_path(tmp_path, snapshot={"other": "value"})

    with pytest.raises(KeyError, match="components"):
        collect_gh_params.collect_params(**paths)


# --- release version extraction ---


def test_release_version_standard_filename(tmp_path: Path) -> None:
    """Extracts version from standard filename."""
    paths = _setup_happy_path(tmp_path, sha_filename="project_v1.2.3_SHA256SUMS")
    collect_gh_params.collect_params(**paths)

    assert paths["result_release_version"].read_text() == "v1.2.3"


def test_release_version_multiple_underscores(tmp_path: Path) -> None:
    """Extracts version from filename with multiple underscores."""
    paths = _setup_happy_path(tmp_path, sha_filename="my_project_name_2.0.0_SHA256SUMS")
    collect_gh_params.collect_params(**paths)

    assert paths["result_release_version"].read_text() == "2.0.0"


def test_release_version_malformed_filename(tmp_path: Path) -> None:
    """RuntimeError when SHA256SUMS filename has no version segment."""
    paths = _setup_happy_path(tmp_path, sha_filename="noversion_SHA256SUMS")

    with pytest.raises(RuntimeError, match="Malformed SHA256SUMS filename"):
        collect_gh_params.collect_params(**paths)


# --- main ---


def test_main_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Returns 0 when all env vars and files are valid."""
    paths = _setup_happy_path(tmp_path)

    monkeypatch.setenv("DATA_DIR", str(paths["data_file"].parent))
    monkeypatch.setenv("DATA_PATH", paths["data_file"].name)
    monkeypatch.setenv("SNAPSHOT_PATH", paths["snapshot_file"].name)
    monkeypatch.setenv("BINARIES_PATH", "binaries")
    monkeypatch.setenv("RESULT_REPOSITORY", str(paths["result_repository"]))
    monkeypatch.setenv("RESULT_RELEASE_VERSION", str(paths["result_release_version"]))
    monkeypatch.setenv("RESULT_GITHUB_SECRET", str(paths["result_github_secret"]))

    assert collect_gh_params.main() == 0
    assert paths["result_github_secret"].read_text() == "my-secret"
    assert paths["result_repository"].read_text() == "https://github.com/org/repo"
    assert paths["result_release_version"].read_text() == "v1.0.0"


def test_main_missing_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """SystemExit when a required env var is missing."""
    monkeypatch.delenv("DATA_DIR", raising=False)

    with pytest.raises(SystemExit):
        collect_gh_params.main()
