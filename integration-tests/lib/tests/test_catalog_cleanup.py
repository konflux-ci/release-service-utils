"""Unit tests for ``catalog_cleanup``."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import catalog_cleanup as uc


def test_ls_remote_head_parses_first_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse the first token of the first non-empty ls-remote line."""
    proc = subprocess.CompletedProcess(
        ["git", "ls-remote"],
        0,
        "abc123def456\trefs/heads/development\n",
        "",
    )
    monkeypatch.setattr(uc.subprocess, "run", MagicMock(return_value=proc))
    sha = uc._ls_remote_head(
        catalog_repo="konflux-ci/release-service-catalog",
        catalog_ref="development",
    )
    assert sha == "abc123def456"


def test_ls_remote_head_returns_empty_on_git_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return empty string when git ls-remote exits non-zero."""
    proc = subprocess.CompletedProcess(["git", "ls-remote"], 1, "", "err")
    monkeypatch.setattr(uc.subprocess, "run", MagicMock(return_value=proc))
    assert uc._ls_remote_head(catalog_repo="org/repo", catalog_ref="main") == ""


def test_ls_remote_head_returns_empty_when_no_output_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return empty string when stdout has no non-empty lines."""
    proc = subprocess.CompletedProcess(["git", "ls-remote"], 0, "\n\n", "")
    monkeypatch.setattr(uc.subprocess, "run", MagicMock(return_value=proc))
    assert uc._ls_remote_head(catalog_repo="org/repo", catalog_ref="main") == ""


def test_warn_catalog_drift_prints_when_head_differs(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Print a warning block when remote head differs from the base SHA."""
    monkeypatch.setattr(
        uc,
        "_ls_remote_head",
        lambda **_: "newsha",
    )
    uc._warn_catalog_drift(
        catalog_repo="org/repo",
        catalog_ref="dev",
        catalog_base_sha="oldsha",
    )
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "oldsha" in out and "newsha" in out


def test_warn_catalog_drift_silent_when_head_matches(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Print nothing when remote head equals the base SHA."""
    monkeypatch.setattr(uc, "_ls_remote_head", lambda **_: "same")
    uc._warn_catalog_drift(
        catalog_repo="org/repo",
        catalog_ref="dev",
        catalog_base_sha="same",
    )
    assert capsys.readouterr().out == ""


def test_warn_catalog_drift_silent_when_ls_remote_empty(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Print nothing when ls-remote yields no current SHA."""
    monkeypatch.setattr(uc, "_ls_remote_head", lambda **_: "")
    uc._warn_catalog_drift(
        catalog_repo="org/repo",
        catalog_ref="dev",
        catalog_base_sha="any",
    )
    assert capsys.readouterr().out == ""


def test_acquire_delete_repository_script_dir_uses_integration_tests_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use INTEGRATION_TESTS_SCRIPTS_DIR when delete-repository.sh exists there."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "delete-repository.sh").write_text("#!/bin/bash\n", encoding="utf-8")
    monkeypatch.setenv("INTEGRATION_TESTS_SCRIPTS_DIR", str(scripts))
    d, clone_root = uc._acquire_delete_repository_script_dir(
        catalog_repo="org/repo",
        catalog_ref="main",
    )
    assert d == scripts
    assert clone_root is None


def test_acquire_delete_repository_script_dir_clone_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Clone catalog layout and return scripts dir plus temp root to remove."""
    td = tmp_path / "clone-root"
    td.mkdir()
    clone_dest = td / "catalog"

    def fake_run(cmd: list, check: bool = False, timeout: object = None, **kwargs):
        if cmd[:2] == ["git", "clone"]:
            clone_dest.mkdir(parents=True)
            dr = clone_dest / "integration-tests" / "scripts" / "delete-repository.sh"
            dr.parent.mkdir(parents=True)
            dr.write_text("#!/bin/bash\n", encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, "", "")
        raise AssertionError(f"unexpected cmd: {cmd}")

    monkeypatch.delenv("INTEGRATION_TESTS_SCRIPTS_DIR", raising=False)
    monkeypatch.setattr(uc.tempfile, "mkdtemp", lambda **kw: str(td))
    monkeypatch.setattr(uc.subprocess, "run", fake_run)

    scripts_dir, clone_root = uc._acquire_delete_repository_script_dir(
        catalog_repo="org/repo",
        catalog_ref="main",
    )
    assert scripts_dir == clone_dest / "integration-tests" / "scripts"
    assert clone_root == td


def test_acquire_delete_repository_script_dir_clone_failure_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """On git clone failure, remove temp dir and exit with code 1."""
    td = tmp_path / "fail-root"
    td.mkdir()

    def fake_run(cmd: list, check: bool = False, timeout: object = None, **kwargs):
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.delenv("INTEGRATION_TESTS_SCRIPTS_DIR", raising=False)
    monkeypatch.setattr(uc.tempfile, "mkdtemp", lambda **kw: str(td))
    monkeypatch.setattr(uc.subprocess, "run", fake_run)

    with pytest.raises(SystemExit) as ei:
        uc._acquire_delete_repository_script_dir(
            catalog_repo="org/repo",
            catalog_ref="main",
        )
    assert ei.value.code == 1
    assert "git clone failed" in capsys.readouterr().err


def test_acquire_delete_repository_script_dir_missing_delete_script_after_clone_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 1 if clone succeeds but delete-repository.sh is absent."""
    td = tmp_path / "bad-clone"
    td.mkdir()

    def fake_run(cmd: list, check: bool = False, timeout: object = None, **kwargs):
        if cmd[:2] == ["git", "clone"]:
            (td / "catalog").mkdir()
            return subprocess.CompletedProcess(cmd, 0, "", "")
        raise AssertionError(f"unexpected cmd: {cmd}")

    monkeypatch.delenv("INTEGRATION_TESTS_SCRIPTS_DIR", raising=False)
    monkeypatch.setattr(uc.tempfile, "mkdtemp", lambda **kw: str(td))
    monkeypatch.setattr(uc.subprocess, "run", fake_run)

    with pytest.raises(SystemExit) as ei:
        uc._acquire_delete_repository_script_dir(
            catalog_repo="org/repo",
            catalog_ref="main",
        )
    assert ei.value.code == 1
    assert "delete-repository.sh missing" in capsys.readouterr().err


def test_main_runs_delete_and_removes_clone_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Run delete-repository.sh then rmtree the clone temp directory in finally."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "delete-repository.sh").write_text("#!/bin/bash\n", encoding="utf-8")
    clone_td = tmp_path / "td"
    clone_td.mkdir()

    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("TEMP_REPO_NAME", "org/temp-repo")
    monkeypatch.delenv("CATALOG_BASE_SHA", raising=False)
    monkeypatch.setattr(
        uc,
        "_acquire_delete_repository_script_dir",
        lambda **_: (scripts, clone_td),
    )

    calls: list[list[str]] = []

    def fake_run(cmd: list, check: bool = False, env: dict | None = None, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(uc.subprocess, "run", fake_run)

    uc.main()

    assert any(c[:2] == ["bash", str(scripts / "delete-repository.sh")] for c in calls)
    assert "org/temp-repo" in calls[-1]
    assert "Deleting temporary repo" in capsys.readouterr().out
    assert not clone_td.exists()


def test_main_skips_drift_when_no_base_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not call drift warning when CATALOG_BASE_SHA is unset."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "delete-repository.sh").write_text("#!/bin/bash\n", encoding="utf-8")

    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("TEMP_REPO_NAME", "org/r")
    monkeypatch.delenv("CATALOG_BASE_SHA", raising=False)
    monkeypatch.setattr(
        uc,
        "_acquire_delete_repository_script_dir",
        lambda **_: (scripts, None),
    )

    with patch.object(uc, "_warn_catalog_drift") as mock_warn:
        monkeypatch.setattr(
            uc.subprocess,
            "run",
            MagicMock(return_value=subprocess.CompletedProcess([], 0, "", "")),
        )
        uc.main()

    mock_warn.assert_not_called()


def test_main_calls_warn_when_catalog_base_sha_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Call drift warning when CATALOG_BASE_SHA is set."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "delete-repository.sh").write_text("#!/bin/bash\n", encoding="utf-8")

    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("TEMP_REPO_NAME", "org/r")
    monkeypatch.setenv("CATALOG_BASE_SHA", "abc")
    monkeypatch.setenv("CATALOG_REPO", "org/catalog")
    monkeypatch.setenv("CATALOG_REF", "dev")
    monkeypatch.setattr(
        uc,
        "_acquire_delete_repository_script_dir",
        lambda **_: (scripts, None),
    )

    with patch.object(uc, "_warn_catalog_drift") as mock_warn:
        monkeypatch.setattr(
            uc.subprocess,
            "run",
            MagicMock(return_value=subprocess.CompletedProcess([], 0, "", "")),
        )
        uc.main()

    mock_warn.assert_called_once_with(
        catalog_repo="org/catalog",
        catalog_ref="dev",
        catalog_base_sha="abc",
    )


def test_main_delete_failure_exits_with_return_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Propagate delete-repository.sh non-zero exit as process exit code."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "delete-repository.sh").write_text("#!/bin/bash\n", encoding="utf-8")

    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("TEMP_REPO_NAME", "org/r")
    monkeypatch.setattr(
        uc,
        "_acquire_delete_repository_script_dir",
        lambda **_: (scripts, None),
    )

    def fake_run(cmd: list, check: bool = False, **kwargs):
        raise subprocess.CalledProcessError(7, cmd)

    monkeypatch.setattr(uc.subprocess, "run", fake_run)

    with pytest.raises(SystemExit) as ei:
        uc.main()
    assert ei.value.code == 7


def test_main_delete_timeout_exits_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 1 when delete-repository.sh exceeds subprocess timeout."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "delete-repository.sh").write_text("#!/bin/bash\n", encoding="utf-8")

    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("TEMP_REPO_NAME", "org/r")
    monkeypatch.setattr(
        uc,
        "_acquire_delete_repository_script_dir",
        lambda **_: (scripts, None),
    )

    def fake_run(cmd: list, check: bool = False, timeout: object = None, **kwargs):
        raise subprocess.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr(uc.subprocess, "run", fake_run)

    with pytest.raises(SystemExit) as ei:
        uc.main()
    assert ei.value.code == 1
    assert "timed out" in capsys.readouterr().err
