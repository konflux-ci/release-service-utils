"""Tests for `vcs.git`."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest import mock

import pytest
from git.exc import GitCommandError

from . import git


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://gitlab.com/org/repo.git", "repo"),
        ("https://gitlab.com/org/repo/", "repo"),
        ("https://gitlab.com/org/repo.git/", "repo"),
        ("https://gitlab.com/org/repo", "repo"),
    ],
)
def test_repository_workdir_name(url: str, expected: str) -> None:
    assert git.repository_workdir_name(url) == expected


def test_append_git_stderr_skips_when_path_none() -> None:
    git._append_git_stderr(None, GitCommandError(["git"], 1, "err"))


def test_append_git_stderr_skips_empty_message(tmp_path: Path) -> None:
    log = tmp_path / "log.txt"
    git._append_git_stderr(log, Exception("   "))
    assert not log.exists()


def test_append_git_stderr_uses_str_when_no_stderr_attr(tmp_path: Path) -> None:
    log = tmp_path / "log.txt"
    git._append_git_stderr(log, Exception("plain"))
    assert "plain" in log.read_text(encoding="utf-8")


def test_configure_git_global_user() -> None:
    with mock.patch("vcs.git.git.Git") as git_cls:
        git.configure_git_global_user("Name", "e@x.com")
    inst = git_cls.return_value
    inst.config.assert_any_call("--global", "user.name", "Name")
    inst.config.assert_any_call("--global", "user.email", "e@x.com")


def test_clone_sparse_shallow_success(tmp_path: Path) -> None:
    repo_dir = tmp_path / "proj"
    mock_repo = mock.MagicMock()
    with mock.patch("vcs.git.git.Repo") as repo_cls:
        repo_cls.clone_from.return_value = mock_repo
        repo_cls.return_value = mock_repo
        out = git.clone_sparse_shallow(
            clone_url="https://x/proj.git",
            repo_url_for_dir_name="https://gitlab.com/g/proj.git",
            revision="main",
            sparse_dirs=["schema"],
            parent_dir=tmp_path,
            stderr_path=None,
        )
    assert out == repo_dir
    mock_repo.git.sparse_checkout.assert_called_once_with("set", "schema")
    mock_repo.git.checkout.assert_called_once_with("main")


def test_clone_sparse_shallow_appends_stderr(tmp_path: Path) -> None:
    log = tmp_path / "log.txt"
    with mock.patch(
        "vcs.git.git.Repo.clone_from", side_effect=GitCommandError(["git"], 1, "clone fail")
    ):
        with pytest.raises(GitCommandError):
            git.clone_sparse_shallow(
                clone_url="https://x/p.git",
                repo_url_for_dir_name="https://gitlab.com/g/p.git",
                revision="main",
                sparse_dirs=["a"],
                parent_dir=tmp_path,
                stderr_path=log,
            )
    assert "clone fail" in log.read_text(encoding="utf-8")


def test_origin_ls_tree_name_only(tmp_path: Path) -> None:
    mock_repo = mock.MagicMock()
    mock_repo.git.ls_tree.return_value = "path/a\npath/b\n"
    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        out = git.origin_ls_tree_name_only(tmp_path, "origin/main", stderr_path=None)
    assert "path/a" in out


def test_origin_ls_tree_logs_stderr(tmp_path: Path) -> None:
    log = tmp_path / "log.txt"
    with mock.patch("vcs.git.git.Repo") as repo_cls:
        repo_cls.return_value.git.ls_tree.side_effect = GitCommandError(["git"], 1, "ls fail")
        with pytest.raises(GitCommandError):
            git.origin_ls_tree_name_only(tmp_path, "origin/main", stderr_path=log)
    assert "ls fail" in log.read_text(encoding="utf-8")


def test_index_add_commit(tmp_path: Path) -> None:
    mock_repo = mock.MagicMock()
    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        git.index_add_commit(tmp_path, ["a.yaml"], "msg", stderr_path=None)
    mock_repo.index.add.assert_called_once_with(["a.yaml"])
    mock_repo.index.commit.assert_called_once_with("msg")


def test_index_add_commit_logs_stderr(tmp_path: Path) -> None:
    log = tmp_path / "log.txt"
    with mock.patch("vcs.git.git.Repo") as repo_cls:
        repo_cls.return_value.index.add.side_effect = GitCommandError(["git"], 1, "add fail")
        with pytest.raises(GitCommandError):
            git.index_add_commit(tmp_path, ["a.yaml"], "msg", stderr_path=log)
    assert "add fail" in log.read_text(encoding="utf-8")


def test_push_origin_with_rebase_retries_success_first_try(tmp_path: Path) -> None:
    mock_repo = mock.MagicMock()
    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        git.push_origin_with_rebase_retries(tmp_path, "main", retries=2, stderr_path=None)
    mock_repo.remotes.origin.push.assert_called_once()


def test_push_origin_with_rebase_retries_raises_after_limit(tmp_path: Path) -> None:
    err_log = tmp_path / "err.log"
    mock_repo = mock.MagicMock()

    def _fail_push() -> None:
        raise GitCommandError(["git", "push", "origin"], 1, "push failed")

    mock_repo.remotes.origin.push.side_effect = _fail_push
    mock_repo.git.pull.return_value = ""

    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        with pytest.raises(subprocess.CalledProcessError):
            git.push_origin_with_rebase_retries(
                tmp_path,
                "main",
                retries=1,
                stderr_path=err_log,
            )
    assert mock_repo.remotes.origin.push.call_count == 2
    assert "push failed" in err_log.read_text(encoding="utf-8")


def test_push_origin_pull_failure_raises(tmp_path: Path) -> None:
    log = tmp_path / "log.txt"
    mock_repo = mock.MagicMock()
    mock_repo.remotes.origin.push.side_effect = GitCommandError(["git", "push"], 1, "push")
    mock_repo.git.pull.side_effect = GitCommandError(["git", "pull"], 1, "pull")

    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        with pytest.raises(GitCommandError):
            git.push_origin_with_rebase_retries(tmp_path, "main", retries=3, stderr_path=log)
    assert "pull" in log.read_text(encoding="utf-8")


def test_push_origin_no_status_on_error(tmp_path: Path) -> None:
    mock_repo = mock.MagicMock()
    err = GitCommandError(["git", "push"], 0, "push")
    del err.status
    mock_repo.remotes.origin.push.side_effect = err
    mock_repo.git.pull.return_value = ""

    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            git.push_origin_with_rebase_retries(tmp_path, "main", retries=0, stderr_path=None)
    assert exc_info.value.returncode == 1
