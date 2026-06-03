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
    """Derive the clone directory name from a repository URL."""
    assert git.repository_workdir_name(url) == expected


def test_append_git_stderr_skips_when_path_none() -> None:
    """Do nothing when no stderr log path is configured."""
    git._append_git_stderr(None, GitCommandError(["git"], 1, "err"))


def test_append_git_stderr_skips_empty_message(tmp_path: Path) -> None:
    """Do not create a log file when the error message is blank."""
    log = tmp_path / "log.txt"
    git._append_git_stderr(log, Exception("   "))
    assert not log.exists()


def test_append_git_stderr_uses_str_when_no_stderr_attr(tmp_path: Path) -> None:
    """Append `str(exc)` when the exception has no `stderr` attribute."""
    log = tmp_path / "log.txt"
    git._append_git_stderr(log, Exception("plain"))
    assert "plain" in log.read_text(encoding="utf-8")


def test_append_git_stderr_redacts_oauth2_url(tmp_path: Path) -> None:
    """Mask oauth2 tokens embedded in clone URLs written to the stderr log."""
    log = tmp_path / "log.txt"
    git._append_git_stderr(
        log,
        GitCommandError(
            ["git", "clone"],
            1,
            "fatal: https://oauth2:secret@gitlab.com/g/r.git",
        ),
    )
    text = log.read_text(encoding="utf-8")
    assert "secret" not in text
    assert "oauth2:[REDACTED]@" in text


def test_configure_git_global_user() -> None:
    """Set global `user.name` and `user.email` via GitPython."""
    with mock.patch("vcs.git.git.Git") as git_cls:
        git.configure_git_global_user("Name", "e@x.com")
    inst = git_cls.return_value
    inst.config.assert_any_call("--global", "user.name", "Name")
    inst.config.assert_any_call("--global", "user.email", "e@x.com")


def test_clone_shallow_sparse(tmp_path: Path) -> None:
    """Sparse-checkout clone checks out the requested revision."""
    repo_dir = tmp_path / "proj"
    mock_repo = mock.MagicMock()
    with mock.patch("vcs.git.git.Repo") as repo_cls:
        repo_cls.clone_from.return_value = mock_repo
        repo_cls.return_value = mock_repo
        out = git.clone(
            tmp_path,
            "https://x/proj.git",
            directory_name="proj",
            revision="main",
            sparse_dirs=["schema"],
        )
    assert out == repo_dir
    mock_repo.git.sparse_checkout.assert_called_once_with("set", "schema")
    mock_repo.git.checkout.assert_called_once_with("main")


def test_clone_appends_stderr(tmp_path: Path) -> None:
    """Write clone failures to the optional stderr log file."""
    log = tmp_path / "log.txt"
    with mock.patch(
        "vcs.git.git.Repo.clone_from", side_effect=GitCommandError(["git"], 1, "clone fail")
    ):
        with pytest.raises(GitCommandError):
            git.clone(
                tmp_path,
                "https://x/p.git",
                revision="main",
                sparse_dirs=["a"],
                stderr_path=log,
            )
    assert "clone fail" in log.read_text(encoding="utf-8")


def test_origin_ls_tree_name_only(tmp_path: Path) -> None:
    """Return `git ls-tree -r --name-only` output for a ref."""
    mock_repo = mock.MagicMock()
    mock_repo.git.ls_tree.return_value = "path/a\npath/b\n"
    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        out = git.origin_ls_tree_name_only(tmp_path, "origin/main", stderr_path=None)
    assert "path/a" in out


def test_origin_ls_tree_logs_stderr(tmp_path: Path) -> None:
    """Append `ls-tree` failures to the optional stderr log file."""
    log = tmp_path / "log.txt"
    with mock.patch("vcs.git.git.Repo") as repo_cls:
        repo_cls.return_value.git.ls_tree.side_effect = GitCommandError(["git"], 1, "ls fail")
        with pytest.raises(GitCommandError):
            git.origin_ls_tree_name_only(tmp_path, "origin/main", stderr_path=log)
    assert "ls fail" in log.read_text(encoding="utf-8")


def test_index_add_commit(tmp_path: Path) -> None:
    """Stage paths and commit without an explicit author."""
    mock_repo = mock.MagicMock()
    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        git.index_add_commit(tmp_path, ["a.yaml"], "msg", stderr_path=None)
    mock_repo.index.add.assert_called_once_with(["a.yaml"])
    mock_repo.index.commit.assert_called_once_with("msg")


def test_index_add_commit_logs_stderr(tmp_path: Path) -> None:
    """Append stage/commit failures to the optional stderr log file."""
    log = tmp_path / "log.txt"
    with mock.patch("vcs.git.git.Repo") as repo_cls:
        repo_cls.return_value.index.add.side_effect = GitCommandError(["git"], 1, "add fail")
        with pytest.raises(GitCommandError):
            git.index_add_commit(tmp_path, ["a.yaml"], "msg", stderr_path=log)
    assert "add fail" in log.read_text(encoding="utf-8")


def test_push_success_first_try(tmp_path: Path) -> None:
    """Return immediately when the first push succeeds."""
    mock_repo = mock.MagicMock()
    push_result = mock.MagicMock()
    mock_repo.remotes.origin.push.return_value = push_result
    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        git.push(tmp_path, "main", retries=2, stderr_path=None)
    mock_repo.remotes.origin.push.assert_called_once_with("main")
    push_result.raise_if_error.assert_called_once()


def test_push_rejected_ref(tmp_path: Path) -> None:
    """Push rejections that only set PushInfo flags must not look like success."""
    err_log = tmp_path / "err.log"
    mock_repo = mock.MagicMock()
    push_result = mock.MagicMock()
    push_result.raise_if_error.side_effect = GitCommandError(
        ["git", "push", "origin"],
        1,
        "error: failed to push some refs",
    )
    mock_repo.remotes.origin.push.return_value = push_result
    mock_repo.git.pull.return_value = ""

    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        with pytest.raises(subprocess.CalledProcessError):
            git.push(tmp_path, "main", retries=0, stderr_path=err_log)
    push_result.raise_if_error.assert_called_once()
    assert "failed to push" in err_log.read_text(encoding="utf-8")


def test_push_raises_after_limit(tmp_path: Path) -> None:
    """Raise `CalledProcessError` after pull/retry cycles are exhausted."""
    err_log = tmp_path / "err.log"
    mock_repo = mock.MagicMock()

    def _fail_push(*_args: object, **_kwargs: object) -> None:
        raise GitCommandError(["git", "push", "origin"], 1, "push failed")

    mock_repo.remotes.origin.push.side_effect = _fail_push
    mock_repo.git.pull.return_value = ""

    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        with pytest.raises(subprocess.CalledProcessError):
            git.push(tmp_path, "main", retries=1, stderr_path=err_log)
    assert mock_repo.remotes.origin.push.call_count == 2
    assert "push failed" in err_log.read_text(encoding="utf-8")


def test_push_pull_failure_raises(tmp_path: Path) -> None:
    """Re-raise when `pull --rebase` fails after a rejected push."""
    log = tmp_path / "log.txt"
    mock_repo = mock.MagicMock()
    mock_repo.remotes.origin.push.side_effect = GitCommandError(["git", "push"], 1, "push")
    mock_repo.git.pull.side_effect = GitCommandError(["git", "pull"], 1, "pull")

    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        with pytest.raises(GitCommandError):
            git.push(tmp_path, "main", retries=3, stderr_path=log)
    assert "pull" in log.read_text(encoding="utf-8")


def test_push_no_status_on_error(tmp_path: Path) -> None:
    """Default to exit code 1 when GitPython omits `status` on push failure."""
    mock_repo = mock.MagicMock()
    err = GitCommandError(["git", "push"], 0, "push")
    del err.status
    mock_repo.remotes.origin.push.side_effect = err
    mock_repo.git.pull.return_value = ""

    with mock.patch("vcs.git.git.Repo", return_value=mock_repo):
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            git.push(tmp_path, "main", retries=0, stderr_path=None)
    assert exc_info.value.returncode == 1
