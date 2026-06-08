"""Tests for `vcs.git`."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest import mock

import pytest

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


def test_append_cmd_stderr_skips_when_path_none() -> None:
    """Do nothing when no stderr log path is configured."""
    git._append_cmd_stderr(None, "err")


def test_append_cmd_stderr_redacts_oauth2_url(tmp_path: Path) -> None:
    """Mask oauth2 tokens embedded in clone URLs written to the stderr log."""
    log = tmp_path / "log.txt"
    git._append_cmd_stderr(
        log,
        "fatal: https://oauth2:secret@gitlab.com/g/r.git",
    )
    text = log.read_text(encoding="utf-8")
    assert "secret" not in text
    assert "oauth2:[REDACTED]@" in text


def test_redact_credential_urls() -> None:
    """Mask oauth2 tokens in arbitrary text (stderr or argv)."""
    text = "fatal: https://oauth2:secret@gitlab.com/g/r.git"
    redacted = git._redact_credential_urls(text)
    assert "secret" not in redacted
    assert "oauth2:[REDACTED]@" in redacted


def test_run_git_cmd_redacts_clone_failure_log(tmp_path: Path) -> None:
    """Failed clone must not write credential-bearing URLs to the stderr log."""
    log = tmp_path / "log.txt"
    clone_url = "https://oauth2:secret@gitlab.com/g/r.git"

    def _fake_run(
        argv: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            1,
            stdout="",
            stderr=f"fatal: unable to access {clone_url}/",
        )

    with mock.patch("vcs.git.subprocess.run", side_effect=_fake_run):
        with pytest.raises(subprocess.CalledProcessError):
            git._run_git_cmd(
                ["git", "clone", clone_url, str(tmp_path / "repo")],
                cwd=tmp_path,
                stderr_path=log,
            )
    text = log.read_text(encoding="utf-8")
    assert "secret" not in text
    assert "oauth2:[REDACTED]@" in text
    assert "command exited with failure" in text


def test_configure_git_global_user() -> None:
    """Set global `user.name` and `user.email` via the git CLI."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.configure_git_global_user("Name", "e@x.com")
    assert run_cmd.call_count == 2


def test_clone_shallow_sparse(tmp_path: Path) -> None:
    """Sparse-checkout clone returns the repository root path."""
    repo_dir = tmp_path / "proj"
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        out = git.clone(
            tmp_path,
            "https://x/proj.git",
            directory_name="proj",
            revision="main",
            sparse_dirs=["schema"],
        )
    assert out == repo_dir
    assert run_cmd.call_count == 3


def test_clone_appends_stderr(tmp_path: Path) -> None:
    """Write clone failures to the optional stderr log file."""
    log = tmp_path / "log.txt"
    with mock.patch.object(
        git,
        "_run_git_cmd",
        side_effect=subprocess.CalledProcessError(1, "git clone"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            git.clone(
                tmp_path,
                "https://x/p.git",
                revision="main",
                sparse_dirs=["a"],
                stderr_path=log,
            )


def test_origin_main_has_path_matching_found(tmp_path: Path) -> None:
    """Return True when a listing line matches *pattern*."""
    listing = tmp_path / "tree.txt"

    def _write_listing(*_args: object, **kwargs: object) -> mock.MagicMock:
        stdout = kwargs.get("stdout")
        assert stdout is not None
        stdout.write("data/advisories/t/2025/0042/advisory.yaml\n")
        return mock.MagicMock()

    with mock.patch.object(git, "_run_git_cmd", side_effect=_write_listing) as run_cmd:
        assert git.origin_main_has_path_matching(
            tmp_path,
            r"data/advisories/.*/2025/0042/",
            listing,
        )
    run_cmd.assert_called_once()
    assert run_cmd.call_args.args[0] == [
        "git",
        "ls-tree",
        "-r",
        "--name-only",
        "origin/main",
    ]
    assert run_cmd.call_args.kwargs["stdout"] is not None


def test_origin_main_has_path_matching_not_found(tmp_path: Path) -> None:
    """Return False when no listing line matches *pattern*."""
    listing = tmp_path / "tree.txt"

    def _write_listing(*_args: object, **kwargs: object) -> mock.MagicMock:
        stdout = kwargs.get("stdout")
        assert stdout is not None
        stdout.write("data/advisories/t/2025/0001/advisory.yaml\n")
        return mock.MagicMock()

    with mock.patch.object(git, "_run_git_cmd", side_effect=_write_listing):
        assert not git.origin_main_has_path_matching(
            tmp_path,
            r"data/advisories/.*/2025/9999/",
            listing,
        )


def test_index_add_commit(tmp_path: Path) -> None:
    """Stage paths and commit via the git CLI."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.index_add_commit(tmp_path, ["a.yaml"], "msg", stderr_path=None)
    assert run_cmd.call_count == 2


def test_commit_and_push(tmp_path: Path) -> None:
    """Stage, commit, and push via the git CLI."""
    with mock.patch.object(git, "index_add_commit") as add:
        with mock.patch.object(git, "push") as push:
            git.commit_and_push(
                tmp_path,
                ["a.yaml"],
                "msg",
                "main",
                retries=2,
                stderr_path=None,
            )
    add.assert_called_once()
    push.assert_called_once_with(
        tmp_path,
        "main",
        remote="origin",
        retries=2,
        stderr_path=None,
    )


def test_push_success_first_try(tmp_path: Path) -> None:
    """Return immediately when the first CLI push succeeds."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.push(tmp_path, "main", retries=2, stderr_path=None)
    run_cmd.assert_called_once()


def test_push_retries_after_rejection_with_backoff(tmp_path: Path) -> None:
    """Retry push after `pull --rebase` with exponential backoff between attempts."""
    sleeps: list[float] = []
    push_calls = {"n": 0}
    pull_calls = {"n": 0}

    def _cmd_side_effect(cmd: list[str], **_kwargs: object) -> mock.MagicMock:
        if "pull" in cmd:
            pull_calls["n"] += 1
            return mock.MagicMock()
        push_calls["n"] += 1
        raise subprocess.CalledProcessError(1, "git push")

    with mock.patch("retry.time.sleep", side_effect=sleeps.append):
        with mock.patch.object(
            git,
            "_run_git_cmd",
            side_effect=_cmd_side_effect,
        ):
            with pytest.raises(subprocess.CalledProcessError):
                git.push(tmp_path, "main", retries=1, stderr_path=None)
    assert push_calls["n"] == 2
    assert pull_calls["n"] == 1
    assert sleeps == [5]


def test_push_zero_retries_skips_pull(tmp_path: Path) -> None:
    """Do not pull when push fails and no recovery retries are configured."""
    pull_calls = {"n": 0}

    def _cmd_side_effect(cmd: list[str], **_kwargs: object) -> mock.MagicMock:
        if "pull" in cmd:
            pull_calls["n"] += 1
            return mock.MagicMock()
        raise subprocess.CalledProcessError(1, "git push")

    with mock.patch.object(git, "_run_git_cmd", side_effect=_cmd_side_effect):
        with pytest.raises(subprocess.CalledProcessError):
            git.push(tmp_path, "main", retries=0, stderr_path=None)
    assert pull_calls["n"] == 0


def test_push_raises_after_limit(tmp_path: Path) -> None:
    """Raise `CalledProcessError` after pull/retry cycles are exhausted."""
    err_log = tmp_path / "err.log"

    def _always_fail(*_args: object, **_kwargs: object) -> None:
        raise subprocess.CalledProcessError(1, "git push")

    with mock.patch.object(git, "_run_git_cmd", side_effect=_always_fail):
        with pytest.raises(subprocess.CalledProcessError):
            git.push(tmp_path, "main", retries=1, stderr_path=err_log)


def test_push_pull_failure_raises(tmp_path: Path) -> None:
    """Re-raise when CLI `pull --rebase` fails after a rejected push."""
    log = tmp_path / "log.txt"
    push_error = subprocess.CalledProcessError(1, "git push")
    pull_error = subprocess.CalledProcessError(1, "git pull")

    def _cmd_side_effect(cmd: list[str], **_kwargs: object) -> mock.MagicMock:
        if "pull" in cmd:
            raise pull_error
        raise push_error

    with mock.patch.object(
        git,
        "_run_git_cmd",
        side_effect=_cmd_side_effect,
    ):
        with pytest.raises(subprocess.CalledProcessError):
            git.push(tmp_path, "main", retries=3, stderr_path=log)
