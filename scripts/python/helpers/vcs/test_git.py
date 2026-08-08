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


def test_run_git_cmd_success(tmp_path: Path) -> None:
    """Return the subprocess result when the git command succeeds."""

    def _fake_run(
        argv: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout="ok\n",
            stderr="",
        )

    with mock.patch("vcs.git.subprocess.run", side_effect=_fake_run):
        result = git._run_git_cmd(["git", "status"], cwd=tmp_path)
    assert result.returncode == 0
    assert result.stdout == "ok\n"


def test_configure_git_global_user() -> None:
    """Set global `user.name` and `user.email` via the git CLI."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.configure_git_global_user("Name", "e@x.com")
    assert run_cmd.call_count == 2


def test_clone_full(tmp_path: Path) -> None:
    """Clone a repository into a named directory under *parent_dir*."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        out = git.clone(
            tmp_path,
            "https://github.com/o/r.git",
            directory_name="cloned",
        )
    run_cmd.assert_called_once()
    assert run_cmd.call_args.args[0][:2] == ["git", "clone"]
    assert out == tmp_path / "cloned"


def test_clone_creates_parent_dir(tmp_path: Path) -> None:
    """Create *parent_dir* when it is missing before running `git clone`."""
    parent_dir = tmp_path / "nested" / "work"
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        out = git.clone(parent_dir, "https://github.com/o/r.git", directory_name="cloned")
    assert parent_dir.is_dir()
    run_cmd.assert_called_once()
    assert out == parent_dir / "cloned"


def test_clone_rejects_existing_destination(tmp_path: Path) -> None:
    """Raise when the clone target directory already exists."""
    (tmp_path / "cloned").mkdir()
    with pytest.raises(FileExistsError, match="clone destination already exists"):
        git.clone(tmp_path, "https://github.com/o/r.git", directory_name="cloned")


def test_clone_shallow_sparse(tmp_path: Path) -> None:
    """Shallow sparse clone checks out the requested revision."""
    repo_dir = tmp_path / "proj"
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        out = git.clone(
            tmp_path,
            "https://x/proj.git",
            directory_name="proj",
            revision="main",
            sparse_dirs=["schema"],
            shallow=True,
        )
    assert out == repo_dir
    assert run_cmd.call_count == 3


def test_clone_shallow_full(tmp_path: Path) -> None:
    """Shallow clone without sparse dirs checks out the full tree at depth 1."""
    repo_dir = tmp_path / "proj"
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        out = git.clone(
            tmp_path,
            "https://x/proj.git",
            directory_name="proj",
            revision="main",
            shallow=True,
        )
    assert out == repo_dir
    run_cmd.assert_called_once_with(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            "main",
            "https://x/proj.git",
            str(repo_dir),
        ],
        cwd=tmp_path,
        stderr_path=None,
    )


def test_clone_shallow_requires_revision(tmp_path: Path) -> None:
    """Shallow clones require a revision."""
    with pytest.raises(ValueError, match="revision"):
        git.clone(tmp_path, "https://x/p.git", shallow=True)


def test_clone_appends_stderr(tmp_path: Path) -> None:
    """Write clone failures to the optional stderr log file."""
    log = tmp_path / "log.txt"
    with mock.patch.object(
        git,
        "_run_git_cmd",
        side_effect=subprocess.CalledProcessError(1, "git clone"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            git.clone(tmp_path, "https://x/p.git", stderr_path=log)


def test_fetch(tmp_path: Path) -> None:
    """Fetch one or more refs from a remote."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.fetch(tmp_path, "origin", "main", "my-app")
    assert run_cmd.call_args_list == [
        mock.call(
            ["git", "fetch", "origin", "main"],
            cwd=tmp_path,
            stderr_path=None,
        ),
        mock.call(
            ["git", "fetch", "origin", "my-app"],
            cwd=tmp_path,
            stderr_path=None,
        ),
    ]


def test_checkout_existing_branch(tmp_path: Path) -> None:
    """Check out a branch that already exists locally."""
    with (
        mock.patch.object(git, "_local_branch_exists", return_value=True),
        mock.patch.object(git, "_run_git_cmd") as run_cmd,
    ):
        git.checkout(tmp_path, "my-app")
    run_cmd.assert_called_once_with(
        ["git", "checkout", "my-app"],
        cwd=tmp_path,
        stderr_path=None,
    )


def test_checkout_creates_when_missing(tmp_path: Path) -> None:
    """Create and check out a branch when it is not present locally."""
    with (
        mock.patch.object(git, "_local_branch_exists", return_value=False),
        mock.patch.object(git, "_run_git_cmd") as run_cmd,
    ):
        git.checkout(tmp_path, "my-app", start_point="origin/main")
    run_cmd.assert_called_once_with(
        ["git", "checkout", "-b", "my-app", "origin/main"],
        cwd=tmp_path,
        stderr_path=None,
    )


def test_checkout_reset_requires_start_point(tmp_path: Path) -> None:
    """Reset checkout requires a start ref."""
    with pytest.raises(ValueError, match="start_point"):
        git.checkout(tmp_path, "my-app", reset=True)


def test_checkout_reset(tmp_path: Path) -> None:
    """Reset checkout recreates the branch from a start ref."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.checkout(tmp_path, "my-app", reset=True, start_point="origin/main")
    run_cmd.assert_called_once_with(
        ["git", "checkout", "-B", "my-app", "origin/main"],
        cwd=tmp_path,
        stderr_path=None,
    )


def test_sync_to_origin_main(tmp_path: Path) -> None:
    """Fetch and hard-reset to the remote default branch."""
    with (
        mock.patch.object(git, "fetch") as fetch,
        mock.patch.object(git, "_run_git_cmd") as run_cmd,
    ):
        git.sync_to_origin_main(tmp_path)
    fetch.assert_called_once_with(tmp_path, "origin", "main", stderr_path=None)
    assert run_cmd.call_args_list[0].args[0] == ["git", "checkout", "main"]
    assert run_cmd.call_args_list[1].args[0] == ["git", "reset", "--hard", "origin/main"]


def test_push_force_branch(tmp_path: Path) -> None:
    """Force-push a named branch via the git CLI."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.push(tmp_path, branch="my-branch", force=True)
    run_cmd.assert_called_once_with(
        ["git", "push", "origin", "--force", "my-branch"],
        cwd=tmp_path,
        stderr_path=None,
    )


def test_push_retries_success_first_try(tmp_path: Path) -> None:
    """Return immediately when the first push succeeds."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.push(
            tmp_path,
            branch="main",
            retries=2,
            rebase_branch="main",
            stderr_path=None,
        )
    run_cmd.assert_called_once()


def test_push_retries_raises_after_limit(tmp_path: Path) -> None:
    """Raise `CalledProcessError` after pull/retry cycles are exhausted."""
    err_log = tmp_path / "err.log"

    def _cmd_side_effect(cmd: list[str], **_kwargs: object) -> mock.MagicMock:
        if "pull" in cmd:
            return mock.MagicMock()
        raise subprocess.CalledProcessError(1, "git push")

    with mock.patch.object(git, "_run_git_cmd", side_effect=_cmd_side_effect):
        with pytest.raises(subprocess.CalledProcessError):
            git.push(
                tmp_path,
                branch="main",
                retries=1,
                rebase_branch="main",
                stderr_path=err_log,
            )


def test_push_pull_failure_raises(tmp_path: Path) -> None:
    """Re-raise when CLI `pull --rebase` fails after a rejected push."""
    log = tmp_path / "log.txt"
    push_error = subprocess.CalledProcessError(1, "git push")
    pull_error = subprocess.CalledProcessError(1, "git pull")

    def _cmd_side_effect(cmd: list[str], **_kwargs: object) -> mock.MagicMock:
        if "pull" in cmd:
            raise pull_error
        raise push_error

    with mock.patch.object(git, "_run_git_cmd", side_effect=_cmd_side_effect):
        with pytest.raises(subprocess.CalledProcessError):
            git.push(
                tmp_path,
                branch="main",
                retries=3,
                rebase_branch="main",
                stderr_path=log,
            )


def test_push_retries_requires_rebase_branch(tmp_path: Path) -> None:
    """Raise when retries are requested without a rebase branch."""
    with pytest.raises(ValueError, match="rebase_branch"):
        git.push(tmp_path, retries=1)


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
    assert run_cmd.call_args_list[1].args[0] == ["git", "commit", "-m", "msg"]


def test_index_add_commit_stage_only(tmp_path: Path) -> None:
    """When ``commit=False``, paths are staged but not committed."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.index_add_commit(tmp_path, ["a.yaml"], "", commit=False)
    run_cmd.assert_called_once_with(
        ["git", "add", "a.yaml"],
        cwd=tmp_path,
        stderr_path=None,
    )


def test_working_tree_diff_cached(tmp_path: Path) -> None:
    """Return cached diff output from the git CLI."""
    with mock.patch.object(
        git,
        "_run_git_cmd",
        return_value=mock.MagicMock(stdout="diff\n"),
    ):
        assert git.working_tree_diff(tmp_path, cached=True) == "diff\n"


def test_working_tree_diff_cached_with_other_ref(tmp_path: Path) -> None:
    """Compare cached diff against another ref when requested."""
    with mock.patch.object(
        git,
        "_run_git_cmd",
        return_value=mock.MagicMock(stdout="diff\n"),
    ) as run_cmd:
        assert git.working_tree_diff(tmp_path, cached=True, other_ref="mr_1") == "diff\n"
    assert run_cmd.call_args.args[0] == ["git", "diff", "--cached", "mr_1"]


def test_rebase_onto_remote_adds_missing_remote(tmp_path: Path) -> None:
    """Add remote when missing, fetch, and rebase."""
    calls: list[list[str]] = []

    def _cmd_side_effect(cmd: list[str], **_kwargs: object) -> mock.MagicMock:
        calls.append(cmd)
        if cmd[:2] == ["git", "remote"] and len(cmd) == 2:
            return mock.MagicMock(stdout="origin\n")
        return mock.MagicMock(stdout="")

    with mock.patch.object(git, "_run_git_cmd", side_effect=_cmd_side_effect):
        git.rebase_onto_remote(
            tmp_path,
            remote_name="glab-base",
            remote_repository="https://gitlab.com/g/up.git",
            revision="main",
        )
    assert ["git", "remote", "add", "glab-base", "https://gitlab.com/g/up.git"] in calls
    assert ["git", "fetch", "glab-base", "main"] in calls
    assert ["git", "rebase", "glab-base/main"] in calls


def test_rebase_onto_remote_skips_existing_remote(tmp_path: Path) -> None:
    """Skip ``git remote add`` when the remote already exists."""
    calls: list[list[str]] = []

    def _cmd_side_effect(cmd: list[str], **_kwargs: object) -> mock.MagicMock:
        calls.append(cmd)
        if cmd[:2] == ["git", "remote"] and len(cmd) == 2:
            return mock.MagicMock(stdout="glab-base\n")
        return mock.MagicMock(stdout="")

    with mock.patch.object(git, "_run_git_cmd", side_effect=_cmd_side_effect):
        git.rebase_onto_remote(
            tmp_path,
            remote_name="glab-base",
            remote_repository="https://gitlab.com/g/up.git",
            revision="main",
        )
    assert not any(cmd[:3] == ["git", "remote", "add"] for cmd in calls)


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
        rebase_branch="main",
        stderr_path=None,
    )


def test_working_tree_diff(tmp_path: Path) -> None:
    """Return `git diff` output for the working tree."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        run_cmd.return_value = mock.MagicMock(stdout="-    newTag: old\n")
        assert git.working_tree_diff(tmp_path) == "-    newTag: old\n"


def test_changed_paths_from_status(tmp_path: Path) -> None:
    """Parse porcelain status lines into repo-relative paths."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        run_cmd.return_value = mock.MagicMock(
            stdout=" M path/a.yaml\n?? path/b.yaml\n",
        )
        out = git.changed_paths_from_status(tmp_path)
    assert out == ["path/a.yaml", "path/b.yaml"]


def test_changed_paths_from_status_skips_short_lines(tmp_path: Path) -> None:
    """Ignore malformed porcelain lines shorter than four characters."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        run_cmd.return_value = mock.MagicMock(stdout="??\n M ok.yaml\n")
        out = git.changed_paths_from_status(tmp_path)
    assert out == ["ok.yaml"]


def test_set_remote_url(tmp_path: Path) -> None:
    """Update the URL for a named remote."""
    with mock.patch.object(git, "_run_git_cmd") as run_cmd:
        git.set_remote_url(tmp_path, "origin", "https://example/repo.git")
    run_cmd.assert_called_once_with(
        ["git", "remote", "set-url", "origin", "https://example/repo.git"],
        cwd=tmp_path,
        stderr_path=None,
    )
