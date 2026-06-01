"""Host-agnostic Git operations via GitPython."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TypeVar

import git
from git.exc import GitCommandError

_T = TypeVar("_T")


def repository_workdir_name(repository_url: str) -> str:
    """Directory name for *repository_url* (strip `.git` suffix)."""
    base = Path(repository_url.rstrip("/")).name
    if base.endswith(".git"):
        return base[: -len(".git")]
    return base


def _append_git_stderr(stderr_path: Path | None, exc: BaseException) -> None:
    # GitPython does not tee subprocess stderr to a file; append on failure only.
    if stderr_path is None:
        return
    err = getattr(exc, "stderr", None)
    if err is None:
        err = str(exc)
    if not str(err).strip():
        return
    safe_text = str(err)
    # Git errors often echo clone URLs with embedded oauth2 / token credentials.
    safe_text = re.sub(
        r"https://([^/@\s:]+):([^@\s]+)@",
        r"https://\1:[REDACTED]@",
        safe_text,
        flags=re.IGNORECASE,
    )
    with open(
        stderr_path,
        "a",
        encoding="utf-8",
        errors="replace",
    ) as errf:
        errf.write(f"\n{safe_text}\n")


def _with_git_stderr(stderr_path: Path | None, action: Callable[[], _T]) -> _T:
    try:
        return action()
    except GitCommandError as exc:
        _append_git_stderr(stderr_path, exc)
        raise


def configure_git_global_user(name: str, email: str) -> None:
    """Set `user.name` and `user.email` in the global Git config."""
    git_global = git.Git()
    git_global.config("--global", "user.name", name)
    git_global.config("--global", "user.email", email)


def clone(
    parent_dir: Path,
    clone_url: str,
    *,
    directory_name: str | None = None,
    revision: str,
    sparse_dirs: Sequence[str],
    stderr_path: Path | None = None,
) -> Path:
    """
    Shallow clone *clone_url* with sparse checkout of *sparse_dirs* at *revision*.
    """
    dir_name = directory_name or repository_workdir_name(clone_url)
    repo_dir = parent_dir / dir_name

    def _clone() -> Path:
        git.Repo.clone_from(
            clone_url,
            str(repo_dir),
            multi_options=[
                "--filter=blob:none",
                "--no-checkout",
                "--depth",
                "1",
                "--branch",
                revision,
            ],
        )
        repo = git.Repo(repo_dir)
        # Sparse paths must be set after clone metadata exists, before checkout.
        repo.git.sparse_checkout("set", *sparse_dirs)
        repo.git.checkout(revision)
        return repo_dir

    return _with_git_stderr(stderr_path, _clone)


def origin_ls_tree_name_only(
    repo_root: Path,
    ref: str,
    *,
    stderr_path: Path | None,
) -> str:
    """Return `git ls-tree -r --name-only` stdout for *ref*."""

    def _ls_tree() -> str:
        return git.Repo(repo_root).git.ls_tree("-r", "--name-only", ref)

    return _with_git_stderr(stderr_path, _ls_tree)


def index_add_commit(
    repo_root: Path,
    relative_paths: Sequence[str],
    message: str,
    *,
    stderr_path: Path | None,
) -> None:
    """Stage *relative_paths* and commit with *message*."""

    def _commit() -> None:
        repo = git.Repo(repo_root)
        repo.index.add(list(relative_paths))
        repo.index.commit(message)

    _with_git_stderr(stderr_path, _commit)


def push(
    repo_root: Path,
    branch: str,
    *,
    remote: str = "origin",
    retries: int = 0,
    stderr_path: Path | None = None,
) -> None:
    """
    Push *branch* to *remote*.

    On rejection, run `pull --rebase` and retry until *retries* recovery cycles
    are exhausted, then raise `CalledProcessError`.
    """
    repo = git.Repo(repo_root)
    attempt = 0
    while True:
        try:
            remote_obj = getattr(repo.remotes, remote)
            remote_obj.push(branch).raise_if_error()
            return
        except GitCommandError as push_error:
            _append_git_stderr(stderr_path, push_error)
            attempt += 1
            try:
                repo.git.pull("--rebase", remote, branch)
            except GitCommandError as pull_error:
                _append_git_stderr(stderr_path, pull_error)
                raise
            # After each failed push: increment counter, pull, then retry push.
            # Stop and raise once `attempt` exceeds *retries*.
            if attempt > retries:
                code = getattr(push_error, "status", None)
                if code is None:
                    code = 1
                raise subprocess.CalledProcessError(
                    int(code),
                    f"git push {remote}",
                ) from push_error
