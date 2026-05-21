"""Host-agnostic Git operations via GitPython (clone, index, push, ls-tree)."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

import git
from git.exc import GitCommandError


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
    with open(
        stderr_path,
        "a",
        encoding="utf-8",
        errors="replace",
    ) as errf:
        errf.write(f"\n{err}\n")


def configure_git_global_user(name: str, email: str) -> None:
    """Set `user.name` and `user.email` in the global Git config."""
    git_global = git.Git()
    git_global.config("--global", "user.name", name)
    git_global.config("--global", "user.email", email)


def clone_sparse_shallow(
    *,
    clone_url: str,
    repo_url_for_dir_name: str,
    revision: str,
    sparse_dirs: Sequence[str],
    parent_dir: Path,
    stderr_path: Path | None,
) -> Path:
    """
    Shallow clone *clone_url* with sparse checkout of *sparse_dirs*.

    *repo_url_for_dir_name* is the display HTTPS URL (no credentials); its last
    path segment names the working directory under *parent_dir*.
    """
    clone_dir_name = repository_workdir_name(repo_url_for_dir_name)
    repo_dir = parent_dir / clone_dir_name
    try:
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
    except GitCommandError as exc:
        _append_git_stderr(stderr_path, exc)
        raise
    return repo_dir


def origin_ls_tree_name_only(
    repo_root: Path,
    ref: str,
    *,
    stderr_path: Path | None,
) -> str:
    """Return `git ls-tree -r --name-only` stdout for *ref*."""
    try:
        repo = git.Repo(repo_root)
        return repo.git.ls_tree("-r", "--name-only", ref)
    except GitCommandError as exc:
        _append_git_stderr(stderr_path, exc)
        raise


def index_add_commit(
    repo_root: Path,
    relative_paths: Sequence[str],
    message: str,
    *,
    stderr_path: Path | None,
) -> None:
    """Stage *relative_paths* and commit with *message*."""
    try:
        repo = git.Repo(repo_root)
        repo.index.add(list(relative_paths))
        repo.index.commit(message)
    except GitCommandError as exc:
        _append_git_stderr(stderr_path, exc)
        raise


def push_origin_with_rebase_retries(
    repo_root: Path,
    branch: str,
    *,
    retries: int,
    stderr_path: Path | None,
) -> None:
    """Push to `origin`; on failure run `pull --rebase` and retry the push.

    After each failed push, the remote branch is rebased locally and the push is
    tried again. If the push still fails after *retries* such cycles, raise
    `subprocess.CalledProcessError`.
    """
    repo = git.Repo(repo_root)
    attempt = 0
    while True:
        try:
            repo.remotes.origin.push()
            return
        except GitCommandError as push_error:
            _append_git_stderr(stderr_path, push_error)
            attempt += 1
            try:
                repo.git.pull("--rebase", "origin", branch)
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
                    "git push origin",
                ) from push_error
