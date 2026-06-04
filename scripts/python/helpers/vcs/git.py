"""Host-agnostic Git operations via the `git` CLI."""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import IO, Any

import retry


def repository_workdir_name(repository_url: str) -> str:
    """Directory name for *repository_url* (strip `.git` suffix)."""
    base = Path(repository_url.rstrip("/")).name
    if base.endswith(".git"):
        return base[: -len(".git")]
    return base


def _redact_credential_urls(text: str) -> str:
    # Git errors often echo clone URLs with embedded oauth2 / token credentials.
    return re.sub(
        r"https://([^/@\s:]+):([^@\s]+)@",
        r"https://\1:[REDACTED]@",
        text,
        flags=re.IGNORECASE,
    )


def _append_cmd_stderr(stderr_path: Path | None, message: str) -> None:
    # CLI git stderr is captured on failure and written here (redacted).
    if stderr_path is None or not message.strip():
        return
    safe_text = _redact_credential_urls(str(message))
    with open(
        stderr_path,
        "a",
        encoding="utf-8",
        errors="replace",
    ) as errf:
        errf.write(f"\n{safe_text}\n")


def _run_git_cmd(
    cmd: Sequence[str | Path],
    *,
    cwd: Path | None = None,
    stderr_path: Path | None = None,
    check: bool = True,
    stdout: int | IO[Any] | None = subprocess.PIPE,
) -> subprocess.CompletedProcess[str]:
    """Run a git CLI command.

    Captures stderr in memory and, on failure, appends redacted stderr plus a
    redacted command line to *stderr_path*. Avoids `subprocess_cmd.run_cmd` so
    credential-bearing clone URLs are never logged verbatim.
    """
    argv = [str(x) for x in cmd]
    result = subprocess.run(
        argv,
        cwd=cwd,
        env=os.environ,
        stdout=stdout,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if stderr_path is not None:
            if result.stderr.strip():
                _append_cmd_stderr(stderr_path, result.stderr)
            _append_cmd_stderr(
                stderr_path,
                "command exited with failure: " + " ".join(argv),
            )
        if check:
            raise subprocess.CalledProcessError(
                result.returncode,
                argv,
                output=result.stdout,
                stderr=result.stderr,
            )
    return result


def configure_git_global_user(name: str, email: str) -> None:
    """Set `user.name` and `user.email` in the global Git config."""
    _run_git_cmd(["git", "config", "--global", "user.name", name])
    _run_git_cmd(["git", "config", "--global", "user.email", email])


def clone(
    parent_dir: Path,
    clone_url: str,
    *,
    directory_name: str | None = None,
    revision: str,
    sparse_dirs: Sequence[str],
    stderr_path: Path | None = None,
) -> Path:
    """Shallow clone *clone_url* with sparse checkout of *sparse_dirs* at *revision*.

    Returns the repository root directory.
    """
    dir_name = directory_name or repository_workdir_name(clone_url)
    repo_dir = parent_dir / dir_name
    _run_git_cmd(
        [
            "git",
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            "--depth",
            "1",
            "--branch",
            revision,
            clone_url,
            str(repo_dir),
        ],
        cwd=parent_dir,
        stderr_path=stderr_path,
    )
    _run_git_cmd(
        ["git", "sparse-checkout", "set", *sparse_dirs],
        cwd=repo_dir,
        stderr_path=stderr_path,
    )
    _run_git_cmd(
        ["git", "checkout", revision],
        cwd=repo_dir,
        stderr_path=stderr_path,
    )
    return repo_dir


def origin_main_has_path_matching(
    repo_root: Path,
    pattern: str,
    listing_path: Path,
    *,
    stderr_path: Path | None = None,
) -> bool:
    """Return True when `git ls-tree -r --name-only origin/main` has a matching line.

    Writes the tree listing to *listing_path* and scans it line-by-line with *pattern*
    so the full output is never held in the Python process (large monorepos can be
    tens of MB).
    """
    listing_path.parent.mkdir(parents=True, exist_ok=True)
    with open(listing_path, "w", encoding="utf-8") as listing_file:
        _run_git_cmd(
            ["git", "ls-tree", "-r", "--name-only", "origin/main"],
            cwd=repo_root,
            stderr_path=stderr_path,
            stdout=listing_file,
        )
    compiled = re.compile(pattern)
    with open(listing_path, encoding="utf-8", errors="replace") as listing_file:
        for line in listing_file:
            if compiled.search(line):
                return True
    return False


def index_add_commit(
    repo_root: Path,
    relative_paths: Sequence[str],
    message: str,
    *,
    stderr_path: Path | None = None,
) -> None:
    """Stage *relative_paths* and commit with *message* via the git CLI."""
    _run_git_cmd(
        ["git", "add", *relative_paths],
        cwd=repo_root,
        stderr_path=stderr_path,
    )
    _run_git_cmd(
        ["git", "commit", "-m", message],
        cwd=repo_root,
        stderr_path=stderr_path,
    )


def commit_and_push(
    repo_root: Path,
    relative_paths: Sequence[str],
    message: str,
    branch: str,
    *,
    remote: str = "origin",
    retries: int = 0,
    stderr_path: Path | None = None,
) -> None:
    """Stage, commit, and push *branch* via the git CLI (with pull --rebase retries)."""
    index_add_commit(
        repo_root,
        relative_paths,
        message,
        stderr_path=stderr_path,
    )
    push(
        repo_root,
        branch,
        remote=remote,
        retries=retries,
        stderr_path=stderr_path,
    )


def push(
    repo_root: Path,
    branch: str,
    *,
    remote: str = "origin",
    retries: int = 0,
    stderr_path: Path | None = None,
) -> None:
    """Push *branch* to *remote* via the git CLI.

    On rejection, run `pull --rebase` and retry with exponential backoff until
    *retries* recovery cycles are exhausted, then raise `CalledProcessError`.
    """
    max_attempts = retries + 1
    attempt = 0

    def _push_with_rebase() -> None:
        nonlocal attempt
        attempt += 1
        current_attempt = attempt
        try:
            _run_git_cmd(
                ["git", "push", remote, branch],
                cwd=repo_root,
                stderr_path=stderr_path,
            )
        except subprocess.CalledProcessError as push_error:
            if current_attempt >= max_attempts:
                raise
            # Pull failure is a CalledProcessError too; let it propagate as-is.
            _run_git_cmd(
                ["git", "pull", "--rebase", remote, branch],
                cwd=repo_root,
                stderr_path=stderr_path,
            )
            # Signal retry without using CalledProcessError (pull uses that too).
            raise RuntimeError("git push rejected") from push_error

    try:
        retry.retry_with_exponential_backoff(
            _push_with_rebase,
            max_attempts=max_attempts,
            retry_on=RuntimeError,
            base_sleep_seconds=5,
        )
    except RuntimeError as retry_error:
        cause = retry_error.__cause__
        if isinstance(cause, subprocess.CalledProcessError):
            raise cause from retry_error
        raise
