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
    """Redact oauth2/token credentials embedded in HTTPS URLs in *text*."""
    return re.sub(
        r"https://([^/@\s:]+):([^@\s]+)@",
        r"https://\1:[REDACTED]@",
        text,
        flags=re.IGNORECASE,
    )


def _append_cmd_stderr(stderr_path: Path | None, message: str) -> None:
    """Append redacted *message* to *stderr_path* when configured."""
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
    revision: str | None = None,
    sparse_dirs: Sequence[str] | None = None,
    shallow: bool = False,
    stderr_path: Path | None = None,
) -> Path:
    """Clone *clone_url* into *parent_dir*.

    When *shallow* is true, perform a blob-filtered shallow clone and optional
    sparse checkout of *sparse_dirs* at *revision*.

    Creates *parent_dir* when it does not exist yet.
    """
    parent_dir.mkdir(parents=True, exist_ok=True)
    dir_name = directory_name or repository_workdir_name(clone_url)
    repo_dir = parent_dir / dir_name
    if repo_dir.exists():
        msg = f"clone destination already exists: {repo_dir}"
        raise FileExistsError(msg)
    if shallow:
        if revision is None:
            msg = "revision is required for a shallow clone"
            raise ValueError(msg)
        if not sparse_dirs:
            msg = "sparse_dirs is required for a shallow clone"
            raise ValueError(msg)
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
    else:
        _run_git_cmd(
            ["git", "clone", clone_url, str(repo_dir)],
            cwd=parent_dir,
            stderr_path=stderr_path,
        )
    return repo_dir


def fetch(
    repo_dir: Path,
    remote: str,
    *refs: str,
    stderr_path: Path | None = None,
) -> None:
    """Fetch *refs* from *remote*."""
    for ref in refs:
        _run_git_cmd(
            ["git", "fetch", remote, ref],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )


def _local_branch_exists(
    repo_dir: Path,
    branch: str,
    *,
    stderr_path: Path | None = None,
) -> bool:
    """Return True when a local branch named *branch* exists in *repo_dir*."""
    result = _run_git_cmd(
        ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=repo_dir,
        stderr_path=stderr_path,
        check=False,
    )
    return result.returncode == 0


def checkout(
    repo_dir: Path,
    branch: str,
    *,
    start_point: str | None = None,
    reset: bool = False,
    stderr_path: Path | None = None,
) -> None:
    """Check out *branch*.

    By default, use an existing local branch or create it when missing.
    With *reset* true, run `checkout -B` from *start_point* (required).
    """
    if reset:
        if start_point is None:
            msg = "start_point is required when reset is true"
            raise ValueError(msg)
        _run_git_cmd(
            ["git", "checkout", "-B", branch, start_point],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return
    if _local_branch_exists(repo_dir, branch, stderr_path=stderr_path):
        _run_git_cmd(
            ["git", "checkout", branch],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return
    if start_point is not None:
        _run_git_cmd(
            ["git", "checkout", "-b", branch, start_point],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return
    _run_git_cmd(
        ["git", "checkout", "-b", branch],
        cwd=repo_dir,
        stderr_path=stderr_path,
    )


def sync_to_origin_main(
    repo_dir: Path,
    *,
    remote: str = "origin",
    base_branch: str = "main",
    stderr_path: Path | None = None,
) -> None:
    """Fetch *base_branch* and reset the working tree to *remote*/*base_branch*."""
    fetch(repo_dir, remote, base_branch, stderr_path=stderr_path)
    _run_git_cmd(
        ["git", "checkout", base_branch],
        cwd=repo_dir,
        stderr_path=stderr_path,
    )
    _run_git_cmd(
        ["git", "reset", "--hard", f"{remote}/{base_branch}"],
        cwd=repo_dir,
        stderr_path=stderr_path,
    )


def working_tree_diff(
    repo_dir: Path,
    *,
    stderr_path: Path | None = None,
) -> str:
    """Return unstaged working-tree diff text for *repo_dir*."""
    return _run_git_cmd(
        ["git", "diff"],
        cwd=repo_dir,
        stderr_path=stderr_path,
    ).stdout


def changed_paths_from_status(
    repo_dir: Path,
    *,
    stderr_path: Path | None = None,
) -> list[str]:
    """Return repo-relative paths with local modifications (porcelain)."""
    status = _run_git_cmd(
        ["git", "status", "-s", "--porcelain"],
        cwd=repo_dir,
        stderr_path=stderr_path,
    ).stdout
    paths: list[str] = []
    for line in status.splitlines():
        if len(line) < 4:
            continue
        paths.append(line[3:].strip())
    return paths


def set_remote_url(
    repo_dir: Path,
    remote_name: str,
    url: str,
    *,
    stderr_path: Path | None = None,
) -> None:
    """Point *remote_name* at *url*."""
    _run_git_cmd(
        ["git", "remote", "set-url", remote_name, url],
        cwd=repo_dir,
        stderr_path=stderr_path,
    )


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
    """Stage *relative_paths* and commit with *message* via the git CLI.

    Call `configure_git_global_user` first so git can identify the committer.
    """
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
        rebase_branch=branch,
        stderr_path=stderr_path,
    )


def _push_argv(
    remote: str,
    branch: str | None,
    *,
    force: bool,
) -> list[str]:
    """Build the `git push` argv for *remote* and optional *branch*."""
    argv = ["git", "push", remote]
    if force:
        argv.append("--force")
    if branch is not None:
        argv.append(branch)
    return argv


def push(
    repo_dir: Path,
    branch: str | None = None,
    *,
    remote: str = "origin",
    force: bool = False,
    retries: int = 0,
    rebase_branch: str | None = None,
    stderr_path: Path | None = None,
) -> None:
    """Push to *remote* via the git CLI.

    When *branch* is set, push that ref; otherwise push the current branch.
    With *rebase_branch* set and *retries* > 0, run `pull --rebase` after each
    rejected push and retry with exponential backoff.
    """
    if retries > 0 and rebase_branch is None:
        msg = "rebase_branch is required when retries > 0"
        raise ValueError(msg)
    if retries == 0 and rebase_branch is None:
        _run_git_cmd(
            _push_argv(remote, branch, force=force),
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return

    rebase_ref = rebase_branch if rebase_branch is not None else branch
    if rebase_ref is None:
        msg = "rebase_branch or branch is required when retries > 0"
        raise ValueError(msg)

    max_attempts = retries + 1
    attempt = 0

    def _push_with_rebase() -> None:
        """Push once, rebasing from *remote* when the push is rejected."""
        nonlocal attempt
        attempt += 1
        current_attempt = attempt
        try:
            _run_git_cmd(
                _push_argv(remote, branch, force=force),
                cwd=repo_dir,
                stderr_path=stderr_path,
            )
        except subprocess.CalledProcessError as push_error:
            if current_attempt >= max_attempts:
                raise
            _run_git_cmd(
                ["git", "pull", "--rebase", remote, rebase_ref],
                cwd=repo_dir,
                stderr_path=stderr_path,
            )
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
