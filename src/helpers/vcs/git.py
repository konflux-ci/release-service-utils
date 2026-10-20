"""Host-agnostic Git operations via the `git` CLI."""

from __future__ import annotations

import logging
import os
import re
import subprocess
import time
from collections.abc import Sequence
from pathlib import Path
from typing import IO, Any

from release_service_utils.framework import BinaryHelper
from release_service_utils.helpers import retry
from release_service_utils.helpers.redact import redact_secrets

logger = logging.getLogger(__name__)
_TRANSIENT_FETCH_HTTP_ERROR = re.compile(r"\bHTTP\s+(?:429|5\d\d)\b", re.IGNORECASE)
_FETCH_MAX_ATTEMPTS = 3


def _append_cmd_stderr(stderr_path: Path | None, message: str) -> None:
    """Append redacted *message* to *stderr_path* when configured."""
    if stderr_path is None or not message.strip():
        return
    safe_text = redact_secrets(str(message))
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


class GitHelper(BinaryHelper):
    """Git CLI wrapper for repository operations."""

    bin = "git"

    def repository_workdir_name(self, repository_url: str) -> str:
        """Directory name for *repository_url* (strip `.git` suffix)."""
        base = Path(repository_url.rstrip("/")).name
        if base.endswith(".git"):
            return base[: -len(".git")]
        return base

    def configure_git_global_user(self, name: str, email: str) -> None:
        """Set `user.name` and `user.email` in the global Git config."""
        _run_git_cmd(["git", "config", "--global", "user.name", name])
        _run_git_cmd(["git", "config", "--global", "user.email", email])

    def clone(
        self,
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
        dir_name = directory_name or self.repository_workdir_name(clone_url)
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
        self,
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

    def remote_branch_exists(
        self,
        repo_dir: Path,
        branch: str,
        *,
        remote: str = "origin",
        stderr_path: Path | None = None,
    ) -> bool:
        """Return True when *remote* has an exact head named *branch*.

        ``git ls-remote`` patterns can match slash-qualified names such as
        ``other/<branch>``. Only ``refs/heads/<branch>`` counts as a hit.
        """
        expected_ref = f"refs/heads/{branch}"
        result = _run_git_cmd(
            ["git", "ls-remote", "--heads", remote, expected_ref],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        for line in (result.stdout or "").splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1] == expected_ref:
                return True
        return False

    def _local_branch_exists(
        self,
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
        self,
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
        if self._local_branch_exists(repo_dir, branch, stderr_path=stderr_path):
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
        self,
        repo_dir: Path,
        *,
        remote: str = "origin",
        base_branch: str = "main",
        stderr_path: Path | None = None,
    ) -> None:
        """Fetch *base_branch* and reset the working tree to *remote*/*base_branch*."""
        for attempt in range(1, _FETCH_MAX_ATTEMPTS + 1):
            try:
                self.fetch(repo_dir, remote, base_branch, stderr_path=stderr_path)
                break
            except subprocess.CalledProcessError as exc:
                if attempt == _FETCH_MAX_ATTEMPTS or not _TRANSIENT_FETCH_HTTP_ERROR.search(
                    exc.stderr or ""
                ):
                    raise
                logger.warning(
                    "retrying git fetch %s %s after a transient HTTP error (%d/%d)",
                    remote,
                    base_branch,
                    attempt,
                    _FETCH_MAX_ATTEMPTS,
                )
                time.sleep(2**attempt)
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
        self,
        repo_dir: Path,
        *,
        cached: bool = False,
        other_ref: str | None = None,
        stderr_path: Path | None = None,
    ) -> str:
        """Return diff text for *repo_dir*.

        By default return unstaged working-tree diff. With *cached* true, return
        staged diff. When *other_ref* is set with *cached*, compare the index
        against that ref.
        """
        cmd = ["git", "diff"]
        if cached:
            cmd.append("--cached")
        if other_ref is not None:
            cmd.append(other_ref)
        result = _run_git_cmd(cmd, cwd=repo_dir, stderr_path=stderr_path)
        return result.stdout or ""

    def changed_paths_from_status(
        self,
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
        self,
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
        self,
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

    def commit_staged(
        self,
        repo_root: Path,
        message: str,
        *,
        stderr_path: Path | None = None,
    ) -> None:
        """Commit staged changes via the git CLI."""
        _run_git_cmd(
            ["git", "commit", "-m", message],
            cwd=repo_root,
            stderr_path=stderr_path,
        )

    def index_add_commit(
        self,
        repo_root: Path,
        relative_paths: Sequence[str],
        message: str,
        *,
        commit: bool = True,
        stderr_path: Path | None = None,
    ) -> None:
        """Stage *relative_paths* and, when *commit* is true, commit with *message*.

        Uses the git CLI. Call `configure_git_global_user` first so git can identify
        the committer when committing.
        """
        _run_git_cmd(
            ["git", "add", *relative_paths],
            cwd=repo_root,
            stderr_path=stderr_path,
        )
        if commit:
            self.commit_staged(repo_root, message, stderr_path=stderr_path)

    def rebase_onto_remote(
        self,
        repo_root: Path,
        *,
        remote_name: str,
        remote_repository: str,
        revision: str,
        stderr_path: Path | None = None,
    ) -> None:
        """Add *remote_name*, fetch *revision*, and rebase onto it."""
        remotes = _run_git_cmd(
            ["git", "remote"],
            cwd=repo_root,
            stderr_path=stderr_path,
        ).stdout.splitlines()
        if remote_name not in remotes:
            _run_git_cmd(
                ["git", "remote", "add", remote_name, remote_repository],
                cwd=repo_root,
                stderr_path=stderr_path,
            )
        self.fetch(repo_root, remote_name, revision, stderr_path=stderr_path)
        _run_git_cmd(
            ["git", "rebase", f"{remote_name}/{revision}"],
            cwd=repo_root,
            stderr_path=stderr_path,
        )

    def working_tree_status(
        self,
        repo_root: Path,
        *,
        stderr_path: Path | None = None,
    ) -> str:
        """Return ``git status`` output."""
        result = _run_git_cmd(
            ["git", "status"],
            cwd=repo_root,
            stderr_path=stderr_path,
        )
        return result.stdout or ""

    def commit_and_push(
        self,
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
        self.index_add_commit(
            repo_root,
            relative_paths,
            message,
            stderr_path=stderr_path,
        )
        self.push(
            repo_root,
            branch,
            remote=remote,
            retries=retries,
            rebase_branch=branch,
            stderr_path=stderr_path,
        )

    @staticmethod
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
        self,
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
                self._push_argv(remote, branch, force=force),
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
                    self._push_argv(remote, branch, force=force),
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

    def diff_files(
        self,
        repo_dir: Path,
        before: str,
        after: str,
        diff_filter: str,
        *,
        stderr_path: Path | None = None,
    ) -> list[str]:
        """Return list of file paths changed between *before* and *after* commits.

        *diff_filter* is passed to `git diff --diff-filter` (e.g., 'A' for added,
        'M' for modified, 'AM' for added or modified).
        """
        result = _run_git_cmd(
            ["git", "diff", "--name-only", f"--diff-filter={diff_filter}", before, after],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def show_file(
        self,
        repo_dir: Path,
        revision: str,
        path: str,
        *,
        stderr_path: Path | None = None,
    ) -> str:
        """Return file content at *revision*:*path* via `git show`."""
        result = _run_git_cmd(
            ["git", "show", f"{revision}:{path}"],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return result.stdout or ""

    def file_change_log(
        self,
        repo_dir: Path,
        path: str,
        from_commit: str,
        to_commit: str,
        *,
        stderr_path: Path | None = None,
    ) -> str:
        """Return formatted git log of changes to *path* between commits.

        Returns log output with format: commit-hash date author <email>
        followed by diff hunks. Excludes diff metadata lines (diff --git, index, ---, +++).
        """
        result = _run_git_cmd(
            [
                "git",
                "log",
                "-p",
                "--no-merges",
                "--format=%h %ad %an <%ae>",
                "--date=iso",
                "--no-show-signature",
                f"{from_commit}..{to_commit}",
                "--",
                path,
            ],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        log_text = result.stdout or ""
        lines = []
        for line in log_text.splitlines():
            if line.startswith(("diff --git", "index ", "--- ", "+++ ")) or not line:
                continue
            lines.append(line)
        return "\n".join(lines)

    def last_commit_date(
        self,
        repo_dir: Path,
        path: str,
        revision: str,
        date_format: str,
        *,
        stderr_path: Path | None = None,
    ) -> str:
        """Return date of last commit that modified *path* at or before *revision*.

        *date_format* is a git --date=format: string (e.g., '%Y-%m-%dT%H:%M:%SZ').
        """
        result = _run_git_cmd(
            [
                "git",
                "log",
                "-1",
                f"--date=format:{date_format}",
                "--pretty=format:%ad",
                revision,
                "--",
                path,
            ],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return result.stdout.strip()

    def rev_parse(
        self,
        repo_dir: Path,
        ref: str,
        *,
        stderr_path: Path | None = None,
    ) -> str:
        """Return the object name for *ref* in *repo_dir*."""
        result = _run_git_cmd(
            ["git", "rev-parse", ref],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return (result.stdout or "").strip()

    def remote_branch_sha(
        self,
        repo_dir: Path,
        branch: str,
        *,
        remote: str = "origin",
        stderr_path: Path | None = None,
    ) -> str | None:
        """Return the commit SHA for *remote*/*branch*, or None when it is missing."""
        if not self.remote_branch_exists(
            repo_dir,
            branch,
            remote=remote,
            stderr_path=stderr_path,
        ):
            return None
        remote_tracking_ref = f"refs/remotes/{remote}/{branch}"
        _run_git_cmd(
            [
                "git",
                "fetch",
                remote,
                f"refs/heads/{branch}:{remote_tracking_ref}",
            ],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )
        return self.rev_parse(repo_dir, remote_tracking_ref, stderr_path=stderr_path)

    def push_new_branch(
        self,
        repo_dir: Path,
        branch: str,
        *,
        remote: str = "origin",
        stderr_path: Path | None = None,
    ) -> None:
        """Create *branch* on *remote* using a create-only ref update.

        Fails when *remote* already has ``refs/heads/<branch>``, including when it
        points at the same commit as the local checkout.
        """
        ref_name = f"refs/heads/{branch}"
        _run_git_cmd(
            [
                "git",
                "push",
                f"--force-with-lease={ref_name}:",
                remote,
                f"HEAD:refs/heads/{branch}",
            ],
            cwd=repo_dir,
            stderr_path=stderr_path,
        )

    def is_ancestor(
        self,
        repo_dir: Path,
        maybe_ancestor: str,
        ref: str,
        *,
        stderr_path: Path | None = None,
    ) -> bool:
        """Return True when *maybe_ancestor* is an ancestor of *ref* (inclusive).

        Uses ``git merge-base --is-ancestor``. A commit is an ancestor of itself.
        """
        cmd = ["git", "merge-base", "--is-ancestor", maybe_ancestor, ref]
        # Exit 1 means "not an ancestor" and is not a command failure — avoid
        # treating it as one in *stderr_path*.
        result = _run_git_cmd(cmd, cwd=repo_dir, stderr_path=None, check=False)
        if result.returncode == 0:
            return True
        if result.returncode == 1:
            return False
        if stderr_path is not None:
            if result.stderr and result.stderr.strip():
                _append_cmd_stderr(stderr_path, result.stderr)
            _append_cmd_stderr(
                stderr_path,
                "command exited with failure: " + " ".join(cmd),
            )
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
        )


# Singleton instance for backward compatibility
_instance = GitHelper()

# Export function wrappers for backward compatibility
repository_workdir_name = _instance.repository_workdir_name
configure_git_global_user = _instance.configure_git_global_user
clone = _instance.clone
fetch = _instance.fetch
remote_branch_exists = _instance.remote_branch_exists
checkout = _instance.checkout
sync_to_origin_main = _instance.sync_to_origin_main
working_tree_diff = _instance.working_tree_diff
changed_paths_from_status = _instance.changed_paths_from_status
set_remote_url = _instance.set_remote_url
origin_main_has_path_matching = _instance.origin_main_has_path_matching
commit_staged = _instance.commit_staged
index_add_commit = _instance.index_add_commit
rebase_onto_remote = _instance.rebase_onto_remote
working_tree_status = _instance.working_tree_status
commit_and_push = _instance.commit_and_push
push = _instance.push
push_new_branch = _instance.push_new_branch
remote_branch_sha = _instance.remote_branch_sha
rev_parse = _instance.rev_parse
diff_files = _instance.diff_files
show_file = _instance.show_file
file_change_log = _instance.file_change_log
last_commit_date = _instance.last_commit_date
is_ancestor = _instance.is_ancestor
