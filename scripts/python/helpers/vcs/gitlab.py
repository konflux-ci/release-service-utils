"""GitLab-specific helpers (OAuth2 git auth, raw file URLs, sparse clone)."""

from __future__ import annotations

import atexit
import os
import stat
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import authentication

from . import git

DEFAULT_BRANCH = "main"


@dataclass(frozen=True)
class GitLabCredentials:
    """GitLab host, token, and Git author identity for repository operations."""

    gitlab_host: str
    access_token: str
    git_author_name: str
    git_author_email: str
    git_repo: str


def read_credentials_from_mount(secret_mount: Path) -> GitLabCredentials:
    """Load credentials from *secret_mount*, where each field is a separate file.

    Expected files: ``gitlab_host``, ``gitlab_access_token``, ``git_author_name``,
    ``git_author_email``, ``git_repo``.
    """
    return GitLabCredentials(
        gitlab_host=authentication.read_mounted_text(secret_mount, "gitlab_host"),
        access_token=authentication.read_mounted_text(secret_mount, "gitlab_access_token"),
        git_author_name=authentication.read_mounted_text(secret_mount, "git_author_name"),
        git_author_email=authentication.read_mounted_text(secret_mount, "git_author_email"),
        git_repo=authentication.read_mounted_text(secret_mount, "git_repo"),
    )


def export_env_for_image_helpers(credentials: GitLabCredentials) -> None:
    """Set env vars some Git helpers in the task image expect."""
    os.environ["GITLAB_HOST"] = credentials.gitlab_host
    os.environ["ACCESS_TOKEN"] = credentials.access_token
    os.environ["GIT_AUTHOR_NAME"] = credentials.git_author_name
    os.environ["GIT_AUTHOR_EMAIL"] = credentials.git_author_email


def configure_git_oauth2_auth(access_token: str) -> None:
    """Set process env so git HTTPS uses OAuth2 without embedding the token in URLs.

    Installs a small `GIT_ASKPASS` helper for clone, fetch, and push in this
    process. Call once before any GitLab git operations.
    """
    fd, path = tempfile.mkstemp(prefix="git-askpass-", suffix=".sh")
    askpass = Path(path)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write('#!/bin/sh\nexec echo "$GITLAB_OAUTH2_TOKEN"\n')
    askpass.chmod(askpass.stat().st_mode | stat.S_IXUSR)
    atexit.register(lambda: askpass.unlink(missing_ok=True))
    os.environ["GIT_TERMINAL_PROMPT"] = "0"
    os.environ["GIT_ASKPASS"] = str(askpass)
    os.environ["GITLAB_OAUTH2_TOKEN"] = access_token


def gitlab_project_path(repository: str) -> str:
    """Normalize *repository* to a ``group/project`` path for the GitLab API."""
    repo = repository.strip()
    if "://" in repo:
        path = repo.split("://", 1)[1]
        if "/" in path:
            path = path.split("/", 1)[1]
        path = path.strip("/")
    else:
        path = repo.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    return path


def raw_file_url(
    git_repo: str,
    repo_relative_path: str,
    *,
    branch: str = DEFAULT_BRANCH,
) -> str:
    """Return the GitLab `/-/raw/<branch>/<path>` URL for a file in the repo."""
    return git_repo.replace(".git", "") + f"/-/raw/{branch}/{repo_relative_path}"


def clone_project_sparse(
    repository: str,
    revision: str,
    sparse_dirs: Sequence[str],
    *,
    parent_dir: Path,
    stderr_path: Path | None,
) -> Path:
    """Shallow sparse clone of a GitLab *repository* HTTPS URL.

    Requires `configure_git_oauth2_auth()` in this process so git can
    authenticate without a token embedded in the clone URL.

    Returns the repository root directory.
    """
    return git.clone(
        parent_dir,
        repository,
        directory_name=git.repository_workdir_name(repository),
        revision=revision,
        sparse_dirs=sparse_dirs,
        shallow=True,
        stderr_path=stderr_path,
    )
