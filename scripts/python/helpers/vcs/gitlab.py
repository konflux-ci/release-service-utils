"""GitLab-specific helpers (OAuth2 clone URLs, raw file URLs, sparse clone)."""

from __future__ import annotations

import os
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
    """
    Load credentials from *secret_mount*, where each field is a separate file:

    ``gitlab_host``, ``gitlab_access_token``, ``git_author_name``,
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


def oauth2_https_clone_url(https_repository_url: str, oauth2_token: str) -> str:
    """
    Build an HTTPS clone URL with embedded token for GitLab.

    Uses username `oauth2`, which is the GitLab convention for project tokens.
    """
    host_and_path = https_repository_url.split("://", 1)[1]
    return f"https://oauth2:{oauth2_token}@{host_and_path}"


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
    access_token: str,
    revision: str,
    sparse_dirs: Sequence[str],
    *,
    parent_dir: Path,
    stderr_path: Path | None,
) -> Path:
    """Shallow sparse clone of a GitLab *repository* HTTPS URL using *access_token*."""
    clone_url = oauth2_https_clone_url(repository, access_token)
    return git.clone_sparse_shallow(
        clone_url=clone_url,
        repo_url_for_dir_name=repository,
        revision=revision,
        sparse_dirs=sparse_dirs,
        parent_dir=parent_dir,
        stderr_path=stderr_path,
    )
