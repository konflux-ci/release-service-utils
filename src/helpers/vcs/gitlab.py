"""GitLab-specific helpers (OAuth2 git auth, raw file URLs, sparse clone, MRs)."""

from __future__ import annotations

import atexit
import logging
import math
import os
import stat
import tempfile
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gitlab import Gitlab

from release_service_utils.helpers import authentication
from release_service_utils.helpers import retry

from . import git

logger = logging.getLogger(__name__)

DEFAULT_BRANCH = "main"
_WAIT_MERGE_MAX_ATTEMPTS = 64


class _MergeRequestNotMerged(Exception):
    """MR refresh succeeded but the merge has not completed yet."""


def _validate_positive_finite(name: str, value: float) -> None:
    """Reject non-finite or non-positive timing values before polling."""
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero")


def _merge_request_display_url(merge_request: Any) -> str:
    """Return a log- and error-friendly URL for *merge_request*."""
    return getattr(merge_request, "web_url", None) or str(getattr(merge_request, "iid", "?"))


def _raise_merge_request_timeout(merge_request: Any) -> None:
    """Raise ``TimeoutError`` with the latest MR state in the message."""
    state = getattr(merge_request, "state", "") or ""
    url = _merge_request_display_url(merge_request)
    merge_status = getattr(merge_request, "merge_status", None)
    raise TimeoutError(
        f"timed out waiting for {url} to merge "
        f"(state={state!r}, merge_status={merge_status!r})"
    )


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


def client(host: str, private_token: str) -> Gitlab:
    """Return a python-gitlab client for *host*."""
    return Gitlab(host, private_token=private_token)


def client_from_credentials(credentials: GitLabCredentials) -> Gitlab:
    """Build a python-gitlab client from mounted *credentials*."""
    return client(credentials.gitlab_host, credentials.access_token)


def get_project(gitlab_client: Gitlab, repository: str) -> Any:
    """Return the python-gitlab project for *repository*."""
    return gitlab_client.projects.get(gitlab_project_path(repository))


def create_merge_request(
    gitlab_client: Gitlab,
    repository: str,
    *,
    source_branch: str,
    target_branch: str,
    title: str,
    description: str,
    remove_source_branch: bool = False,
) -> Any:
    """Create a merge request in *repository* and return the API object."""
    project = get_project(gitlab_client, repository)
    payload: dict[str, Any] = {
        "source_branch": source_branch,
        "target_branch": target_branch,
        "title": title,
        "description": description,
    }
    if remove_source_branch:
        payload["remove_source_branch"] = True
    merge_request = project.mergerequests.create(payload)
    if not getattr(merge_request, "web_url", None):
        raise ValueError("merge request created but web_url was empty")
    return merge_request


def iter_open_merge_requests(
    project: Any,
    *,
    search: str | None = None,
    per_page: int = 100,
) -> Iterator[Any]:
    """Yield open merge requests for an already-resolved *project*.

    Call ``get_project`` first when callers need distinct error handling for
    project lookup versus merge-request listing.
    """
    page = 1
    while True:
        list_kwargs: dict[str, Any] = {
            "state": "opened",
            "per_page": per_page,
            "page": page,
        }
        if search is not None:
            list_kwargs["search"] = search
        batch = project.mergerequests.list(**list_kwargs)
        if not batch:
            break
        yield from batch
        page += 1


def find_open_merge_request_by_source_branch(
    gitlab_client: Gitlab,
    repository: str,
    source_branch: str,
) -> Any | None:
    """Return the open merge request for *source_branch*, if any."""
    project = get_project(gitlab_client, repository)
    found = project.mergerequests.list(
        state="opened",
        source_branch=source_branch,
        per_page=1,
    )
    if not found:
        return None
    return found[0]


def enable_auto_merge(
    merge_request: Any,
    *,
    should_remove_source_branch: bool = True,
) -> Any:
    """Enable merge-when-pipeline-succeeds on *merge_request*."""
    merge_request.merge(
        merge_when_pipeline_succeeds=True,
        should_remove_source_branch=should_remove_source_branch,
    )
    return merge_request


def wait_until_merged(
    merge_request: Any,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 10,
) -> Any:
    """Poll *merge_request* until it is merged.

    Uses exponential backoff between unmerged polls. Raises ``ValueError`` when
    *timeout_seconds* or *poll_interval_seconds* are not finite and positive.
    Raises ``RuntimeError`` if the MR is closed or locked. Raises ``TimeoutError``
    if it is still unmerged when the deadline is reached.
    """
    _validate_positive_finite("timeout_seconds", timeout_seconds)
    _validate_positive_finite("poll_interval_seconds", poll_interval_seconds)
    deadline = time.monotonic() + timeout_seconds
    base_sleep_seconds = max(1, math.ceil(poll_interval_seconds))

    def _deadline_aware_sleep(backoff_seconds: float) -> None:
        """Sleep for backoff, but never past *deadline*."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(backoff_seconds, remaining))

    def _poll_merge_status() -> Any:
        if time.monotonic() >= deadline:
            _raise_merge_request_timeout(merge_request)
        merge_request.refresh()
        state = getattr(merge_request, "state", "") or ""
        url = _merge_request_display_url(merge_request)
        if state == "merged":
            logger.info("merge request %s is merged", url)
            return merge_request
        if state in ("closed", "locked"):
            raise RuntimeError(f"merge request {url} is {state}")
        logger.info(
            "waiting for merge request %s (state=%s, merge_status=%s)",
            url,
            state,
            getattr(merge_request, "merge_status", None),
        )
        raise _MergeRequestNotMerged()

    try:
        return retry.retry_with_exponential_backoff(
            _poll_merge_status,
            max_attempts=_WAIT_MERGE_MAX_ATTEMPTS,
            retry_on=_MergeRequestNotMerged,
            base_sleep_seconds=base_sleep_seconds,
            sleep_fn=_deadline_aware_sleep,
        )
    except _MergeRequestNotMerged:
        _raise_merge_request_timeout(merge_request)
