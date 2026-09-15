"""GitLab-specific helpers (OAuth2 git auth, raw file URLs, sparse clone, MRs)."""

from __future__ import annotations

import atexit
import logging
import math
import os
import stat
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gitlab import Gitlab
from gitlab.exceptions import GitlabConnectionError, GitlabError

from release_service_utils.helpers import authentication
from release_service_utils.helpers import retry

from . import git

logger = logging.getLogger(__name__)

DEFAULT_BRANCH = "main"
_WAIT_MERGE_MAX_ATTEMPTS = 64
_DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS = 60.0


class _MergeRequestNotMerged(Exception):
    """MR refresh succeeded but the merge has not completed yet."""


class _MergeRequestNotFound(Exception):
    """No open merge request matched the poll criteria yet."""


class _TransientGitlabPollError(Exception):
    """Transient GitLab error during merge-request polling."""


_TRANSIENT_GITLAB_LOOKUP_CODES = frozenset({429, 500, 502, 503, 504})


def is_transient_gitlab_error(exc: BaseException) -> bool:
    """Return True when *exc* is a retryable GitLab API failure."""
    if isinstance(exc, GitlabConnectionError):
        return True
    if isinstance(exc, GitlabError):
        code = getattr(exc, "response_code", None)
        return code in _TRANSIENT_GITLAB_LOOKUP_CODES
    return False


def _validate_positive_finite(name: str, value: float) -> None:
    """Reject non-finite or non-positive timing values before polling."""
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero")


def _merge_request_display_url(merge_request: Any) -> str:
    """Return a log- and error-friendly URL for *merge_request*."""
    return getattr(merge_request, "web_url", None) or str(getattr(merge_request, "iid", "?"))


def _poll_remaining_seconds(
    deadline: float,
    *,
    on_elapsed: Callable[[], None] | None = None,
) -> float:
    """Return seconds until *deadline*, calling *on_elapsed* when it has passed."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        if on_elapsed is not None:
            on_elapsed()
        return 0.0
    return remaining


def _deadline_aware_sleep_fn(deadline: float) -> Callable[[float], None]:
    """Return a sleep function that never waits past *deadline*."""

    def _sleep(backoff_seconds: float) -> None:
        remaining = _poll_remaining_seconds(deadline)
        if remaining <= 0:
            return
        time.sleep(min(backoff_seconds, remaining))

    return _sleep


def _reraise_transient_gitlab_poll_error(
    exc: GitlabError,
    *,
    context: str,
) -> None:
    """Raise ``_TransientGitlabPollError`` when *exc* is retryable."""
    if not is_transient_gitlab_error(exc):
        return
    logger.warning("transient GitLab error %s: %s", context, exc)
    raise _TransientGitlabPollError() from exc


def _refresh_merge_request(merge_request: Any, *, deadline: float) -> None:
    """Refresh *merge_request*, retrying transient GitLab failures."""

    def _on_timeout() -> None:
        _raise_merge_request_timeout(merge_request)

    request_timeout = _poll_remaining_seconds(deadline, on_elapsed=_on_timeout)
    try:
        merge_request.refresh(timeout=request_timeout)
    except GitlabError as exc:
        _reraise_transient_gitlab_poll_error(
            exc,
            context=f"refreshing merge request {_merge_request_display_url(merge_request)}",
        )
        raise
    _poll_remaining_seconds(deadline, on_elapsed=_on_timeout)


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


def client(
    host: str,
    private_token: str,
    *,
    timeout: float = _DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS,
) -> Gitlab:
    """Return a python-gitlab client for *host*."""
    return Gitlab(host, private_token=private_token, timeout=timeout)


def client_from_credentials(credentials: GitLabCredentials) -> Gitlab:
    """Build a python-gitlab client from mounted *credentials*."""
    return client(credentials.gitlab_host, credentials.access_token)


def get_project(gitlab_client: Gitlab, repository: str, **kwargs: Any) -> Any:
    """Return the python-gitlab project for *repository*."""
    return gitlab_client.projects.get(gitlab_project_path(repository), **kwargs)


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
    *,
    request_timeout: float | None = None,
) -> Any | None:
    """Return the open merge request for *source_branch*, if any."""
    request_kwargs: dict[str, Any] = {}
    if request_timeout is not None:
        request_kwargs["timeout"] = request_timeout
    project = get_project(gitlab_client, repository, **request_kwargs)
    found = project.mergerequests.list(
        state="opened",
        source_branch=source_branch,
        per_page=1,
        **request_kwargs,
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


def merge_request_has_conflict(merge_request: Any) -> bool:
    """Return True when GitLab reports the MR cannot merge due to conflicts."""
    merge_status = (getattr(merge_request, "merge_status", None) or "").lower()
    detailed = (getattr(merge_request, "detailed_merge_status", None) or "").lower()
    if merge_status == "cannot_be_merged":
        return True
    return detailed in {"conflict", "conflict_severity_blocked"}


def merge_request_is_mergeable(merge_request: Any) -> bool:
    """Return True when GitLab reports the MR can be merged."""
    merge_status = (getattr(merge_request, "merge_status", None) or "").lower()
    detailed = (getattr(merge_request, "detailed_merge_status", None) or "").lower()
    if merge_status == "can_be_merged":
        return True
    return detailed == "mergeable"


def close_merge_request(merge_request: Any) -> None:
    """Close an open merge request."""
    merge_request.state_event = "close"
    merge_request.save()


def delete_remote_branch(gitlab_client: Gitlab, repository: str, branch: str) -> None:
    """Delete *branch* from *repository*."""
    project = get_project(gitlab_client, repository)
    project.branches.delete(branch)


def cleanup_merge_request_branch(
    gitlab_client: Gitlab,
    repository: str,
    merge_request: Any,
    source_branch: str,
) -> None:
    """Close *merge_request* and delete *source_branch*, logging failures."""
    url = _merge_request_display_url(merge_request)
    try:
        close_merge_request(merge_request)
    except Exception:
        logger.exception("failed to close merge request %s", url)
    try:
        delete_remote_branch(gitlab_client, repository, source_branch)
    except Exception:
        logger.exception("failed to delete branch %s", source_branch)


def accept_merge_request(merge_request: Any) -> None:
    """Merge *merge_request* immediately and remove its source branch."""
    merge_request.merge(should_remove_source_branch=True)


def get_or_create_merge_request(
    gitlab_client: Gitlab,
    repository: str,
    *,
    source_branch: str,
    target_branch: str,
    title: str,
    description: str,
) -> Any:
    """Return an open MR for *source_branch*, creating one when missing."""
    existing = find_open_merge_request_by_source_branch(
        gitlab_client,
        repository,
        source_branch,
    )
    if existing is not None:
        return existing
    try:
        return create_merge_request(
            gitlab_client,
            repository,
            source_branch=source_branch,
            target_branch=target_branch,
            title=title,
            description=description,
            remove_source_branch=True,
        )
    except GitlabError:
        existing = find_open_merge_request_by_source_branch(
            gitlab_client,
            repository,
            source_branch,
        )
        if existing is not None:
            return existing
        raise


def _poll_merge_request_until_merged(
    merge_request: Any,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float,
    when_mergeable: Callable[[Any], None] | None = None,
    on_conflict: Callable[[], None] | None = None,
) -> Any:
    """Poll *merge_request* until merged, optionally merging when mergeable."""
    _validate_positive_finite("timeout_seconds", timeout_seconds)
    _validate_positive_finite("poll_interval_seconds", poll_interval_seconds)
    deadline = time.monotonic() + timeout_seconds
    base_sleep_seconds = max(1, math.ceil(poll_interval_seconds))
    merge_poll_retry_errors = (_MergeRequestNotMerged, _TransientGitlabPollError)

    def _poll_merge() -> Any:
        _refresh_merge_request(merge_request, deadline=deadline)
        url = _merge_request_display_url(merge_request)
        state = getattr(merge_request, "state", "") or ""
        if state == "merged":
            logger.info("merge request %s is merged", url)
            return merge_request
        if state in ("closed", "locked"):
            raise RuntimeError(f"merge request {url} is {state}")
        if merge_request_has_conflict(merge_request):
            if on_conflict is not None:
                on_conflict()
            raise RuntimeError(f"merge request {url} has a merge conflict")
        if when_mergeable is not None and merge_request_is_mergeable(merge_request):
            logger.info("merging merge request %s", url)
            when_mergeable(merge_request)
            _refresh_merge_request(merge_request, deadline=deadline)
            if (getattr(merge_request, "state", "") or "") == "merged":
                logger.info("merge request %s is merged", url)
                return merge_request
        logger.info(
            "waiting for merge request %s (state=%s, merge_status=%s)",
            url,
            state,
            getattr(merge_request, "merge_status", None),
        )
        raise _MergeRequestNotMerged()

    try:
        return retry.retry_with_exponential_backoff(
            _poll_merge,
            max_attempts=_WAIT_MERGE_MAX_ATTEMPTS,
            retry_on=merge_poll_retry_errors,
            base_sleep_seconds=base_sleep_seconds,
            sleep_fn=_deadline_aware_sleep_fn(deadline),
        )
    except merge_poll_retry_errors:
        _raise_merge_request_timeout(merge_request)


def wait_for_open_merge_request_by_source_branch(
    gitlab_client: Gitlab,
    repository: str,
    source_branch: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 10,
) -> Any:
    """Poll until an open merge request exists for *source_branch*."""
    _validate_positive_finite("timeout_seconds", timeout_seconds)
    _validate_positive_finite("poll_interval_seconds", poll_interval_seconds)
    deadline = time.monotonic() + timeout_seconds
    base_sleep_seconds = max(1, math.ceil(poll_interval_seconds))

    def _open_merge_request_timeout() -> None:
        raise TimeoutError(
            f"timed out waiting for open merge request on branch {source_branch}"
        )

    def _poll_open_merge_request() -> Any:
        request_timeout = _poll_remaining_seconds(
            deadline,
            on_elapsed=_open_merge_request_timeout,
        )
        try:
            merge_request = find_open_merge_request_by_source_branch(
                gitlab_client,
                repository,
                source_branch,
                request_timeout=request_timeout,
            )
        except GitlabError as exc:
            _reraise_transient_gitlab_poll_error(
                exc,
                context=f"looking up merge request for branch {source_branch}",
            )
            raise
        _poll_remaining_seconds(deadline, on_elapsed=_open_merge_request_timeout)
        if merge_request is not None:
            return merge_request
        logger.info(
            "waiting for open merge request on branch %s",
            source_branch,
        )
        raise _MergeRequestNotFound()

    poll_retry_errors = (_MergeRequestNotFound, _TransientGitlabPollError)
    try:
        return retry.retry_with_exponential_backoff(
            _poll_open_merge_request,
            max_attempts=_WAIT_MERGE_MAX_ATTEMPTS,
            retry_on=poll_retry_errors,
            base_sleep_seconds=base_sleep_seconds,
            sleep_fn=_deadline_aware_sleep_fn(deadline),
        )
    except poll_retry_errors:
        raise TimeoutError(
            f"timed out waiting for open merge request on branch {source_branch}"
        ) from None


def push_merge_request_to_main(
    gitlab_client: Gitlab,
    repository: str,
    merge_request: Any,
    source_branch: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 10,
) -> Any:
    """Merge *merge_request* when ready and wait until it lands on *target*.

    On merge conflict, close the MR, delete *source_branch*, and raise.
    """

    def _on_conflict() -> None:
        cleanup_merge_request_branch(
            gitlab_client,
            repository,
            merge_request,
            source_branch,
        )

    return _poll_merge_request_until_merged(
        merge_request,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        when_mergeable=accept_merge_request,
        on_conflict=_on_conflict,
    )


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
    return _poll_merge_request_until_merged(
        merge_request,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
