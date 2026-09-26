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
from gitlab.exceptions import GitlabConnectionError, GitlabError

from release_service_utils.helpers import authentication

from . import git

logger = logging.getLogger(__name__)

DEFAULT_BRANCH = "main"
_DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS = 60.0
# Cap merge-poll backoff so a finished pipeline is still noticed promptly.
_MAX_MERGE_POLL_INTERVAL_SECONDS = 60.0


_TRANSIENT_GITLAB_LOOKUP_CODES = frozenset({429, 500, 502, 503, 504})


def is_transient_gitlab_error(exc: BaseException) -> bool:
    """Return True when *exc* is a retryable GitLab API failure."""
    if isinstance(exc, GitlabConnectionError):
        return True
    if isinstance(exc, GitlabError):
        code = getattr(exc, "response_code", None)
        return code in _TRANSIENT_GITLAB_LOOKUP_CODES
    return False


def is_insufficient_scope_error(exc: BaseException) -> bool:
    """Return True when *exc* is a GitLab 403 from missing PAT API scopes."""
    if not isinstance(exc, GitlabError):
        return False
    if getattr(exc, "response_code", None) != 403:
        return False
    return "insufficient_scope" in str(exc).lower()


def _validate_positive_finite(name: str, value: float) -> None:
    """Reject non-finite or non-positive timing values before polling."""
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero")


def _merge_request_display_url(merge_request: Any) -> str:
    """Return a log- and error-friendly URL for *merge_request*."""
    return getattr(merge_request, "web_url", None) or str(getattr(merge_request, "iid", "?"))


def _poll_remaining_seconds(deadline: float) -> float:
    """Return seconds until *deadline*, or ``0.0`` when it has passed."""
    return max(0.0, deadline - time.monotonic())


def _gitlab_request_timeout_seconds(deadline: float) -> float:
    """Return a per-request timeout capped by the normal GitLab limit.

    Uses the smaller of the remaining poll budget and
    ``_DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS`` so one stalled call cannot
    consume nearly all retry time.
    """
    remaining = _poll_remaining_seconds(deadline)
    if remaining <= 0:
        return 0.0
    return min(remaining, _DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS)


def _sleep_for_poll_interval(deadline: float, poll_interval_seconds: float) -> None:
    """Sleep for *poll_interval_seconds*, capped so we do not pass *deadline*."""
    remaining = _poll_remaining_seconds(deadline)
    if remaining <= 0:
        return
    time.sleep(min(poll_interval_seconds, remaining))


def _next_merge_poll_interval(current_interval: float, *, base_interval: float) -> float:
    """Return the next backoff interval, doubling up to the merge-poll cap.

    The cap is at least *base_interval* so an explicitly large poll interval is
    not reduced on later iterations.
    """
    max_interval = max(base_interval, _MAX_MERGE_POLL_INTERVAL_SECONDS)
    return min(max_interval, current_interval * 2)


def _refresh_merge_request(merge_request: Any, *, deadline: float) -> None:
    """Re-fetch *merge_request* from the API.

    ``ProjectMergeRequest`` does not include ``RefreshMixin`` in python-gitlab
    4+, so reload via the merge-request manager instead of ``.refresh()``.
    """
    request_timeout = _gitlab_request_timeout_seconds(deadline)
    if request_timeout <= 0:
        _raise_merge_request_timeout(merge_request)
    fresh = merge_request.manager.get(
        merge_request.get_id(),
        timeout=request_timeout,
    )
    # Use server fields only. ``attributes`` includes parent ids that belong
    # on ``_parent_attrs``, not in the MR ``_attrs`` map.
    fresh_attrs = getattr(fresh, "_attrs", None)
    if isinstance(fresh_attrs, dict):
        merge_request._update_attrs(dict(fresh_attrs))
    else:
        merge_request._update_attrs(dict(fresh.attributes))
    if _poll_remaining_seconds(deadline) <= 0:
        _raise_merge_request_timeout(merge_request)


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


def normalize_gitlab_url(gitlab_host: str) -> str:
    """Return a python-gitlab URL, adding ``https://`` when *gitlab_host* has no scheme.

    Production secrets store a hostname (e.g. ``gitlab.cee.redhat.com``). python-gitlab
    expects a full URL, so hostname-only values fail before an MR can be found or
    created. Already-complete URLs are returned unchanged.
    """
    host = gitlab_host.strip()
    if not host:
        raise ValueError("gitlab_host is required")
    if "://" in host:
        return host
    return f"https://{host}"


def client(
    host: str,
    private_token: str,
    *,
    timeout: float = _DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS,
) -> Gitlab:
    """Return a python-gitlab client for *host*."""
    return Gitlab(
        normalize_gitlab_url(host),
        private_token=private_token,
        timeout=timeout,
    )


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
    deadline: float | None = None,
) -> Any | None:
    """Return the open merge request for *source_branch*, if any.

    When *deadline* is set, each API call recalculates a capped request timeout
    from the remaining poll budget. Otherwise *request_timeout* is used as a
    fixed per-call timeout when provided.
    """

    def _request_kwargs() -> dict[str, Any]:
        if deadline is not None:
            timeout = _gitlab_request_timeout_seconds(deadline)
            if timeout <= 0:
                raise TimeoutError(
                    "timed out waiting for open merge request on branch " f"{source_branch}"
                )
            return {"timeout": timeout}
        if request_timeout is not None:
            return {"timeout": request_timeout}
        return {}

    project = get_project(gitlab_client, repository, **_request_kwargs())
    found = project.mergerequests.list(
        state="opened",
        source_branch=source_branch,
        per_page=1,
        **_request_kwargs(),
    )
    if not found:
        return None
    return found[0]


def enable_auto_merge(
    merge_request: Any,
    *,
    request_timeout: float,
    should_remove_source_branch: bool = True,
) -> Any:
    """Enable merge-when-pipeline-succeeds on *merge_request*.

    Projects that require a green pipeline reject an immediate ``merge()`` with
    HTTP 405 while CI is still running. Auto-merge waits for the pipeline, then
    merges and removes the source branch.

    *request_timeout* bounds the individual ``merge()`` HTTP call so a stall
    cannot outlive the caller's polling budget.
    """
    merge_request.merge(
        merge_when_pipeline_succeeds=True,
        should_remove_source_branch=should_remove_source_branch,
        timeout=request_timeout,
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


def merge_request_is_merged(merge_request: Any) -> bool:
    """Return True when *merge_request* is fully merged onto the target branch.

    Require both ``state=merged`` and a non-empty ``merge_commit_sha``. Auto-merge
    can look finished in some API responses while the MR is still open and
    ``merge_commit_sha`` is still null.
    """
    if (getattr(merge_request, "state", "") or "") != "merged":
        return False
    return bool(getattr(merge_request, "merge_commit_sha", None))


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
) -> Any:
    """Poll *merge_request* until it is merged.

    Checks immediately, then sleeps with bounded exponential backoff so long
    pipeline waits do not hammer the API at a fixed high rate.
    """
    _validate_positive_finite("timeout_seconds", timeout_seconds)
    _validate_positive_finite("poll_interval_seconds", poll_interval_seconds)
    deadline = time.monotonic() + timeout_seconds
    base_interval = float(max(1, math.ceil(poll_interval_seconds)))
    interval = base_interval

    while True:
        try:
            _refresh_merge_request(merge_request, deadline=deadline)
        except GitlabError as exc:
            if not is_transient_gitlab_error(exc):
                raise
            logger.warning(
                "transient GitLab error refreshing merge request %s: %s",
                _merge_request_display_url(merge_request),
                exc,
            )
        else:
            url = _merge_request_display_url(merge_request)
            if merge_request_is_merged(merge_request):
                logger.info("merge request %s is merged", url)
                return merge_request
            state = getattr(merge_request, "state", "") or ""
            if state in ("closed", "locked"):
                raise RuntimeError(f"merge request {url} is {state}")
            if merge_request_has_conflict(merge_request):
                raise RuntimeError(f"merge request {url} has a merge conflict")
            logger.info(
                "waiting for merge request %s (state=%s, merge_status=%s, "
                "merge_when_pipeline_succeeds=%s)",
                url,
                getattr(merge_request, "state", None),
                getattr(merge_request, "merge_status", None),
                getattr(merge_request, "merge_when_pipeline_succeeds", None),
            )

        if _poll_remaining_seconds(deadline) <= 0:
            _raise_merge_request_timeout(merge_request)
        _sleep_for_poll_interval(deadline, interval)
        interval = _next_merge_poll_interval(interval, base_interval=base_interval)


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
    interval = max(1, math.ceil(poll_interval_seconds))

    while True:
        if _gitlab_request_timeout_seconds(deadline) <= 0:
            raise TimeoutError(
                f"timed out waiting for open merge request on branch {source_branch}"
            )
        try:
            merge_request = find_open_merge_request_by_source_branch(
                gitlab_client,
                repository,
                source_branch,
                deadline=deadline,
            )
        except GitlabError as exc:
            if not is_transient_gitlab_error(exc):
                raise
            logger.warning(
                "transient GitLab error looking up merge request for branch %s: %s",
                source_branch,
                exc,
            )
            merge_request = None
        else:
            if _poll_remaining_seconds(deadline) <= 0:
                raise TimeoutError(
                    "timed out waiting for open merge request on branch " f"{source_branch}"
                )
            if merge_request is not None:
                return merge_request
            logger.info(
                "waiting for open merge request on branch %s",
                source_branch,
            )

        if _poll_remaining_seconds(deadline) <= 0:
            raise TimeoutError(
                f"timed out waiting for open merge request on branch {source_branch}"
            )
        _sleep_for_poll_interval(deadline, interval)


def push_merge_request_to_main(
    gitlab_client: Gitlab,
    repository: str,
    merge_request: Any,
    source_branch: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 10,
) -> Any:
    """Merge *merge_request* when ready and wait until it lands on main.

    Arms merge-when-pipeline-succeeds once the MR is mergeable. On merge
    conflict, close the MR, delete *source_branch*, and raise.

    Polls immediately, then uses bounded exponential backoff between checks so
    long-running pipelines do not generate a fixed high rate of API calls.
    """
    _validate_positive_finite("timeout_seconds", timeout_seconds)
    _validate_positive_finite("poll_interval_seconds", poll_interval_seconds)
    deadline = time.monotonic() + timeout_seconds
    base_interval = float(max(1, math.ceil(poll_interval_seconds)))
    interval = base_interval

    while True:
        try:
            _refresh_merge_request(merge_request, deadline=deadline)
        except GitlabError as exc:
            if not is_transient_gitlab_error(exc):
                raise
            logger.warning(
                "transient GitLab error refreshing merge request %s: %s",
                _merge_request_display_url(merge_request),
                exc,
            )
        else:
            url = _merge_request_display_url(merge_request)
            if merge_request_is_merged(merge_request):
                logger.info("merge request %s is merged", url)
                return merge_request
            state = getattr(merge_request, "state", "") or ""
            if state in ("closed", "locked"):
                raise RuntimeError(f"merge request {url} is {state}")
            if merge_request_has_conflict(merge_request):
                cleanup_merge_request_branch(
                    gitlab_client,
                    repository,
                    merge_request,
                    source_branch,
                )
                raise RuntimeError(f"merge request {url} has a merge conflict")
            auto_merge_armed = bool(
                getattr(merge_request, "merge_when_pipeline_succeeds", False)
            )
            if not auto_merge_armed and merge_request_is_mergeable(merge_request):
                logger.info(
                    "accepting merge request %s when the pipeline succeeds",
                    url,
                )
                request_timeout = _gitlab_request_timeout_seconds(deadline)
                if request_timeout <= 0:
                    _raise_merge_request_timeout(merge_request)
                enable_auto_merge(
                    merge_request,
                    request_timeout=request_timeout,
                )
                # Only trust a fresh GET for merged state — the merge() response
                # can look "done" when auto-merge was merely armed.
                try:
                    _refresh_merge_request(merge_request, deadline=deadline)
                except GitlabError as exc:
                    if not is_transient_gitlab_error(exc):
                        raise
                    logger.warning(
                        "transient GitLab error refreshing merge request %s: %s",
                        url,
                        exc,
                    )
                else:
                    if merge_request_is_merged(merge_request):
                        logger.info("merge request %s is merged", url)
                        return merge_request
                    auto_merge_armed = bool(
                        getattr(merge_request, "merge_when_pipeline_succeeds", False)
                    )
            logger.info(
                "waiting for merge request %s (state=%s, merge_status=%s, "
                "merge_when_pipeline_succeeds=%s)",
                url,
                getattr(merge_request, "state", None),
                getattr(merge_request, "merge_status", None),
                auto_merge_armed,
            )

        if _poll_remaining_seconds(deadline) <= 0:
            _raise_merge_request_timeout(merge_request)
        _sleep_for_poll_interval(deadline, interval)
        interval = _next_merge_poll_interval(interval, base_interval=base_interval)


def wait_until_merged(
    merge_request: Any,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 10,
) -> Any:
    """Poll *merge_request* until it is merged.

    Checks immediately, then sleeps with bounded exponential backoff starting
    at *poll_interval_seconds*. Raises ``ValueError`` when *timeout_seconds* or
    *poll_interval_seconds* are not finite and positive. Raises ``RuntimeError``
    if the MR is closed or locked. Raises ``TimeoutError`` if it is still
    unmerged when the deadline is reached.
    """
    return _poll_merge_request_until_merged(
        merge_request,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
