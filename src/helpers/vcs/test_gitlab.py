"""Tests for `vcs.gitlab`."""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest import mock

import pytest

from . import git
from gitlab.exceptions import GitlabConnectionError, GitlabError

from . import gitlab


def _stub_merge_request_reload(
    merge_request: mock.Mock,
    *,
    get_side_effect: object | None = None,
) -> None:
    """Wire ``manager.get`` + ``_update_attrs`` like a real python-gitlab MR."""
    merge_request.get_id.return_value = 1

    def _fresh_from_current() -> mock.Mock:
        fresh = mock.Mock()
        attrs = {
            "state": merge_request.state,
            "merge_status": getattr(merge_request, "merge_status", None),
            "detailed_merge_status": getattr(merge_request, "detailed_merge_status", None),
            "merge_when_pipeline_succeeds": getattr(
                merge_request, "merge_when_pipeline_succeeds", None
            ),
            "merge_commit_sha": getattr(merge_request, "merge_commit_sha", None),
            "web_url": merge_request.web_url,
        }
        # Prefer plain dicts so refresh uses ``_attrs`` like python-gitlab.
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    if get_side_effect is None:
        merge_request.manager.get.side_effect = lambda *_args, **_kwargs: _fresh_from_current()
    else:
        merge_request.manager.get.side_effect = get_side_effect

    def _update_attrs(attrs: dict) -> None:
        for key, value in attrs.items():
            setattr(merge_request, key, value)

    merge_request._update_attrs.side_effect = _update_attrs


def test_read_credentials_from_mount(tmp_path: Path) -> None:
    """Load GitLab host, token, author, and repo URL from a secret mount."""
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "gitlab_host").write_text("gitlab.example.com", encoding="utf-8")
    (secret / "gitlab_access_token").write_text("tok", encoding="utf-8")
    (secret / "git_author_name").write_text("Author", encoding="utf-8")
    (secret / "git_author_email").write_text("a@example.com", encoding="utf-8")
    (secret / "git_repo").write_text("https://gitlab.example.com/g/r.git", encoding="utf-8")
    creds = gitlab.read_credentials_from_mount(secret)
    assert creds.gitlab_host == "gitlab.example.com"
    assert creds.access_token == "tok"
    assert creds.git_repo.endswith("r.git")


def test_export_env_for_image_helpers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Export GitLab credentials to env vars for image helper scripts."""
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "gitlab_host").write_text("h", encoding="utf-8")
    (secret / "gitlab_access_token").write_text("t", encoding="utf-8")
    (secret / "git_author_name").write_text("n", encoding="utf-8")
    (secret / "git_author_email").write_text("e", encoding="utf-8")
    (secret / "git_repo").write_text("https://gitlab.example.com/g/r.git", encoding="utf-8")
    creds = gitlab.read_credentials_from_mount(secret)
    gitlab.export_env_for_image_helpers(creds)
    assert os.environ["GITLAB_HOST"] == "h"
    assert os.environ["ACCESS_TOKEN"] == "t"
    for var in ("GITLAB_HOST", "ACCESS_TOKEN", "GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL"):
        monkeypatch.delenv(var, raising=False)


def test_raw_file_url() -> None:
    """Build a GitLab raw file URL for a path on the default branch."""
    url = gitlab.raw_file_url(
        "https://gitlab.example.com/g/r.git",
        "path/to/file.yaml",
    )
    assert url == "https://gitlab.example.com/g/r/-/raw/main/path/to/file.yaml"


def test_configure_git_oauth2_auth_sets_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure OAuth2 token and GIT_ASKPASS for non-interactive git."""
    monkeypatch.delenv("GIT_ASKPASS", raising=False)
    gitlab.configure_git_oauth2_auth("my-token")
    assert os.environ["GITLAB_OAUTH2_TOKEN"] == "my-token"
    assert os.environ["GIT_TERMINAL_PROMPT"] == "0"
    assert Path(os.environ["GIT_ASKPASS"]).is_file()


def test_clone_project_sparse_delegates_to_git(tmp_path: Path) -> None:
    """Sparse-clone via `git.clone` using an OAuth2-authenticated URL."""
    repo_root = tmp_path / "repo"
    with mock.patch.object(git, "clone", return_value=repo_root) as m:
        out = gitlab.clone_project_sparse(
            "https://gitlab.example.com/g/r.git",
            "main",
            ["schema"],
            parent_dir=tmp_path,
            stderr_path=None,
        )
    assert out is repo_root
    m.assert_called_once()
    assert m.call_args.args[1] == "https://gitlab.example.com/g/r.git"
    assert m.call_args.kwargs["shallow"] is True
    assert "oauth2:" not in m.call_args.args[1]


@pytest.mark.parametrize(
    ("repository", "expected"),
    [
        ("https://gitlab.com/org/up.git", "org/up"),
        ("org/up", "org/up"),
    ],
)
def test_gitlab_project_path(repository: str, expected: str) -> None:
    """Normalize repository URLs to ``group/project`` paths."""
    assert gitlab.gitlab_project_path(repository) == expected


def _mock_gitlab_client(
    *,
    create_mr: object | None = None,
    list_pages: list[list[object]] | None = None,
    list_return: list[object] | None = None,
) -> mock.Mock:
    mock_mr = mock.Mock()
    mock_mr.web_url = "https://gitlab.example.com/g/r/-/merge_requests/7"
    mock_mr.iid = 7
    mock_mrs = mock.Mock()
    if list_pages is not None:

        def _list(**kwargs: object) -> list[object]:
            page = int(kwargs["page"])
            return list_pages[page - 1] if page <= len(list_pages) else []

        mock_mrs.list.side_effect = _list
    elif list_return is not None:
        mock_mrs.list.return_value = list_return
    else:
        mock_mrs.list.return_value = []
    mock_mrs.create.return_value = create_mr if create_mr is not None else mock_mr
    mock_project = mock.Mock()
    mock_project.mergerequests = mock_mrs
    mock_gl = mock.Mock()
    mock_gl.projects.get.return_value = mock_project
    return mock_gl


def test_client_builds_python_gitlab_client() -> None:
    """Build a python-gitlab client from host and token."""
    mock_gl = mock.Mock()
    with mock.patch.object(gitlab, "Gitlab", return_value=mock_gl) as mk:
        out = gitlab.client("gitlab.example.com", "tok")
    assert out is mock_gl
    mk.assert_called_once_with(
        "https://gitlab.example.com",
        private_token="tok",
        timeout=gitlab._DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS,
    )


@pytest.mark.parametrize(
    ("gitlab_host", "expected"),
    [
        ("gitlab.cee.redhat.com", "https://gitlab.cee.redhat.com"),
        ("gitlab.example.com", "https://gitlab.example.com"),
        ("https://gitlab.example.com", "https://gitlab.example.com"),
        ("http://gitlab.example.com", "http://gitlab.example.com"),
        ("  gitlab.cee.redhat.com  ", "https://gitlab.cee.redhat.com"),
        ("https://gitlab.example.com/", "https://gitlab.example.com/"),
    ],
)
def test_normalize_gitlab_url(gitlab_host: str, expected: str) -> None:
    """Hostname-form secrets get an https scheme; complete URLs are preserved."""
    assert gitlab.normalize_gitlab_url(gitlab_host) == expected


def test_normalize_gitlab_url_rejects_empty() -> None:
    """Empty gitlab_host cannot be turned into a python-gitlab URL."""
    with pytest.raises(ValueError, match="gitlab_host is required"):
        gitlab.normalize_gitlab_url("   ")


def test_is_insufficient_scope_error() -> None:
    """Detect GitLab PAT scope failures."""
    assert gitlab.is_insufficient_scope_error(
        GitlabError("insufficient_scope", response_code=403),
    )
    assert not gitlab.is_insufficient_scope_error(
        GitlabError("forbidden", response_code=403),
    )


def test_client_from_credentials(tmp_path: Path) -> None:
    """Build a client from credentials loaded from a secret mount."""
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "gitlab_host").write_text("gitlab.example.com", encoding="utf-8")
    (secret / "gitlab_access_token").write_text("tok", encoding="utf-8")
    (secret / "git_author_name").write_text("Author", encoding="utf-8")
    (secret / "git_author_email").write_text("a@example.com", encoding="utf-8")
    (secret / "git_repo").write_text("https://gitlab.example.com/g/r.git", encoding="utf-8")
    creds = gitlab.read_credentials_from_mount(secret)
    mock_gl = mock.Mock()
    with mock.patch.object(gitlab, "client", return_value=mock_gl) as mk:
        out = gitlab.client_from_credentials(creds)
    assert out is mock_gl
    mk.assert_called_once_with("gitlab.example.com", "tok")


def test_get_project_uses_normalized_path() -> None:
    """Look up the project using the normalized group/project path."""
    client = _mock_gitlab_client()
    gitlab.get_project(client, "https://gitlab.example.com/g/r.git")
    client.projects.get.assert_called_once_with("g/r")


def test_create_merge_request_returns_mr() -> None:
    """Create an MR and return the python-gitlab object."""
    client = _mock_gitlab_client()
    merge_request = gitlab.create_merge_request(
        client,
        "https://gitlab.example.com/g/r.git",
        source_branch="feat",
        target_branch="main",
        title="t",
        description="d",
    )
    assert merge_request.web_url.endswith("/merge_requests/7")
    client.projects.get.return_value.mergerequests.create.assert_called_once_with(
        {
            "source_branch": "feat",
            "target_branch": "main",
            "title": "t",
            "description": "d",
        }
    )


def test_create_merge_request_remove_source_branch() -> None:
    """Pass remove_source_branch when requested."""
    client = _mock_gitlab_client()
    gitlab.create_merge_request(
        client,
        "g/r",
        source_branch="feat",
        target_branch="main",
        title="t",
        description="d",
        remove_source_branch=True,
    )
    payload = client.projects.get.return_value.mergerequests.create.call_args.args[0]
    assert payload["remove_source_branch"] is True


def test_create_merge_request_raises_when_web_url_missing() -> None:
    """Empty web_url is treated as a failed create."""
    bare_mr = mock.Mock()
    bare_mr.web_url = ""
    client = _mock_gitlab_client(create_mr=bare_mr)
    with pytest.raises(ValueError, match="web_url was empty"):
        gitlab.create_merge_request(
            client,
            "g/r",
            source_branch="feat",
            target_branch="main",
            title="t",
            description="d",
        )


def test_iter_open_merge_requests_paginates() -> None:
    """Yield each page of open merge requests."""
    first = mock.Mock()
    first.iid = 1
    second = mock.Mock()
    second.iid = 2
    client = _mock_gitlab_client(list_pages=[[first], [second], []])
    project = client.projects.get("g/r")
    items = list(gitlab.iter_open_merge_requests(project, search="Konflux"))
    assert [item.iid for item in items] == [1, 2]
    project.mergerequests.list.assert_any_call(
        state="opened",
        search="Konflux",
        per_page=100,
        page=1,
    )


def test_find_open_merge_request_by_source_branch() -> None:
    """Return the first open MR for the source branch."""
    found = mock.Mock()
    found.iid = 9
    client = _mock_gitlab_client(list_return=[found])
    out = gitlab.find_open_merge_request_by_source_branch(client, "g/r", "feat")
    assert out is found
    client.projects.get.return_value.mergerequests.list.assert_called_once_with(
        state="opened",
        source_branch="feat",
        per_page=1,
    )


def test_find_open_merge_request_by_source_branch_missing() -> None:
    """Return None when no open MR uses the source branch."""
    client = _mock_gitlab_client(list_return=[])
    assert gitlab.find_open_merge_request_by_source_branch(client, "g/r", "feat") is None


def test_merge_request_is_merged_requires_commit_sha() -> None:
    """Treat state=merged without merge_commit_sha as still open."""
    merge_request = mock.Mock()
    merge_request.state = "merged"
    merge_request.merge_commit_sha = None
    assert not gitlab.merge_request_is_merged(merge_request)
    merge_request.merge_commit_sha = "abc123"
    assert gitlab.merge_request_is_merged(merge_request)
    merge_request.state = "opened"
    assert not gitlab.merge_request_is_merged(merge_request)


def test_enable_auto_merge() -> None:
    """Enable merge-when-pipeline-succeeds and drop the source branch."""
    merge_request = mock.Mock()
    out = gitlab.enable_auto_merge(merge_request, request_timeout=12.5)
    assert out is merge_request
    merge_request.merge.assert_called_once_with(
        merge_when_pipeline_succeeds=True,
        should_remove_source_branch=True,
        timeout=12.5,
    )


def test_wait_until_merged_success() -> None:
    """Return the MR once a reload reports state merged."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_commit_sha = None

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        merge_request.state = "merged"
        merge_request.merge_commit_sha = "abc123"
        fresh = mock.Mock()
        attrs = {
            "state": "merged",
            "merge_commit_sha": "abc123",
            "web_url": merge_request.web_url,
        }
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    out = gitlab.wait_until_merged(merge_request, timeout_seconds=10)
    assert out is merge_request
    merge_request.manager.get.assert_called_once()


def test_wait_until_merged_closed_raises() -> None:
    """Closed merge requests fail instead of waiting."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "closed"
    _stub_merge_request_reload(merge_request)
    with pytest.raises(RuntimeError, match="is closed"):
        gitlab.wait_until_merged(merge_request, timeout_seconds=10)


def test_wait_until_merged_timeout_caps_poll_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Poll sleeps are capped so the timeout is not exceeded."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "checking"
    _stub_merge_request_reload(merge_request)
    clock = {"t": 0.0}
    slept: list[float] = []

    def _now() -> float:
        return clock["t"]

    def _sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["t"] += seconds

    monkeypatch.setattr(gitlab.time, "monotonic", _now)
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)

    with pytest.raises(TimeoutError, match="timed out waiting"):
        gitlab.wait_until_merged(
            merge_request,
            timeout_seconds=2,
            poll_interval_seconds=10,
        )

    assert clock["t"] == 2.0
    assert slept == [2.0]


def test_wait_until_merged_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise TimeoutError when the deadline is reached while still open."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "checking"
    _stub_merge_request_reload(merge_request)
    clock = {"t": 0.0}

    def _now() -> float:
        return clock["t"]

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    monkeypatch.setattr(gitlab.time, "monotonic", _now)
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)

    with pytest.raises(TimeoutError, match="timed out waiting"):
        gitlab.wait_until_merged(
            merge_request,
            timeout_seconds=0.5,
            poll_interval_seconds=1,
        )


@pytest.mark.parametrize(
    ("timeout_seconds", "poll_interval_seconds"),
    [
        (0, 10),
        (-1, 10),
        (float("nan"), 10),
        (float("inf"), 10),
        (10, 0),
        (10, -1),
        (10, float("nan")),
        (10, float("inf")),
    ],
)
def test_wait_until_merged_rejects_invalid_timing(
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> None:
    """Non-finite or non-positive timing values are rejected before polling."""
    with pytest.raises(ValueError, match="must be a finite number greater than zero"):
        gitlab.wait_until_merged(
            mock.Mock(),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )


def test_poll_remaining_seconds_uses_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the remaining poll time, or zero once the deadline has passed."""
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: 2.0)
    assert gitlab._poll_remaining_seconds(10.0) == 8.0
    assert gitlab._poll_remaining_seconds(1.0) == 0.0


def test_wait_until_merged_passes_request_timeout_to_refresh() -> None:
    """Pass a capped per-request timeout through to merge-request reload."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = "abc123"
    _stub_merge_request_reload(merge_request)
    gitlab.wait_until_merged(
        merge_request,
        timeout_seconds=10,
        poll_interval_seconds=1,
    )
    timeout = merge_request.manager.get.call_args.kwargs["timeout"]
    assert 0 < timeout <= 10
    assert timeout <= gitlab._DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS


def test_gitlab_request_timeout_caps_at_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not let one API call use more than the normal GitLab timeout."""
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: 0.0)
    assert gitlab._gitlab_request_timeout_seconds(10_000.0) == (
        gitlab._DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS
    )
    assert gitlab._gitlab_request_timeout_seconds(30.0) == 30.0
    assert gitlab._gitlab_request_timeout_seconds(0.0) == 0.0


def test_wait_until_merged_rejects_success_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat a reload that completes after the deadline as a timeout."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = "abc123"
    clock = {"t": 0.0}

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        clock["t"] = 11.0
        fresh = mock.Mock()
        attrs = {
            "state": "merged",
            "merge_commit_sha": "abc123",
            "web_url": merge_request.web_url,
        }
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])

    with pytest.raises(TimeoutError, match="timed out waiting"):
        gitlab.wait_until_merged(
            merge_request,
            timeout_seconds=10,
            poll_interval_seconds=1,
        )


def test_wait_until_merged_retries_transient_refresh_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry transient GitLab failures while reloading a merge request."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = "abc123"
    merged = mock.Mock()
    attrs = {
        "state": "merged",
        "merge_commit_sha": "abc123",
        "web_url": merge_request.web_url,
    }
    merged._attrs = attrs
    merged.attributes = attrs
    _stub_merge_request_reload(
        merge_request,
        get_side_effect=[
            GitlabError("service unavailable", response_code=503),
            merged,
        ],
    )
    clock = {"t": 0.0}

    def _now() -> float:
        return clock["t"]

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    monkeypatch.setattr(gitlab.time, "monotonic", _now)
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    out = gitlab.wait_until_merged(
        merge_request,
        timeout_seconds=10,
        poll_interval_seconds=1,
    )
    assert out is merge_request
    assert merge_request.manager.get.call_count == 2


@pytest.mark.parametrize(
    ("merge_status", "detailed", "expected"),
    [
        ("cannot_be_merged", "", True),
        ("", "conflict", True),
        ("", "conflict_severity_blocked", True),
        ("can_be_merged", "", False),
        ("", "mergeable", False),
    ],
)
def test_merge_request_has_conflict(
    merge_status: str,
    detailed: str,
    expected: bool,
) -> None:
    """Detect merge conflicts from GitLab merge status fields."""
    merge_request = mock.Mock()
    merge_request.merge_status = merge_status
    merge_request.detailed_merge_status = detailed
    assert gitlab.merge_request_has_conflict(merge_request) is expected


@pytest.mark.parametrize(
    ("merge_status", "detailed", "expected"),
    [
        ("can_be_merged", "", True),
        ("", "mergeable", True),
        ("cannot_be_merged", "", False),
        ("", "conflict", False),
    ],
)
def test_merge_request_is_mergeable(
    merge_status: str,
    detailed: str,
    expected: bool,
) -> None:
    """Detect mergeable MRs from GitLab merge status fields."""
    merge_request = mock.Mock()
    merge_request.merge_status = merge_status
    merge_request.detailed_merge_status = detailed
    assert gitlab.merge_request_is_mergeable(merge_request) is expected


def test_get_or_create_merge_request_retries_after_create_race() -> None:
    """Return a concurrently created MR when create fails."""
    existing = mock.Mock()
    client = _mock_gitlab_client()
    with (
        mock.patch.object(
            gitlab,
            "find_open_merge_request_by_source_branch",
            side_effect=[None, existing],
        ),
        mock.patch.object(
            gitlab,
            "create_merge_request",
            side_effect=GitlabError("already exists"),
        ),
    ):
        out = gitlab.get_or_create_merge_request(
            client,
            "g/r",
            source_branch="feat",
            target_branch="main",
            title="t",
            description="d",
        )
    assert out is existing


def test_get_or_create_merge_request_returns_existing() -> None:
    """Reuse an open MR for the source branch when one exists."""
    existing = mock.Mock()
    client = _mock_gitlab_client(list_return=[existing])
    with mock.patch.object(
        gitlab,
        "find_open_merge_request_by_source_branch",
        return_value=existing,
    ) as find_mr:
        out = gitlab.get_or_create_merge_request(
            client,
            "g/r",
            source_branch="feat",
            target_branch="main",
            title="t",
            description="d",
        )
    assert out is existing
    find_mr.assert_called_once()
    client.projects.get.return_value.mergerequests.create.assert_not_called()


def test_wait_for_open_merge_request_passes_request_timeout_to_lookup() -> None:
    """Pass the polling deadline through to merge-request lookup."""
    found = mock.Mock()
    found.iid = 3
    client = _mock_gitlab_client(list_return=[found])
    with mock.patch.object(
        gitlab,
        "find_open_merge_request_by_source_branch",
        return_value=found,
    ) as find_mr:
        out = gitlab.wait_for_open_merge_request_by_source_branch(
            client,
            "g/r",
            "feat",
            timeout_seconds=10,
            poll_interval_seconds=1,
        )
    assert out is found
    deadline = find_mr.call_args.kwargs["deadline"]
    assert deadline > time.monotonic()


def test_wait_for_open_merge_request_rejects_success_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat a lookup that completes after the deadline as a timeout."""
    found = mock.Mock()
    found.iid = 3
    client = _mock_gitlab_client()
    clock = {"t": 0.0}

    def _find_mr(*_args: object, **_kwargs: object) -> mock.Mock:
        clock["t"] = 11.0
        return found

    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    with mock.patch.object(
        gitlab,
        "find_open_merge_request_by_source_branch",
        side_effect=_find_mr,
    ):
        with pytest.raises(TimeoutError, match="timed out waiting"):
            gitlab.wait_for_open_merge_request_by_source_branch(
                client,
                "g/r",
                "feat",
                timeout_seconds=10,
                poll_interval_seconds=1,
            )


def test_wait_for_open_merge_request_propagates_auth_errors() -> None:
    """Do not retry authorization failures while polling for an open MR."""
    client = _mock_gitlab_client()
    with (
        mock.patch.object(
            gitlab,
            "find_open_merge_request_by_source_branch",
            side_effect=GitlabError("forbidden", response_code=403),
        ),
        pytest.raises(GitlabError),
    ):
        gitlab.wait_for_open_merge_request_by_source_branch(
            client,
            "g/r",
            "feat",
            timeout_seconds=10,
            poll_interval_seconds=1,
        )


def test_wait_for_open_merge_request_retries_transient_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry transient GitLab lookup failures while polling for an open MR."""
    found = mock.Mock()
    found.iid = 3
    client = _mock_gitlab_client()
    clock = {"t": 0.0}

    def _now() -> float:
        return clock["t"]

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    monkeypatch.setattr(gitlab.time, "monotonic", _now)
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    with mock.patch.object(
        gitlab,
        "find_open_merge_request_by_source_branch",
        side_effect=[
            GitlabError("service unavailable", response_code=503),
            found,
        ],
    ):
        out = gitlab.wait_for_open_merge_request_by_source_branch(
            client,
            "g/r",
            "feat",
            timeout_seconds=10,
            poll_interval_seconds=1,
        )
    assert out is found


def test_wait_for_open_merge_request_by_source_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return once an open merge request appears for the source branch."""
    found = mock.Mock()
    found.iid = 3
    client = _mock_gitlab_client()
    clock = {"t": 0.0}

    def _now() -> float:
        return clock["t"]

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    monkeypatch.setattr(gitlab.time, "monotonic", _now)
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    with mock.patch.object(
        gitlab,
        "find_open_merge_request_by_source_branch",
        side_effect=[None, found],
    ):
        out = gitlab.wait_for_open_merge_request_by_source_branch(
            client,
            "g/r",
            "feat",
            timeout_seconds=10,
            poll_interval_seconds=1,
        )
    assert out is found


def test_push_merge_request_to_main_merges_when_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept with merge-when-pipeline-succeeds when the MR is mergeable."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "can_be_merged"
    merge_request.merge_when_pipeline_succeeds = False
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        if merge_request.merge.called:
            merge_request.state = "merged"
            merge_request.merge_when_pipeline_succeeds = True
            merge_request.merge_commit_sha = "abc123"
        fresh = mock.Mock()
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_when_pipeline_succeeds": (merge_request.merge_when_pipeline_succeeds),
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    client = _mock_gitlab_client()
    out = gitlab.push_merge_request_to_main(
        client,
        "g/r",
        merge_request,
        "feat",
        timeout_seconds=10,
    )
    assert out is merge_request
    merge_request.merge.assert_called_once_with(
        merge_when_pipeline_succeeds=True,
        should_remove_source_branch=True,
        timeout=10.0,
    )


def test_push_merge_request_to_main_waits_for_auto_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not re-accept once auto-merge is armed; wait until merged."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "can_be_merged"
    merge_request.merge_when_pipeline_succeeds = False
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        if merge_request.merge.called:
            merge_request.merge_when_pipeline_succeeds = True
            if clock["t"] >= 2.0:
                merge_request.state = "merged"
                merge_request.merge_commit_sha = "abc123"
        fresh = mock.Mock()
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_when_pipeline_succeeds": (merge_request.merge_when_pipeline_succeeds),
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    client = _mock_gitlab_client()
    out = gitlab.push_merge_request_to_main(
        client,
        "g/r",
        merge_request,
        "feat",
        timeout_seconds=10,
        poll_interval_seconds=1,
    )
    assert out is merge_request
    merge_request.merge.assert_called_once_with(
        merge_when_pipeline_succeeds=True,
        should_remove_source_branch=True,
        timeout=10.0,
    )


def test_push_merge_request_to_main_auto_merge_uses_capped_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bound auto-merge HTTP timeout by remaining poll budget, capped at default."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "can_be_merged"
    merge_request.merge_when_pipeline_succeeds = False
    merge_request.merge_commit_sha = None
    # Remaining after refresh is 90s; individual request must still cap at 60s.
    clock = {"t": 10.0}

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        if merge_request.merge.called:
            merge_request.state = "merged"
            merge_request.merge_when_pipeline_succeeds = True
            merge_request.merge_commit_sha = "abc123"
        fresh = mock.Mock()
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_when_pipeline_succeeds": (merge_request.merge_when_pipeline_succeeds),
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    client = _mock_gitlab_client()
    # deadline = 10 + 100 = 110; remaining at arm time = 100; capped to 60
    out = gitlab.push_merge_request_to_main(
        client,
        "g/r",
        merge_request,
        "feat",
        timeout_seconds=100,
    )
    assert out is merge_request
    merge_request.merge.assert_called_once_with(
        merge_when_pipeline_succeeds=True,
        should_remove_source_branch=True,
        timeout=gitlab._DEFAULT_GITLAB_REQUEST_TIMEOUT_SECONDS,
    )


def test_push_merge_request_to_main_auto_merge_timeout_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise when the poll budget is exhausted before arming auto-merge."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "can_be_merged"
    merge_request.merge_when_pipeline_succeeds = False
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}
    # Refresh gets a positive timeout; auto-merge sees the budget exhausted.
    request_timeouts = iter([10.0, 0.0])

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        fresh = mock.Mock()
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_when_pipeline_succeeds": (merge_request.merge_when_pipeline_succeeds),
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(
        gitlab,
        "_gitlab_request_timeout_seconds",
        lambda _deadline: next(request_timeouts),
    )
    client = _mock_gitlab_client()
    with pytest.raises(TimeoutError, match="timed out"):
        gitlab.push_merge_request_to_main(
            client,
            "g/r",
            merge_request,
            "feat",
            timeout_seconds=10,
        )
    merge_request.merge.assert_not_called()


def test_push_merge_request_to_main_conflict_cleans_up() -> None:
    """Close the MR and delete the branch when a conflict is detected."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "cannot_be_merged"
    _stub_merge_request_reload(merge_request)
    client = _mock_gitlab_client()
    with mock.patch.object(gitlab, "cleanup_merge_request_branch") as cleanup:
        with pytest.raises(RuntimeError, match="merge conflict"):
            gitlab.push_merge_request_to_main(
                client,
                "g/r",
                merge_request,
                "feat",
                timeout_seconds=10,
            )
    cleanup.assert_called_once_with(client, "g/r", merge_request, "feat")


def test_is_transient_gitlab_error_connection_and_unrelated() -> None:
    """Classify connection errors and unrelated exceptions."""
    assert gitlab.is_transient_gitlab_error(GitlabConnectionError("down"))
    assert not gitlab.is_transient_gitlab_error(GitlabError("not found", response_code=404))
    assert not gitlab.is_transient_gitlab_error(ValueError("nope"))


@pytest.mark.parametrize("response_code", sorted(gitlab._TRANSIENT_GITLAB_LOOKUP_CODES))
def test_is_transient_gitlab_error_accepted_status_codes(response_code: int) -> None:
    """Treat every accepted transient response code as retryable."""
    assert gitlab.is_transient_gitlab_error(
        GitlabError("unavailable", response_code=response_code)
    )


def test_is_insufficient_scope_error_rejects_unrelated() -> None:
    """Non-GitLab and non-403 errors are not scope failures."""
    assert not gitlab.is_insufficient_scope_error(ValueError("x"))
    assert not gitlab.is_insufficient_scope_error(
        GitlabError("insufficient_scope", response_code=401)
    )


def test_poll_remaining_seconds_elapsed_without_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return zero when the deadline has passed and no callback is set."""
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: 5.0)
    assert gitlab._poll_remaining_seconds(1.0) == 0.0


def test_sleep_for_poll_interval_skips_when_deadline_elapsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not sleep when the poll deadline is already past."""
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: 10.0)
    slept: list[float] = []
    monkeypatch.setattr(gitlab.time, "sleep", slept.append)
    gitlab._sleep_for_poll_interval(deadline=5.0, poll_interval_seconds=1.0)
    assert slept == []


def test_refresh_merge_request_uses_attributes_without_attrs() -> None:
    """Fall back to ``attributes`` when ``_attrs`` is not a plain dict."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.get_id.return_value = 1
    fresh = mock.Mock(spec=["attributes"])
    fresh.attributes = {"state": "opened", "web_url": merge_request.web_url}
    merge_request.manager.get.return_value = fresh

    def _update_attrs(attrs: dict) -> None:
        for key, value in attrs.items():
            setattr(merge_request, key, value)

    merge_request._update_attrs.side_effect = _update_attrs
    gitlab._refresh_merge_request(merge_request, deadline=time.monotonic() + 10)
    merge_request._update_attrs.assert_called_once_with(
        {"state": "opened", "web_url": merge_request.web_url}
    )


def test_find_open_merge_request_passes_request_timeout() -> None:
    """Forward an explicit request timeout to project and list calls."""
    client = _mock_gitlab_client(list_return=[])
    gitlab.find_open_merge_request_by_source_branch(
        client,
        "g/r",
        "feat",
        request_timeout=7.5,
    )
    client.projects.get.assert_called_once()
    assert client.projects.get.call_args.kwargs["timeout"] == 7.5
    listed = client.projects.get.return_value.mergerequests.list
    listed.assert_called_once()
    assert listed.call_args.kwargs["timeout"] == 7.5


def test_find_open_merge_request_recalculates_deadline_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recalculate the capped timeout before each GitLab API call."""
    client = _mock_gitlab_client(list_return=[])
    project = client.projects.get.return_value
    clock = {"t": 0.0}
    timeouts: list[float] = []

    def _get(*_args: object, **kwargs: object) -> mock.Mock:
        timeouts.append(float(kwargs["timeout"]))
        clock["t"] = 30.0
        return project

    client.projects.get.side_effect = _get
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    gitlab.find_open_merge_request_by_source_branch(
        client,
        "g/r",
        "feat",
        deadline=50.0,
    )
    timeouts.append(float(project.mergerequests.list.call_args.kwargs["timeout"]))
    assert timeouts == [50.0, 20.0]


def test_close_and_delete_remote_branch() -> None:
    """Close an MR and delete its source branch via the project API."""
    merge_request = mock.Mock()
    gitlab.close_merge_request(merge_request)
    assert merge_request.state_event == "close"
    merge_request.save.assert_called_once()

    client = _mock_gitlab_client()
    gitlab.delete_remote_branch(client, "g/r", "feat")
    client.projects.get.return_value.branches.delete.assert_called_once_with("feat")


def test_cleanup_merge_request_branch_logs_failures() -> None:
    """Continue cleanup when close or delete raises."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    client = _mock_gitlab_client()
    with (
        mock.patch.object(
            gitlab,
            "close_merge_request",
            side_effect=RuntimeError("close failed"),
        ),
        mock.patch.object(
            gitlab,
            "delete_remote_branch",
            side_effect=RuntimeError("delete failed"),
        ),
        mock.patch.object(gitlab.logger, "exception") as log_exc,
    ):
        gitlab.cleanup_merge_request_branch(client, "g/r", merge_request, "feat")
    assert log_exc.call_count == 2


def test_get_or_create_merge_request_reraises_when_race_unresolved() -> None:
    """Propagate create failures when no concurrent MR appears."""
    client = _mock_gitlab_client()
    with (
        mock.patch.object(
            gitlab,
            "find_open_merge_request_by_source_branch",
            return_value=None,
        ),
        mock.patch.object(
            gitlab,
            "create_merge_request",
            side_effect=GitlabError("create failed"),
        ),
        pytest.raises(GitlabError, match="create failed"),
    ):
        gitlab.get_or_create_merge_request(
            client,
            "g/r",
            source_branch="feat",
            target_branch="main",
            title="t",
            description="d",
        )


def test_wait_until_merged_propagates_non_transient_refresh_error() -> None:
    """Surface permanent GitLab errors while reloading a merge request."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    _stub_merge_request_reload(
        merge_request,
        get_side_effect=GitlabError("forbidden", response_code=403),
    )
    with pytest.raises(GitlabError, match="forbidden"):
        gitlab.wait_until_merged(merge_request, timeout_seconds=10)


def test_push_merge_request_continues_after_transient_post_accept_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep waiting when the post-accept refresh hits a transient GitLab error."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "can_be_merged"
    merge_request.merge_when_pipeline_succeeds = False
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}
    calls = {"n": 0}

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        calls["n"] += 1
        if calls["n"] == 2:
            raise GitlabError("unavailable", response_code=503)
        if merge_request.merge.called and clock["t"] >= 1.0:
            merge_request.state = "merged"
            merge_request.merge_commit_sha = "abc123"
            merge_request.merge_when_pipeline_succeeds = True
        elif merge_request.merge.called:
            merge_request.merge_when_pipeline_succeeds = True
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_when_pipeline_succeeds": (merge_request.merge_when_pipeline_succeeds),
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh = mock.Mock()
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    out = gitlab.push_merge_request_to_main(
        _mock_gitlab_client(),
        "g/r",
        merge_request,
        "feat",
        timeout_seconds=10,
        poll_interval_seconds=1,
    )
    assert out is merge_request
    merge_request.merge.assert_called_once()


def test_wait_for_open_merge_request_times_out_when_never_created(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise TimeoutError once the deadline passes with no open MR."""
    client = _mock_gitlab_client(list_return=[])
    clock = {"t": 0.0}

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    with pytest.raises(TimeoutError, match="timed out waiting for open merge request"):
        gitlab.wait_for_open_merge_request_by_source_branch(
            client,
            "g/r",
            "feat",
            timeout_seconds=2,
            poll_interval_seconds=1,
        )


def test_push_merge_request_propagates_non_transient_post_accept_refresh() -> None:
    """Surface permanent errors from the refresh after auto-merge is armed."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "can_be_merged"
    merge_request.merge_when_pipeline_succeeds = False
    merge_request.merge_commit_sha = None
    calls = {"n": 0}

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        calls["n"] += 1
        if calls["n"] >= 2:
            raise GitlabError("forbidden", response_code=403)
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_when_pipeline_succeeds": False,
            "merge_commit_sha": None,
            "web_url": merge_request.web_url,
        }
        fresh = mock.Mock()
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    with pytest.raises(GitlabError, match="forbidden"):
        gitlab.push_merge_request_to_main(
            _mock_gitlab_client(),
            "g/r",
            merge_request,
            "feat",
            timeout_seconds=10,
            poll_interval_seconds=1,
        )


def test_wait_until_merged_sleeps_between_open_polls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sleep before rechecking while the MR stays open."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "checking"
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}
    slept: list[float] = []

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        if clock["t"] >= 1.0:
            merge_request.state = "merged"
            merge_request.merge_commit_sha = "abc123"
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh = mock.Mock()
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    def _sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["t"] += seconds

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    out = gitlab.wait_until_merged(
        merge_request,
        timeout_seconds=10,
        poll_interval_seconds=1,
    )
    assert out is merge_request
    assert slept == [1]


def test_wait_until_merged_uses_bounded_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Double the poll interval up to the merge-poll cap while waiting."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "checking"
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}
    slept: list[float] = []

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        # Stay open through the first few backoff steps, then merge.
        if clock["t"] >= 15.0:
            merge_request.state = "merged"
            merge_request.merge_commit_sha = "abc123"
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh = mock.Mock()
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    def _sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["t"] += seconds

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    out = gitlab.wait_until_merged(
        merge_request,
        timeout_seconds=100,
        poll_interval_seconds=1,
    )
    assert out is merge_request
    assert slept == [1.0, 2.0, 4.0, 8.0]


def test_next_merge_poll_interval_caps_at_max() -> None:
    """Backoff doubles until the configured merge-poll ceiling."""
    assert gitlab._next_merge_poll_interval(10.0, base_interval=10.0) == 20.0
    assert gitlab._next_merge_poll_interval(40.0, base_interval=10.0) == 60.0
    assert (
        gitlab._next_merge_poll_interval(60.0, base_interval=10.0)
        == gitlab._MAX_MERGE_POLL_INTERVAL_SECONDS
    )
    # An explicit base above the default cap must not shrink on later polls.
    assert gitlab._next_merge_poll_interval(90.0, base_interval=90.0) == 90.0


def test_push_merge_request_to_main_uses_bounded_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """push_merge_request_to_main backs off between open-MR checks."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "can_be_merged"
    merge_request.merge_when_pipeline_succeeds = False
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}
    slept: list[float] = []

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        if merge_request.merge.called:
            merge_request.merge_when_pipeline_succeeds = True
            if clock["t"] >= 7.0:
                merge_request.state = "merged"
                merge_request.merge_commit_sha = "abc123"
        fresh = mock.Mock()
        attrs = {
            "state": merge_request.state,
            "merge_status": merge_request.merge_status,
            "merge_when_pipeline_succeeds": (merge_request.merge_when_pipeline_succeeds),
            "merge_commit_sha": merge_request.merge_commit_sha,
            "web_url": merge_request.web_url,
        }
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    def _sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["t"] += seconds

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    out = gitlab.push_merge_request_to_main(
        _mock_gitlab_client(),
        "g/r",
        merge_request,
        "feat",
        timeout_seconds=100,
        poll_interval_seconds=1,
    )
    assert out is merge_request
    assert slept == [1.0, 2.0, 4.0]


def test_wait_for_open_merge_request_times_out_after_transient_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout after transient lookup failures exhaust the deadline."""
    client = _mock_gitlab_client()
    clock = {"t": 0.0}

    def _find(*_args: object, **_kwargs: object) -> None:
        raise GitlabError("unavailable", response_code=503)

    def _sleep(seconds: float) -> None:
        clock["t"] += seconds

    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", _sleep)
    with (
        mock.patch.object(
            gitlab,
            "find_open_merge_request_by_source_branch",
            side_effect=_find,
        ),
        pytest.raises(TimeoutError, match="timed out waiting for open merge request"),
    ):
        gitlab.wait_for_open_merge_request_by_source_branch(
            client,
            "g/r",
            "feat",
            timeout_seconds=2,
            poll_interval_seconds=1,
        )


def test_wait_until_merged_times_out_after_successful_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise TimeoutError when the deadline elapses after an open MR refresh."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "checking"
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}

    def _get(*_args: object, **_kwargs: object) -> mock.Mock:
        # Expire the deadline during the refresh so the post-check path runs.
        clock["t"] = 5.0
        attrs = {
            "state": "opened",
            "merge_status": "checking",
            "merge_commit_sha": None,
            "web_url": merge_request.web_url,
        }
        fresh = mock.Mock()
        fresh._attrs = attrs
        fresh.attributes = attrs
        return fresh

    _stub_merge_request_reload(merge_request, get_side_effect=_get)
    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", lambda _seconds: None)
    with pytest.raises(TimeoutError, match="timed out waiting"):
        gitlab.wait_until_merged(
            merge_request,
            timeout_seconds=2,
            poll_interval_seconds=1,
        )


def test_wait_for_open_merge_request_times_out_after_lookup_exhausts_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise TimeoutError when a lookup itself consumes the remaining deadline."""
    client = _mock_gitlab_client()
    clock = {"t": 0.0}

    def _find(*_args: object, **_kwargs: object) -> None:
        clock["t"] = 5.0
        raise GitlabError("unavailable", response_code=503)

    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", lambda _seconds: None)
    with (
        mock.patch.object(
            gitlab,
            "find_open_merge_request_by_source_branch",
            side_effect=_find,
        ),
        pytest.raises(TimeoutError, match="timed out waiting for open merge request"),
    ):
        gitlab.wait_for_open_merge_request_by_source_branch(
            client,
            "g/r",
            "feat",
            timeout_seconds=2,
            poll_interval_seconds=1,
        )


def test_wait_until_merged_times_out_after_waiting_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise TimeoutError when the deadline elapses after logging a wait."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "checking"
    merge_request.merge_commit_sha = None
    clock = {"t": 0.0}
    _stub_merge_request_reload(merge_request)

    def _info(*_args: object, **_kwargs: object) -> None:
        clock["t"] = 5.0

    monkeypatch.setattr(gitlab.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gitlab.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(gitlab.logger, "info", _info)
    with pytest.raises(TimeoutError, match="timed out waiting"):
        gitlab.wait_until_merged(
            merge_request,
            timeout_seconds=2,
            poll_interval_seconds=1,
        )
