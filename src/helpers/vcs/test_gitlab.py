"""Tests for `vcs.gitlab`."""

from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

import pytest

from . import git
from . import gitlab


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
    mk.assert_called_once_with("gitlab.example.com", private_token="tok")


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


def test_enable_auto_merge() -> None:
    """Enable merge-when-pipeline-succeeds and drop the source branch."""
    merge_request = mock.Mock()
    out = gitlab.enable_auto_merge(merge_request)
    assert out is merge_request
    merge_request.merge.assert_called_once_with(
        merge_when_pipeline_succeeds=True,
        should_remove_source_branch=True,
    )


def test_wait_until_merged_success() -> None:
    """Return the MR once refresh reports state merged."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"

    def _refresh() -> None:
        merge_request.state = "merged"

    merge_request.refresh.side_effect = _refresh
    out = gitlab.wait_until_merged(merge_request, timeout_seconds=10)
    assert out is merge_request
    merge_request.refresh.assert_called_once()


def test_wait_until_merged_closed_raises() -> None:
    """Closed merge requests fail instead of waiting."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "closed"
    merge_request.refresh.return_value = None
    with pytest.raises(RuntimeError, match="is closed"):
        gitlab.wait_until_merged(merge_request, timeout_seconds=10)


def test_wait_until_merged_timeout_caps_backoff_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backoff sleeps are capped so timeout is not exceeded."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_status = "checking"
    merge_request.refresh.return_value = None
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
    merge_request.refresh.return_value = None
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


def test_wait_until_merged_uses_exponential_backoff() -> None:
    """Unmerged polls retry through the shared exponential-backoff helper."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.refresh.return_value = None
    with mock.patch.object(
        gitlab.retry,
        "retry_with_exponential_backoff",
        side_effect=gitlab._MergeRequestNotMerged(),
    ) as retry_backoff:
        with pytest.raises(TimeoutError, match="timed out waiting"):
            gitlab.wait_until_merged(merge_request, timeout_seconds=10)
    retry_backoff.assert_called_once()
    assert retry_backoff.call_args.kwargs["retry_on"] is gitlab._MergeRequestNotMerged
