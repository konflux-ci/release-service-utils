"""Tests for `vcs.github`."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from unittest import mock

import pytest
import requests

from . import github


def _session() -> github.GitHubAppSession:
    return github.GitHubAppSession(api_url="https://api.github.com", token="tok")


def test_owner_repo_from_url() -> None:
    """Extract `owner/repo` from GitHub HTTPS URLs."""
    assert github.owner_repo_from_url("https://github.com/org/repo") == "org/repo"
    assert github.owner_repo_from_url("https://github.com/org/repo/") == "org/repo"


def test_branch_name_from_origin_repo() -> None:
    """Use the last path segment of the origin repo URL as the branch name."""
    assert github.branch_name_from_origin_repo("https://github.com/org/my-app") == "my-app"


def test_app_jwt_builds_token(tmp_path: Path) -> None:
    """Build a three-part JWT signed with the app private key."""
    key = tmp_path / "key.pem"
    key.write_text("fake-key", encoding="utf-8")
    with mock.patch("vcs.github.subprocess.run") as run:
        run.return_value = mock.MagicMock(stdout=b"signature-bytes")
        token = github.app_jwt(key, "12345", expire_seconds=60)
    assert token.count(".") == 2
    run.assert_called_once()


def test_open_session(tmp_path: Path) -> None:
    """Exchange an app JWT for an installation access token."""
    key = tmp_path / "key.pem"
    key.write_text("fake", encoding="utf-8")
    with mock.patch.object(github, "app_jwt", return_value="jwt"):
        with mock.patch("vcs.github.requests.post") as post:
            post.return_value = mock.MagicMock(
                status_code=201,
                json=lambda: {"token": "inst-token"},
            )
            post.return_value.raise_for_status = mock.MagicMock()
            session = github.open_session(
                api_url="https://api.github.com",
                private_key_path=key,
                app_id="1",
                installation_id="2",
            )
    assert session.token == "inst-token"


def test_open_session_missing_token_raises(tmp_path: Path) -> None:
    """Raise when the installation token response omits `token`."""
    key = tmp_path / "key.pem"
    key.write_text("fake", encoding="utf-8")
    with mock.patch.object(github, "app_jwt", return_value="jwt"):
        with mock.patch("vcs.github.requests.post") as post:
            post.return_value = mock.MagicMock(
                status_code=201,
                json=lambda: {},
            )
            post.return_value.raise_for_status = mock.MagicMock()
            with pytest.raises(RuntimeError, match="authentication failed"):
                github.open_session(
                    api_url="https://api.github.com",
                    private_key_path=key,
                    app_id="1",
                    installation_id="2",
                )


def test_api_request_absolute_url_and_json_body() -> None:
    """Pass JSON bodies and extra headers through to `requests.request`."""
    session = _session()
    response = mock.MagicMock()
    with mock.patch("vcs.github.requests.request", return_value=response) as req:
        out = github.api_request(
            session,
            "PATCH",
            "https://api.github.com/repos/o/r/pulls/1",
            json_body={"body": "x"},
            extra_headers={"Accept": "application/vnd.github.v3+json"},
        )
    assert out is response
    _, kwargs = req.call_args
    assert kwargs["json"] == {"body": "x"}
    assert "Accept" in kwargs["headers"]


def test_get_json_uses_http_client() -> None:
    """GitHub GET helpers delegate to `http_client.get_text`."""
    session = _session()
    payload = {"items": []}
    with mock.patch.object(
        github.http_client,
        "get_text",
        return_value=json.dumps(payload),
    ) as get_text:
        out = github._get_json(session, "/search/issues?q=abc")
    assert out == payload
    get_text.assert_called_once()
    assert "Authorization" in get_text.call_args.kwargs["headers"]


def test_configure_git_askpass_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write an askpass script and configure git HTTPS token env vars."""
    askpass_path = tmp_path / "git-askpass.sh"
    fd = os.open(askpass_path, os.O_WRONLY | os.O_CREAT, stat.S_IRUSR | stat.S_IWUSR)
    monkeypatch.delenv("GIT_TERMINAL_PROMPT", raising=False)
    monkeypatch.delenv("GIT_ASKPASS", raising=False)
    monkeypatch.delenv("GITHUB_ACCESS_TOKEN", raising=False)

    with (
        mock.patch("vcs.github.tempfile.mkstemp", return_value=(fd, str(askpass_path))),
        mock.patch("vcs.github.atexit.register") as register,
    ):
        github.configure_git_askpass_auth("inst-token")

    assert askpass_path.read_text(encoding="utf-8") == (
        '#!/bin/sh\nexec echo "$GITHUB_ACCESS_TOKEN"\n'
    )
    assert os.access(askpass_path, os.X_OK)
    assert os.environ["GIT_TERMINAL_PROMPT"] == "0"
    assert os.environ["GIT_ASKPASS"] == str(askpass_path)
    assert os.environ["GITHUB_ACCESS_TOKEN"] == "inst-token"
    register.assert_called_once()


def test_force_push_updated_files(tmp_path: Path) -> None:
    """Recreate the branch at HEAD, commit, and force-push to GitHub."""
    session = _session()
    with (
        mock.patch.object(github, "configure_git_askpass_auth") as askpass,
        mock.patch.object(github.git, "configure_git_global_user") as git_user,
        mock.patch.object(github.git, "set_remote_url") as set_url,
        mock.patch.object(github.git, "checkout") as checkout,
        mock.patch.object(github.git, "fetch") as fetch,
        mock.patch.object(github.git, "index_add_commit") as commit,
        mock.patch.object(github.git, "push") as push,
    ):
        github.force_push_updated_files(
            session,
            clone_dir=tmp_path,
            target_repo="org/infra",
            branch="my-app",
            relative_paths=["a.yaml"],
        )
    askpass.assert_called_once_with("tok")
    git_user.assert_called_once_with("release-service", "release-service@redhat.com")
    set_url.assert_called_once_with(
        tmp_path,
        "origin",
        "https://github.com/org/infra.git",
    )
    checkout.assert_called_once_with(
        tmp_path,
        "my-app",
        reset=True,
        start_point="HEAD",
    )
    fetch.assert_not_called()
    commit.assert_called_once()
    push.assert_called_once_with(
        tmp_path,
        remote="origin",
        branch="my-app",
        force=True,
    )


def test_create_pull_request() -> None:
    """Open a pull request and return the JSON response body."""
    session = _session()
    response = mock.MagicMock(status_code=201)
    response.json.return_value = {"url": "https://github.com/o/r/pull/1"}
    with mock.patch.object(github, "api_request", return_value=response):
        out = github.create_pull_request(
            session,
            "org/infra",
            head_branch="my-app",
            title="my-app update",
        )
    assert out["url"].endswith("/pull/1")


def test_create_pull_request_returns_422_json() -> None:
    """Return the error JSON on 422 without raising."""
    session = _session()
    response = mock.MagicMock(status_code=422)
    response.json.return_value = {"message": "A pull request already exists."}
    with mock.patch.object(github, "api_request", return_value=response):
        out = github.create_pull_request(
            session,
            "org/infra",
            head_branch="my-app",
            title="my-app update",
        )
    assert "already exists" in out["message"]


def test_find_open_pull_request_by_branch_found() -> None:
    """Return the open PR whose head ref matches the branch."""
    session = _session()
    payload = [
        {
            "head": {"ref": "my-app"},
            "url": "https://api.github.com/repos/o/i/pulls/9",
        }
    ]
    with mock.patch.object(github, "_get_json", return_value=payload) as get_json:
        out = github.find_open_pull_request_by_branch(session, "org/infra", "my-app")
    assert out is not None
    assert out["head"]["ref"] == "my-app"
    assert "head=org:my-app" in get_json.call_args[0][1]


def test_find_open_pull_request_by_branch_not_found() -> None:
    """Return `None` when no open PR matches the branch."""
    session = _session()
    with mock.patch.object(github, "_get_json", return_value=[]):
        assert github.find_open_pull_request_by_branch(session, "org/infra", "my-app") is None


def test_pull_request_url_for_commit_sha() -> None:
    """Resolve a commit SHA to its pull request HTML URL via search."""
    session = _session()
    payload = {"items": [{"pull_request": {"html_url": "https://github.com/o/r/pull/2"}}]}
    with mock.patch.object(github, "_get_json", return_value=payload):
        url = github.pull_request_url_for_commit_sha(session, "abc123")
    assert url.endswith("/pull/2")


def test_pull_request_url_for_commit_sha_no_items() -> None:
    """Return None when the search API returns no matching issues."""
    session = _session()
    with mock.patch.object(github, "_get_json", return_value={"items": []}):
        assert github.pull_request_url_for_commit_sha(session, "abc123") is None


def test_pull_request_url_for_commit_sha_missing_html_url() -> None:
    """Return None when the search hit has no `pull_request.html_url`."""
    session = _session()
    with mock.patch.object(
        github,
        "_get_json",
        return_value={"items": [{"pull_request": {}}]},
    ):
        assert github.pull_request_url_for_commit_sha(session, "abc123") is None


def test_compare_changelog_non_200() -> None:
    """Return an empty string when the compare API is not successful."""
    session = _session()
    http_response = mock.MagicMock(status_code=404)
    with mock.patch.object(
        github,
        "_get_json",
        side_effect=requests.HTTPError(response=http_response),
    ):
        assert (
            github.compare_changelog(
                session,
                "https://github.com/org/repo",
                "old",
                "new",
            )
            == ""
        )


def test_compare_changelog_empty_commits() -> None:
    """Return an empty string when the compare response has no commits."""
    session = _session()
    with mock.patch.object(github, "_get_json", return_value={"commits": []}):
        assert (
            github.compare_changelog(
                session,
                "https://github.com/org/repo",
                "old",
                "new",
            )
            == ""
        )


def test_compare_changelog_malformed_response() -> None:
    """Return an empty string when the compare payload cannot be parsed."""
    session = _session()
    with mock.patch.object(github, "_get_json", return_value={"commits": [{}]}):
        assert (
            github.compare_changelog(
                session,
                "https://github.com/org/repo",
                "old",
                "new",
            )
            == ""
        )


def test_compare_changelog_get_json_failure() -> None:
    """Return an empty string when the compare GET or JSON decode fails."""
    session = _session()
    with mock.patch.object(
        github,
        "_get_json",
        side_effect=ValueError("invalid JSON"),
    ):
        assert (
            github.compare_changelog(
                session,
                "https://github.com/org/repo",
                "old",
                "new",
            )
            == ""
        )


def test_compare_changelog_with_github_login() -> None:
    """Format changelog entries with `@login` when GitHub author metadata exists."""
    session = _session()
    payload = {
        "commits": [
            {
                "sha": "a" * 40,
                "html_url": "https://github.com/o/r/commit/aaa",
                "commit": {"message": "fix things\n\nbody", "author": {"name": "A"}},
                "author": {"login": "dev"},
            }
        ]
    }
    with mock.patch.object(github, "_get_json", return_value=payload):
        out = github.compare_changelog(
            session,
            "https://github.com/org/repo",
            "old",
            "new",
        )
    assert "## Changelog" in out
    assert "@dev" in out


def test_compare_changelog_without_github_login() -> None:
    """Fall back to the commit author name when GitHub login is absent."""
    session = _session()
    payload = {
        "commits": [
            {
                "sha": "b" * 40,
                "html_url": "https://github.com/o/r/commit/bbb",
                "commit": {"message": "feat", "author": {"name": "Release Bot"}},
                "author": None,
            }
        ]
    }
    with mock.patch.object(github, "_get_json", return_value=payload):
        out = github.compare_changelog(
            session,
            "https://github.com/org/repo",
            "old",
            "new",
        )
    assert "Release Bot" in out
    assert "@" not in out.split("\n", 1)[1]


def test_update_pull_request_body() -> None:
    """PATCH the pull request description and return the updated JSON."""
    session = _session()
    response = mock.MagicMock()
    response.json.return_value = {"body": "updated"}
    with mock.patch.object(github, "api_request", return_value=response):
        out = github.update_pull_request_body(
            session,
            "https://api.github.com/repos/o/i/pulls/1",
            "new body",
        )
    assert out["body"] == "updated"
