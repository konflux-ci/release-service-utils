"""Verify close_advisory_issues task logic."""

from __future__ import annotations

import json
import logging
import runpy
from pathlib import Path
from typing import Any
from unittest import mock

import close_advisory_issues
import pytest
import requests
from requests.auth import HTTPBasicAuth


@pytest.fixture(autouse=True)
def _propagate_release_logger() -> None:
    """Allow caplog to capture records from the `release` logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


def _write_data(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")


def _write_jira_secret(path: Path) -> None:
    """Write dummy Jira credentials under *path*."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "email").write_text("team@domain.com\n", encoding="utf-8")
    (path / "token").write_text("abcdefg\n", encoding="utf-8")


def _fixed_issue(
    issue_id: str,
    source: str = "redhat.atlassian.net",
) -> dict[str, str]:
    """Build one releaseNotes fixed-issue entry."""
    return {"id": issue_id, "source": source}


def test_normalize_issue_server_maps_legacy_host() -> None:
    """Convert issues.redhat.com to redhat.atlassian.net."""
    assert (
        close_advisory_issues.normalize_issue_server("issues.redhat.com")
        == "redhat.atlassian.net"
    )


def test_is_jira_eligible_issue_accepts_eligible_rows() -> None:
    """Return true for supported Jira fixed-issue rows."""
    assert close_advisory_issues.is_jira_eligible_issue(_fixed_issue("ISSUE-123"))
    assert close_advisory_issues.is_jira_eligible_issue(
        _fixed_issue("ISSUE-123", "issues.redhat.com"),
    )
    assert close_advisory_issues.is_jira_eligible_issue(_fixed_issue("123456"))
    assert close_advisory_issues.is_jira_eligible_issue(_fixed_issue("RHOSP-12345"))
    assert close_advisory_issues.is_jira_eligible_issue(
        _fixed_issue("ISSUE-123", " redhat.atlassian.net "),
    )


def test_is_jira_eligible_issue_rejects_unsupported_tracker() -> None:
    """Return false for non-Jira trackers."""
    assert not close_advisory_issues.is_jira_eligible_issue(
        _fixed_issue("12345", "bugzilla.redhat.com"),
    )


def test_is_jira_eligible_issue_rejects_missing_fields() -> None:
    """Return false when id or source is missing."""
    assert not close_advisory_issues.is_jira_eligible_issue(
        {"source": "redhat.atlassian.net"},
    )
    assert not close_advisory_issues.is_jira_eligible_issue({"id": "ISSUE-123"})


def test_is_jira_eligible_issue_rejects_invalid_id() -> None:
    """Return false when the id would alter the Jira REST path."""
    assert not close_advisory_issues.is_jira_eligible_issue(
        _fixed_issue("ISSUE-123/extra"),
    )
    assert not close_advisory_issues.is_jira_eligible_issue(
        _fixed_issue("ISSUE-123?query=1"),
    )
    assert not close_advisory_issues.is_jira_eligible_issue(_fixed_issue("../admin"))


def test_load_fixed_issues_returns_empty_when_issues_not_object() -> None:
    """Return an empty list when releaseNotes issues is not an object."""
    data = {"releaseNotes": {"issues": []}}
    assert close_advisory_issues.load_fixed_issues(data) == []


def test_load_fixed_issues_returns_empty_when_fixed_not_array() -> None:
    """Return an empty list when fixed issues is not an array."""
    data = {"releaseNotes": {"issues": {"fixed": "bad"}}}
    assert close_advisory_issues.load_fixed_issues(data) == []


def test_issue_status_name_returns_empty_for_invalid_payload() -> None:
    """Return an empty string when status fields are missing or invalid."""
    assert close_advisory_issues.issue_status_name({}) == ""
    assert close_advisory_issues.issue_status_name({"fields": []}) == ""
    assert (
        close_advisory_issues.issue_status_name(
            {"fields": {"status": "bad"}},
        )
        == ""
    )
    assert (
        close_advisory_issues.issue_status_name(
            {"fields": {"status": {"name": 123}}},
        )
        == ""
    )


def test_closed_transition_id_ignores_invalid_entries() -> None:
    """Skip invalid transition entries when searching for Closed."""
    payload = {
        "transitions": [
            "bad",
            {"id": "11", "name": "New"},
            {"id": "91", "name": "Closed"},
        ],
    }
    assert close_advisory_issues.closed_transition_id(payload) == "91"
    assert close_advisory_issues.closed_transition_id({"transitions": "bad"}) is None


def test_jira_get_json_returns_object_payload() -> None:
    """Return parsed JSON objects from successful GET responses."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.json.return_value = {"fields": {"status": {"name": "Open"}}}
    response.raise_for_status.return_value = None
    session.get.return_value = response
    auth = HTTPBasicAuth("user", "token")
    payload = close_advisory_issues.jira_get_json(
        session,
        "https://example.test/issue",
        auth,
    )
    assert payload["fields"]["status"]["name"] == "Open"


def test_close_issue_with_comment_posts_transition_payload() -> None:
    """POST a close transition with an advisory comment."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch("close_advisory_issues.jira_post_json") as post_json:
        close_advisory_issues.close_issue_with_comment(
            session,
            "https://example.test/issue/ISSUE-123",
            auth,
            "91",
            "Fixed in Konflux Advisory https://example.test/advisory",
        )
    post_json.assert_called_once_with(
        session,
        "https://example.test/issue/ISSUE-123/transitions",
        auth,
        {
            "transition": {"id": "91"},
            "update": {
                "comment": [
                    {
                        "add": {
                            "body": (
                                "Fixed in Konflux Advisory " "https://example.test/advisory"
                            ),
                        },
                    },
                ],
            },
        },
    )


def test_add_issue_comment_posts_body_payload() -> None:
    """POST a standalone comment to the issue."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch("close_advisory_issues.jira_post_json") as post_json:
        close_advisory_issues.add_issue_comment(
            session,
            "https://example.test/issue/ISSUE-123",
            auth,
            "Fixed in Konflux Advisory https://example.test/advisory",
        )
    post_json.assert_called_once_with(
        session,
        "https://example.test/issue/ISSUE-123/comment",
        auth,
        {"body": "Fixed in Konflux Advisory https://example.test/advisory"},
    )


def test_process_fixed_issue_skips_missing_source(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip rows that do not include an issue source."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with caplog.at_level(logging.WARNING, logger="release"):
        close_advisory_issues.process_fixed_issue(
            {"id": "ISSUE-123"},
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            auth=auth,
            session=session,
        )
    assert "missing source" in caplog.text


def test_close_advisory_issues_skips_jira_setup_without_eligible_issues(
    tmp_path: Path,
) -> None:
    """Do not read Jira secrets when there are no Jira-eligible fixed issues."""
    _write_data(tmp_path / "data.json", {"foo": "bar"})
    with mock.patch("close_advisory_issues.read_jira_credentials") as read_creds:
        with mock.patch(
            "close_advisory_issues.http_client.get_retry_session",
        ) as create_session:
            close_advisory_issues.close_advisory_issues(
                data_dir=tmp_path,
                data_path=Path("data.json"),
                advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                secret_path=tmp_path / "secrets",
            )
    read_creds.assert_not_called()
    create_session.assert_not_called()


def test_close_advisory_issues_skips_jira_setup_for_unsupported_trackers_only(
    tmp_path: Path,
) -> None:
    """Do not read Jira secrets when every fixed issue is on an unsupported tracker."""
    _write_data(
        tmp_path / "data.json",
        {
            "releaseNotes": {
                "issues": {
                    "fixed": [_fixed_issue("12345", "bugzilla.redhat.com")],
                },
            },
        },
    )
    with mock.patch("close_advisory_issues.read_jira_credentials") as read_creds:
        with mock.patch(
            "close_advisory_issues.http_client.get_retry_session",
        ) as create_session:
            close_advisory_issues.close_advisory_issues(
                data_dir=tmp_path,
                data_path=Path("data.json"),
                advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                secret_path=tmp_path / "secrets",
            )
    read_creds.assert_not_called()
    create_session.assert_not_called()


def test_close_advisory_issues_creates_retry_session_when_jira_issue_present(
    tmp_path: Path,
) -> None:
    """Create a retry-enabled session when at least one Jira issue is eligible."""
    _write_jira_secret(tmp_path / "secrets")
    _write_data(
        tmp_path / "data.json",
        {"releaseNotes": {"issues": {"fixed": [_fixed_issue("ISSUE-123")]}}},
    )
    session = mock.MagicMock()
    with mock.patch(
        "close_advisory_issues.http_client.get_retry_session",
        return_value=session,
    ) as create_session:
        with mock.patch("close_advisory_issues.process_fixed_issue") as process_issue:
            close_advisory_issues.close_advisory_issues(
                data_dir=tmp_path,
                data_path=Path("data.json"),
                advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                secret_path=tmp_path / "secrets",
            )
    create_session.assert_called_once_with(
        total=5,
        connect=3,
        read=3,
        status=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
    )
    process_issue.assert_called_once()


def test_load_fixed_issues_returns_empty_when_missing() -> None:
    """Return an empty list when releaseNotes issues are absent."""
    assert close_advisory_issues.load_fixed_issues({"foo": "bar"}) == []
    assert (
        close_advisory_issues.load_fixed_issues(
            {"releaseNotes": {"issues": {}}},
        )
        == []
    )


def test_load_fixed_issues_returns_object_rows_only() -> None:
    """Keep only JSON object entries from the fixed issues array."""
    data = {
        "releaseNotes": {
            "issues": {
                "fixed": [
                    _fixed_issue("ISSUE-123"),
                    "bad",
                ],
            },
        },
    }
    assert close_advisory_issues.load_fixed_issues(data) == [_fixed_issue("ISSUE-123")]


def test_close_comment_includes_advisory_url() -> None:
    """Build the advisory comment with the provided URL."""
    url = "https://access.redhat.com/errata/RHBA-2025:1111"
    assert close_advisory_issues.close_comment(url) == (f"Fixed in Konflux Advisory {url}")


def test_read_jira_credentials_reads_secret_files(tmp_path: Path) -> None:
    """Load email and token from mounted secret files."""
    _write_jira_secret(tmp_path)
    assert close_advisory_issues.read_jira_credentials(tmp_path) == (
        "team@domain.com",
        "abcdefg",
    )


def test_read_jira_credentials_rejects_blank_values(tmp_path: Path) -> None:
    """Reject secrets when email or token is blank."""
    _write_jira_secret(tmp_path)
    (tmp_path / "email").write_text(" \n", encoding="utf-8")
    with pytest.raises(ValueError, match="must include email and token"):
        close_advisory_issues.read_jira_credentials(tmp_path)


def test_jira_issue_url_builds_atlassian_api_url() -> None:
    """Build the REST URL for a Jira issue id."""
    assert (
        close_advisory_issues.jira_issue_url(
            "redhat.atlassian.net",
            "ISSUE-123",
        )
        == "https://redhat.atlassian.net/rest/api/2/issue/ISSUE-123"
    )


def test_api_path_for_server_rejects_unknown_host() -> None:
    """Raise when no tracker mapping exists for the server."""
    with pytest.raises(ValueError, match="no API mapping"):
        close_advisory_issues.api_path_for_server("example.com")


def test_issue_status_name_reads_nested_field() -> None:
    """Extract the status name from a Jira issue payload."""
    issue = {"fields": {"status": {"name": "Closed"}}}
    assert close_advisory_issues.issue_status_name(issue) == "Closed"


def test_closed_transition_id_returns_closed_id() -> None:
    """Return the id for the Closed transition when present."""
    payload = {
        "transitions": [
            {"id": "91", "name": "Closed"},
            {"id": "11", "name": "New"},
        ],
    }
    assert close_advisory_issues.closed_transition_id(payload) == "91"


def test_closed_transition_id_returns_none_when_missing() -> None:
    """Return None when no Closed transition exists."""
    assert close_advisory_issues.closed_transition_id({"transitions": []}) is None


def test_jira_get_json_rejects_non_object_payload() -> None:
    """Raise when the response body is not a JSON object."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.json.return_value = []
    response.raise_for_status.return_value = None
    session.get.return_value = response
    auth = HTTPBasicAuth("user", "token")
    with pytest.raises(ValueError, match="expected JSON object"):
        close_advisory_issues.jira_get_json(session, "https://example.test", auth)


def test_process_fixed_issue_skips_invalid_jira_issue_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip Jira issues whose id would alter the REST path."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with caplog.at_level(logging.WARNING, logger="release"):
        close_advisory_issues.process_fixed_issue(
            _fixed_issue("ISSUE-123/extra"),
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            auth=auth,
            session=session,
        )
    session.get.assert_not_called()
    assert "invalid Jira id" in caplog.text


def test_process_fixed_issue_skips_unsupported_tracker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip Bugzilla issues without calling the Jira API."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with caplog.at_level(logging.WARNING, logger="release"):
        close_advisory_issues.process_fixed_issue(
            _fixed_issue("12345", "bugzilla.redhat.com"),
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            auth=auth,
            session=session,
        )
    session.get.assert_not_called()
    assert "Skipping issue" in caplog.text


def test_process_fixed_issue_skips_already_closed_issue(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip closing when the issue is already Closed."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch(
        "close_advisory_issues.jira_get_json",
        return_value={"fields": {"status": {"name": "Closed"}}},
    ) as get_json:
        with caplog.at_level(logging.INFO, logger="release"):
            close_advisory_issues.process_fixed_issue(
                _fixed_issue("CLOSED-987"),
                advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                auth=auth,
                session=session,
            )
    get_json.assert_called_once()
    session.post.assert_not_called()
    assert "already in Closed state" in caplog.text


def test_process_fixed_issue_closes_open_issue() -> None:
    """Close an open issue when the Closed transition is available."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch(
        "close_advisory_issues.jira_get_json",
        side_effect=[
            {"fields": {"status": {"name": "Open"}}},
            {"transitions": [{"id": "91", "name": "Closed"}]},
        ],
    ) as get_json:
        with mock.patch(
            "close_advisory_issues.close_issue_with_comment",
        ) as close_issue:
            close_advisory_issues.process_fixed_issue(
                _fixed_issue("ISSUE-123"),
                advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                auth=auth,
                session=session,
            )
    assert get_json.call_count == 2
    close_issue.assert_called_once()


def test_process_fixed_issue_adds_comment_when_no_closed_transition(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Add a comment when the Closed transition id cannot be resolved."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch(
        "close_advisory_issues.jira_get_json",
        side_effect=[
            {"fields": {"status": {"name": "Open"}}},
            {"transitions": []},
        ],
    ):
        with mock.patch("close_advisory_issues.add_issue_comment") as add_comment:
            with caplog.at_level(logging.WARNING, logger="release"):
                close_advisory_issues.process_fixed_issue(
                    _fixed_issue("NOCLOSE-555"),
                    advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                    auth=auth,
                    session=session,
                )
    add_comment.assert_called_once()
    assert "failed to fetch the closed state id" in caplog.text


def test_process_fixed_issue_adds_comment_when_close_request_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Add a comment when the close transition POST fails."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch(
        "close_advisory_issues.jira_get_json",
        side_effect=[
            {"fields": {"status": {"name": "Open"}}},
            {"transitions": [{"id": "91", "name": "Closed"}]},
        ],
    ):
        with mock.patch(
            "close_advisory_issues.close_issue_with_comment",
            side_effect=requests.HTTPError("close failed"),
        ):
            with mock.patch("close_advisory_issues.add_issue_comment") as add_comment:
                with caplog.at_level(logging.WARNING, logger="release"):
                    close_advisory_issues.process_fixed_issue(
                        _fixed_issue("FAIL-999"),
                        advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                        auth=auth,
                        session=session,
                    )
    add_comment.assert_called_once()
    assert "failed to close issue" in caplog.text


def test_process_fixed_issue_warns_when_comment_also_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log a warning when both close and comment requests fail."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch(
        "close_advisory_issues.jira_get_json",
        side_effect=[
            {"fields": {"status": {"name": "Open"}}},
            {"transitions": []},
        ],
    ):
        with mock.patch(
            "close_advisory_issues.add_issue_comment",
            side_effect=requests.HTTPError("comment failed"),
        ):
            with caplog.at_level(logging.WARNING, logger="release"):
                close_advisory_issues.process_fixed_issue(
                    _fixed_issue("FAIL-999"),
                    advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                    auth=auth,
                    session=session,
                )
    assert "failed to add comment to issue" in caplog.text


def test_process_fixed_issue_uses_atlassian_for_legacy_source() -> None:
    """Query redhat.atlassian.net when the issue source is issues.redhat.com."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch(
        "close_advisory_issues.jira_get_json",
        return_value={"fields": {"status": {"name": "Closed"}}},
    ) as get_json:
        close_advisory_issues.process_fixed_issue(
            _fixed_issue("ISSUE-123", "issues.redhat.com"),
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            auth=auth,
            session=session,
        )
    assert get_json.call_args.args[1].startswith(
        "https://redhat.atlassian.net/rest/api/2/issue/ISSUE-123",
    )


def test_process_fixed_issue_strips_source_before_normalization() -> None:
    """Normalize Jira hostnames after trimming surrounding whitespace."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with mock.patch(
        "close_advisory_issues.jira_get_json",
        return_value={"fields": {"status": {"name": "Closed"}}},
    ) as get_json:
        close_advisory_issues.process_fixed_issue(
            _fixed_issue("ISSUE-123", " issues.redhat.com "),
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            auth=auth,
            session=session,
        )
    assert get_json.call_args.args[1].startswith(
        "https://redhat.atlassian.net/rest/api/2/issue/ISSUE-123",
    )


def test_process_fixed_issue_skips_missing_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip rows that do not include an issue id."""
    session = mock.MagicMock()
    auth = HTTPBasicAuth("user", "token")
    with caplog.at_level(logging.WARNING, logger="release"):
        close_advisory_issues.process_fixed_issue(
            {"source": "redhat.atlassian.net"},
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            auth=auth,
            session=session,
        )
    assert "missing id" in caplog.text


def test_close_advisory_issues_happy_path(tmp_path: Path) -> None:
    """Close supported Jira issues and skip unsupported trackers."""
    _write_jira_secret(tmp_path / "secrets")
    _write_data(
        tmp_path / "data.json",
        {
            "releaseNotes": {
                "issues": {
                    "fixed": [
                        _fixed_issue("ISSUE-123"),
                        _fixed_issue("12345", "bugzilla.redhat.com"),
                    ],
                },
            },
        },
    )
    with mock.patch("close_advisory_issues.process_fixed_issue") as process_issue:
        close_advisory_issues.close_advisory_issues(
            data_dir=tmp_path,
            data_path=Path("data.json"),
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            secret_path=tmp_path / "secrets",
        )
    assert process_issue.call_count == 2


def test_close_advisory_issues_no_issues(tmp_path: Path) -> None:
    """Succeed when release data has no fixed issues."""
    _write_jira_secret(tmp_path / "secrets")
    _write_data(tmp_path / "data.json", {"foo": "bar"})
    with mock.patch("close_advisory_issues.process_fixed_issue") as process_issue:
        close_advisory_issues.close_advisory_issues(
            data_dir=tmp_path,
            data_path=Path("data.json"),
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            secret_path=tmp_path / "secrets",
        )
    process_issue.assert_not_called()


def test_close_advisory_issues_missing_data_file(tmp_path: Path) -> None:
    """Fail when the release data file is missing."""
    _write_jira_secret(tmp_path / "secrets")
    with pytest.raises(FileNotFoundError, match="No data JSON was provided"):
        close_advisory_issues.close_advisory_issues(
            data_dir=tmp_path,
            data_path=Path("missing.json"),
            advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
            secret_path=tmp_path / "secrets",
        )


def test_close_advisory_issues_propagates_issue_lookup_failure(
    tmp_path: Path,
) -> None:
    """Propagate Jira lookup failures for supported issues."""
    _write_jira_secret(tmp_path / "secrets")
    _write_data(
        tmp_path / "data.json",
        {"releaseNotes": {"issues": {"fixed": [_fixed_issue("ISSUE-123")]}}},
    )
    with mock.patch(
        "close_advisory_issues.process_fixed_issue",
        side_effect=requests.HTTPError("lookup failed"),
    ):
        with pytest.raises(requests.HTTPError, match="lookup failed"):
            close_advisory_issues.close_advisory_issues(
                data_dir=tmp_path,
                data_path=Path("data.json"),
                advisory_url="https://access.redhat.com/errata/RHBA-2025:1111",
                secret_path=tmp_path / "secrets",
            )


def test_jira_post_json_raises_on_http_error() -> None:
    """Raise when a Jira POST response is not successful."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError("bad request")
    session.post.return_value = response
    auth = HTTPBasicAuth("user", "token")
    with pytest.raises(requests.HTTPError, match="bad request"):
        close_advisory_issues.jira_post_json(
            session,
            "https://example.test/comment",
            auth,
            {"body": "hello"},
        )


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit zero after a successful run."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv(
        "PARAM_ADVISORY_URL",
        "https://access.redhat.com/errata/RHBA-2025:1111",
    )
    monkeypatch.setenv("JIRA_SECRET_PATH", str(tmp_path / "secrets"))
    with mock.patch("close_advisory_issues.close_advisory_issues") as run:
        assert close_advisory_issues.main() == 0
    run.assert_called_once()


def test_module_main_guard_propagates_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Executing the module as `__main__` propagates failures from main()."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_DATA_PATH", "missing.json")
    monkeypatch.setenv(
        "PARAM_ADVISORY_URL",
        "https://access.redhat.com/errata/RHBA-2025:1111",
    )
    monkeypatch.setenv("JIRA_SECRET_PATH", str(tmp_path / "secrets"))
    with pytest.raises(FileNotFoundError, match="No data JSON was provided"):
        runpy.run_module("close_advisory_issues", run_name="__main__")
