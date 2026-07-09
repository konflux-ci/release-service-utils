"""Tests for the jira helper module."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import jira
import pytest
import requests
from requests.auth import HTTPBasicAuth


class TestNormalizeIssueServer:
    """Test legacy server name normalization."""

    def test_maps_legacy_host(self) -> None:
        """Convert issues.redhat.com to redhat.atlassian.net."""
        assert jira.normalize_issue_server("issues.redhat.com") == "redhat.atlassian.net"

    def test_passes_through_other_servers(self) -> None:
        """Return non-legacy servers unchanged."""
        assert jira.normalize_issue_server("redhat.atlassian.net") == "redhat.atlassian.net"
        assert jira.normalize_issue_server("jira.atlassian.com") == "jira.atlassian.com"
        assert jira.normalize_issue_server("bugzilla.redhat.com") == "bugzilla.redhat.com"


class TestApiPathForServer:
    """Test REST API path resolution from server names."""

    def test_returns_jira_api_path(self) -> None:
        """Return the Jira REST path for known Jira servers."""
        assert jira.api_path_for_server("redhat.atlassian.net") == "rest/api/2/issue"
        assert jira.api_path_for_server("jira.atlassian.com") == "rest/api/2/issue"
        assert jira.api_path_for_server("issues.redhat.com") == "rest/api/2/issue"

    def test_returns_bugzilla_api_path(self) -> None:
        """Return the Bugzilla REST path for bugzilla.redhat.com."""
        assert jira.api_path_for_server("bugzilla.redhat.com") == "rest/bug"

    def test_raises_for_unknown_server(self) -> None:
        """Raise ValueError for servers not in the tracker map."""
        with pytest.raises(ValueError, match="no API mapping"):
            jira.api_path_for_server("example.com")


class TestReadJiraCredentials:
    """Test reading credentials from mounted secret files."""

    def test_reads_email_and_token(self, tmp_path: Path) -> None:
        """Load email and token from mounted secret files."""
        (tmp_path / "email").write_text("team@domain.com\n", encoding="utf-8")
        (tmp_path / "token").write_text("abcdefg\n", encoding="utf-8")
        assert jira.read_jira_credentials(tmp_path) == ("team@domain.com", "abcdefg")

    def test_rejects_blank_email(self, tmp_path: Path) -> None:
        """Raise when email is blank."""
        (tmp_path / "email").write_text(" \n", encoding="utf-8")
        (tmp_path / "token").write_text("abcdefg\n", encoding="utf-8")
        with pytest.raises(ValueError, match="must include email and token"):
            jira.read_jira_credentials(tmp_path)

    def test_rejects_blank_token(self, tmp_path: Path) -> None:
        """Raise when token is blank."""
        (tmp_path / "email").write_text("team@domain.com\n", encoding="utf-8")
        (tmp_path / "token").write_text(" \n", encoding="utf-8")
        with pytest.raises(ValueError, match="must include email and token"):
            jira.read_jira_credentials(tmp_path)


class TestJiraIssueUrl:
    """Test API URL construction."""

    def test_builds_atlassian_url(self) -> None:
        """Build the REST URL for a Jira issue."""
        assert (
            jira.jira_issue_url("redhat.atlassian.net", "ISSUE-123")
            == "https://redhat.atlassian.net/rest/api/2/issue/ISSUE-123"
        )

    def test_builds_bugzilla_url(self) -> None:
        """Build the REST URL for a Bugzilla bug."""
        assert (
            jira.jira_issue_url("bugzilla.redhat.com", "12345")
            == "https://bugzilla.redhat.com/rest/bug/12345"
        )


class TestJiraGetJson:
    """Test authenticated JSON GET requests."""

    def test_returns_parsed_json(self) -> None:
        """Return the parsed JSON object from a successful response."""
        session = mock.MagicMock(spec=requests.Session)
        response = mock.MagicMock()
        response.json.return_value = {"fields": {"status": {"name": "Open"}}}
        response.raise_for_status.return_value = None
        session.get.return_value = response
        auth = HTTPBasicAuth("user", "token")

        result = jira.jira_get_json(session, "https://example.test/issue", auth)
        assert result == {"fields": {"status": {"name": "Open"}}}
        session.get.assert_called_once_with(
            "https://example.test/issue", auth=auth, timeout=60.0
        )

    def test_raises_on_non_object_response(self) -> None:
        """Raise ValueError when the response is not a JSON object."""
        session = mock.MagicMock(spec=requests.Session)
        response = mock.MagicMock()
        response.json.return_value = []
        response.raise_for_status.return_value = None
        session.get.return_value = response
        auth = HTTPBasicAuth("user", "token")

        with pytest.raises(ValueError, match="expected JSON object"):
            jira.jira_get_json(session, "https://example.test", auth)

    def test_raises_on_http_error(self) -> None:
        """Propagate HTTP errors from raise_for_status."""
        session = mock.MagicMock(spec=requests.Session)
        response = mock.MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")
        session.get.return_value = response
        auth = HTTPBasicAuth("user", "token")

        with pytest.raises(requests.HTTPError, match="401 Unauthorized"):
            jira.jira_get_json(session, "https://example.test", auth)


class TestJiraPostJson:
    """Test authenticated JSON POST requests."""

    def test_posts_payload(self) -> None:
        """POST the payload with correct headers."""
        session = mock.MagicMock(spec=requests.Session)
        response = mock.MagicMock()
        response.raise_for_status.return_value = None
        session.post.return_value = response
        auth = HTTPBasicAuth("user", "token")

        jira.jira_post_json(session, "https://example.test/comment", auth, {"body": "hi"})
        session.post.assert_called_once_with(
            "https://example.test/comment",
            auth=auth,
            json={"body": "hi"},
            headers={"Content-Type": "application/json"},
            timeout=60.0,
        )

    def test_raises_on_http_error(self) -> None:
        """Propagate HTTP errors from raise_for_status."""
        session = mock.MagicMock(spec=requests.Session)
        response = mock.MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError("bad request")
        session.post.return_value = response
        auth = HTTPBasicAuth("user", "token")

        with pytest.raises(requests.HTTPError, match="bad request"):
            jira.jira_post_json(session, "https://example.test/comment", auth, {"body": "hi"})
