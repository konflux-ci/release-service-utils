"""Tests for embargo_check task logic."""

from __future__ import annotations

import json
import runpy
import subprocess
from pathlib import Path
from typing import Any
from unittest import mock

import embargo_check
import pytest
import requests
from internal_request import InternalRequestWaitError


def _write_data(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _write_jira_secret(path: Path) -> None:
    """Write dummy Jira credentials under *path*."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "email").write_text("team@domain.com\n", encoding="utf-8")
    (path / "token").write_text("abcdefg\n", encoding="utf-8")


@pytest.fixture()
def jira_secret_path(tmp_path: Path) -> Path:
    """Write dummy Jira credentials and return the secret directory."""
    secret_dir = tmp_path / "secrets"
    _write_jira_secret(secret_dir)
    return secret_dir


def _mock_jira_response(json_data: dict[str, Any]) -> mock.MagicMock:
    """Build a mock requests.Response with status 200 and *json_data*."""
    response = mock.MagicMock()
    response.status_code = 200
    response.raise_for_status.return_value = None
    response.json.return_value = json_data
    return response


def _data_with_issues(
    issues: list[dict[str, Any]],
    content_type: str = "images",
    content_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a minimal data dict with fixed issues and optional content."""
    data: dict[str, Any] = {
        "releaseNotes": {
            "issues": {"fixed": issues},
            "content": {},
        },
    }
    if content_items is not None:
        data["releaseNotes"]["content"][content_type] = content_items
    return data


def _data_with_cves(
    cve_ids: list[str],
    content_type: str = "images",
) -> dict[str, Any]:
    """Build a minimal data dict with CVEs in content."""
    items = [{"cves": {"fixed": {cve: {} for cve in cve_ids}}}]
    return {
        "releaseNotes": {
            "issues": {},
            "content": {content_type: items},
        },
    }


class TestGetContentItems:
    """Test _get_content_items."""

    def test_returns_images(self) -> None:
        """Return images list when images content exists."""
        data = {"releaseNotes": {"content": {"images": [{"name": "img1"}]}}}
        assert embargo_check._get_content_items(data) == [{"name": "img1"}]

    def test_returns_artifacts(self) -> None:
        """Return artifacts list when only artifacts content exists."""
        data = {"releaseNotes": {"content": {"artifacts": [{"name": "art1"}]}}}
        assert embargo_check._get_content_items(data) == [{"name": "art1"}]

    def test_returns_empty_list_for_empty_content(self) -> None:
        """Return empty list when the key exists but is empty."""
        data = {"releaseNotes": {"content": {"images": []}}}
        assert embargo_check._get_content_items(data) == []

    def test_returns_none(self) -> None:
        """Return None when no content exists."""
        assert embargo_check._get_content_items({}) is None
        assert embargo_check._get_content_items({"releaseNotes": {}}) is None
        assert embargo_check._get_content_items({"releaseNotes": {"content": {}}}) is None


class TestExtractCves:
    """Test _extract_cves."""

    def test_extracts_cves_from_images(self) -> None:
        """Extract unique CVE IDs from images content."""
        data = _data_with_cves(["CVE-2024-001", "CVE-2024-002"])
        assert embargo_check._extract_cves(data) == ["CVE-2024-001", "CVE-2024-002"]

    def test_extracts_cves_from_artifacts(self) -> None:
        """Extract unique CVE IDs from artifacts content."""
        data = _data_with_cves(["CVE-2024-001"], content_type="artifacts")
        assert embargo_check._extract_cves(data) == ["CVE-2024-001"]

    def test_returns_empty_no_content(self) -> None:
        """Return empty list when no content exists."""
        assert embargo_check._extract_cves({}) == []

    def test_deduplicates_cves(self) -> None:
        """Return unique sorted CVEs."""
        data = {
            "releaseNotes": {
                "content": {
                    "images": [
                        {"cves": {"fixed": {"CVE-2024-001": {}}}},
                        {"cves": {"fixed": {"CVE-2024-001": {}, "CVE-2024-002": {}}}},
                    ],
                },
            },
        }
        assert embargo_check._extract_cves(data) == ["CVE-2024-001", "CVE-2024-002"]


class TestCheckIssueVisibility:
    """Test _check_issue_visibility."""

    def test_returns_true_on_success(self) -> None:
        """Return True when GET succeeds with 2xx."""
        session = mock.MagicMock(spec=requests.Session)
        success = mock.MagicMock()
        success.status_code = 200
        session.get.return_value = success
        assert embargo_check._check_issue_visibility(session, "https://example.test") is True

    def test_returns_false_on_non_2xx(self) -> None:
        """Return False when GET returns non-2xx."""
        session = mock.MagicMock(spec=requests.Session)
        unauthorized = mock.MagicMock()
        unauthorized.status_code = 401
        unauthorized.raise_for_status.side_effect = requests.HTTPError("401")
        session.get.return_value = unauthorized
        assert embargo_check._check_issue_visibility(session, "https://example.test") is False

    def test_returns_false_on_exception(self) -> None:
        """Return False when GET raises an exception."""
        session = mock.MagicMock(spec=requests.Session)
        session.get.side_effect = requests.ConnectionError("timeout")
        assert embargo_check._check_issue_visibility(session, "https://example.test") is False


class TestGetWithJira404Retry:
    """Test _get_with_jira_404_retry."""

    @mock.patch("embargo_check.time.sleep")
    def test_retries_on_transient_404(self, mock_sleep: mock.MagicMock) -> None:
        """Retry on 404 and succeed on next attempt."""
        session = mock.MagicMock(spec=requests.Session)
        auth = mock.MagicMock()
        not_found = mock.MagicMock()
        not_found.status_code = 404
        success = mock.MagicMock()
        success.status_code = 200
        expected = {"fields": {"security": None}}
        success.json.return_value = expected
        session.get.side_effect = [not_found, success]

        result = embargo_check._get_with_jira_404_retry(
            session, "https://jira.test/issue/X-1", auth
        )
        assert result == expected
        assert session.get.call_count == 2
        mock_sleep.assert_called_once()

    @mock.patch("embargo_check.time.sleep")
    def test_raises_after_exhausting_404_retries(self, mock_sleep: mock.MagicMock) -> None:
        """Raise HTTPError when all retry attempts return 404."""
        session = mock.MagicMock(spec=requests.Session)
        auth = mock.MagicMock()
        not_found = mock.MagicMock()
        not_found.status_code = 404
        not_found.raise_for_status.side_effect = requests.HTTPError("404")
        session.get.return_value = not_found

        with pytest.raises(requests.HTTPError):
            embargo_check._get_with_jira_404_retry(
                session, "https://jira.test/issue/X-1", auth
            )
        assert session.get.call_count == embargo_check.MAX_JIRA_404_RETRIES

    def test_no_retry_on_non_retryable_error(self) -> None:
        """Do not retry when status is not 404."""
        session = mock.MagicMock(spec=requests.Session)
        auth = mock.MagicMock()
        forbidden = mock.MagicMock()
        forbidden.status_code = 403
        forbidden.raise_for_status.side_effect = requests.HTTPError("403")
        session.get.return_value = forbidden

        with pytest.raises(requests.HTTPError):
            embargo_check._get_with_jira_404_retry(
                session, "https://jira.test/issue/X-1", auth
            )
        assert session.get.call_count == 1

    def test_returns_immediately_on_success(self) -> None:
        """Return parsed JSON on 200 without retrying."""
        session = mock.MagicMock(spec=requests.Session)
        auth = mock.MagicMock()
        success = mock.MagicMock()
        success.status_code = 200
        expected = {"fields": {"security": None}}
        success.json.return_value = expected
        session.get.return_value = success

        result = embargo_check._get_with_jira_404_retry(
            session, "https://jira.test/issue/X-1", auth
        )
        assert result == expected
        assert session.get.call_count == 1

    def test_raises_on_non_dict_json(self) -> None:
        """Raise ValueError when Jira returns non-dict JSON."""
        session = mock.MagicMock(spec=requests.Session)
        auth = mock.MagicMock()
        resp = mock.MagicMock()
        resp.status_code = 200
        resp.json.return_value = ["not", "a", "dict"]
        session.get.return_value = resp

        with pytest.raises(ValueError, match="expected JSON object"):
            embargo_check._get_with_jira_404_retry(
                session, "https://jira.test/issue/X-1", auth
            )


class TestFormatTimeout:
    """Test _format_timeout."""

    def test_formats_seconds_only(self) -> None:
        """Format 45 seconds as 00h00m45s."""
        assert embargo_check._format_timeout(45) == "00h00m45s"

    def test_formats_minutes_and_seconds(self) -> None:
        """Format 3000 seconds as 00h50m00s."""
        assert embargo_check._format_timeout(3000) == "00h50m00s"

    def test_formats_hours(self) -> None:
        """Format 3661 seconds as 01h01m01s."""
        assert embargo_check._format_timeout(3661) == "01h01m01s"


class TestCheckIssues:
    """Test check_issues function."""

    def test_no_issues(self, jira_secret_path: Path) -> None:
        """Return empty errors when no issues exist."""
        data: dict[str, Any] = {"releaseNotes": {"issues": {}}}
        session = mock.MagicMock(spec=requests.Session)
        result_data, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []
        assert result_data == data

    def test_jira_issue_visible_public(self, jira_secret_path: Path) -> None:
        """Mark issue as public when security is null and unauthenticated access works."""
        data = _data_with_issues([{"id": "ISSUE-123", "source": "redhat.atlassian.net"}])
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {"fields": {"security": None, "issuetype": {"name": "Bug"}}}
        )

        result_data, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []
        assert result_data["releaseNotes"]["issues"]["fixed"][0]["public"] is True

    def test_jira_issue_not_public(self, jira_secret_path: Path) -> None:
        """Mark issue as not public when security field has a value."""
        data = _data_with_issues([{"id": "ISSUE-123", "source": "redhat.atlassian.net"}])
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {"fields": {"security": {"name": "Red Hat"}, "issuetype": {"name": "Bug"}}}
        )

        result_data, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []
        assert result_data["releaseNotes"]["issues"]["fixed"][0]["public"] is False

    def test_jira_issue_not_visible(self, jira_secret_path: Path) -> None:
        """Return error when issue is not visible."""
        data = _data_with_issues([{"id": "ISSUE-123", "source": "redhat.atlassian.net"}])
        session = mock.MagicMock(spec=requests.Session)
        session.get.side_effect = requests.HTTPError("401 Unauthorized")

        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert len(errors) == 1
        assert "not visible" in errors[0]

    def test_bugzilla_issue_not_visible(self, jira_secret_path: Path) -> None:
        """Return error when Bugzilla issue is not visible."""
        data = _data_with_issues([{"id": "12345", "source": "bugzilla.redhat.com"}])
        session = mock.MagicMock(spec=requests.Session)
        session.get.side_effect = requests.HTTPError("404")

        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert len(errors) == 1
        assert "not visible" in errors[0]

    def test_bugzilla_issue_visible(self, jira_secret_path: Path) -> None:
        """Mark Bugzilla issue as public=False (security field check)."""
        data = _data_with_issues([{"id": "12345", "source": "bugzilla.redhat.com"}])
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {"fields": {"security": {"name": "embargo"}}}
        )

        result_data, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []
        assert result_data["releaseNotes"]["issues"]["fixed"][0]["public"] is False

    def test_bugzilla_issue_null_security_is_public(self, jira_secret_path: Path) -> None:
        """Mark Bugzilla issue as public when security is null."""
        data = _data_with_issues([{"id": "12345", "source": "bugzilla.redhat.com"}])
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response({"fields": {"security": None}})

        result_data, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []
        assert result_data["releaseNotes"]["issues"]["fixed"][0]["public"] is True
        assert session.get.call_count == 1

    def test_bugzilla_non_dict_json_returns_error(self, jira_secret_path: Path) -> None:
        """Return error when a non-Jira server responds with non-dict JSON."""
        data = _data_with_issues([{"id": "12345", "source": "bugzilla.redhat.com"}])
        session = mock.MagicMock(spec=requests.Session)
        response = mock.MagicMock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = []
        session.get.return_value = response

        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert len(errors) == 1
        assert "returned unexpected JSON" in errors[0]

    def test_issues_redhat_com_normalized(self, jira_secret_path: Path) -> None:
        """Normalize issues.redhat.com to redhat.atlassian.net."""
        data = _data_with_issues([{"id": "ISSUE-123", "source": "issues.redhat.com"}])
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {"fields": {"security": {"name": "Red Hat"}, "issuetype": {"name": "Bug"}}}
        )

        result_data, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []
        session.get.assert_called_once()
        call_url = session.get.call_args[0][0]
        assert "redhat.atlassian.net" in call_url

    def test_vulnerability_cve_present(self, jira_secret_path: Path) -> None:
        """No error when Vulnerability issue CVE is in content."""
        data = _data_with_issues(
            [{"id": "VULN-123", "source": "redhat.atlassian.net"}],
            content_type="images",
            content_items=[{"cves": {"fixed": {"CVE-2024-001": {}}}}],
        )
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {
                "fields": {
                    "security": {"name": "public"},
                    "issuetype": {"name": "Vulnerability"},
                    "customfield_10667": "CVE-2024-001",
                },
            }
        )

        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []

    def test_vulnerability_cve_missing(self, jira_secret_path: Path) -> None:
        """Error when Vulnerability issue CVE is not in content."""
        data = _data_with_issues(
            [{"id": "VULN-123", "source": "redhat.atlassian.net"}],
            content_type="images",
            content_items=[{"cves": {"fixed": {"CVE-OTHER": {}}}}],
        )
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {
                "fields": {
                    "security": {"name": "public"},
                    "issuetype": {"name": "Vulnerability"},
                    "customfield_10667": "CVE-2024-001",
                },
            }
        )

        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert len(errors) == 1
        assert "CVE-2024-001" in errors[0]

    def test_vulnerability_no_content(self, jira_secret_path: Path) -> None:
        """No error when Vulnerability but no content section."""
        data = _data_with_issues(
            [{"id": "VULN-123", "source": "redhat.atlassian.net"}],
        )
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {
                "fields": {
                    "security": {"name": "public"},
                    "issuetype": {"name": "Vulnerability"},
                    "customfield_10667": "CVE-2024-001",
                },
            }
        )

        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []

    def test_unknown_server_errors(self, jira_secret_path: Path) -> None:
        """Fail closed for issues from unknown servers."""
        data = _data_with_issues([{"id": "ISSUE-123", "source": "unknown.example.com"}])
        session = mock.MagicMock(spec=requests.Session)
        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert len(errors) == 1
        assert "unsupported issue tracker" in errors[0]
        session.get.assert_not_called()

    def test_non_vulnerability_skips_cve_check(self, jira_secret_path: Path) -> None:
        """Non-vulnerability issues skip CVE validation."""
        data = _data_with_issues(
            [{"id": "ISSUE-123", "source": "redhat.atlassian.net"}],
            content_type="images",
            content_items=[{"cves": {"fixed": {"CVE-2024-001": {}}}}],
        )
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_jira_response(
            {
                "fields": {
                    "security": {"name": "public"},
                    "issuetype": {"name": "Bug"},
                },
            }
        )

        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []

    def test_empty_fixed_list(self, jira_secret_path: Path) -> None:
        """Return empty errors when fixed list is empty."""
        data = _data_with_issues([])
        session = mock.MagicMock(spec=requests.Session)
        _, errors = embargo_check.check_issues(
            data, secret_path=jira_secret_path, session=session
        )
        assert errors == []


def _kubectl_result(stdout: str) -> subprocess.CompletedProcess[str]:
    """Build a CompletedProcess mimicking run_cmd output."""
    return subprocess.CompletedProcess(args=["kubectl"], returncode=0, stdout=stdout)


class TestCheckCves:
    """Test check_cves function."""

    def test_no_cves(self) -> None:
        """Return empty errors when no CVEs exist."""
        data: dict[str, Any] = {"releaseNotes": {"content": {}}}
        errors = embargo_check.check_cves(
            data,
            pipeline_run_uid="uid-123",
            request_timeout=2700,
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )
        assert errors == []

    @mock.patch("embargo_check.create")
    def test_internal_request_fails(self, mock_create: mock.MagicMock) -> None:
        """Return error when internal-request creation or wait fails."""
        data = _data_with_cves(["CVE-2024-001"])
        mock_create.side_effect = InternalRequestWaitError(
            "At least one InternalRequest failed", 21
        )
        errors = embargo_check.check_cves(
            data,
            pipeline_run_uid="uid-123",
            request_timeout=2700,
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )
        assert len(errors) == 1
        assert "internal-request failed" in errors[0]

    @mock.patch("embargo_check.run_cmd")
    @mock.patch("embargo_check.create", return_value="ir-abc")
    def test_kubectl_invalid_json(
        self,
        mock_create: mock.MagicMock,
        mock_run: mock.MagicMock,
    ) -> None:
        """Return error when kubectl output is not valid JSON."""
        data = _data_with_cves(["CVE-2024-001"])
        mock_run.return_value = _kubectl_result("not json")
        errors = embargo_check.check_cves(
            data,
            pipeline_run_uid="uid-123",
            request_timeout=2700,
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )
        assert len(errors) == 1
        assert "Could not parse InternalRequest results" in errors[0]

    @mock.patch("embargo_check.run_cmd")
    @mock.patch("embargo_check.create", return_value="ir-abc")
    def test_success_no_embargoed_cves(
        self,
        mock_create: mock.MagicMock,
        mock_run: mock.MagicMock,
    ) -> None:
        """Return empty errors when result is Success."""
        data = _data_with_cves(["CVE-2024-001"])
        mock_run.return_value = _kubectl_result(json.dumps({"result": "Success"}))
        errors = embargo_check.check_cves(
            data,
            pipeline_run_uid="uid-123",
            request_timeout=2700,
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )
        assert errors == []

    @mock.patch("embargo_check.run_cmd")
    @mock.patch("embargo_check.create", return_value="ir-abc")
    def test_embargoed_cves_found(
        self,
        mock_create: mock.MagicMock,
        mock_run: mock.MagicMock,
    ) -> None:
        """Return error listing embargoed CVEs."""
        data = _data_with_cves(["CVE-2024-001", "CVE-2024-002"])
        mock_run.return_value = _kubectl_result(
            json.dumps({"result": "Failure", "embargoed_cves": "CVE-2024-001"})
        )
        errors = embargo_check.check_cves(
            data,
            pipeline_run_uid="uid-123",
            request_timeout=2700,
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )
        assert len(errors) == 1
        assert "CVE-2024-001" in errors[0]

    @mock.patch("embargo_check.run_cmd")
    @mock.patch("embargo_check.create", return_value="ir-abc")
    def test_create_called_with_correct_args(
        self,
        mock_create: mock.MagicMock,
        mock_run: mock.MagicMock,
    ) -> None:
        """Verify correct arguments are passed to internal_request.create."""
        data = _data_with_cves(["CVE-2024-001"])
        mock_run.return_value = _kubectl_result(json.dumps({"result": "Success"}))
        embargo_check.check_cves(
            data,
            pipeline_run_uid="uid-123",
            request_timeout=2700,
            task_git_url="https://github.com/test/repo",
            task_git_revision="v1",
        )
        mock_create.assert_called_once_with(
            "check-embargoed-cves",
            params={
                "cves": "CVE-2024-001",
                "taskGitUrl": "https://github.com/test/repo",
                "taskGitRevision": "v1",
            },
            labels={
                "internal-services.appstudio.openshift.io/pipelinerun-uid": "uid-123",
            },
            sync=True,
            timeout=3000,
            pipeline_timeout="00h50m00s",
            task_timeout="00h45m00s",
        )


class TestRun:
    """Test the run() orchestrator."""

    def test_missing_data_file(self, tmp_path: Path) -> None:
        """Raise RuntimeError when data file is missing."""
        _write_jira_secret(tmp_path / "secrets")
        with pytest.raises(RuntimeError, match="No data JSON"):
            embargo_check.run(
                tmp_path / "missing.json",
                secret_path=tmp_path / "secrets",
                pipeline_run_uid="uid",
                request_timeout=2700,
                task_git_url="https://example.test",
                task_git_revision="main",
            )

    @mock.patch("embargo_check.check_cves", return_value=[])
    @mock.patch("embargo_check.check_issues")
    @mock.patch("embargo_check.http_client.get_retry_session")
    def test_success(
        self,
        mock_session: mock.MagicMock,
        mock_check_issues: mock.MagicMock,
        mock_check_cves: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Complete successfully when no errors occur."""
        data = {"releaseNotes": {"issues": {}}}
        _write_data(tmp_path / "data.json", data)
        _write_jira_secret(tmp_path / "secrets")
        mock_check_issues.return_value = (data, [])

        embargo_check.run(
            tmp_path / "data.json",
            secret_path=tmp_path / "secrets",
            pipeline_run_uid="uid",
            request_timeout=2700,
            task_git_url="https://example.test",
            task_git_revision="main",
        )
        mock_check_issues.assert_called_once()
        mock_check_cves.assert_called_once()

    @mock.patch("embargo_check.check_cves", return_value=[])
    @mock.patch("embargo_check.check_issues")
    @mock.patch("embargo_check.http_client.get_retry_session")
    def test_writes_modified_data(
        self,
        mock_session: mock.MagicMock,
        mock_check_issues: mock.MagicMock,
        mock_check_cves: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Write modified data back to the file."""
        original = {"releaseNotes": {"issues": {}}}
        modified = {"releaseNotes": {"issues": {}, "modified": True}}
        _write_data(tmp_path / "data.json", original)
        _write_jira_secret(tmp_path / "secrets")
        mock_check_issues.return_value = (modified, [])

        embargo_check.run(
            tmp_path / "data.json",
            secret_path=tmp_path / "secrets",
            pipeline_run_uid="uid",
            request_timeout=2700,
            task_git_url="https://example.test",
            task_git_revision="main",
        )
        written = json.loads((tmp_path / "data.json").read_text())
        assert written.get("releaseNotes", {}).get("modified") is True

    @mock.patch("embargo_check.check_cves")
    @mock.patch("embargo_check.check_issues")
    @mock.patch("embargo_check.http_client.get_retry_session")
    def test_issue_errors_skip_cve_check(
        self,
        mock_session: mock.MagicMock,
        mock_check_issues: mock.MagicMock,
        mock_check_cves: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Raise on issue errors without running CVE check."""
        data = {"releaseNotes": {"issues": {}}}
        _write_data(tmp_path / "data.json", data)
        _write_jira_secret(tmp_path / "secrets")
        mock_check_issues.return_value = (data, ["issue error"])

        with pytest.raises(RuntimeError, match="issue error"):
            embargo_check.run(
                tmp_path / "data.json",
                secret_path=tmp_path / "secrets",
                pipeline_run_uid="uid",
                request_timeout=2700,
                task_git_url="https://example.test",
                task_git_revision="main",
            )
        mock_check_cves.assert_not_called()

    @mock.patch("embargo_check.check_cves", return_value=["CVE error"])
    @mock.patch("embargo_check.check_issues")
    @mock.patch("embargo_check.http_client.get_retry_session")
    def test_raises_on_cve_errors(
        self,
        mock_session: mock.MagicMock,
        mock_check_issues: mock.MagicMock,
        mock_check_cves: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Raise RuntimeError when CVE check finds errors."""
        data = {"releaseNotes": {"issues": {}}}
        _write_data(tmp_path / "data.json", data)
        _write_jira_secret(tmp_path / "secrets")
        mock_check_issues.return_value = (data, [])

        with pytest.raises(RuntimeError, match="CVE error"):
            embargo_check.run(
                tmp_path / "data.json",
                secret_path=tmp_path / "secrets",
                pipeline_run_uid="uid",
                request_timeout=2700,
                task_git_url="https://example.test",
                task_git_revision="main",
            )


class TestMain:
    """Test the main() entry point."""

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Exit zero on success."""
        data = {"releaseNotes": {"issues": {}}}
        _write_data(tmp_path / "data.json", data)
        _write_jira_secret(tmp_path / "secrets")
        monkeypatch.setenv("DATA_FILE", str(tmp_path / "data.json"))
        monkeypatch.setenv("JIRA_SECRET_PATH", str(tmp_path / "secrets"))
        monkeypatch.setenv("PIPELINE_RUN_UID", "uid-123")
        monkeypatch.setenv("REQUEST_TIMEOUT", "2700")
        monkeypatch.setenv("TASK_GIT_URL", "https://github.com/test/repo")
        monkeypatch.setenv("TASK_GIT_REVISION", "main")

        with mock.patch("embargo_check.run") as mock_run:
            assert embargo_check.main() == 0
        mock_run.assert_called_once()

    def test_missing_required_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Exit non-zero when required env vars are missing."""
        monkeypatch.delenv("DATA_FILE", raising=False)
        with pytest.raises(SystemExit):
            embargo_check.main()

    def test_module_main_guard(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Executing as __main__ calls main()."""
        data = {"releaseNotes": {"issues": {}}}
        _write_data(tmp_path / "data.json", data)
        _write_jira_secret(tmp_path / "secrets")
        monkeypatch.setenv("DATA_FILE", str(tmp_path / "data.json"))
        monkeypatch.setenv("JIRA_SECRET_PATH", str(tmp_path / "secrets"))
        monkeypatch.setenv("PIPELINE_RUN_UID", "uid-123")
        monkeypatch.setenv("REQUEST_TIMEOUT", "2700")
        monkeypatch.setenv("TASK_GIT_URL", "https://github.com/test/repo")
        monkeypatch.setenv("TASK_GIT_REVISION", "main")

        with mock.patch("embargo_check.run"):
            with pytest.raises(SystemExit) as exc_info:
                runpy.run_module("embargo_check", run_name="__main__")
            assert exc_info.value.code == 0
