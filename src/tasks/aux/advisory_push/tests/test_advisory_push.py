"""Tests for advisory_push module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.aux.advisory_push.advisory_push import (
    AdvisoryProcessingError,
    advisory_yaml_to_pyxis_payload,
)


def test_advisory_yaml_to_pyxis_payload() -> None:
    """Test YAML to Pyxis payload transformation."""
    advisory_doc = {
        "metadata": {
            "name": "2024-001",
            "ship_date": "2024-01-15T00:00:00Z",
        },
        "spec": {
            "type": "RHSA",
            "description": "Security advisory for container images",
            "severity": "Important",
            "solution": "Update to the latest version",
            "synopsis": "Important security update",
            "topic": "Security fixes",
            "content": {
                "images": [
                    {
                        "containerImage": "registry.example.com/foo@sha256:abc123",
                        "cves": {
                            "fixed": {
                                "CVE-2024-0001": {},
                                "CVE-2024-0002": {},
                            }
                        },
                    },
                    {
                        "containerImage": "registry.example.com/bar@sha256:def456",
                        "cves": {
                            "fixed": {
                                "CVE-2024-0002": {},
                                "CVE-2024-0003": {},
                            }
                        },
                    },
                ]
            },
            "issues": {
                "fixed": [
                    {"id": "JIRA-123", "source": "jira", "public": True},
                    {"id": "JIRA-456", "source": "jira", "public": False},
                    {"id": "JIRA-789", "source": "jira", "public": True},
                ]
            },
        },
    }

    payload = advisory_yaml_to_pyxis_payload(advisory_doc)

    assert payload["_id"] == "RHSA-2024-001"
    assert payload["content_type"] == "CONTAINER"
    assert payload["type"] == "RHSA"
    assert payload["description"] == "Security advisory for container images"
    assert payload["severity"] == "Important"
    assert payload["ship_date"] == "2024-01-15T00:00:00Z"
    assert payload["solution"] == "Update to the latest version"
    assert payload["synopsis"] == "Important security update"
    assert payload["topic"] == "Security fixes"

    # Check unique CVEs
    assert len(payload["cves"]) == 3
    cve_ids = {cve["id"] for cve in payload["cves"]}
    assert cve_ids == {"CVE-2024-0001", "CVE-2024-0002", "CVE-2024-0003"}
    for cve in payload["cves"]:
        assert cve["url"] == f"https://access.redhat.com/security/cve/{cve['id']}"

    # Check public issues only
    assert len(payload["issues"]) == 2
    issue_ids = {issue["id"] for issue in payload["issues"]}
    assert issue_ids == {"JIRA-123", "JIRA-789"}
    for issue in payload["issues"]:
        assert issue["issue_tracker"] == "jira"


def test_advisory_yaml_to_pyxis_payload_minimal() -> None:
    """Test transformation with minimal advisory data."""
    advisory_doc = {
        "metadata": {"name": "2024-002"},
        "spec": {"type": "RHBA"},
    }

    payload = advisory_yaml_to_pyxis_payload(advisory_doc)

    assert payload["_id"] == "RHBA-2024-002"
    assert payload["content_type"] == "CONTAINER"
    assert payload["type"] == "RHBA"
    assert payload["cves"] == []
    assert payload["issues"] == []
    assert payload["severity"] == "None"


def test_advisory_processing_error() -> None:
    """Test AdvisoryProcessingError aggregates failures."""
    failures = [
        "data/advisories/2024-001/advisory.yaml: Network timeout",
        "data/advisories/2024-002/advisory.yaml: Invalid YAML",
    ]
    error = AdvisoryProcessingError(failures)

    assert len(error.failures) == 2
    assert "2 advisory operation(s) failed" in str(error)
    assert "Network timeout" in str(error)
    assert "Invalid YAML" in str(error)


@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.git.clone")
@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.git.diff_files")
@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.git.checkout")
@patch(
    "release_service_utils.tasks.aux.advisory_push.advisory_push.gitlab.configure_git_oauth2_auth"  # noqa: E501
)
@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.subprocess.run")
def test_clone_and_find_changes(
    mock_subprocess: MagicMock,
    mock_auth: MagicMock,
    mock_checkout: MagicMock,
    mock_diff: MagicMock,
    mock_clone: MagicMock,
    tmp_path: Path,
) -> None:
    """Test clone_and_find_changes function."""
    from release_service_utils.tasks.aux.advisory_push.advisory_push import (
        clone_and_find_changes,
    )

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    mock_clone.return_value = repo_dir
    mock_diff.side_effect = [
        ["data/advisories/2024-001/advisory.yaml", "data/advisories/2024-002/advisory.yaml"],
        ["data/advisories/2024-003/advisory.yaml"],
    ]

    result_dir, new, updated = clone_and_find_changes(
        "https://gitlab.example.com/advisories.git",
        "abc123",
        "def456",
        "main",
        "test-token",
    )

    assert result_dir == repo_dir
    assert new == [
        "data/advisories/2024-001/advisory.yaml",
        "data/advisories/2024-002/advisory.yaml",
    ]
    assert updated == ["data/advisories/2024-003/advisory.yaml"]

    mock_auth.assert_called_once_with("test-token")
    mock_clone.assert_called_once()


@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.subprocess.run")
@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.git.show_file")
def test_send_messages_new_advisory(
    mock_show_file: MagicMock,
    mock_subprocess: MagicMock,
    tmp_path: Path,
) -> None:
    """Test sending messages for new advisories."""
    from release_service_utils.tasks.aux.advisory_push.advisory_push import send_messages

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    advisory_yaml = """
metadata:
  name: "2024-001"
  ship_date: "2024-01-15T00:00:00Z"
spec:
  type: RHSA
  description: Test advisory
"""
    mock_show_file.return_value = advisory_yaml

    send_messages(
        repo_dir,
        "abc123",
        ["data/advisories/2024-001/advisory.yaml"],
        [],
        "kafka.topic",
        "/secrets/kafka/user",
        "/secrets/kafka/pass",
        "/secrets/kafka/servers",
    )

    # Verify subprocess was called for Kafka
    assert mock_subprocess.call_count == 1
    kafka_call = mock_subprocess.call_args
    assert "/home/kafka/producer.py" in kafka_call.args[0]
    assert "advisory_state=new" in kafka_call.args[0]


@patch("release_service_utils.helpers.pyxis_api.pyxis_api.http_client.get_retry_session")
@patch(
    "release_service_utils.tasks.aux.advisory_push.advisory_push.retry.retry_with_exponential_backoff"  # noqa: E501
)
@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.git.show_file")
def test_update_pyxis(
    mock_show_file: MagicMock,
    mock_retry: MagicMock,
    mock_session: MagicMock,
    tmp_path: Path,
) -> None:
    """Test Pyxis update operations."""
    from release_service_utils.tasks.aux.advisory_push.advisory_push import update_pyxis

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    advisory_yaml = """
metadata:
  name: "2024-001"
spec:
  type: RHSA
  description: Test advisory
  content:
    images:
      - containerImage: "registry.example.com/foo@sha256:abc123def456"
        repository: "registry.access.redhat.com/foo/bar"
"""
    mock_show_file.return_value = advisory_yaml

    # Mock HTTP session responses
    mock_session_obj = mock_session.return_value
    import json
    import requests
    from unittest import mock

    def get_side_effect(url, **kwargs):
        resp = mock.MagicMock()
        if "advisories/id" in url:
            resp.status_code = 404
            resp.raise_for_status.side_effect = requests.HTTPError("404 Error", response=resp)
        elif "images?page_size" in url:
            resp.status_code = 200
            resp.json.return_value = {"data": [{"_id": "image-123"}]}
            resp.text = json.dumps({"data": [{"_id": "image-123"}]})
        elif "images/id" in url:
            resp.status_code = 200
            image_payload = {
                "_id": "image-123",
                "repositories": [
                    {
                        "registry": "registry.access.redhat.com",
                        "repository": "foo/bar",
                    }
                ],
            }
            resp.json.return_value = image_payload
            resp.text = json.dumps(image_payload)
        else:
            resp.status_code = 200
        return resp

    mock_session_obj.get.side_effect = get_side_effect

    mock_post_response = mock_session_obj.post.return_value
    mock_post_response.status_code = 201

    mock_patch_response = mock_session_obj.patch.return_value
    mock_patch_response.status_code = 200

    # Mock retry to just execute the function immediately
    mock_retry.side_effect = lambda func, **kwargs: func()

    update_pyxis(
        repo_dir,
        "abc123",
        ["data/advisories/2024-001/advisory.yaml"],
        "https://pyxis.example.com/v1/advisories",
        "https://pyxis.example.com/v1/images",
        "/secrets/pyxis/cert",
        "/secrets/pyxis/key",
    )

    # Verify advisory was created (POST call)
    assert mock_session_obj.post.called
    post_call = mock_session_obj.post.call_args
    assert "advisories" in post_call.args[0]

    # Verify image was fetched and updated
    assert mock_session_obj.get.called
    assert mock_session_obj.patch.called


@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.git.show_file")
@patch("release_service_utils.tasks.aux.advisory_push.advisory_push.create_or_update_advisory")
def test_update_pyxis_failure_collection(
    mock_create: MagicMock,
    mock_show_file: MagicMock,
    tmp_path: Path,
) -> None:
    """Test that Pyxis failures are collected and raised together."""
    from release_service_utils.tasks.aux.advisory_push.advisory_push import update_pyxis

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    advisory_yaml = """
metadata:
  name: "2024-001"
spec:
  type: RHSA
  description: Test advisory
"""
    mock_show_file.return_value = advisory_yaml
    mock_create.side_effect = Exception("Pyxis API error")

    with pytest.raises(AdvisoryProcessingError) as exc_info:
        update_pyxis(
            repo_dir,
            "abc123",
            [
                "data/advisories/2024-001/advisory.yaml",
                "data/advisories/2024-002/advisory.yaml",
            ],
            "https://pyxis.example.com/v1/advisories",
            "https://pyxis.example.com/v1/images",
            "/secrets/pyxis/cert",
            "/secrets/pyxis/key",
        )

    assert len(exc_info.value.failures) == 2
    assert "2 advisory operation(s) failed" in str(exc_info.value)
