"""Test the verify_access_to_resources task."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import verify_access_to_resources


def test_parse_namespaced_resource_valid() -> None:
    """Split a valid 'namespace/name' string."""
    assert verify_access_to_resources.parse_namespaced_resource("ns/name") == ("ns", "name")


def test_parse_namespaced_resource_with_slashes_in_name() -> None:
    """Keep everything after the first slash as the name."""
    assert verify_access_to_resources.parse_namespaced_resource("ns/a/b") == ("ns", "a/b")


def test_parse_namespaced_resource_no_slash() -> None:
    """Reject values without a slash."""
    with pytest.raises(ValueError, match="Expected 'namespace/name'"):
        verify_access_to_resources.parse_namespaced_resource("no-slash")


def test_parse_namespaced_resource_empty_namespace() -> None:
    """Reject values with an empty namespace."""
    with pytest.raises(ValueError, match="Expected 'namespace/name'"):
        verify_access_to_resources.parse_namespaced_resource("/name")


def test_parse_namespaced_resource_empty_name() -> None:
    """Reject values with an empty name."""
    with pytest.raises(ValueError, match="Expected 'namespace/name'"):
        verify_access_to_resources.parse_namespaced_resource("ns/")


@patch("verify_access_to_resources.kubectl.auth_can_i", return_value=True)
def test_run_all_access_granted(mock_auth: MagicMock) -> None:
    """Succeed when all access checks pass without internal services."""
    verify_access_to_resources.run(
        release="origin-ns/my-release",
        release_plan="origin-ns/my-plan",
        release_plan_admission="target-ns/my-rpa",
        release_service_config="rsc-ns/my-rsc",
        snapshot="origin-ns/my-snapshot",
        require_internal_services=False,
    )
    assert mock_auth.call_count == 5


@patch("verify_access_to_resources.kubectl.auth_can_i", return_value=True)
def test_run_with_internal_services(mock_auth: MagicMock) -> None:
    """Check internalrequest creation when requireInternalServices is true."""
    verify_access_to_resources.run(
        release="origin-ns/my-release",
        release_plan="origin-ns/my-plan",
        release_plan_admission="target-ns/my-rpa",
        release_service_config="rsc-ns/my-rsc",
        snapshot="origin-ns/my-snapshot",
        require_internal_services=True,
    )
    assert mock_auth.call_count == 6
    mock_auth.assert_any_call("create", "internalrequest", namespace="target-ns")


@patch("verify_access_to_resources.kubectl.auth_can_i", return_value=True)
def test_run_correct_namespaces(mock_auth: MagicMock) -> None:
    """Verify each resource is checked against the correct namespace."""
    verify_access_to_resources.run(
        release="origin-ns/my-release",
        release_plan="origin-ns/my-plan",
        release_plan_admission="target-ns/my-rpa",
        release_service_config="rsc-ns/my-rsc",
        snapshot="origin-ns/my-snapshot",
        require_internal_services=True,
    )
    mock_auth.assert_any_call("get", "release", name="my-release", namespace="origin-ns")
    mock_auth.assert_any_call("get", "releaseplan", name="my-plan", namespace="origin-ns")
    mock_auth.assert_any_call(
        "get",
        "releaseplanadmission",
        name="my-rpa",
        namespace="target-ns",
    )
    mock_auth.assert_any_call(
        "get",
        "releaseserviceconfig",
        name="my-rsc",
        namespace="rsc-ns",
    )
    mock_auth.assert_any_call("get", "snapshot", name="my-snapshot", namespace="origin-ns")
    mock_auth.assert_any_call("create", "internalrequest", namespace="target-ns")


@patch("verify_access_to_resources.kubectl.auth_can_i")
def test_run_read_denied_raises(mock_auth: MagicMock) -> None:
    """Fail when a resource read check returns False."""
    mock_auth.side_effect = [True, True, False, True, True]
    with pytest.raises(RuntimeError, match="Cannot read or create required"):
        verify_access_to_resources.run(
            release="origin-ns/my-release",
            release_plan="origin-ns/my-plan",
            release_plan_admission="target-ns/my-rpa",
            release_service_config="rsc-ns/my-rsc",
            snapshot="origin-ns/my-snapshot",
            require_internal_services=False,
        )


@patch("verify_access_to_resources.kubectl.auth_can_i")
def test_run_internal_request_denied_raises(mock_auth: MagicMock) -> None:
    """Fail when internalrequest creation is denied."""
    mock_auth.side_effect = [True, True, True, True, True, False]
    with pytest.raises(RuntimeError, match="Cannot read or create required"):
        verify_access_to_resources.run(
            release="origin-ns/my-release",
            release_plan="origin-ns/my-plan",
            release_plan_admission="target-ns/my-rpa",
            release_service_config="rsc-ns/my-rsc",
            snapshot="origin-ns/my-snapshot",
            require_internal_services=True,
        )


@patch("verify_access_to_resources.kubectl.auth_can_i", return_value=True)
def test_main_success(mock_auth: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit zero when all resources are accessible."""
    monkeypatch.setenv("PARAM_RELEASE", "origin-ns/my-release")
    monkeypatch.setenv("PARAM_RELEASE_PLAN", "origin-ns/my-plan")
    monkeypatch.setenv("PARAM_RELEASE_PLAN_ADMISSION", "target-ns/my-rpa")
    monkeypatch.setenv("PARAM_RELEASE_SERVICE_CONFIG", "rsc-ns/my-rsc")
    monkeypatch.setenv("PARAM_SNAPSHOT", "origin-ns/my-snapshot")
    monkeypatch.setenv("PARAM_REQUIRE_INTERNAL_SERVICES", "false")
    assert verify_access_to_resources.main() == 0


@patch("verify_access_to_resources.kubectl.auth_can_i", return_value=True)
def test_main_require_internal_services_true(
    mock_auth: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parse requireInternalServices as true and check all six resources."""
    monkeypatch.setenv("PARAM_RELEASE", "origin-ns/my-release")
    monkeypatch.setenv("PARAM_RELEASE_PLAN", "origin-ns/my-plan")
    monkeypatch.setenv("PARAM_RELEASE_PLAN_ADMISSION", "target-ns/my-rpa")
    monkeypatch.setenv("PARAM_RELEASE_SERVICE_CONFIG", "rsc-ns/my-rsc")
    monkeypatch.setenv("PARAM_SNAPSHOT", "origin-ns/my-snapshot")
    monkeypatch.setenv("PARAM_REQUIRE_INTERNAL_SERVICES", "true")
    assert verify_access_to_resources.main() == 0
    assert mock_auth.call_count == 6


@patch("verify_access_to_resources.kubectl.auth_can_i", return_value=True)
def test_main_require_internal_services_defaults_to_false(
    mock_auth: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Default to false when PARAM_REQUIRE_INTERNAL_SERVICES is unset."""
    monkeypatch.setenv("PARAM_RELEASE", "origin-ns/my-release")
    monkeypatch.setenv("PARAM_RELEASE_PLAN", "origin-ns/my-plan")
    monkeypatch.setenv("PARAM_RELEASE_PLAN_ADMISSION", "target-ns/my-rpa")
    monkeypatch.setenv("PARAM_RELEASE_SERVICE_CONFIG", "rsc-ns/my-rsc")
    monkeypatch.setenv("PARAM_SNAPSHOT", "origin-ns/my-snapshot")
    monkeypatch.delenv("PARAM_REQUIRE_INTERNAL_SERVICES", raising=False)
    assert verify_access_to_resources.main() == 0
    assert mock_auth.call_count == 5


def test_main_missing_required_env_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit with code 1 when a required env var is missing."""
    monkeypatch.delenv("PARAM_RELEASE", raising=False)
    with pytest.raises(SystemExit, match="1"):
        verify_access_to_resources.main()
