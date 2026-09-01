"""Tests for `kubectl`."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.helpers.kubectl import (
    ConfigMapNotFoundError,
    auth_can_i,
    get_configmap,
)


def test_get_configmap_runs_kubectl_and_returns_parsed_json() -> None:
    """Kubectl is called with the correct arguments and its output is parsed as JSON."""
    cm_json = json.dumps({"data": {"SIG_KEY_NAME": "some-key"}})
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=cm_json, returncode=0)
        result = get_configmap("signing-config-map")

    mock_run.assert_called_once_with(
        ["kubectl", "get", "cm/signing-config-map", "-ojson"], check=False
    )
    assert result == {"data": {"SIG_KEY_NAME": "some-key"}}


def test_get_configmap_with_namespace() -> None:
    """Kubectl is called with namespace flag when namespace is provided."""
    cm_json = json.dumps({"data": {"key": "value"}})
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=cm_json, returncode=0)
        result = get_configmap("cluster-config", namespace="konflux-info")

    mock_run.assert_called_once_with(
        ["kubectl", "get", "cm/cluster-config", "-ojson", "-n", "konflux-info"], check=False
    )
    assert result == {"data": {"key": "value"}}


def test_get_configmap_raises_not_found_error_when_configmap_missing() -> None:
    """ConfigMapNotFoundError (a RuntimeError subclass) is raised for a NotFound response."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr='Error from server (NotFound): configmaps "signing-config-map" not found',
        )
        with pytest.raises(ConfigMapNotFoundError, match="signing-config-map"):
            get_configmap("signing-config-map")


def test_get_configmap_raises_plain_runtime_error_on_unrelated_not_found() -> None:
    """A NotFound error for a different resource does not raise ConfigMapNotFoundError.

    Regression test: a bare "NotFound" substring match would misclassify e.g. a
    missing namespace as the ConfigMap itself being absent, silently masking a
    configuration/infra problem as an intentional absence.
    """
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr='Error from server (NotFound): namespaces "konflux-info" not found',
        )
        with pytest.raises(RuntimeError, match="signing-config-map") as exc_info:
            get_configmap("signing-config-map", namespace="konflux-info")
        assert not isinstance(exc_info.value, ConfigMapNotFoundError)


def test_get_configmap_raises_plain_runtime_error_on_other_failures() -> None:
    """A non-NotFound kubectl failure raises RuntimeError, not ConfigMapNotFoundError.

    This lets callers distinguish "genuinely absent" from infra/permission failures
    (e.g. RBAC denied, API server unreachable) instead of silently treating both the
    same way.
    """
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr='Error from server (Forbidden): configmaps "signing-config-map" is '
            "forbidden: User cannot get resource",
        )
        with pytest.raises(RuntimeError, match="signing-config-map") as exc_info:
            get_configmap("signing-config-map")
        assert not isinstance(exc_info.value, ConfigMapNotFoundError)


def test_auth_can_i_returns_true_when_allowed() -> None:
    """Return True when kubectl reports 'yes'."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="yes\n", returncode=0, stderr="")
        assert auth_can_i("get", "release", name="my-rel", namespace="ns") is True

    mock_run.assert_called_once_with(
        ["kubectl", "auth", "can-i", "get", "release/my-rel", "-n", "ns"],
        check=False,
    )


def test_auth_can_i_returns_false_when_denied() -> None:
    """Return False when kubectl reports 'no'."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="no\n", returncode=0, stderr="")
        assert auth_can_i("get", "snapshot", name="snap", namespace="ns") is False


def test_auth_can_i_without_name() -> None:
    """Omit the resource name from the kubectl command when not provided."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="yes\n", returncode=0, stderr="")
        auth_can_i("create", "internalrequest", namespace="target-ns")

    mock_run.assert_called_once_with(
        ["kubectl", "auth", "can-i", "create", "internalrequest", "-n", "target-ns"],
        check=False,
    )


def test_auth_can_i_without_namespace() -> None:
    """Omit the namespace flag when not provided."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="yes\n", returncode=0, stderr="")
        auth_can_i("get", "release", name="my-rel")

    mock_run.assert_called_once_with(
        ["kubectl", "auth", "can-i", "get", "release/my-rel"],
        check=False,
    )


def test_auth_can_i_raises_on_nonzero_returncode() -> None:
    """Raise RuntimeError when kubectl exits with a non-zero return code."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="error: unknown resource"
        )
        with pytest.raises(
            RuntimeError, match=r"Failed to run 'kubectl auth can-i get release/my-rel -n ns'"
        ):
            auth_can_i("get", "release", name="my-rel", namespace="ns")


def test_auth_can_i_raises_includes_stderr_in_message() -> None:
    """Include stderr content in the RuntimeError message."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(returncode=2, stdout="", stderr="connection refused")
        with pytest.raises(RuntimeError, match="connection refused"):
            auth_can_i("create", "internalrequest", namespace="ns")


def test_auth_can_i_raises_falls_back_to_stdout_when_no_stderr() -> None:
    """Use stdout in the error message when stderr is empty."""
    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="some output", stderr="")
        with pytest.raises(RuntimeError, match="some output"):
            auth_can_i("get", "release", name="r", namespace="ns")
