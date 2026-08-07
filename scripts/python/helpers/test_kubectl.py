"""Tests for `kubectl`."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from kubectl import auth_can_i, get_configmap


def test_get_configmap_runs_kubectl_and_returns_parsed_json() -> None:
    """Kubectl is called with the correct arguments and its output is parsed as JSON."""
    cm_json = json.dumps({"data": {"SIG_KEY_NAME": "some-key"}})
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=cm_json, returncode=0)
        result = get_configmap("signing-config-map")

    mock_run.assert_called_once_with(
        ["kubectl", "get", "cm/signing-config-map", "-ojson"], check=False
    )
    assert result == {"data": {"SIG_KEY_NAME": "some-key"}}


def test_get_configmap_with_namespace() -> None:
    """Kubectl is called with namespace flag when namespace is provided."""
    cm_json = json.dumps({"data": {"key": "value"}})
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=cm_json, returncode=0)
        result = get_configmap("cluster-config", namespace="konflux-info")

    mock_run.assert_called_once_with(
        ["kubectl", "get", "cm/cluster-config", "-ojson", "-n", "konflux-info"], check=False
    )
    assert result == {"data": {"key": "value"}}


def test_get_configmap_raises_on_kubectl_failure() -> None:
    """RuntimeError is raised with the configmap name and stderr when kubectl fails."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1, stderr="Error from server (NotFound): configmaps not found"
        )
        with pytest.raises(RuntimeError, match="signing-config-map"):
            get_configmap("signing-config-map")


def test_auth_can_i_returns_true_when_allowed() -> None:
    """Return True when kubectl reports 'yes'."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="yes\n", returncode=0, stderr="")
        auth_can_i("get", "release", name="my-rel", namespace="ns")

    print(mock_run.mock_calls)
    mock_run.assert_called_once_with(
        ["kubectl", "auth", "can-i", "get", "release/my-rel", "-n", "ns"],
        check=False,
    )


def test_auth_can_i_returns_false_when_denied() -> None:
    """Return False when kubectl reports 'no'."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="no\n", returncode=0, stderr="")
        auth_can_i("get", "snapshot", name="snap", namespace="ns")
        assert auth_can_i("get", "snapshot", name="snap", namespace="ns") is False


def test_auth_can_i_without_name() -> None:
    """Omit the resource name from the kubectl command when not provided."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="yes\n", returncode=0, stderr="")
        auth_can_i("create", "internalrequest", namespace="target-ns")

    mock_run.assert_called_once_with(
        ["kubectl", "auth", "can-i", "create", "internalrequest", "-n", "target-ns"],
        check=False,
    )


def test_auth_can_i_without_namespace() -> None:
    """Omit the namespace flag when not provided."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="yes\n", returncode=0, stderr="")
        auth_can_i("get", "release", name="my-rel")

    mock_run.assert_called_once_with(
        ["kubectl", "auth", "can-i", "get", "release/my-rel"],
        check=False,
    )


def test_auth_can_i_raises_on_nonzero_returncode() -> None:
    """Raise RuntimeError when kubectl exits with a non-zero return code."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="error: unknown resource"
        )
        with pytest.raises(RuntimeError, match="Failed to run 'kubectl auth can-i'"):
            auth_can_i("get", "release", name="my-rel", namespace="ns")


def test_auth_can_i_raises_includes_stderr_in_message() -> None:
    """Include stderr content in the RuntimeError message."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(returncode=2, stdout="", stderr="connection refused")
        with pytest.raises(RuntimeError, match="connection refused"):
            auth_can_i("create", "internalrequest", namespace="ns")


def test_auth_can_i_raises_falls_back_to_stdout_when_no_stderr() -> None:
    """Use stdout in the error message when stderr is empty."""
    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="some output", stderr="")
        with pytest.raises(RuntimeError, match="some output"):
            auth_can_i("get", "release", name="r", namespace="ns")
