"""Tests for `kubectl`."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from release_service_utils.helpers.kubectl import get_configmap


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


def test_get_configmap_raises_on_kubectl_failure() -> None:
    """RuntimeError is raised with the configmap name and stderr when kubectl fails."""
    import pytest

    with patch("release_service_utils.helpers.kubectl.kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1, stderr="Error from server (NotFound): configmaps not found"
        )
        with pytest.raises(RuntimeError, match="signing-config-map"):
            get_configmap("signing-config-map")
