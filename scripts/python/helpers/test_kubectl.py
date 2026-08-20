"""Tests for `kubectl`."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from kubectl import ConfigMapNotFoundError, get_configmap


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


def test_get_configmap_raises_not_found_error_when_configmap_missing() -> None:
    """ConfigMapNotFoundError (a RuntimeError subclass) is raised for a NotFound response."""
    import pytest

    with patch("kubectl.run_cmd") as mock_run:
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
    import pytest

    with patch("kubectl.run_cmd") as mock_run:
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
    import pytest

    with patch("kubectl.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr='Error from server (Forbidden): configmaps "signing-config-map" is '
            "forbidden: User cannot get resource",
        )
        with pytest.raises(RuntimeError, match="signing-config-map") as exc_info:
            get_configmap("signing-config-map")
        assert not isinstance(exc_info.value, ConfigMapNotFoundError)
