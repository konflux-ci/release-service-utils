"""Tests for get_resource module.

Uses unittest.mock to patch subprocess.run calls, simulating kubectl and
kubectl-ka behavior without requiring actual cluster access.
"""

import json
import os

import pytest
from unittest.mock import patch

from utils.get_resource import (
    main,
    extract_jsonpath,
    format_jsonpath_result,
    ka_enabled,
    ensure_ka_config,
    get_from_ka,
    _resource_version,
)


def test_extract_jsonpath_simple_field():
    data = {"spec": {"app": "myapp"}}
    assert extract_jsonpath(data, "{.spec}") == {"app": "myapp"}


def test_extract_jsonpath_nested_field():
    data = {"metadata": {"name": "snap1", "namespace": "ns1"}}
    assert extract_jsonpath(data, "{.metadata.name}") == "snap1"


def test_extract_jsonpath_missing_field():
    data = {"spec": {}}
    assert extract_jsonpath(data, "{.metadata.name}") is None


def test_extract_jsonpath_wildcard():
    data = {
        "spec": {
            "components": [
                {"name": "comp-a"},
                {"name": "comp-b"},
            ]
        }
    }
    result = extract_jsonpath(data, "{.spec.components[*].name}")
    assert result == ["comp-a", "comp-b"]


def test_extract_jsonpath_labels():
    data = {
        "metadata": {
            "labels": {"app": "myapp", "version": "v1"},
        }
    }
    result = extract_jsonpath(data, "{.metadata.labels}")
    assert result == {"app": "myapp", "version": "v1"}


def test_format_jsonpath_result_string():
    assert format_jsonpath_result("hello") == "hello"


def test_format_jsonpath_result_dict():
    result = format_jsonpath_result({"a": 1})
    assert json.loads(result) == {"a": 1}


def test_format_jsonpath_result_list_of_strings():
    assert format_jsonpath_result(["a", "b"]) == "a b"


def test_format_jsonpath_result_list_of_dicts():
    result = format_jsonpath_result([{"a": 1}])
    assert "a" in result


def test_ka_enabled_snapshot():
    assert ka_enabled("snapshot") is True


def test_ka_enabled_snapshots():
    assert ka_enabled("snapshots") is True


def test_ka_enabled_deployment():
    assert ka_enabled("deployment") is False


def test_ka_enabled_pod():
    assert ka_enabled("pod") is False


def test_ka_enabled_release():
    assert ka_enabled("release") is False


def test_resource_version_single_item():
    items = [{"metadata": {"resourceVersion": "100"}}]
    assert max(items, key=_resource_version) == items[0]


def test_resource_version_multiple_items():
    items = [
        {"metadata": {"resourceVersion": "50"}, "spec": {"v": "old"}},
        {"metadata": {"resourceVersion": "200"}, "spec": {"v": "newest"}},
        {"metadata": {"resourceVersion": "100"}, "spec": {"v": "middle"}},
    ]
    best = max(items, key=_resource_version)
    assert best["spec"]["v"] == "newest"


def test_resource_version_non_numeric():
    items = [
        {"metadata": {"resourceVersion": "abc"}, "data": "fallback"},
        {"metadata": {"resourceVersion": "10"}, "data": "numeric"},
    ]
    best = max(items, key=_resource_version)
    assert best["data"] == "numeric"


def test_ensure_ka_config_already_exists(tmp_path):
    config_file = tmp_path / "ka-config"
    config_file.touch()
    with patch.dict(os.environ, {"KUBECTL_KA_CONFIG_PATH": str(config_file)}):
        ensure_ka_config()


@patch("utils.get_resource._run")
def test_ensure_ka_config_configmap_not_found(mock_run, tmp_path):
    config_file = tmp_path / "ka-config"
    mock_run.return_value = (1, "", "not found")
    with patch.dict(os.environ, {"KUBECTL_KA_CONFIG_PATH": str(config_file)}):
        with pytest.raises(RuntimeError, match="kubearchive-api-url ConfigMap not found"):
            ensure_ka_config()


@patch("utils.get_resource._run")
def test_ensure_ka_config_creation_succeeds(mock_run, tmp_path):
    config_file = tmp_path / "ka-config"

    def side_effect(cmd):
        if "configmap" in cmd:
            return (0, "https://ka.example.com", "")
        if cmd[:3] == ["kubectl", "ka", "config"]:
            config_file.touch()
            return (0, "", "")
        return (1, "", "")

    mock_run.side_effect = side_effect
    with patch.dict(os.environ, {"KUBECTL_KA_CONFIG_PATH": str(config_file)}):
        ensure_ka_config()


@patch("utils.get_resource._run")
def test_ensure_ka_config_set_host_fails(mock_run, tmp_path):
    config_file = tmp_path / "ka-config"
    mock_run.side_effect = [
        (0, "https://ka.example.com", ""),
        (1, "", "error: unable to write config"),
    ]
    with patch.dict(os.environ, {"KUBECTL_KA_CONFIG_PATH": str(config_file)}):
        with pytest.raises(RuntimeError, match="Failed to set KubeArchive host"):
            ensure_ka_config()


@patch("utils.get_resource._run")
def test_ensure_ka_config_set_ca_fails(mock_run, tmp_path):
    config_file = tmp_path / "ka-config"
    mock_run.side_effect = [
        (0, "https://ka.example.com", ""),
        (0, "", ""),
        (1, "", "error: unable to write CA"),
    ]
    with patch.dict(
        os.environ,
        {
            "KUBECTL_KA_CONFIG_PATH": str(config_file),
            "SSL_CERT_FILE": "/path/to/cert.pem",
        },
    ):
        with pytest.raises(RuntimeError, match="Failed to set KubeArchive CA"):
            ensure_ka_config()


@patch("utils.get_resource._run")
def test_ensure_ka_config_ssl_cert_file_used(mock_run, tmp_path):
    config_file = tmp_path / "ka-config"

    calls_made = []

    def side_effect(cmd):
        calls_made.append(cmd)
        if "configmap" in cmd:
            return (0, "https://ka.example.com", "")
        if cmd[:3] == ["kubectl", "ka", "config"]:
            config_file.touch()
            return (0, "", "")
        return (1, "", "")

    mock_run.side_effect = side_effect
    with patch.dict(
        os.environ,
        {
            "KUBECTL_KA_CONFIG_PATH": str(config_file),
            "SSL_CERT_FILE": "/path/to/cert.pem",
        },
    ):
        ensure_ka_config()

    ca_calls = [c for c in calls_made if "ca" in c]
    assert len(ca_calls) == 1
    assert "/path/to/cert.pem" in ca_calls[0]


@patch("utils.get_resource.ensure_ka_config", side_effect=RuntimeError("config unavailable"))
def test_get_from_ka_config_unavailable(_):
    with pytest.raises(RuntimeError, match="config unavailable"):
        get_from_ka("snapshot", "ns1", "snap1")


@patch("utils.get_resource.ensure_ka_config")
@patch("utils.get_resource._run")
def test_get_from_ka_named_get_success(mock_run, _):
    ka_response = {
        "items": [
            {
                "metadata": {
                    "name": "snap1",
                    "namespace": "ns1",
                    "resourceVersion": "100",
                },
                "spec": {"app": "myapp"},
            }
        ]
    }
    mock_run.return_value = (0, json.dumps(ka_response), "")
    result = get_from_ka("snapshot", "ns1", "snap1")
    assert result is not None
    data = json.loads(result)
    assert data["metadata"]["name"] == "snap1"


@patch("utils.get_resource.ensure_ka_config")
@patch("utils.get_resource._run")
def test_get_from_ka_list_fallback_filters_by_name(mock_run, _):
    list_response = {
        "items": [
            {
                "metadata": {
                    "name": "other",
                    "namespace": "ns1",
                    "resourceVersion": "999",
                },
                "spec": {"wrong": True},
            },
            {
                "metadata": {
                    "name": "snap1",
                    "namespace": "ns1",
                    "resourceVersion": "10",
                },
                "spec": {"correct": True},
            },
        ]
    }
    mock_run.side_effect = [
        (1, "", "named get failed"),
        (0, json.dumps(list_response), ""),
    ]
    result = get_from_ka("snapshot", "ns1", "snap1")
    assert result is not None
    data = json.loads(result)
    assert data["spec"]["correct"] is True
    assert "wrong" not in data.get("spec", {})


@patch("utils.get_resource.ensure_ka_config")
@patch("utils.get_resource._run")
def test_get_from_ka_list_fallback_picks_highest_version(mock_run, _):
    list_response = {
        "items": [
            {
                "metadata": {
                    "name": "snap1",
                    "namespace": "ns1",
                    "resourceVersion": "50",
                },
                "spec": {"version": "old"},
            },
            {
                "metadata": {
                    "name": "snap1",
                    "namespace": "ns1",
                    "resourceVersion": "200",
                },
                "spec": {"version": "newest"},
            },
            {
                "metadata": {
                    "name": "snap1",
                    "namespace": "ns1",
                    "resourceVersion": "100",
                },
                "spec": {"version": "middle"},
            },
        ]
    }
    mock_run.side_effect = [
        (1, "", ""),
        (0, json.dumps(list_response), ""),
    ]
    result = get_from_ka("snapshot", "ns1", "snap1")
    data = json.loads(result)
    assert data["spec"]["version"] == "newest"


@patch("utils.get_resource.ensure_ka_config")
@patch("utils.get_resource._run")
def test_get_from_ka_no_matching_items(mock_run, _):
    list_response = {
        "items": [
            {
                "metadata": {
                    "name": "other",
                    "namespace": "ns1",
                    "resourceVersion": "100",
                }
            }
        ]
    }
    mock_run.side_effect = [
        (1, "", ""),
        (0, json.dumps(list_response), ""),
    ]
    with pytest.raises(RuntimeError, match="not found in KubeArchive"):
        get_from_ka("snapshot", "ns1", "snap1")


@patch("utils.get_resource.ensure_ka_config")
@patch("utils.get_resource._run")
def test_get_from_ka_both_get_and_list_fail(mock_run, _):
    mock_run.side_effect = [
        (1, "", ""),
        (1, "", ""),
    ]
    with pytest.raises(RuntimeError, match="get and list both failed"):
        get_from_ka("snapshot", "ns1", "snap1")


def test_main_no_arguments(capsys):
    with patch("sys.argv", ["get-resource"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2
    assert "required" in capsys.readouterr().err


def test_main_one_argument(capsys):
    with patch("sys.argv", ["get-resource", "snapshot"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2
    assert "required" in capsys.readouterr().err


def test_main_invalid_namespaced_name(capsys):
    with patch("sys.argv", ["get-resource", "snapshot", "badformat"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2
    assert "expected namespace/name" in capsys.readouterr().err


@patch("utils.get_resource._run")
def test_main_kubectl_success_json(mock_run, capsys):
    resource_json = json.dumps(
        {
            "kind": "Snapshot",
            "metadata": {"name": "snap1", "namespace": "ns1"},
        }
    )
    mock_run.return_value = (0, resource_json, "")
    with patch("sys.argv", ["get-resource", "snapshot", "ns1/snap1"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    out = capsys.readouterr().out
    assert json.loads(out)["kind"] == "Snapshot"


@patch("utils.get_resource._run")
def test_main_kubectl_success_jsonpath(mock_run, capsys):
    mock_run.return_value = (0, "snap1", "")
    with patch(
        "sys.argv",
        ["get-resource", "snapshot", "ns1/snap1", "{.metadata.name}"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    assert capsys.readouterr().out == "snap1"


@patch("utils.get_resource._run")
def test_main_non_ka_type_no_jsonpath_exits_with_error(mock_run, capsys):
    mock_run.return_value = (
        1,
        "",
        'Error from server (NotFound): pods "mypod" not found',
    )
    with patch("sys.argv", ["get-resource", "pod", "ns1/mypod"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    assert "NotFound" in capsys.readouterr().err


@patch("utils.get_resource._run")
def test_main_non_ka_type_jsonpath_returns_empty_object(mock_run, capsys):
    mock_run.return_value = (1, "", "not found")
    with patch(
        "sys.argv",
        ["get-resource", "pod", "ns1/mypod", "{.metadata.name}"],
    ):
        main()
    assert capsys.readouterr().out.strip() == "{}"


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_ka_fallback_success(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "not found")
    ka_result = {
        "kind": "Snapshot",
        "metadata": {
            "name": "snap1",
            "namespace": "ns1",
            "resourceVersion": "100",
        },
        "spec": {"app": "myapp"},
    }
    mock_ka.return_value = json.dumps(ka_result, indent=2)

    with patch("sys.argv", ["get-resource", "snapshot", "ns1/snap1"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["metadata"]["name"] == "snap1"


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_ka_fallback_with_jsonpath(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "not found")
    ka_result = {
        "metadata": {
            "name": "snap1",
            "namespace": "ns1",
            "resourceVersion": "100",
        },
        "spec": {"application": "myapp"},
    }
    mock_ka.return_value = json.dumps(ka_result)

    with patch(
        "sys.argv",
        ["get-resource", "snapshot", "ns1/snap1", "{.spec.application}"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == "myapp"


@patch("utils.get_resource.get_from_ka", side_effect=RuntimeError("KA failed"))
@patch("utils.get_resource._run")
def test_main_ka_fails_jsonpath_returns_empty(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "")

    with patch(
        "sys.argv",
        ["get-resource", "snapshot", "ns1/snap1", "{.spec.application}"],
    ):
        main()
    assert capsys.readouterr().out.strip() == "{}"


@patch("utils.get_resource.get_from_ka", side_effect=RuntimeError("KA failed"))
@patch("utils.get_resource._run")
def test_main_ka_fails_no_jsonpath_exits_nonzero(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "resource not found")

    with patch("sys.argv", ["get-resource", "snapshot", "ns1/snap1"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_ka_fallback_wildcard_jsonpath(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "")
    ka_result = {
        "metadata": {
            "name": "snap1",
            "namespace": "ns1",
            "resourceVersion": "1",
        },
        "spec": {
            "components": [
                {"name": "comp-a"},
                {"name": "comp-b"},
            ]
        },
    }
    mock_ka.return_value = json.dumps(ka_result)

    with patch(
        "sys.argv",
        [
            "get-resource",
            "snapshot",
            "ns1/snap1",
            "{.spec.components[*].name}",
        ],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    out = capsys.readouterr().out.strip()
    assert "comp-a" in out
    assert "comp-b" in out


@patch("utils.get_resource.get_from_ka", side_effect=RuntimeError("KA unavailable"))
@patch("utils.get_resource._run")
def test_main_ka_not_available_exits_nonzero(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "")

    with patch("sys.argv", ["get-resource", "snapshot", "ns1/snap1"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_snapshot_uses_ka(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "")
    ka_data = {"metadata": {"name": "s", "namespace": "n", "resourceVersion": "1"}}
    mock_ka.return_value = json.dumps(ka_data)

    with patch("sys.argv", ["get-resource", "snapshot", "n/s"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    mock_ka.assert_called_once()


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_snapshots_uses_ka(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "")
    ka_data = {"metadata": {"name": "s", "namespace": "n", "resourceVersion": "1"}}
    mock_ka.return_value = json.dumps(ka_data)

    with patch("sys.argv", ["get-resource", "snapshots", "n/s"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    mock_ka.assert_called_once()


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_deployment_no_ka(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "not found")

    with patch("sys.argv", ["get-resource", "deployment", "ns1/mydep"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    mock_ka.assert_not_called()


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_pod_no_ka(mock_run, mock_ka, capsys):
    mock_run.return_value = (1, "", "not found")

    with patch("sys.argv", ["get-resource", "pod", "ns1/mypod"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    mock_ka.assert_not_called()
