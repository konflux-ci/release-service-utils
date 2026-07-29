"""Tests for get_resource module.

Uses unittest.mock to patch subprocess.run calls, simulating kubectl and
kubectl-ka behavior without requiring actual cluster access.
"""

from __future__ import annotations

import json
import os

import pytest
from unittest.mock import MagicMock, patch

from utils.get_resource import (
    main,
    get_resource,
    get_resource_dict,
    ResourceFetchError,
    extract_jsonpath,
    format_jsonpath_result,
    ka_enabled,
    ensure_ka_config,
    get_from_ka,
    _resource_version,
    _run,
)


def test_extract_jsonpath_simple_field():
    """Test extract jsonpath simple field."""
    data = {"spec": {"app": "myapp"}}
    assert extract_jsonpath(data, "{.spec}") == {"app": "myapp"}


def test_extract_jsonpath_nested_field():
    """Test extract jsonpath nested field."""
    data = {"metadata": {"name": "snap1", "namespace": "ns1"}}
    assert extract_jsonpath(data, "{.metadata.name}") == "snap1"


def test_extract_jsonpath_missing_field():
    """Test extract jsonpath missing field."""
    data = {"spec": {}}
    assert extract_jsonpath(data, "{.metadata.name}") is None


def test_extract_jsonpath_wildcard():
    """Test extract jsonpath wildcard."""
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
    """Test extract jsonpath labels."""
    data = {
        "metadata": {
            "labels": {"app": "myapp", "version": "v1"},
        }
    }
    result = extract_jsonpath(data, "{.metadata.labels}")
    assert result == {"app": "myapp", "version": "v1"}


def test_format_jsonpath_result_string():
    """Test format jsonpath result string."""
    assert format_jsonpath_result("hello") == "hello"


def test_format_jsonpath_result_dict():
    """Test format jsonpath result dict."""
    result = format_jsonpath_result({"a": 1})
    assert json.loads(result) == {"a": 1}


def test_format_jsonpath_result_list_of_strings():
    """Test format jsonpath result list of strings."""
    assert format_jsonpath_result(["a", "b"]) == "a b"


def test_format_jsonpath_result_list_of_dicts():
    """Test format jsonpath result list of dicts."""
    result = format_jsonpath_result([{"a": 1}])
    assert "a" in result


def test_ka_enabled_snapshot():
    """Test ka enabled snapshot."""
    assert ka_enabled("snapshot") is True


def test_ka_enabled_snapshots():
    """Test ka enabled snapshots."""
    assert ka_enabled("snapshots") is True


def test_ka_enabled_deployment():
    """Test ka enabled deployment."""
    assert ka_enabled("deployment") is False


def test_ka_enabled_pod():
    """Test ka enabled pod."""
    assert ka_enabled("pod") is False


def test_ka_enabled_release():
    """Test ka enabled release."""
    assert ka_enabled("release") is False


def test_resource_version_single_item():
    """Test resource version single item."""
    items = [{"metadata": {"resourceVersion": "100"}}]
    assert max(items, key=_resource_version) == items[0]


def test_resource_version_multiple_items():
    """Test resource version multiple items."""
    items = [
        {"metadata": {"resourceVersion": "50"}, "spec": {"v": "old"}},
        {"metadata": {"resourceVersion": "200"}, "spec": {"v": "newest"}},
        {"metadata": {"resourceVersion": "100"}, "spec": {"v": "middle"}},
    ]
    best = max(items, key=_resource_version)
    assert best["spec"]["v"] == "newest"


def test_resource_version_non_numeric():
    """Test resource version non numeric."""
    items = [
        {"metadata": {"resourceVersion": "abc"}, "data": "fallback"},
        {"metadata": {"resourceVersion": "10"}, "data": "numeric"},
    ]
    best = max(items, key=_resource_version)
    assert best["data"] == "numeric"


def test_ensure_ka_config_already_exists(tmp_path):
    """Test ensure ka config already exists."""
    config_file = tmp_path / "ka-config"
    config_file.touch()
    with patch.dict(os.environ, {"KUBECTL_KA_CONFIG_PATH": str(config_file)}):
        ensure_ka_config()


@patch("utils.get_resource._run")
def test_ensure_ka_config_configmap_not_found(mock_run, tmp_path):
    """Test ensure ka config configmap not found."""
    config_file = tmp_path / "ka-config"
    mock_run.return_value = (1, "", "not found")
    with patch.dict(os.environ, {"KUBECTL_KA_CONFIG_PATH": str(config_file)}):
        with pytest.raises(RuntimeError, match="kubearchive-api-url ConfigMap not found"):
            ensure_ka_config()


@patch("utils.get_resource._run")
def test_ensure_ka_config_creation_succeeds(mock_run, tmp_path):
    """Test ensure ka config creation succeeds."""
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
    """Test ensure ka config set host fails."""
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
    """Test ensure ka config set ca fails."""
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
    """Test ensure ka config ssl cert file used."""
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
    """Test get from ka config unavailable."""
    with pytest.raises(RuntimeError, match="config unavailable"):
        get_from_ka("snapshot", "ns1", "snap1")


@patch("utils.get_resource.ensure_ka_config")
@patch("utils.get_resource._run")
def test_get_from_ka_named_get_success(mock_run, _):
    """Test get from ka named get success."""
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
    """Test get from ka list fallback filters by name."""
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
    """Test get from ka list fallback picks highest version."""
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
    """Test get from ka no matching items."""
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
    """Test get from ka both get and list fail."""
    mock_run.side_effect = [
        (1, "", ""),
        (1, "", ""),
    ]
    with pytest.raises(RuntimeError, match="get and list both failed"):
        get_from_ka("snapshot", "ns1", "snap1")


def test_main_no_arguments(capsys):
    """Test main no arguments."""
    with patch("sys.argv", ["get-resource"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2
    assert "required" in capsys.readouterr().err


def test_main_one_argument(capsys):
    """Test main one argument."""
    with patch("sys.argv", ["get-resource", "snapshot"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2
    assert "required" in capsys.readouterr().err


def test_main_invalid_namespaced_name(capsys):
    """Test main invalid namespaced name."""
    with patch("sys.argv", ["get-resource", "snapshot", "badformat"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2
    assert "expected namespace/name" in capsys.readouterr().err


@patch("utils.get_resource._run")
def test_main_kubectl_success_json(mock_run, capsys):
    """Test main kubectl success json."""
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
    """Test main kubectl success jsonpath."""
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
    """Test main non ka type no jsonpath exits with error."""
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
    """Test main non ka type jsonpath returns empty object."""
    mock_run.return_value = (1, "", "not found")
    with patch(
        "sys.argv",
        ["get-resource", "pod", "ns1/mypod", "{.metadata.name}"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == "{}"


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_ka_fallback_success(mock_run, mock_ka, capsys):
    """Test main ka fallback success."""
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
    """Test main ka fallback with jsonpath."""
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
    """Test main ka fails jsonpath returns empty."""
    mock_run.return_value = (1, "", "")

    with patch(
        "sys.argv",
        ["get-resource", "snapshot", "ns1/snap1", "{.spec.application}"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == "{}"


@patch("utils.get_resource.get_from_ka", side_effect=RuntimeError("KA failed"))
@patch("utils.get_resource._run")
def test_main_ka_fails_no_jsonpath_exits_nonzero(mock_run, mock_ka, capsys):
    """Test main ka fails no jsonpath exits nonzero."""
    mock_run.return_value = (1, "", "resource not found")

    with patch("sys.argv", ["get-resource", "snapshot", "ns1/snap1"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_ka_fallback_wildcard_jsonpath(mock_run, mock_ka, capsys):
    """Test main ka fallback wildcard jsonpath."""
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
    """Test main ka not available exits nonzero."""
    mock_run.return_value = (1, "", "")

    with patch("sys.argv", ["get-resource", "snapshot", "ns1/snap1"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_snapshot_uses_ka(mock_run, mock_ka, capsys):
    """Test main snapshot uses ka."""
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
    """Test main snapshots uses ka."""
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
    """Test main deployment no ka."""
    mock_run.return_value = (1, "", "not found")

    with patch("sys.argv", ["get-resource", "deployment", "ns1/mydep"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    mock_ka.assert_not_called()


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_main_pod_no_ka(mock_run, mock_ka, capsys):
    """Test main pod no ka."""
    mock_run.return_value = (1, "", "not found")

    with patch("sys.argv", ["get-resource", "pod", "ns1/mypod"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    mock_ka.assert_not_called()


# --- Tests for the get_resource() library function ---


@patch("utils.get_resource._run")
def test_get_resource_full_json_success(mock_run: MagicMock) -> None:
    """Test get resource full json success."""
    resource_json = json.dumps({"kind": "Release", "metadata": {"name": "r1"}})
    mock_run.return_value = (0, resource_json, "")
    result = get_resource("release", "ns1", "r1")
    assert json.loads(result)["kind"] == "Release"


@patch("utils.get_resource._run")
def test_get_resource_full_json_failure_raises(mock_run: MagicMock) -> None:
    """Test get resource full json failure raises."""
    mock_run.return_value = (1, "", "not found")
    with pytest.raises(ResourceFetchError) as exc_info:
        get_resource("release", "ns1", "r1")
    assert exc_info.value.exit_code == 1


@patch("utils.get_resource._run")
def test_get_resource_jsonpath_success(mock_run: MagicMock) -> None:
    """Test get resource jsonpath success."""
    mock_run.return_value = (0, '{"foo":"bar"}', "")
    result = get_resource("release", "ns1", "r1", "{.spec.data}")
    assert result == '{"foo":"bar"}'


@patch("utils.get_resource._run")
def test_get_resource_jsonpath_failure_returns_empty(mock_run: MagicMock) -> None:
    """Test get resource jsonpath failure returns empty."""
    mock_run.return_value = (1, "", "not found")
    result = get_resource("pod", "ns1", "p1", "{.spec}")
    assert result == "{}"


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_get_resource_ka_fallback_full(mock_run: MagicMock, mock_ka: MagicMock) -> None:
    """Test get resource ka fallback full."""
    mock_run.return_value = (1, "", "not found")
    ka_data = {"metadata": {"name": "s1", "namespace": "ns1", "resourceVersion": "1"}}
    mock_ka.return_value = json.dumps(ka_data)
    result = get_resource("snapshot", "ns1", "s1")
    assert json.loads(result)["metadata"]["name"] == "s1"


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_get_resource_ka_fallback_jsonpath(mock_run: MagicMock, mock_ka: MagicMock) -> None:
    """Test get resource ka fallback jsonpath."""
    mock_run.return_value = (1, "", "not found")
    ka_data = {"metadata": {"name": "s1", "resourceVersion": "1"}, "spec": {"app": "x"}}
    mock_ka.return_value = json.dumps(ka_data)
    result = get_resource("snapshot", "ns1", "s1", "{.spec.app}")
    assert result == "x"


@patch("utils.get_resource._run")
def test_get_resource_dict_success(mock_run: MagicMock) -> None:
    """Test get resource dict success."""
    resource = {"kind": "Release", "metadata": {"name": "r1"}}
    mock_run.return_value = (0, json.dumps(resource), "")
    result = get_resource_dict("release", "ns1", "r1")
    assert isinstance(result, dict)
    assert result["kind"] == "Release"
    assert result["metadata"]["name"] == "r1"


@patch("utils.get_resource._run")
def test_get_resource_dict_failure_raises(mock_run: MagicMock) -> None:
    """Test get resource dict failure raises."""
    mock_run.return_value = (1, "", "not found")
    with pytest.raises(ResourceFetchError):
        get_resource_dict("release", "ns1", "r1")


@patch("utils.get_resource.get_from_ka")
@patch("utils.get_resource._run")
def test_get_resource_dict_ka_fallback(mock_run: MagicMock, mock_ka: MagicMock) -> None:
    """Test get resource dict ka fallback."""
    mock_run.return_value = (1, "", "not found")
    ka_data = {"metadata": {"name": "s1", "resourceVersion": "1"}}
    mock_ka.return_value = json.dumps(ka_data)
    result = get_resource_dict("snapshot", "ns1", "s1")
    assert isinstance(result, dict)
    assert result["metadata"]["name"] == "s1"


@patch("utils.get_resource.subprocess_cmd.run_cmd")
def test_run_delegates_to_subprocess_cmd(mock_run_cmd: MagicMock) -> None:
    """Test _run delegates to subprocess_cmd and normalises stderr."""
    mock_run_cmd.return_value = type(
        "Result", (), {"returncode": 0, "stdout": "ok", "stderr": None}
    )()
    rc, stdout, stderr = _run(["echo", "hi"])
    mock_run_cmd.assert_called_once_with(["echo", "hi"], check=False)
    assert (rc, stdout, stderr) == (0, "ok", "")


def test_resource_fetch_error_attributes() -> None:
    """Test resource fetch error attributes."""
    err = ResourceFetchError("something broke", exit_code=42)
    assert str(err) == "something broke"
    assert err.exit_code == 42


def test_resource_fetch_error_default_exit_code() -> None:
    """Test resource fetch error default exit code."""
    err = ResourceFetchError("fail")
    assert err.exit_code == 1
