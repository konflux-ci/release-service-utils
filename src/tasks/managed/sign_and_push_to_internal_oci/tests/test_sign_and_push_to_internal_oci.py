"""Test managed signing orchestration, request labels, failures, and entry points."""

from __future__ import annotations

import importlib
import json
import runpy
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.helpers import internal_request
from release_service_utils.tasks.managed.sign_and_push_to_internal_oci import (
    extract_origin,
    main,
    run,
    sign_and_push_to_internal_oci as task,
)

TASK = "release_service_utils.tasks.managed.sign_and_push_to_internal_oci"
IMPLEMENTATION = f"{TASK}.sign_and_push_to_internal_oci"
ENV_ARGS = {
    "PARAM_DATA_DIR": "data_dir",
    "PARAM_SNAPSHOT_PATH": "snapshot_path",
    "PARAM_RELEASE_PLAN_ADMISSION_PATH": "rpa_path",
    "PARAM_DATA_PATH": "data_path",
    "PARAM_PIPELINE_RUN_UID": "pipeline_run_uid",
    "PARAM_TASK_RUN_UID": "task_run_uid",
    "PARAM_RESULTS_DIR_PATH": "results_dir_path",
    "PARAM_MAC_SIGNING_SCRIPT": "mac_signing_script",
    "PARAM_WINDOWS_SIGNING_SCRIPT": "windows_signing_script",
    "PARAM_TASK_GIT_URL": "task_git_url",
    "PARAM_TASK_GIT_REVISION": "task_git_revision",
}


@pytest.fixture
def workflow_args(tmp_path: Path) -> dict[str, Any]:
    """Create release inputs and return managed workflow arguments."""
    inputs = tmp_path / "release" / "pipeline-uid"
    inputs.mkdir(parents=True)
    snapshot = {
        "application": "test-app",
        "components": [
            {
                "name": "mac-component",
                "containerImage": "quay.io/test/mac@sha256:abc",
                "metadata": {"labels": [{"name": "large", "value": "metadata"}]},
                "staged": {"files": [{"filename": "app.dmg"}]},
            },
            {
                "name": "windows-component",
                "metadata": {"env_variables": {"KEY": "VALUE"}},
                "staged": {"files": [{"filename": "app.exe"}, {"filename": "app.zip"}]},
            },
            {"name": "unstaged-component"},
        ],
    }
    (inputs / "snapshot.json").write_text(json.dumps(snapshot), encoding="utf-8")
    (inputs / "data.json").write_text('{"intention":"production"}', encoding="utf-8")
    (inputs / "rpa.json").write_text(
        '{"spec":{"origin":"tenant-namespace"}}', encoding="utf-8"
    )
    return {
        "data_dir": tmp_path / "release",
        "snapshot_path": "pipeline-uid/snapshot.json",
        "rpa_path": "pipeline-uid/rpa.json",
        "data_path": "pipeline-uid/data.json",
        "pipeline_run_uid": "pipeline-uid",
        "task_run_uid": "task-uid",
        "results_dir_path": "pipeline-uid/results",
        "mac_signing_script": "/opt/custom_mac_signing.py",
        "windows_signing_script": "C:\\signing\\custom_windows_signing.ps1",
        "task_git_url": "https://github.com/konflux-ci/release-service-catalog.git",
        "task_git_revision": "test-revision",
    }


@pytest.fixture
def request_mocks() -> Iterator[tuple[MagicMock, MagicMock]]:
    """Mock cluster operations while retaining the real request helper constants."""
    with (
        patch(
            f"{IMPLEMENTATION}.internal_request.create", return_value="sign-request-123"
        ) as create,
        patch(
            f"{IMPLEMENTATION}.internal_request.fetch_results",
            return_value={"result": "Success"},
        ) as fetch_results,
    ):
        yield create, fetch_results


@pytest.fixture
def task_environment(
    monkeypatch: pytest.MonkeyPatch, workflow_args: dict[str, Any]
) -> dict[str, str]:
    """Inject the workflow arguments through the Tekton environment contract."""
    env = {name: str(workflow_args[arg]) for name, arg in ENV_ARGS.items()}
    env["PARAM_QUAY_URL"] = "quay.io/konflux-artifacts"
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return env


def test_extract_origin(tmp_path: Path) -> None:
    """Read the originating tenant namespace from the ReleasePlanAdmission spec."""
    rpa_path = tmp_path / "rpa.json"
    rpa_path.write_text('{"spec":{"origin":"tenant-namespace"}}', encoding="utf-8")

    assert extract_origin(rpa_path) == "tenant-namespace"


@pytest.mark.parametrize("rpa", [{}, {"spec": {}}])
def test_extract_origin_missing_fields(tmp_path: Path, rpa: dict[str, Any]) -> None:
    """Fail when the ReleasePlanAdmission cannot identify an origin."""
    rpa_path = tmp_path / "rpa.json"
    rpa_path.write_text(json.dumps(rpa), encoding="utf-8")

    with pytest.raises(KeyError):
        extract_origin(rpa_path)


@pytest.mark.parametrize(
    ("intention", "quay_url_override", "quay_url"),
    [
        ("staging", None, "quay.io/konflux-artifacts/nonprod"),
        ("production", None, "quay.io/konflux-artifacts/prod"),
        (None, None, "quay.io/konflux-artifacts/prod"),
        ("staging", "quay.io/konflux-artifacts", "quay.io/konflux-artifacts/nonprod"),
        ("production", "quay.io/konflux-artifacts", "quay.io/konflux-artifacts/prod"),
        ("staging", "quay.example.com/team/artifacts", "quay.example.com/team/artifacts"),
        ("production", "quay.example.com/team/artifacts", "quay.example.com/team/artifacts"),
        (None, "quay.example.com/team/artifacts", "quay.example.com/team/artifacts"),
        ("staging", "quay.io/konflux-artifacts/prod", "quay.io/konflux-artifacts/prod"),
    ],
)
def test_run_success(
    workflow_args: dict[str, Any],
    request_mocks: tuple[MagicMock, MagicMock],
    intention: str | None,
    quay_url_override: str | None,
    quay_url: str,
) -> None:
    """Submit the signing request with default or custom Quay URLs and staged filenames."""
    data = {} if intention is None else {"intention": intention}
    data_file = workflow_args["data_dir"] / workflow_args["data_path"]
    data_file.write_text(json.dumps(data), encoding="utf-8")
    create, fetch_results = request_mocks
    if quay_url_override is not None:
        workflow_args["quay_url"] = quay_url_override

    run(**workflow_args)

    params = create.call_args.kwargs["params"]
    snapshot = json.loads(params["snapshot_json"])
    assert snapshot == {
        "application": "test-app",
        "components": [
            {
                "name": "mac-component",
                "containerImage": "quay.io/test/mac@sha256:abc",
                "staged": {"files": [{"filename": "app.dmg"}]},
            },
            {
                "name": "windows-component",
                "staged": {"files": [{"filename": "app.exe"}, {"filename": "app.zip"}]},
            },
            {"name": "unstaged-component"},
        ],
    }
    assert params["snapshot_json"] == json.dumps(snapshot, separators=(",", ":"))
    create.assert_called_once_with(
        "sign-and-push-to-internal-oci",
        params={
            "snapshot_json": params["snapshot_json"],
            "quayURL": quay_url,
            "destQuayURL": "quay.io/redhat-user-workloads",
            "origin": "tenant-namespace",
            "macSigningScript": "/opt/custom_mac_signing.py",
            "windowsSigningScript": "C:\\signing\\custom_windows_signing.ps1",
            "destQuaySecret": "quay-internal-oci",
            "taskGitUrl": "https://github.com/konflux-ci/release-service-catalog.git",
            "taskGitRevision": "test-revision",
        },
        labels={
            "internal-services.appstudio.openshift.io/pipelinerun-uid": "pipeline-uid",
            "internal-services.appstudio.openshift.io/group-id": "task-uid",
        },
        sync=True,
        timeout=86700,
        service_account="release-service-account",
        pipeline_timeout="24h0m0s",
        task_timeout="23h50m0s",
        finally_timeout="0h10m0s",
    )
    fetch_results.assert_called_once_with("sign-request-123")
    results_dir = workflow_args["data_dir"] / workflow_args["results_dir_path"]
    assert json.loads((results_dir / "sign-internal-oci.json").read_text()) == {
        "artifacts": ["app.dmg", "app.exe", "app.zip"]
    }
    assert not (results_dir / "push-artifacts-results.json").exists()


@pytest.mark.parametrize("path_arg", ["snapshot_path", "data_path", "rpa_path"])
def test_run_missing_input(
    workflow_args: dict[str, Any],
    request_mocks: tuple[MagicMock, MagicMock],
    path_arg: str,
) -> None:
    """Fail before submitting a request when a required input file is missing."""
    workflow_args[path_arg] = "missing.json"
    create, fetch_results = request_mocks

    with pytest.raises(FileNotFoundError):
        run(**workflow_args)

    create.assert_not_called()
    fetch_results.assert_not_called()


@pytest.mark.parametrize("path_arg", ["snapshot_path", "data_path", "rpa_path"])
def test_run_invalid_json(
    workflow_args: dict[str, Any],
    request_mocks: tuple[MagicMock, MagicMock],
    path_arg: str,
) -> None:
    """Reject malformed input JSON before submitting an InternalRequest."""
    input_file = workflow_args["data_dir"] / workflow_args[path_arg]
    input_file.write_text("invalid JSON", encoding="utf-8")
    create, fetch_results = request_mocks

    with pytest.raises(json.JSONDecodeError):
        run(**workflow_args)

    create.assert_not_called()
    fetch_results.assert_not_called()


@pytest.mark.parametrize(
    "exit_code", [internal_request.EXIT_FAILED, internal_request.EXIT_TIMEOUT]
)
def test_run_request_wait_failure(
    workflow_args: dict[str, Any],
    request_mocks: tuple[MagicMock, MagicMock],
    exit_code: int,
) -> None:
    """Chain request failures and timeouts without fetching incomplete results."""
    create, fetch_results = request_mocks
    error = internal_request.InternalRequestWaitError("request did not succeed", exit_code)
    create.side_effect = error

    with pytest.raises(RuntimeError, match="request did not succeed") as exc_info:
        run(**workflow_args)

    assert exc_info.value.__cause__ is error
    fetch_results.assert_not_called()


@pytest.mark.parametrize("operation", ["create", "fetch_results"])
def test_run_api_failure(
    workflow_args: dict[str, Any],
    request_mocks: tuple[MagicMock, MagicMock],
    operation: str,
) -> None:
    """Propagate cluster errors instead of reporting a successful managed task."""
    create, fetch_results = request_mocks
    error = OSError("cluster unavailable")
    failing_mock = create if operation == "create" else fetch_results
    failing_mock.side_effect = error

    with pytest.raises(OSError, match="cluster unavailable") as exc_info:
        run(**workflow_args)

    assert exc_info.value is error
    if operation == "create":
        fetch_results.assert_not_called()


@pytest.mark.parametrize(
    "results", [{"result": "Failed"}, {"result": ""}, {"result": None}, {}]
)
def test_run_unsuccessful_result(
    workflow_args: dict[str, Any],
    request_mocks: tuple[MagicMock, MagicMock],
    results: dict[str, Any],
) -> None:
    """Reject unsuccessful or absent pipeline results after the request completes."""
    _, fetch_results = request_mocks
    fetch_results.return_value = results

    with pytest.raises(RuntimeError, match="Internal pipeline failed"):
        run(**workflow_args)

    fetch_results.assert_called_once_with("sign-request-123")


@pytest.mark.parametrize(
    ("quay_url", "intention", "expected_quay_url"),
    [
        (None, "staging", "quay.io/konflux-artifacts/nonprod"),
        (None, "production", "quay.io/konflux-artifacts/prod"),
        ("quay.io/konflux-artifacts", "staging", "quay.io/konflux-artifacts/nonprod"),
        ("quay.io/konflux-artifacts", "production", "quay.io/konflux-artifacts/prod"),
        ("quay.example.com/team/artifacts", "staging", "quay.example.com/team/artifacts"),
        ("quay.example.com/team/artifacts", "production", "quay.example.com/team/artifacts"),
    ],
)
def test_main(
    task_environment: dict[str, str],
    request_mocks: tuple[MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    quay_url: str | None,
    intention: str,
    expected_quay_url: str,
) -> None:
    """Read Tekton configuration, resolve Quay overrides, and attach the TaskRun UID."""
    create, fetch_results = request_mocks
    if quay_url is None:
        monkeypatch.delenv("PARAM_QUAY_URL")
    else:
        monkeypatch.setenv("PARAM_QUAY_URL", quay_url)
    data_file = Path(task_environment["PARAM_DATA_DIR"]) / task_environment["PARAM_DATA_PATH"]
    data_file.write_text(json.dumps({"intention": intention}), encoding="utf-8")

    assert main() == 0

    assert create.call_args.kwargs["params"]["quayURL"] == expected_quay_url
    assert create.call_args.kwargs["labels"] == {
        internal_request.PIPELINERUN_UID_LABEL: task_environment["PARAM_PIPELINE_RUN_UID"],
        internal_request.TASK_GROUP_LABEL: task_environment["PARAM_TASK_RUN_UID"],
    }
    assert (
        create.call_args.kwargs["params"]["macSigningScript"]
        == task_environment["PARAM_MAC_SIGNING_SCRIPT"]
    )
    assert (
        create.call_args.kwargs["params"]["windowsSigningScript"]
        == task_environment["PARAM_WINDOWS_SIGNING_SCRIPT"]
    )
    fetch_results.assert_called_once_with("sign-request-123")


@pytest.mark.parametrize("env_name", list(ENV_ARGS))
def test_main_missing_environment(
    task_environment: dict[str, str],
    request_mocks: tuple[MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
) -> None:
    """Reject missing required configuration before any cluster operation."""
    monkeypatch.delenv(env_name)
    create, fetch_results = request_mocks

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 1
    create.assert_not_called()
    fetch_results.assert_not_called()


def test_main_failure_propagates(
    task_environment: dict[str, str], request_mocks: tuple[MagicMock, MagicMock]
) -> None:
    """Let workflow failures escape the managed entry point."""
    _, fetch_results = request_mocks
    fetch_results.return_value = {"result": "Failed"}

    with pytest.raises(RuntimeError, match="Internal pipeline failed"):
        main()


@pytest.mark.parametrize("filename", ["sign_and_push_to_internal_oci.py", "__main__.py"])
def test_entry_point(
    task_environment: dict[str, str],
    request_mocks: tuple[MagicMock, MagicMock],
    filename: str,
) -> None:
    """Execute the script and package entry points with the full Tekton environment."""
    create, fetch_results = request_mocks
    script = Path(task.__file__).with_name(filename)

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(str(script), run_name="__main__")

    assert exc_info.value.code == 0
    assert create.call_args.kwargs["labels"][internal_request.TASK_GROUP_LABEL] == "task-uid"
    fetch_results.assert_called_once_with("sign-request-123")


def test_import_entry_point(request_mocks: tuple[MagicMock, MagicMock]) -> None:
    """Import the package entry point without starting the workflow."""
    entry_point = importlib.import_module(f"{TASK}.__main__")
    create, fetch_results = request_mocks

    assert entry_point.main is main
    create.assert_not_called()
    fetch_results.assert_not_called()
