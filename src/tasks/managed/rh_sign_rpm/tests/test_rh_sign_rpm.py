"""Test the rh_sign_rpm task: request params, failures, and entry points."""

from __future__ import annotations

import base64
import gzip
import importlib
import json
import runpy
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.helpers import internal_request
from release_service_utils.tasks.managed.rh_sign_rpm import (
    build_sign_request_params,
    main,
    rh_sign_rpm as task,
    run,
)

TASK = "release_service_utils.tasks.managed.rh_sign_rpm"
IMPLEMENTATION = f"{TASK}.rh_sign_rpm"
ENV_ARGS = {
    "PARAM_DATA_DIR": "data_dir",
    "PARAM_DATA_PATH": "data_path",
    "PARAM_ARTIFACTS_JSON_PATH": "artifacts_json_path",
    "PARAM_REQUESTER": "requester",
    "PARAM_REQUEST_TIMEOUT": "request_timeout",
    "PARAM_PIPELINE_TIMEOUT": "pipeline_timeout",
    "PARAM_TASK_TIMEOUT": "task_timeout",
    "PARAM_PIPELINE_RUN_UID": "pipeline_run_uid",
    "PARAM_OCI_STORAGE": "oci_storage",
    "PARAM_DESTINATION_ARTIFACT_STORAGE_DOMAIN": "destination_artifact_storage_domain",
    "PARAM_USE_TARGET_ARCH_REPO": "use_target_arch_repo",
    "PARAM_CREATE_OCI_ARTIFACT": "create_oci_artifact",
    "PARAM_OCI_ARTIFACT_EXPIRES_AFTER": "oci_artifact_expires_after",
    "PARAM_SIGNING_REPO": "signing_repo",
    "PARAM_SIGNING_REVISION": "signing_revision",
}

DATA = {
    "signOptions": {
        "signKeytab": {
            "principal": "test-principal@REDHAT.COM",
            "name": "test-keytab",
            "secret": "test-keytab-secret",
        },
        "signKeyAlias": {"key": "devkey1"},
    }
}

ARTIFACTS_JSON = {
    "artifacts": {
        "test-rpm-1.0-1.el9.x86_64.rpm": {
            "labels": {"arch": "x86_64"},
            "url": "https://pulp.example.com/x86_64/test-rpm-1.0-1.el9.x86_64.rpm",
            "sha256": "abc123",
        },
    },
    "distributions": {"x86_64": "https://pulp.example.com/x86_64/"},
}


@pytest.fixture
def workflow_args(tmp_path: Path) -> dict[str, Any]:
    """Create release inputs and return managed workflow arguments."""
    inputs = tmp_path / "release" / "pipeline-uid"
    inputs.mkdir(parents=True)
    (inputs / "data.json").write_text(json.dumps(DATA), encoding="utf-8")
    (inputs / "artifacts.json").write_text(json.dumps(ARTIFACTS_JSON), encoding="utf-8")
    return {
        "data_dir": tmp_path / "release",
        "data_path": "pipeline-uid/data.json",
        "artifacts_json_path": "pipeline-uid/artifacts.json",
        "requester": "testuser-rpm",
        "request_timeout": "1800",
        "pipeline_timeout": "0h30m0s",
        "task_timeout": "0h25m0s",
        "pipeline_run_uid": "pipeline-uid",
        "oci_storage": "quay.io/org/ta",
        "destination_artifact_storage_domain": "rok-storage",
        "use_target_arch_repo": "false",
        "create_oci_artifact": "false",
        "oci_artifact_expires_after": "1d",
        "signing_repo": "https://gitlab.cee.redhat.com/signing/signing.git",
        "signing_revision": "main",
        "result_path": tmp_path / "release" / "pipeline-uid" / "result.txt",
    }


@pytest.fixture
def request_mocks() -> Iterator[tuple[MagicMock, MagicMock]]:
    """Mock cluster operations while retaining the real request helper constants."""
    with (
        patch(
            f"{IMPLEMENTATION}.internal_request.create", return_value="rpm-sign-123"
        ) as create,
        patch(
            f"{IMPLEMENTATION}.internal_request.fetch_results",
            return_value={
                "signed_rpms_oci_artifact": "oci://registry.example.com/signed@sha256:abc"
            },
        ) as fetch_results,
    ):
        yield create, fetch_results


@pytest.fixture
def task_environment(
    monkeypatch: pytest.MonkeyPatch, workflow_args: dict[str, Any]
) -> dict[str, str]:
    """Inject the workflow arguments through the Tekton environment contract."""
    env = {name: str(workflow_args[arg]) for name, arg in ENV_ARGS.items()}
    env["RESULT_SIGNED_RPMS_OCI_ARTIFACTS"] = str(workflow_args["result_path"])
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return env


def test_build_sign_request_params_defaults() -> None:
    """Fill in defaults for optional sign options and non-default service account."""
    params, service_account = build_sign_request_params(
        DATA,
        ARTIFACTS_JSON,
        oci_storage="quay.io/org/ta",
        destination_artifact_storage_domain="rok-storage",
        use_target_arch_repo="false",
        create_oci_artifact="false",
        oci_artifact_expires_after="1d",
        requester="testuser-rpm",
        signing_repo="https://gitlab.cee.redhat.com/signing/signing.git",
        signing_revision="main",
    )

    assert service_account == "release-service-account"
    assert params["kerberos_principal"] == "test-principal@REDHAT.COM"
    assert params["kerberos_keytab"] == "test-keytab"
    assert params["kerberos_keytab_secret"] == "test-keytab-secret"
    assert params["signing_alias"] == "devkey1"
    assert params["artifact_storage_secret"] == "signing-artifact-storage-preprod"
    assert params["skip_upload_to_pulp"] == "false"
    assert params["force"] == "false"
    assert params["taskGitUrl"] == "https://gitlab.cee.redhat.com/signing/signing.git"
    assert params["taskGitRevision"] == "main"

    decoded = json.loads(gzip.decompress(base64.standard_b64decode(params["artifact_json"])))
    assert decoded == ARTIFACTS_JSON


@pytest.mark.parametrize(
    ("skip_upload_to_pulp", "expected"),
    [(True, "true"), (False, "false"), ("true", "true"), ("false", "false")],
)
def test_build_sign_request_params_skip_upload_to_pulp(
    skip_upload_to_pulp: bool | str, expected: str
) -> None:
    """Render skipUploadToPulp as a lowercase string regardless of input type."""
    data = {
        "signOptions": {
            **DATA["signOptions"],
            "skipUploadToPulp": skip_upload_to_pulp,
        }
    }

    params, _ = build_sign_request_params(
        data,
        ARTIFACTS_JSON,
        oci_storage="quay.io/org/ta",
        destination_artifact_storage_domain="rok-storage",
        use_target_arch_repo="false",
        create_oci_artifact="false",
        oci_artifact_expires_after="1d",
        requester="testuser-rpm",
        signing_repo="https://gitlab.cee.redhat.com/signing/signing.git",
        signing_revision="main",
    )

    assert params["skip_upload_to_pulp"] == expected


def test_build_sign_request_params_custom_overrides() -> None:
    """Honor custom keytab name, storage secret, and service account overrides."""
    data = {
        "signOptions": {
            "signKeytab": {
                "principal": "svc@REALM",
                "name": "custom-keytab",
                "secret": "custom-secret",
            },
            "signKeyAlias": {"key": "key2"},
            "artifactStorageSecret": "custom-storage-secret",
            "signPipelineServiceAccount": "custom-sa",
        }
    }

    params, service_account = build_sign_request_params(
        data,
        ARTIFACTS_JSON,
        oci_storage="quay.io/org/ta",
        destination_artifact_storage_domain="rok-storage",
        use_target_arch_repo="true",
        create_oci_artifact="true",
        oci_artifact_expires_after="2d",
        requester="tester",
        signing_repo="https://gitlab.cee.redhat.com/signing/signing.git",
        signing_revision="main",
    )

    assert service_account == "custom-sa"
    assert params["kerberos_keytab"] == "custom-keytab"
    assert params["artifact_storage_secret"] == "custom-storage-secret"
    assert params["use_target_arch_repo"] == "true"
    assert params["create_oci_artifact"] == "true"
    assert params["oci_artifact_expires_after"] == "2d"


def test_run_success(
    workflow_args: dict[str, Any], request_mocks: tuple[MagicMock, MagicMock]
) -> None:
    """Create the signing InternalRequest and write the signed OCI artifact result."""
    create, fetch_results = request_mocks

    run(**workflow_args)

    create.assert_called_once()
    assert create.call_args.args[0] == "direct-rpm-signing"
    assert create.call_args.kwargs["labels"] == {
        internal_request.PIPELINERUN_UID_LABEL: "pipeline-uid",
    }
    assert create.call_args.kwargs["sync"] is True
    assert create.call_args.kwargs["timeout"] == 1800
    assert create.call_args.kwargs["service_account"] == "release-service-account"
    assert create.call_args.kwargs["pipeline_timeout"] == "0h30m0s"
    assert create.call_args.kwargs["task_timeout"] == "0h25m0s"
    fetch_results.assert_called_once_with("rpm-sign-123")

    assert (
        workflow_args["result_path"].read_text()
        == "oci://registry.example.com/signed@sha256:abc"
    )


@pytest.mark.parametrize("path_arg", ["data_path", "artifacts_json_path"])
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


def test_run_request_wait_failure(
    workflow_args: dict[str, Any], request_mocks: tuple[MagicMock, MagicMock]
) -> None:
    """Chain InternalRequest failures instead of fetching incomplete results."""
    create, fetch_results = request_mocks
    error = internal_request.InternalRequestWaitError(
        "At least one InternalRequest failed", internal_request.EXIT_FAILED
    )
    create.side_effect = error

    with pytest.raises(RuntimeError, match="At least one InternalRequest failed") as exc_info:
        run(**workflow_args)

    assert exc_info.value.__cause__ is error
    fetch_results.assert_not_called()


@pytest.mark.parametrize("results", [{}, {"signed_rpms_oci_artifact": ""}])
def test_run_missing_signed_artifact(
    workflow_args: dict[str, Any],
    request_mocks: tuple[MagicMock, MagicMock],
    results: dict[str, Any],
) -> None:
    """Reject a completed request whose results omit the signed OCI artifact."""
    _, fetch_results = request_mocks
    fetch_results.return_value = results

    with pytest.raises(RuntimeError, match="signed_rpms_oci_artifact not found"):
        run(**workflow_args)

    assert not workflow_args["result_path"].exists()


def test_main(
    task_environment: dict[str, str], request_mocks: tuple[MagicMock, MagicMock]
) -> None:
    """Read Tekton configuration end to end and write the result file."""
    create, fetch_results = request_mocks

    assert main() == 0

    assert create.call_args.kwargs["params"]["requester"] == "testuser-rpm"
    assert create.call_args.kwargs["params"]["oci_storage"] == "quay.io/org/ta"
    fetch_results.assert_called_once_with("rpm-sign-123")
    result_path = Path(task_environment["RESULT_SIGNED_RPMS_OCI_ARTIFACTS"])
    assert result_path.read_text() == "oci://registry.example.com/signed@sha256:abc"


@pytest.mark.parametrize("env_name", list(ENV_ARGS) + ["RESULT_SIGNED_RPMS_OCI_ARTIFACTS"])
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


@pytest.mark.parametrize("filename", ["rh_sign_rpm.py", "__main__.py"])
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
    create.assert_called_once()
    fetch_results.assert_called_once_with("rpm-sign-123")


def test_import_entry_point(request_mocks: tuple[MagicMock, MagicMock]) -> None:
    """Import the package entry point without starting the workflow."""
    entry_point = importlib.import_module(f"{TASK}.__main__")

    assert entry_point.main is main
