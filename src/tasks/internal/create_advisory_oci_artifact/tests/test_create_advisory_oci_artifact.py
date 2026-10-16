"""Tests for create_advisory_oci_artifact."""

from __future__ import annotations

import gzip
import hashlib
import runpy
import subprocess
import tarfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
from release_service_utils.tasks.internal.create_advisory_oci_artifact.create_advisory_oci_artifact import (  # noqa: E501
    ARCHIVE_NAME,
    DEFAULT_DOCKERCONFIG,
    DOCKERCONFIG_ENV,
    PROG,
    STORE,
    _write_gzip_tar,
    main,
    run,
)

TASK = (
    "release_service_utils.tasks.internal"
    ".create_advisory_oci_artifact.create_advisory_oci_artifact"
)
ADVISORY_YAML = "---\nproduct: mock\n"
ADVISORY_URL = "https://example.com/advisory.yaml"


def _result_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """Set Tekton result and IR name env vars; return the result paths."""
    paths = {
        "RESULT_RESULT": tmp_path / "result",
        "RESULT_ADVISORY_OCI_ARTIFACT": tmp_path / "artifact",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME": tmp_path / "internal_pr",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME": tmp_path / "internal_task",
    }
    for key, path in paths.items():
        monkeypatch.setenv(key, str(path))
    monkeypatch.setenv("INTERNAL_REQUEST_PIPELINE_RUN_NAME", "pr-1")
    monkeypatch.setenv("INTERNAL_REQUEST_TASK_RUN_NAME", "tr-1")
    monkeypatch.setenv("PARAM_ADVISORY_URL", ADVISORY_URL)
    return paths


def test_write_gzip_tar_is_extractable(tmp_path: Path) -> None:
    """The archive is a gzip tar whose members match the source directory."""
    source = tmp_path / "src"
    source.mkdir()
    (source / "advisory.yaml").write_text(ADVISORY_YAML, encoding="utf-8")
    archive = tmp_path / "advisory"
    _write_gzip_tar(source, archive)

    with gzip.open(archive, "rb") as gz, tarfile.open(fileobj=gz, mode="r") as tar:
        names = tar.getnames()
    assert any(name.endswith("advisory.yaml") for name in names)


def test_run_returns_file_digest_uri(tmp_path: Path) -> None:
    """The OCI URI uses the tar-file SHA-256, not the oras manifest digest."""
    captured: dict[str, object] = {}

    def fake_push(
        tag: str,
        directory: Path,
        subdirectory: str,
        component_name: str,
        *,
        extra_args: list[str] | None = None,
    ) -> str:
        captured["tag"] = tag
        captured["directory"] = directory
        captured["subdirectory"] = subdirectory
        captured["component_name"] = component_name
        captured["extra_args"] = extra_args
        captured["digest"] = (directory / subdirectory).read_bytes()
        return "sha256:manifest-digest-must-not-be-used"

    auth = tmp_path / "auth.json"
    auth.write_text("{}", encoding="utf-8")

    @contextmanager
    def fake_registry_auth(reference: str, *, stderr_path: Path | None = None):
        assert reference == STORE
        yield auth

    with (
        patch(f"{TASK}.http_client.get_text", return_value=ADVISORY_YAML) as mock_get,
        patch(f"{TASK}.registry_auth_file", fake_registry_auth),
        patch(f"{TASK}.oras_push", side_effect=fake_push) as mock_push,
    ):
        uri = run(ADVISORY_URL)

    mock_get.assert_called_once_with(ADVISORY_URL)
    mock_push.assert_called_once()
    assert captured["tag"] == STORE
    assert captured["subdirectory"] == ARCHIVE_NAME
    assert captured["component_name"] == "advisory"
    assert captured["extra_args"] == ["--registry-config", str(auth)]
    expected = hashlib.sha256(captured["digest"]).hexdigest()
    assert uri == f"oci:{STORE}@sha256:{expected}"


def test_run_propagates_http_error() -> None:
    """HTTP download failures raise from run()."""
    error = requests.HTTPError("404")
    with (
        patch(f"{TASK}.http_client.get_text", side_effect=error),
        pytest.raises(requests.HTTPError),
    ):
        run(ADVISORY_URL)


def test_run_propagates_oras_push_error(tmp_path: Path) -> None:
    """oras_push failures raise from run() after the archive is built."""
    auth = tmp_path / "auth.json"
    auth.write_text("{}", encoding="utf-8")

    @contextmanager
    def fake_registry_auth(reference: str, *, stderr_path: Path | None = None):
        yield auth

    with (
        patch(f"{TASK}.http_client.get_text", return_value=ADVISORY_YAML),
        patch(f"{TASK}.registry_auth_file", fake_registry_auth),
        patch(f"{TASK}.oras_push", side_effect=RuntimeError("push failed")),
        pytest.raises(RuntimeError, match="push failed"),
    ):
        run(ADVISORY_URL)


def test_run_propagates_select_oci_auth_error() -> None:
    """select-oci-auth failures raise from run()."""
    with (
        patch(f"{TASK}.http_client.get_text", return_value=ADVISORY_YAML),
        patch(
            f"{TASK}.registry_auth_file",
            side_effect=subprocess.CalledProcessError(1, "select-oci-auth"),
        ),
        pytest.raises(subprocess.CalledProcessError),
    ):
        run(ADVISORY_URL)


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() writes Success, the URI, and IR names, then returns 0."""
    paths = _result_env(tmp_path, monkeypatch)
    uri = f"oci:{STORE}@sha256:abc"

    with (
        patch(f"{TASK}.authentication.setup_ca_cert") as mock_ca,
        patch(f"{TASK}.authentication.setup_docker_config") as mock_docker,
        patch(f"{TASK}.run", return_value=uri) as mock_run,
    ):
        assert main() == 0

    mock_ca.assert_called_once_with()
    mock_docker.assert_called_once_with(Path(DEFAULT_DOCKERCONFIG), optional=True)
    mock_run.assert_called_once_with(ADVISORY_URL)
    assert paths["RESULT_RESULT"].read_text(encoding="utf-8") == "Success"
    assert paths["RESULT_ADVISORY_OCI_ARTIFACT"].read_text(encoding="utf-8") == uri
    assert (
        paths["RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME"].read_text(encoding="utf-8")
        == "pr-1"
    )
    assert paths["RESULT_INTERNAL_REQUEST_TASK_RUN_NAME"].read_text(encoding="utf-8") == "tr-1"


def test_main_download_failure_writes_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HTTP failures are written to RESULT_RESULT; the process still exits 0."""
    paths = _result_env(tmp_path, monkeypatch)
    with (
        patch(f"{TASK}.authentication.setup_ca_cert"),
        patch(f"{TASK}.authentication.setup_docker_config"),
        patch(
            f"{TASK}.run",
            side_effect=requests.HTTPError("Failed to download"),
        ),
    ):
        assert main() == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert PROG in text
    assert "Failed to download" in text
    assert paths["RESULT_ADVISORY_OCI_ARTIFACT"].read_text(encoding="utf-8") == ""
    assert (
        paths["RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME"].read_text(encoding="utf-8")
        == "pr-1"
    )


def test_main_oras_failure_writes_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """oras_push failures are written to RESULT_RESULT; the process still exits 0."""
    paths = _result_env(tmp_path, monkeypatch)
    with (
        patch(f"{TASK}.authentication.setup_ca_cert"),
        patch(f"{TASK}.authentication.setup_docker_config"),
        patch(f"{TASK}.run", side_effect=RuntimeError("oras push failed")),
    ):
        assert main() == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "oras push failed" in text
    assert paths["RESULT_ADVISORY_OCI_ARTIFACT"].read_text(encoding="utf-8") == ""


def test_main_missing_dockerconfig_still_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing optional dockerconfig does not prevent a successful run."""
    paths = _result_env(tmp_path, monkeypatch)
    missing = tmp_path / "missing" / ".dockerconfigjson"
    monkeypatch.setenv(DOCKERCONFIG_ENV, str(missing))
    uri = f"oci:{STORE}@sha256:abc"

    with (
        patch(f"{TASK}.authentication.setup_ca_cert"),
        patch(f"{TASK}.run", return_value=uri),
    ):
        assert main() == 0
    assert paths["RESULT_RESULT"].read_text(encoding="utf-8") == "Success"


def test_main_empty_dockerconfig_still_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty optional dockerconfig does not prevent a successful run."""
    paths = _result_env(tmp_path, monkeypatch)
    empty = tmp_path / ".dockerconfigjson"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv(DOCKERCONFIG_ENV, str(empty))
    uri = f"oci:{STORE}@sha256:abc"

    with (
        patch(f"{TASK}.authentication.setup_ca_cert"),
        patch(f"{TASK}.run", return_value=uri),
    ):
        assert main() == 0
    assert paths["RESULT_RESULT"].read_text(encoding="utf-8") == "Success"


def test_main_missing_result_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when result path env vars are unset."""
    monkeypatch.delenv("RESULT_RESULT", raising=False)
    monkeypatch.delenv("RESULT_ADVISORY_OCI_ARTIFACT", raising=False)
    monkeypatch.delenv("RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME", raising=False)
    monkeypatch.delenv("RESULT_INTERNAL_REQUEST_TASK_RUN_NAME", raising=False)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1


@pytest.mark.parametrize("value", [None, "", "  "])
def test_main_missing_or_empty_advisory_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    """Unset or blank PARAM_ADVISORY_URL writes a failure result and still exits 0."""
    paths = _result_env(tmp_path, monkeypatch)
    if value is None:
        monkeypatch.delenv("PARAM_ADVISORY_URL", raising=False)
    else:
        monkeypatch.setenv("PARAM_ADVISORY_URL", value)

    assert main() == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert PROG in text
    assert "PARAM_ADVISORY_URL must be set" in text
    assert paths["RESULT_ADVISORY_OCI_ARTIFACT"].read_text(encoding="utf-8") == ""
    assert (
        paths["RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME"].read_text(encoding="utf-8")
        == "pr-1"
    )


def test_dunder_main_invokes_main() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.internal.create_advisory_oci_artifact"
    with (
        patch(f"{TASK}.main", return_value=0) as mock_main,
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()
