"""Tests for the managed request_advisory_creation task script."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any
from unittest import mock

import advisory_data
import pytest

_MODULE_PATH = Path(__file__).with_name("request_advisory_creation.py")
_SPEC = importlib.util.spec_from_file_location("request_advisory_creation", _MODULE_PATH)
assert _SPEC and _SPEC.loader
request_advisory_creation = importlib.util.module_from_spec(_SPEC)
sys.modules["request_advisory_creation"] = request_advisory_creation
_SPEC.loader.exec_module(request_advisory_creation)


def _task_params(tmp_path: Path) -> request_advisory_creation.TaskParams:
    """Build minimal task params for orchestration tests."""
    return request_advisory_creation.TaskParams(
        data_dir=tmp_path,
        data_path=Path("data.json"),
        snapshot_path=Path("snapshot.json"),
        release_plan_admission_path=Path("rpa.json"),
        results_dir_path=Path("results"),
        environment="stage",
        request_pipeline="create-advisory",
        synchronously="true",
        pipeline_run_uid="uid-1",
        task_git_url="https://example.test/catalog.git",
        task_git_revision="main",
        task_name="create-advisory",
        checksum_map="oci:checksum",
        dockerconfig_path=tmp_path / "missing-dockerconfig",
        advisory_url_result=tmp_path / "advisory_url",
        advisory_internal_url_result=tmp_path / "advisory_internal_url",
    )


def _write_release_files(tmp_path: Path, *, advisory_type: str | None = "RHBA") -> None:
    """Write snapshot, RPA, and data files for a happy-path run."""
    (tmp_path / "snapshot.json").write_text(
        json.dumps({"componentGroup": "my-group"}) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "rpa.json").write_text(
        json.dumps({"spec": {"origin": "my-origin"}}) + "\n",
        encoding="utf-8",
    )
    release_notes: dict[str, Any] = {
        "type": advisory_type,
        "content": {"images": [{"cves": {"fixed": ["CVE-2024-1"]}}]},
    }
    if advisory_type is None:
        release_notes.pop("type")
    (tmp_path / "data.json").write_text(
        json.dumps(
            {
                "sign": {"configMapName": "signing-config"},
                "mapping": {"components": [{"contentType": "image"}]},
                "releaseNotes": release_notes,
            },
        )
        + "\n",
        encoding="utf-8",
    )


def test_prepare_advisory_data_defaults_missing_type_to_rhba() -> None:
    """Default advisory type to RHBA when releaseNotes.type is null."""
    advisory = {"content": {"images": []}}
    out = request_advisory_creation._prepare_advisory_data(
        advisory,
        ".content.images",
    )
    assert out["type"] == "RHBA"


def test_prepare_advisory_data_rejects_rhsa_without_cves() -> None:
    """Reject RHSA advisories that do not list fixed CVEs."""
    with pytest.raises(ValueError, match="no fixed CVEs were listed"):
        request_advisory_creation._prepare_advisory_data(
            {"type": "RHSA", "content": {"images": []}},
            ".content.images",
        )


def test_prepare_advisory_data_accepts_rhsa_with_fixed_cve_map() -> None:
    """Accept RHSA when fixed CVEs are stored as a map (populate-release-notes format)."""
    advisory = {
        "type": "RHSA",
        "content": {
            "images": [
                {
                    "cves": {
                        "fixed": {
                            "CVE-2024-1234": {"packages": ["pkg:example/foo@1.0"]},
                        },
                    },
                },
            ],
        },
    }
    out = request_advisory_creation._prepare_advisory_data(advisory, ".content.images")
    assert out["type"] == "RHSA"


def test_resolve_content_type_uses_github_generic_default() -> None:
    """Treat GitHub-only data as generic content."""
    assert request_advisory_creation._resolve_content_type({"github": {}}) == "generic"


def test_run_request_advisory_creation_calls_update_purl_before_internal_request(
    tmp_path: Path,
) -> None:
    """Run PURL updates before creating the InternalRequest."""
    _write_release_files(tmp_path, advisory_type=None)
    params = _task_params(tmp_path)
    calls: list[str] = []

    with (
        mock.patch.object(
            request_advisory_creation.release_notes_purl,
            "update_artifact_purls",
            side_effect=lambda *_a, **_k: calls.append("purl"),
        ),
        mock.patch.object(
            request_advisory_creation,
            "_create_internal_request",
            return_value="create-advisory-abc",
        ) as run_ir,
        mock.patch.object(
            request_advisory_creation.internal_request,
            "fetch_results",
            return_value={
                "result": "Success",
                "advisory_url": "url",
                "advisory_internal_url": "internal",
            },
        ),
    ):
        request_advisory_creation.run_request_advisory_creation(params)

    assert calls == ["purl"]
    run_ir.assert_called_once()


def test_run_request_advisory_creation_happy_path(tmp_path: Path) -> None:
    """Submit an InternalRequest and write advisory URLs on success."""
    _write_release_files(tmp_path, advisory_type=None)
    params = _task_params(tmp_path)
    ir_results = {
        "result": "Success",
        "advisory_url": "https://access.redhat.com/errata/RHBA-2025:1111",
        "advisory_internal_url": "https://gitlab.example/advisory",
    }

    with (
        mock.patch.object(
            request_advisory_creation.release_notes_purl, "update_artifact_purls"
        ),
        mock.patch.object(
            request_advisory_creation,
            "_create_internal_request",
            return_value="create-advisory-abc",
        ),
        mock.patch.object(
            request_advisory_creation.internal_request,
            "fetch_results",
            return_value=ir_results,
        ),
    ):
        request_advisory_creation.run_request_advisory_creation(params)

    url_text = params.advisory_url_result.read_text(encoding="utf-8")
    assert url_text == ir_results["advisory_url"]


def test_encode_advisory_json_matches_decoder() -> None:
    """Round-trip advisory JSON through gzip/base64 encoding."""
    payload = {"type": "RHBA", "content": {"artifacts": []}}
    encoded = advisory_data.encode_advisory_param(payload)
    assert advisory_data.decode_advisory_param(encoded) == payload


def test_create_internal_request_calls_python_helper() -> None:
    """Submit the InternalRequest through the Python internal_request helper."""
    params = request_advisory_creation.TaskParams(
        data_dir=Path("/tmp"),
        data_path=Path("data.json"),
        snapshot_path=Path("snap.json"),
        release_plan_admission_path=Path("rpa.json"),
        results_dir_path=Path("results"),
        environment="stage",
        request_pipeline="create-advisory",
        synchronously="true",
        pipeline_run_uid="uid-42",
        task_git_url="https://example.test/catalog.git",
        task_git_revision="main",
        task_name="create-advisory",
        checksum_map="",
        dockerconfig_path=Path("/tmp/dockerconfig"),
        advisory_url_result=Path("/tmp/advisory_url"),
        advisory_internal_url_result=Path("/tmp/advisory_internal_url"),
    )

    with mock.patch.object(
        request_advisory_creation.internal_request, "create", return_value="ir-abc"
    ) as ir:
        name = request_advisory_creation._create_internal_request(
            params,
            component_group="grp",
            origin="origin-ws",
            advisory_json="encoded",
            config_map_name="cm",
            content_type="image",
            advisory_secret_name="adv-secret",
            errata_secret_name="errata-secret",
        )

    assert name == "ir-abc"
    ir.assert_called_once_with(
        "create-advisory",
        params={
            "componentGroup": "grp",
            "origin": "origin-ws",
            "advisory_json": "encoded",
            "config_map_name": "cm",
            "contentType": "image",
            "advisory_secret_name": "adv-secret",
            "errata_secret_name": "errata-secret",
            "taskGitUrl": "https://example.test/catalog.git",
            "taskGitRevision": "main",
        },
        labels={
            "internal-services.appstudio.openshift.io/pipelinerun-uid": "uid-42",
        },
        sync=True,
        timeout=request_advisory_creation._IR_WAIT_TIMEOUT_SECONDS,
        pipeline_timeout=request_advisory_creation._IR_PIPELINE_TIMEOUT,
        task_timeout=request_advisory_creation._IR_TASK_TIMEOUT,
        finally_timeout=request_advisory_creation._IR_FINALLY_TIMEOUT,
    )


def test_main_success(tmp_path: Path) -> None:
    """Exit 0 when orchestration completes."""
    with (
        mock.patch.object(
            request_advisory_creation,
            "_params_from_env",
            return_value=_task_params(tmp_path),
        ),
        mock.patch.object(request_advisory_creation, "run_request_advisory_creation"),
    ):
        assert request_advisory_creation.main() == 0


def test_resolve_content_type_defaults_to_image() -> None:
    """Default to image when no mapping or GitHub content is present."""
    assert request_advisory_creation._resolve_content_type({}) == "image"


def test_content_path_for_type_uses_artifacts_for_binary() -> None:
    """Binary releases store CVEs under content.artifacts."""
    assert request_advisory_creation._content_path_for_type("binary") == ".content.artifacts"


def test_count_fixed_cves_counts_list_fixed_cves() -> None:
    """Count fixed CVEs stored as a list."""
    advisory = {
        "content": {
            "images": [{"cves": {"fixed": ["CVE-1", "CVE-2"]}}],
        },
    }
    assert request_advisory_creation._count_fixed_cves(advisory, ".content.images") == 2


def test_count_fixed_cves_skips_invalid_rows() -> None:
    """Ignore non-dict content rows and non-dict cves blocks."""
    advisory = {
        "content": {
            "images": ["invalid", {"cves": "invalid"}],
        },
    }
    assert request_advisory_creation._count_fixed_cves(advisory, ".content.images") == 0


def test_prepare_advisory_data_rejects_live_id_without_flag() -> None:
    """Reject live_id unless allow_custom_live_id is true."""
    with pytest.raises(ValueError, match="live id is only allowed"):
        request_advisory_creation._prepare_advisory_data(
            {
                "live_id": "RHBA-1",
                "type": "RHBA",
                "content": {"images": []},
            },
            ".content.images",
        )


def test_prepare_advisory_data_rejects_invalid_type() -> None:
    """Reject advisory types outside RHSA, RHBA, and RHEA."""
    with pytest.raises(ValueError, match="advisory type must be one of"):
        request_advisory_creation._prepare_advisory_data(
            {"type": "INVALID", "content": {"images": []}},
            ".content.images",
        )


def test_resolve_secret_names_image_uses_staging_secrets() -> None:
    """Image releases use staging secrets when environment is stage."""
    assert request_advisory_creation._resolve_secret_names(
        "image",
        environment="staging",
        data={},
    ) == (
        advisory_data.ADVISORY_SECRET_STAGE,
        advisory_data.ERRATA_SECRET_STAGE,
    )


def test_resolve_secret_names_image_uses_prod_secrets() -> None:
    """Image releases use production secrets for non-stage environments."""
    assert request_advisory_creation._resolve_secret_names(
        "image",
        environment="production",
        data={},
    ) == (
        advisory_data.ADVISORY_SECRET_PROD,
        advisory_data.ERRATA_SECRET_PROD,
    )


def test_resolve_secret_names_binary_uses_intention() -> None:
    """Binary releases select secrets from data.intention."""
    prod = request_advisory_creation._resolve_secret_names(
        "binary",
        environment="",
        data={"intention": "production"},
    )
    staging = request_advisory_creation._resolve_secret_names(
        "binary",
        environment="",
        data={"intention": "staging"},
    )
    assert prod == (
        advisory_data.ADVISORY_SECRET_PROD,
        advisory_data.ERRATA_SECRET_PROD,
    )
    assert staging == (
        advisory_data.ADVISORY_SECRET_STAGE,
        advisory_data.ERRATA_SECRET_STAGE,
    )


def test_resolve_secret_names_rejects_unsupported_intention() -> None:
    """Reject binary/generic releases with unknown intention values."""
    with pytest.raises(ValueError, match="unsupported intention"):
        request_advisory_creation._resolve_secret_names(
            "binary",
            environment="",
            data={"intention": "dev"},
        )


def test_sync_from_param_parses_false() -> None:
    """Parse synchronously=false as a boolean false."""
    assert request_advisory_creation._sync_from_param("false") is False


def test_create_internal_request_wraps_wait_error(tmp_path: Path) -> None:
    """Surface InternalRequest wait failures as RuntimeError."""
    params = _task_params(tmp_path)
    with (
        mock.patch.object(
            request_advisory_creation.internal_request,
            "create",
            side_effect=request_advisory_creation.internal_request.InternalRequestWaitError(
                "timed out",
                1,
            ),
        ),
        pytest.raises(RuntimeError, match="timed out"),
    ):
        request_advisory_creation._create_internal_request(
            params,
            component_group="grp",
            origin="origin-ws",
            advisory_json="encoded",
            config_map_name="cm",
            content_type="image",
            advisory_secret_name="adv-secret",
            errata_secret_name="errata-secret",
        )


def test_write_task_results_raises_on_failure(tmp_path: Path) -> None:
    """Fail the step when InternalRequest results are not Success."""
    params = _task_params(tmp_path)
    params.advisory_internal_url_result.write_text("stale-internal", encoding="utf-8")
    with pytest.raises(RuntimeError, match="advisory creation failed"):
        request_advisory_creation._write_task_results(
            params,
            {"result": "Failed", "message": "boom"},
        )
    assert params.advisory_url_result.read_text(encoding="utf-8") == ""
    assert params.advisory_internal_url_result.read_text(encoding="utf-8") == ""


def test_ir_wait_timeout_covers_pipeline_budget() -> None:
    """Script wait timeout must cover pipeline budget plus spawn overhead."""
    import internal_request

    pipeline_seconds = internal_request.duration_to_seconds(
        request_advisory_creation._IR_PIPELINE_TIMEOUT,
    )
    assert request_advisory_creation._IR_WAIT_TIMEOUT_SECONDS == (
        pipeline_seconds + internal_request.SPAWN_OVERHEAD_SECONDS
    )


def test_run_request_advisory_creation_rejects_release_notes_not_dict(
    tmp_path: Path,
) -> None:
    """Require releaseNotes to be a JSON object."""
    _write_release_files(tmp_path)
    data = json.loads((tmp_path / "data.json").read_text(encoding="utf-8"))
    data["releaseNotes"] = "invalid"
    (tmp_path / "data.json").write_text(json.dumps(data) + "\n", encoding="utf-8")
    params = _task_params(tmp_path)
    with (
        mock.patch.object(
            request_advisory_creation.release_notes_purl, "update_artifact_purls"
        ),
        pytest.raises(TypeError, match="releaseNotes must be a JSON object"),
    ):
        request_advisory_creation.run_request_advisory_creation(params)


def test_params_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Build TaskParams from Tekton-style environment variables."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("PARAM_RELEASE_PLAN_ADMISSION_PATH", "rpa.json")
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
    monkeypatch.setenv("PARAM_ENVIRONMENT", "stage")
    monkeypatch.setenv("PARAM_REQUEST", "create-advisory")
    monkeypatch.setenv("PARAM_SYNCHRONOUSLY", "false")
    monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "uid-99")
    monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://example.test/catalog.git")
    monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "main")
    monkeypatch.setenv("PARAM_TASK_NAME", "create-advisory")
    monkeypatch.setenv("PARAM_CHECKSUM_MAP", "oci:checksum")
    monkeypatch.setenv("PARAM_TA_DOCKERCONFIG_PATH", str(tmp_path / "dockerconfig"))
    monkeypatch.setenv("RESULT_ADVISORY_URL", str(tmp_path / "advisory_url"))
    monkeypatch.setenv(
        "RESULT_ADVISORY_INTERNAL_URL",
        str(tmp_path / "advisory_internal_url"),
    )

    params = request_advisory_creation._params_from_env()

    assert params.data_dir == tmp_path / "data"
    assert params.environment == "stage"
    assert params.synchronously == "false"
    assert params.dockerconfig_path == tmp_path / "dockerconfig"
    assert params.advisory_url_result == tmp_path / "advisory_url"
