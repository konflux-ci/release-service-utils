"""Test prepare_fbc_parameters behavior."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest import mock

import pytest

import release_service_utils.tasks.managed.prepare_fbc_parameters as prepare_fbc_parameters
from release_service_utils.helpers import tekton

TASK = "release_service_utils.tasks.managed.prepare_fbc_parameters"


def _write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _make_snapshot(components: list[dict]) -> dict:
    return {"application": "test-app", "components": components}


def _make_data(
    *,
    hotfix: bool = False,
    pre_ga: bool = False,
    staged_index: bool = False,
    allowed_packages: list[str] | None = None,
) -> dict:
    return {
        "fbc": {
            "hotfix": hotfix,
            "preGA": pre_ga,
            "stagedIndex": staged_index,
            "allowedPackages": allowed_packages or [],
        }
    }


def _catalog_entries(packages: list[str], bundles: list[str] | None = None) -> list[dict]:
    entries = [{"schema": "olm.package", "name": p} for p in packages]
    for b in bundles or []:
        entries.append({"schema": "olm.bundle", "image": f"{b}@sha256:abc"})
    return entries


# --- detect_release_mode ---


def test_detect_release_mode_standard() -> None:
    """Return standard when no special flags are set."""
    assert prepare_fbc_parameters.detect_release_mode(_make_data()) == "standard"


def test_detect_release_mode_hotfix() -> None:
    """Return hotfix when hotfix flag is set."""
    assert prepare_fbc_parameters.detect_release_mode(_make_data(hotfix=True)) == "hotfix"


def test_detect_release_mode_prega() -> None:
    """Return preGA when pre_ga flag is set."""
    assert prepare_fbc_parameters.detect_release_mode(_make_data(pre_ga=True)) == "preGA"


def test_detect_release_mode_staged() -> None:
    """Return stagedIndex when staged_index flag is set."""
    assert (
        prepare_fbc_parameters.detect_release_mode(_make_data(staged_index=True))
        == "stagedIndex"
    )


def test_detect_release_mode_multiple_raises() -> None:
    """Raise ValueError when multiple release modes are set."""
    with pytest.raises(ValueError, match="Multiple release modes"):
        prepare_fbc_parameters.detect_release_mode(_make_data(hotfix=True, pre_ga=True))


# --- extract_packages ---


def test_extract_packages() -> None:
    """Return sorted unique package names from catalog entries."""
    entries = _catalog_entries(["beta", "alpha", "alpha"])
    assert prepare_fbc_parameters.extract_packages(entries) == [
        "alpha",
        "beta",
    ]


def test_extract_packages_empty() -> None:
    """Return empty list when no catalog entries are provided."""
    assert prepare_fbc_parameters.extract_packages([]) == []


# --- extract_bundle_images ---


def test_extract_bundle_images() -> None:
    """Return deduplicated bundle images preserving insertion order."""
    entries = _catalog_entries([], ["quay.io/a", "quay.io/b", "quay.io/a"])
    result = prepare_fbc_parameters.extract_bundle_images(entries)
    assert result == ["quay.io/a", "quay.io/b"]


# --- validate_allowed_packages ---


def test_validate_allowed_packages_all_allowed() -> None:
    """Return empty list when all packages are in the allowed set."""
    assert (
        prepare_fbc_parameters.validate_allowed_packages(
            ["a", "b"],
            ["a", "b", "c"],
        )
        == []
    )


def test_validate_allowed_packages_some_disallowed() -> None:
    """Return disallowed package names sorted."""
    result = prepare_fbc_parameters.validate_allowed_packages(
        ["a", "b", "c"],
        ["a"],
    )
    assert result == ["b", "c"]


# --- validate_no_duplicate_packages ---


def test_validate_no_duplicate_packages_no_duplicates() -> None:
    """Return empty list when no packages overlap across components."""
    ocp = {"v4.14": [0, 1]}
    pkgs = {0: ["pkg-a"], 1: ["pkg-b"]}
    snapshot = _make_snapshot(
        [
            {"name": "comp0"},
            {"name": "comp1"},
        ]
    )
    assert (
        prepare_fbc_parameters.validate_no_duplicate_packages(
            ocp,
            pkgs,
            snapshot,
        )
        == []
    )


def test_validate_no_duplicate_packages_with_duplicates() -> None:
    """Report duplicate package names shared across components."""
    ocp = {"v4.14": [0, 1]}
    pkgs = {0: ["pkg-a", "pkg-b"], 1: ["pkg-b"]}
    snapshot = _make_snapshot(
        [
            {"name": "comp0"},
            {"name": "comp1"},
        ]
    )
    errors = prepare_fbc_parameters.validate_no_duplicate_packages(
        ocp,
        pkgs,
        snapshot,
    )
    assert len(errors) == 1
    assert "pkg-b" in errors[0]


def test_validate_no_duplicate_packages_single_component() -> None:
    """Skip duplicate check when only one component targets a version."""
    ocp = {"v4.14": [0]}
    pkgs = {0: ["pkg-a"]}
    snapshot = _make_snapshot([{"name": "comp0"}])
    assert (
        prepare_fbc_parameters.validate_no_duplicate_packages(
            ocp,
            pkgs,
            snapshot,
        )
        == []
    )


# --- aggregate_opt_in ---


def test_aggregate_opt_in_all_true() -> None:
    """Return True when all components have fbcOptIn True."""
    results = [
        {"containerImage": "a", "fbcOptIn": True},
        {"containerImage": "b", "fbcOptIn": True},
    ]
    assert prepare_fbc_parameters.aggregate_opt_in(results) is True


def test_aggregate_opt_in_some_false() -> None:
    """Return False when any component has fbcOptIn False."""
    results = [
        {"containerImage": "a", "fbcOptIn": True},
        {"containerImage": "b", "fbcOptIn": False},
    ]
    assert prepare_fbc_parameters.aggregate_opt_in(results) is False


def test_aggregate_opt_in_missing_key() -> None:
    """Return False when fbcOptIn key is absent from a result."""
    results = [
        {"containerImage": "a", "fbcOptIn": True},
        {"containerImage": "b"},
    ]
    assert prepare_fbc_parameters.aggregate_opt_in(results) is False


def test_aggregate_opt_in_empty() -> None:
    """Return False when no opt-in results are provided."""
    assert prepare_fbc_parameters.aggregate_opt_in([]) is False


# --- compute_publishing_decisions ---


def test_publishing_decisions_staged() -> None:
    """Staged mode disables all publishing flags."""
    assert prepare_fbc_parameters.compute_publishing_decisions(
        "stagedIndex",
        True,
    ) == (False, False, False)


def test_publishing_decisions_hotfix() -> None:
    """Hotfix mode enables publish and sign but not overwrite."""
    assert prepare_fbc_parameters.compute_publishing_decisions(
        "hotfix",
        False,
    ) == (True, True, False)


def test_publishing_decisions_prega() -> None:
    """Pre-GA mode enables publish and sign but not overwrite."""
    assert prepare_fbc_parameters.compute_publishing_decisions(
        "preGA",
        False,
    ) == (True, True, False)


def test_publishing_decisions_standard_opt_in() -> None:
    """Standard mode with opt-in enables all publishing flags."""
    assert prepare_fbc_parameters.compute_publishing_decisions(
        "standard",
        True,
    ) == (True, True, True)


def test_publishing_decisions_standard_opt_out() -> None:
    """Standard mode without opt-in disables all publishing flags."""
    assert prepare_fbc_parameters.compute_publishing_decisions(
        "standard",
        False,
    ) == (False, False, False)


# --- select_iib_service_account ---


def test_iib_sa_prod() -> None:
    """Select prod service account when not staged."""
    assert (
        prepare_fbc_parameters.select_iib_service_account(False) == "iib-service-account-prod"
    )


def test_iib_sa_stage() -> None:
    """Select stage service account when staged."""
    assert (
        prepare_fbc_parameters.select_iib_service_account(True) == "iib-service-account-stage"
    )


# --- render_fbc_fragment ---


def test_render_fbc_fragment() -> None:
    """Parse opm render stdout into catalog entry dicts."""
    stdout = (
        '{"schema":"olm.package","name":"pkg-a"}\n'
        '{"schema":"olm.bundle","image":"q.io/b@sha256:x"}\n'
    )
    fake_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=stdout,
        stderr="",
    )

    def fake_run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
        assert cmd == ["opm", "render", "registry.io/img@sha256:abc"]
        return fake_result

    entries = prepare_fbc_parameters.render_fbc_fragment(
        "registry.io/img@sha256:abc",
        run=fake_run,
    )
    assert len(entries) == 2
    assert entries[0]["name"] == "pkg-a"


# --- fetch_ir_opt_in_results ---


def test_fetch_ir_opt_in_results() -> None:
    """Extract optInResults from InternalRequest status."""
    ir_data = {
        "status": {
            "results": {
                "optInResults": json.dumps(
                    [
                        {"containerImage": "a", "fbcOptIn": True},
                    ]
                ),
            },
        },
    }
    fake_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(ir_data),
        stderr="",
    )

    results = prepare_fbc_parameters.fetch_ir_opt_in_results(
        "test-ir",
        run=lambda *a, **kw: fake_result,
    )
    assert results == [{"containerImage": "a", "fbcOptIn": True}]


def test_fetch_ir_opt_in_results_empty_raises() -> None:
    """Raise CheckStepError when optInResults is missing from status."""
    ir_data = {"status": {"results": {}}}
    fake_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(ir_data),
        stderr="",
    )
    with pytest.raises(tekton.CheckStepError, match="empty optInResults"):
        prepare_fbc_parameters.fetch_ir_opt_in_results(
            "test-ir",
            run=lambda *a, **kw: fake_result,
        )


# --- run_prepare ---


def _setup_run_prepare(
    tmp_path: Path,
    *,
    components: list[dict] | None = None,
    data: dict | None = None,
) -> tuple[Path, Path]:
    if components is None:
        components = [
            {
                "name": "comp0",
                "containerImage": "registry.io/img0@sha256:aaa",
                "ocpVersion": ["v4.14"],
            },
        ]
    snapshot = _make_snapshot(components)
    if data is None:
        data = _make_data(allowed_packages=["pkg-a"])
    snap_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    _write_json(snap_path, snapshot)
    _write_json(data_path, data)
    return snap_path, data_path


def test_run_prepare_success(tmp_path: Path) -> None:
    """Return correct result dict for a standard opt-in release."""
    snap_path, data_path = _setup_run_prepare(tmp_path)

    def fake_render(fbc_fragment: str, **_kw: object) -> list[dict]:
        return _catalog_entries(["pkg-a"], ["quay.io/bundle-a"])

    def fake_create_ir(pipeline: str, **kwargs: object) -> str:
        return "test-ir-name"

    ir_data = {
        "status": {
            "results": {
                "optInResults": json.dumps(
                    [
                        {"containerImage": "quay.io/bundle-a", "fbcOptIn": True},
                    ]
                ),
            },
        },
    }
    fake_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(ir_data),
        stderr="",
    )

    results = prepare_fbc_parameters.run_prepare(
        snap_path,
        data_path,
        task_git_url="http://localhost",
        task_git_revision="main",
        pipeline_run_uid="uid-123",
        render=fake_render,
        create_ir=fake_create_ir,
        run=lambda *a, **kw: fake_result,
    )
    assert results["fbcOptIn"] == "true"
    assert results["validationPassed"] == "true"
    assert results["mustPublishIndexImage"] == "true"
    assert results["mustSignIndexImage"] == "true"
    assert results["mustOverwriteFromIndexImage"] == "true"
    assert results["iibServiceAccountSecret"] == "iib-service-account-prod"


def test_run_prepare_disallowed_packages(tmp_path: Path) -> None:
    """Raise CheckStepError when packages are not in the allowed set."""
    snap_path, data_path = _setup_run_prepare(
        tmp_path,
        data=_make_data(allowed_packages=["other-pkg"]),
    )

    def fake_render(fbc_fragment: str, **_kw: object) -> list[dict]:
        return _catalog_entries(["pkg-a"])

    with pytest.raises(tekton.CheckStepError, match="Validation failed"):
        prepare_fbc_parameters.run_prepare(
            snap_path,
            data_path,
            task_git_url="http://localhost",
            task_git_revision="main",
            pipeline_run_uid="uid-123",
            render=fake_render,
        )


def test_run_prepare_empty_components(tmp_path: Path) -> None:
    """Raise CheckStepError when snapshot has no components."""
    snap_path, data_path = _setup_run_prepare(
        tmp_path,
        components=[],
    )
    with pytest.raises(tekton.CheckStepError, match="No components"):
        prepare_fbc_parameters.run_prepare(
            snap_path,
            data_path,
            task_git_url="http://localhost",
            task_git_revision="main",
            pipeline_run_uid="uid-123",
        )


def test_run_prepare_multiple_modes(tmp_path: Path) -> None:
    """Raise ValueError when multiple release modes are active."""
    snap_path, data_path = _setup_run_prepare(
        tmp_path,
        data=_make_data(hotfix=True, pre_ga=True, allowed_packages=["x"]),
    )
    with pytest.raises(ValueError, match="Multiple release modes"):
        prepare_fbc_parameters.run_prepare(
            snap_path,
            data_path,
            task_git_url="http://localhost",
            task_git_revision="main",
            pipeline_run_uid="uid-123",
        )


def test_run_prepare_hotfix_mode(tmp_path: Path) -> None:
    """Hotfix mode sets publish and sign but not overwrite."""
    snap_path, data_path = _setup_run_prepare(
        tmp_path,
        data=_make_data(hotfix=True, allowed_packages=["pkg-a"]),
    )

    def fake_render(fbc_fragment: str, **_kw: object) -> list[dict]:
        return _catalog_entries(["pkg-a"], ["quay.io/bundle-a"])

    def fake_create_ir(pipeline: str, **kwargs: object) -> str:
        return "test-ir-name"

    ir_data = {
        "status": {
            "results": {
                "optInResults": json.dumps(
                    [
                        {"containerImage": "quay.io/bundle-a", "fbcOptIn": False},
                    ]
                ),
            },
        },
    }
    fake_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(ir_data),
        stderr="",
    )

    results = prepare_fbc_parameters.run_prepare(
        snap_path,
        data_path,
        task_git_url="http://localhost",
        task_git_revision="main",
        pipeline_run_uid="uid-123",
        render=fake_render,
        create_ir=fake_create_ir,
        run=lambda *a, **kw: fake_result,
    )
    assert results["mustPublishIndexImage"] == "true"
    assert results["mustSignIndexImage"] == "true"
    assert results["mustOverwriteFromIndexImage"] == "false"


def test_run_prepare_staged_mode(tmp_path: Path) -> None:
    """Staged mode disables all publishing and uses stage SA."""
    snap_path, data_path = _setup_run_prepare(
        tmp_path,
        data=_make_data(staged_index=True, allowed_packages=["pkg-a"]),
    )

    def fake_render(fbc_fragment: str, **_kw: object) -> list[dict]:
        return _catalog_entries(["pkg-a"], ["quay.io/bundle-a"])

    def fake_create_ir(pipeline: str, **kwargs: object) -> str:
        return "test-ir-name"

    ir_data = {
        "status": {
            "results": {
                "optInResults": json.dumps(
                    [
                        {"containerImage": "quay.io/bundle-a", "fbcOptIn": True},
                    ]
                ),
            },
        },
    }
    fake_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(ir_data),
        stderr="",
    )

    results = prepare_fbc_parameters.run_prepare(
        snap_path,
        data_path,
        task_git_url="http://localhost",
        task_git_revision="main",
        pipeline_run_uid="uid-123",
        render=fake_render,
        create_ir=fake_create_ir,
        run=lambda *a, **kw: fake_result,
    )
    assert results["mustPublishIndexImage"] == "false"
    assert results["mustSignIndexImage"] == "false"
    assert results["mustOverwriteFromIndexImage"] == "false"
    assert results["iibServiceAccountSecret"] == "iib-service-account-stage"


def test_run_prepare_duplicate_packages(tmp_path: Path) -> None:
    """Raise CheckStepError when components share duplicate packages."""
    snap_path, data_path = _setup_run_prepare(
        tmp_path,
        components=[
            {
                "name": "comp0",
                "containerImage": "reg.io/img0@sha256:a",
                "ocpVersion": ["v4.14"],
            },
            {
                "name": "comp1",
                "containerImage": "reg.io/img1@sha256:b",
                "ocpVersion": ["v4.14"],
            },
        ],
        data=_make_data(allowed_packages=["pkg-a"]),
    )

    def fake_render(fbc_fragment: str, **_kw: object) -> list[dict]:
        return _catalog_entries(["pkg-a"], ["quay.io/bundle"])

    with pytest.raises(tekton.CheckStepError, match="Validation failed"):
        prepare_fbc_parameters.run_prepare(
            snap_path,
            data_path,
            task_git_url="http://localhost",
            task_git_revision="main",
            pipeline_run_uid="uid-123",
            render=fake_render,
        )


# --- main ---


def _setup_main_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> dict[str, Path]:
    result_paths: dict[str, Path] = {}
    for name in (
        "RESULT_FBC_OPT_IN",
        "RESULT_VALIDATION_PASSED",
        "RESULT_MUST_PUBLISH_INDEX_IMAGE",
        "RESULT_MUST_SIGN_INDEX_IMAGE",
        "RESULT_MUST_OVERWRITE_FROM_INDEX_IMAGE",
        "RESULT_IIB_SERVICE_ACCOUNT_SECRET",
    ):
        p = tmp_path / name.lower()
        monkeypatch.setenv(name, str(p))
        result_paths[name] = p

    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("TASK_GIT_URL", "http://localhost")
    monkeypatch.setenv("TASK_GIT_REVISION", "main")
    monkeypatch.setenv("PIPELINE_RUN_UID", "uid-123")
    return result_paths


def test_main_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Write all result files and return 0 on success."""
    result_paths = _setup_main_env(monkeypatch, tmp_path)
    fake_results = {
        "fbcOptIn": "true",
        "validationPassed": "true",
        "mustPublishIndexImage": "true",
        "mustSignIndexImage": "true",
        "mustOverwriteFromIndexImage": "true",
        "iibServiceAccountSecret": "iib-service-account-prod",
    }
    with mock.patch.object(
        prepare_fbc_parameters.prepare_fbc_parameters,
        "run_prepare",
        return_value=fake_results,
    ):
        assert prepare_fbc_parameters.main() == 0

    assert result_paths["RESULT_FBC_OPT_IN"].read_text(encoding="utf-8") == "true"
    assert (
        result_paths["RESULT_MUST_PUBLISH_INDEX_IMAGE"].read_text(encoding="utf-8") == "true"
    )


def test_main_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Propagate CheckStepError from run_prepare without catching."""
    _setup_main_env(monkeypatch, tmp_path)
    err = tekton.CheckStepError("validating", ValueError("boom"))
    with mock.patch.object(
        prepare_fbc_parameters.prepare_fbc_parameters,
        "run_prepare",
        side_effect=err,
    ):
        with pytest.raises(tekton.CheckStepError, match="boom"):
            prepare_fbc_parameters.main()


def test_main_missing_task_git_url(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Raise CheckStepError when TASK_GIT_URL is empty."""
    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("TASK_GIT_URL", "")
    with pytest.raises(tekton.CheckStepError, match="TASK_GIT_URL must be set"):
        prepare_fbc_parameters.main()
