"""Tests for `pulp_push_disk_images`."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.tasks.internal import pulp_push_disk_images
from release_service_utils.helpers import tekton
from release_service_utils.helpers import subprocess_cmd


def _patch_cert_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip real certificate validation in ``run_push`` integration tests."""
    monkeypatch.setattr(
        pulp_push_disk_images.pulp_push_disk_images.push_artifacts,
        "_check_cert_expiration",
        lambda *_args: None,
    )


def test_normalize_docker_config_strips_k8s_quotes() -> None:
    """Kubernetes-style quoted dockerconfigjson is normalized to valid JSON."""
    raw = '"{"auths":{"quay.io":{"auth":"abc"}}}"'
    out = pulp_push_disk_images.pulp_push_disk_images.normalize_docker_config(raw)
    assert out == '{"auths":{"quay.io":{"auth":"abc"}}}'


def test_build_staged_payload_lists_files(tmp_path: Path) -> None:
    """Staged payload lists files under the disk image directory with version."""
    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "a" / "b" / "disk.qcow2").write_text("x", encoding="utf-8")
    payload = pulp_push_disk_images.pulp_push_disk_images.build_staged_payload(tmp_path, "1.3")
    assert payload["header"]["version"] == "0.2"
    assert len(payload["payload"]["files"]) == 1
    assert payload["payload"]["files"][0]["filename"] == "disk.qcow2"
    assert payload["payload"]["files"][0]["version"] == "1.3"


def test_require_json_field_missing() -> None:
    """Missing nested JSON fields raise ValueError with a clear path."""
    with pytest.raises(ValueError, match="Missing contentGateway value for component"):
        pulp_push_disk_images.pulp_push_disk_images.require_json_field(
            {}, "contentGateway", "productName"
        )


def test_require_staged_files_field_missing() -> None:
    """Missing staged.files[] keys use paths compatible with Tekton result checks."""
    with pytest.raises(
        ValueError,
        match=r"Missing staged\.files\[\]\.filename value for component",
    ):
        pulp_push_disk_images.pulp_push_disk_images.require_staged_files_field(
            {"source": "disk.qcow2"}, "filename"
        )


def test_main_writes_check_step_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``CheckStepError`` from ``run_push`` is written to RESULT_RESULT via tekton helper."""
    result = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result))
    monkeypatch.setenv(
        "SNAPSHOT_JSON",
        json.dumps({"components": [_valid_component()]}),
    )
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")

    err = tekton.CheckStepError(
        "validating staged version",
        ValueError("version not specified in .components[0].staged.version"),
    )
    with mock.patch.object(
        pulp_push_disk_images.pulp_push_disk_images, "run_push", side_effect=err
    ):
        assert pulp_push_disk_images.pulp_push_disk_images.main() == 0

    text = result.read_text(encoding="utf-8")
    assert "validating staged version" in text
    assert "version not specified" in text


def test_run_push_calls_wrappers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run_push invokes pulp_push_wrapper and developer_portal_wrapper."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus = tmp_path / "exodus"
    pulp = tmp_path / "pulp"
    udc = tmp_path / "udc"
    workloads = tmp_path / "workloads"
    cgw = tmp_path / "cgw"
    for d in (exodus, pulp, udc, workloads, cgw):
        d.mkdir()
    (exodus / "cert").write_text("c", encoding="utf-8")
    (exodus / "key").write_text("k", encoding="utf-8")
    (exodus / "url").write_text("https://exodus", encoding="utf-8")
    (pulp / "pulp_url").write_text("https://pulp.com", encoding="utf-8")
    (pulp / "konflux-release-rhsm-pulp.crt").write_text("pc", encoding="utf-8")
    (pulp / "konflux-release-rhsm-pulp.key").write_text("pk", encoding="utf-8")
    (udc / "url").write_text("https://udc", encoding="utf-8")
    (udc / "cert").write_text("uc", encoding="utf-8")
    (udc / "key").write_text("uk", encoding="utf-8")
    (workloads / ".dockerconfigjson").write_text('{"auths":{}}', encoding="utf-8")
    (cgw / "username").write_text("user", encoding="utf-8")
    (cgw / "token").write_text("tok", encoding="utf-8")

    calls: list[list[str]] = []
    env_by_cmd: dict[str, dict[str, str]] = {}

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append([str(x) for x in cmd])
        env = kwargs.get("env")
        if env is not None:
            env_by_cmd[str(cmd[0])] = dict(env)
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            cwd = kwargs.get("cwd")
            assert cwd is not None
            Path(cwd, "disk.qcow2").write_text("data", encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[0] == "yq":
            return subprocess.CompletedProcess(
                cmd, 0, stdout="payload:\n  files: []\n", stderr=""
            )
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess_cmd, "run_cmd", fake_run_cmd)

    snapshot = {
        "components": [
            {
                "containerImage": "quay.io/org/image@sha256:abc",
                "contentGateway": {
                    "productName": "Disk",
                    "productCode": "DISK",
                    "productVersionName": "1.3",
                    "filePrefix": "amd",
                },
                "staged": {
                    "destination": "x86_64-isos",
                    "version": "1.3",
                    "files": [{"source": "disk.qcow2", "filename": "amd.qcow2"}],
                },
            }
        ]
    }

    pulp_push_disk_images.pulp_push_disk_images.run_push(
        snapshot,
        concurrent_limit=1,
        exodus_gw_env="pre",
        cgw_hostname="https://content-gateway.com",
        cert_warn_days=7,
        exodus_mount=exodus,
        pulp_mount=pulp,
        udcache_mount=udc,
        workloads_mount=workloads,
        cgw_mount=cgw,
        run_cmd=fake_run_cmd,
    )

    joined = "\n".join(" ".join(c) for c in calls)
    assert "pulp_push_wrapper" in joined
    assert "developer_portal_wrapper" in joined
    assert env_by_cmd["developer_portal_wrapper"]["CGW_USERNAME"] == "user"
    assert env_by_cmd["developer_portal_wrapper"]["CGW_PASSWORD"] == "tok"


def _setup_mount_secrets(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    """Create dummy secret mount directories used by ``run_push`` tests."""
    exodus = tmp_path / "exodus"
    pulp = tmp_path / "pulp"
    udc = tmp_path / "udc"
    workloads = tmp_path / "workloads"
    cgw = tmp_path / "cgw"
    for d in (exodus, pulp, udc, workloads, cgw):
        d.mkdir()
    (exodus / "cert").write_text("c", encoding="utf-8")
    (exodus / "key").write_text("k", encoding="utf-8")
    (exodus / "url").write_text("https://exodus", encoding="utf-8")
    (pulp / "pulp_url").write_text("https://pulp.com", encoding="utf-8")
    (pulp / "konflux-release-rhsm-pulp.crt").write_text("pc", encoding="utf-8")
    (pulp / "konflux-release-rhsm-pulp.key").write_text("pk", encoding="utf-8")
    (udc / "url").write_text("https://udc", encoding="utf-8")
    (udc / "cert").write_text("uc", encoding="utf-8")
    (udc / "key").write_text("uk", encoding="utf-8")
    (workloads / ".dockerconfigjson").write_text('{"auths":{}}', encoding="utf-8")
    (cgw / "username").write_text("user", encoding="utf-8")
    (cgw / "token").write_text("tok", encoding="utf-8")
    return exodus, pulp, udc, workloads, cgw


def test_run_push_developer_portal_uses_staged_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each component's CGW upload uses ``staged.destination``, not staged file index."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus, pulp, udc, workloads, cgw = _setup_mount_secrets(tmp_path)
    portal_dirs: list[str] = []

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            cwd = kwargs.get("cwd")
            assert cwd is not None
            Path(cwd, "disk.qcow2").write_text("data", encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[0] == "yq":
            return subprocess.CompletedProcess(
                cmd, 0, stdout="payload:\n  files: []\n", stderr=""
            )
        if cmd[0] == "developer_portal_wrapper":
            idx = cmd.index("--content-directory")
            portal_dirs.append(str(cmd[idx + 1]))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess_cmd, "run_cmd", fake_run_cmd)

    snapshot = {
        "components": [
            {
                "containerImage": "quay.io/org/image-a@sha256:aaa",
                "contentGateway": {
                    "productName": "DiskA",
                    "productCode": "DISKA",
                    "productVersionName": "1.0",
                    "filePrefix": "amd",
                },
                "staged": {
                    "destination": "x86_64-isos",
                    "version": "1.0",
                    "files": [
                        {"source": "disk.qcow2", "filename": "amd1.qcow2"},
                        {"source": "disk.qcow2", "filename": "amd2.qcow2"},
                    ],
                },
            },
            {
                "containerImage": "quay.io/org/image-b@sha256:bbb",
                "contentGateway": {
                    "productName": "DiskB",
                    "productCode": "DISKB",
                    "productVersionName": "1.0",
                    "filePrefix": "arm",
                },
                "staged": {
                    "destination": "aarch64-isos",
                    "version": "1.0",
                    "files": [{"source": "disk.qcow2", "filename": "arm.qcow2"}],
                },
            },
        ]
    }

    pulp_push_disk_images.pulp_push_disk_images.run_push(
        snapshot,
        concurrent_limit=1,
        exodus_gw_env="pre",
        cgw_hostname="https://content-gateway.com",
        cert_warn_days=7,
        exodus_mount=exodus,
        pulp_mount=pulp,
        udcache_mount=udc,
        workloads_mount=workloads,
        cgw_mount=cgw,
        run_cmd=fake_run_cmd,
    )

    assert len(portal_dirs) == 2
    assert any(d.endswith("/x86_64-isos/FILES") for d in portal_dirs)
    assert any(d.endswith("/aarch64-isos/FILES") for d in portal_dirs)


def test_main_writes_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`main` writes Success to RESULT_RESULT when run_push completes."""
    result = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result))
    monkeypatch.setenv(
        "SNAPSHOT_JSON", json.dumps({"components": [{"staged": {"version": "1"}}]})
    )
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")

    with mock.patch.object(pulp_push_disk_images.pulp_push_disk_images, "run_push"):
        assert pulp_push_disk_images.pulp_push_disk_images.main() == 0
    assert result.read_text(encoding="utf-8") == "Success"


def test_main_missing_snapshot_exits_before_results() -> None:
    """`main` exits with code 1 when SNAPSHOT_JSON is unset."""
    with pytest.raises(SystemExit) as exc:
        pulp_push_disk_images.pulp_push_disk_images.main()
    assert exc.value.code == 1


def _valid_component() -> dict[str, object]:
    return {
        "containerImage": "quay.io/org/image@sha256:abc",
        "contentGateway": {
            "productName": "Disk",
            "productCode": "DISK",
            "productVersionName": "1.3",
            "filePrefix": "amd",
        },
        "staged": {
            "destination": "x86_64-isos",
            "version": "1.3",
            "files": [{"source": "disk.qcow2", "filename": "amd.qcow2"}],
        },
    }


@pytest.mark.parametrize(
    ("component", "match"),
    [
        ({}, "Missing containerImage"),
        (
            {"containerImage": "quay.io/org/image@sha256:abc"},
            "Missing staged value for component",
        ),
    ],
)
def test_process_component_missing_fields(component: dict[str, object], match: str) -> None:
    """Missing containerImage or staged.destination fail during pull/stage."""
    with pytest.raises(ValueError, match=match):
        pulp_push_disk_images.pulp_push_disk_images.process_component(
            component,
            Path("/tmp/disk"),
            stderr_path=Path("/tmp/stderr.txt"),
        )


@pytest.mark.parametrize(
    ("component", "match"),
    [
        (
            {
                "containerImage": "quay.io/org/image@sha256:abc",
                "staged": {"destination": "x86_64-isos"},
            },
            "Missing contentGateway value for component",
        ),
        (
            {
                "containerImage": "quay.io/org/image@sha256:abc",
                "contentGateway": {
                    "productName": "Disk",
                    "productVersionName": "1.3",
                    "filePrefix": "amd",
                },
                "staged": {"destination": "x86_64-isos"},
            },
            "Missing contentGateway.productCode",
        ),
        (
            {
                "containerImage": "quay.io/org/image@sha256:abc",
                "contentGateway": {
                    "productName": "Disk",
                    "productCode": "DISK",
                    "filePrefix": "amd",
                },
                "staged": {"destination": "x86_64-isos"},
            },
            "Missing contentGateway.productVersionName",
        ),
        (
            {
                "containerImage": "quay.io/org/image@sha256:abc",
                "contentGateway": {
                    "productName": "Disk",
                    "productCode": "DISK",
                    "productVersionName": "1.3",
                },
                "staged": {"destination": "x86_64-isos"},
            },
            "Missing contentGateway.filePrefix",
        ),
    ],
)
def test_process_component_for_developer_portal_missing_fields(
    component: dict[str, object], match: str
) -> None:
    """Missing contentGateway fields fail during developer portal upload."""
    with pytest.raises(ValueError, match=match):
        pulp_push_disk_images.pulp_push_disk_images.process_component_for_developer_portal(
            component,
            Path("/tmp/content"),
            "https://content-gateway.com",
            stderr_path=Path("/tmp/stderr.txt"),
        )


def test_require_staged_files_field_source_missing() -> None:
    """Missing staged.files[].source matches legacy Tekton fail test."""
    with pytest.raises(
        ValueError,
        match=r"Missing staged\.files\[\]\.source value for component",
    ):
        pulp_push_disk_images.pulp_push_disk_images.require_staged_files_field(
            {"filename": "amd.qcow2"}, "source"
        )


def test_process_component_duplicate_filename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two staged files must not target the same destination filename."""
    stderr_path = tmp_path / "stderr.txt"

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            cwd = kwargs.get("cwd")
            assert cwd is not None
            Path(cwd, "disk.qcow2").write_text("a", encoding="utf-8")
            Path(cwd, "disk.raw").write_text("b", encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess_cmd, "run_cmd", fake_run_cmd)

    component = _valid_component()
    staged = component["staged"]
    assert isinstance(staged, dict)
    staged["files"] = [
        {"source": "disk.qcow2", "filename": "amd.qcow2"},
        {"source": "disk.raw", "filename": "amd.qcow2"},
    ]

    with pytest.raises(ValueError, match="Multiple files use the same destination"):
        pulp_push_disk_images.pulp_push_disk_images.process_component(
            component,
            tmp_path / "disk",
            stderr_path=stderr_path,
            run_cmd=fake_run_cmd,
        )


def test_run_push_missing_staged_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing .components[0].staged.version fails before wrappers run."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus, pulp, udc, workloads, cgw = _setup_mount_secrets(tmp_path)

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    snapshot = {
        "components": [
            {
                "containerImage": "quay.io/org/image@sha256:abc",
                "staged": {"destination": "x86_64-isos", "files": []},
            }
        ]
    }

    with pytest.raises(tekton.CheckStepError, match="validating staged version"):
        pulp_push_disk_images.pulp_push_disk_images.run_push(
            snapshot,
            concurrent_limit=1,
            exodus_gw_env="pre",
            cgw_hostname="https://content-gateway.com",
            cert_warn_days=7,
            exodus_mount=exodus,
            pulp_mount=pulp,
            udcache_mount=udc,
            workloads_mount=workloads,
            cgw_mount=cgw,
            run_cmd=fake_run_cmd,
        )


def test_run_push_oras_pull_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing oras pull surfaces as CalledProcessError from run_push."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus, pulp, udc, workloads, cgw = _setup_mount_secrets(tmp_path)

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            raise subprocess.CalledProcessError(
                1, cmd, stderr="Simulating failing oras pull call"
            )
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess_cmd, "run_cmd", fake_run_cmd)

    snapshot = {"components": [_valid_component()]}

    with pytest.raises(subprocess.CalledProcessError):
        pulp_push_disk_images.pulp_push_disk_images.run_push(
            snapshot,
            concurrent_limit=1,
            exodus_gw_env="pre",
            cgw_hostname="https://content-gateway.com",
            cert_warn_days=7,
            exodus_mount=exodus,
            pulp_mount=pulp,
            udcache_mount=udc,
            workloads_mount=workloads,
            cgw_mount=cgw,
            run_cmd=fake_run_cmd,
        )


def test_run_push_gzip_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing gzip decompress surfaces as CalledProcessError from run_push."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus, pulp, udc, workloads, cgw = _setup_mount_secrets(tmp_path)

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            cwd = kwargs.get("cwd")
            assert cwd is not None
            Path(cwd, "disk.qcow2").write_text("a", encoding="utf-8")
            Path(cwd, "fail_gzip.raw.gz").write_text("gz", encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[0] == "gzip":
            raise subprocess.CalledProcessError(1, cmd, stderr="gzip failed")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess_cmd, "run_cmd", fake_run_cmd)

    component = _valid_component()
    staged = component["staged"]
    assert isinstance(staged, dict)
    staged["files"] = [
        {"source": "disk.qcow2", "filename": "amd.qcow2"},
        {"source": "fail_gzip.raw", "filename": "amd.raw"},
    ]
    snapshot = {"components": [component]}

    with pytest.raises(subprocess.CalledProcessError):
        pulp_push_disk_images.pulp_push_disk_images.run_push(
            snapshot,
            concurrent_limit=1,
            exodus_gw_env="pre",
            cgw_hostname="https://content-gateway.com",
            cert_warn_days=7,
            exodus_mount=exodus,
            pulp_mount=pulp,
            udcache_mount=udc,
            workloads_mount=workloads,
            cgw_mount=cgw,
            run_cmd=fake_run_cmd,
        )


def test_run_push_pulp_push_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing pulp_push_wrapper surfaces as CalledProcessError from run_push."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus, pulp, udc, workloads, cgw = _setup_mount_secrets(tmp_path)
    (pulp / "pulp_url").write_text("https://failing-pulp.com", encoding="utf-8")

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            cwd = kwargs.get("cwd")
            assert cwd is not None
            Path(cwd, "disk.qcow2").write_text("a", encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[0] == "yq":
            return subprocess.CompletedProcess(
                cmd, 0, stdout="payload:\n  files: []\n", stderr=""
            )
        if cmd[0] == "pulp_push_wrapper":
            raise subprocess.CalledProcessError(
                1,
                cmd,
                stderr="Mocked failure of pulp_push_wrapper",
            )
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess_cmd, "run_cmd", fake_run_cmd)

    snapshot = {"components": [_valid_component()]}

    with pytest.raises(subprocess.CalledProcessError, match="pulp_push_wrapper"):
        pulp_push_disk_images.pulp_push_disk_images.run_push(
            snapshot,
            concurrent_limit=1,
            exodus_gw_env="pre",
            cgw_hostname="https://content-gateway.com",
            cert_warn_days=7,
            exodus_mount=exodus,
            pulp_mount=pulp,
            udcache_mount=udc,
            workloads_mount=workloads,
            cgw_mount=cgw,
            run_cmd=fake_run_cmd,
        )


def test_main_writes_failure_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Validation errors are written to RESULT_RESULT and main exits zero."""
    result = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result))
    monkeypatch.setenv(
        "SNAPSHOT_JSON",
        json.dumps({"components": [_valid_component()]}),
    )
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")

    with mock.patch.object(
        pulp_push_disk_images.pulp_push_disk_images,
        "run_push",
        side_effect=ValueError("Missing containerImage value for component"),
    ):
        assert pulp_push_disk_images.pulp_push_disk_images.main() == 0

    assert "Missing containerImage value for component" in result.read_text(encoding="utf-8")
