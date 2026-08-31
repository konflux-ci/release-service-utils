"""Tests for `pulp_push_disk_images`."""

from __future__ import annotations

import json
import subprocess
import tarfile
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


def test_require_json_field_empty_value_raises() -> None:
    """An empty-string leaf value is treated the same as a missing key."""
    with pytest.raises(ValueError, match="Missing containerImage value for component"):
        pulp_push_disk_images.pulp_push_disk_images.require_json_field(
            {"containerImage": ""}, "containerImage"
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
    (workloads / ".dockerconfigjson").write_text(
        '"{"auths":{"quay.io":{"auth":"abc"}}}"', encoding="utf-8"
    )
    (cgw / "username").write_text("user", encoding="utf-8")
    (cgw / "token").write_text("tok", encoding="utf-8")

    calls: list[list[str]] = []
    env_by_cmd: dict[str, dict[str, str]] = {}
    kwargs_by_cmd: dict[str, dict[str, object]] = {}

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append([str(x) for x in cmd])
        kwargs_by_cmd[str(cmd[0])] = kwargs
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
        pulp_task_timeout=7200,
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
    # Both long-running, high-output wrappers stream live progress to the step
    # log instead of being buffered/hidden until they exit.
    assert kwargs_by_cmd["pulp_push_wrapper"]["stream_stdout"] is True
    assert kwargs_by_cmd["developer_portal_wrapper"]["stream_stdout"] is True

    docker_config = json.loads((tmp_path / ".docker" / "config.json").read_text())
    assert docker_config == {"auths": {"quay.io": {"auth": "abc"}}}

    pulp_push_call = next(c for c in calls if c[0] == "pulp_push_wrapper")
    idx = pulp_push_call.index("--pulp-task-timeout-seconds")
    assert pulp_push_call[idx + 1] == "7200"


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
        pulp_task_timeout=7200,
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


def test_main_missing_result_result_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """`main` exits with code 1 when RESULT_RESULT itself is unset."""
    monkeypatch.delenv("RESULT_RESULT", raising=False)
    with pytest.raises(SystemExit) as exc:
        pulp_push_disk_images.pulp_push_disk_images.main()
    assert exc.value.code == 1


def test_main_missing_snapshot_exits_before_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`main` exits with code 1 when SNAPSHOT_JSON is unset."""
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path / "result"))
    monkeypatch.delenv("SNAPSHOT_JSON", raising=False)
    with pytest.raises(SystemExit) as exc:
        pulp_push_disk_images.pulp_push_disk_images.main()
    assert exc.value.code == 1


def test_main_missing_exodus_gw_env_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`main` exits with code 1 when EXODUS_GW_ENV is unset."""
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path / "result"))
    monkeypatch.setenv(
        "SNAPSHOT_JSON", json.dumps({"components": [{"staged": {"version": "1"}}]})
    )
    monkeypatch.delenv("EXODUS_GW_ENV", raising=False)
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")
    with pytest.raises(SystemExit) as exc:
        pulp_push_disk_images.pulp_push_disk_images.main()
    assert exc.value.code == 1


def test_main_missing_cgw_hostname_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`main` exits with code 1 when CGW_HOSTNAME is unset."""
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path / "result"))
    monkeypatch.setenv(
        "SNAPSHOT_JSON", json.dumps({"components": [{"staged": {"version": "1"}}]})
    )
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.delenv("CGW_HOSTNAME", raising=False)
    with pytest.raises(SystemExit) as exc:
        pulp_push_disk_images.pulp_push_disk_images.main()
    assert exc.value.code == 1


def test_main_rejects_non_dict_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SNAPSHOT_JSON that decodes to a non-object value fails via RESULT_RESULT."""
    result = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result))
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps([]))
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")

    assert pulp_push_disk_images.pulp_push_disk_images.main() == 0
    assert "SNAPSHOT_JSON must decode to a JSON object" in result.read_text(encoding="utf-8")


def test_main_defaults_pulp_task_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`main` defaults PULP_TASK_TIMEOUT to 7200 when unset."""
    result = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result))
    monkeypatch.setenv(
        "SNAPSHOT_JSON", json.dumps({"components": [{"staged": {"version": "1"}}]})
    )
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")
    monkeypatch.delenv("PULP_TASK_TIMEOUT", raising=False)

    with mock.patch.object(
        pulp_push_disk_images.pulp_push_disk_images, "run_push"
    ) as mock_run_push:
        assert pulp_push_disk_images.pulp_push_disk_images.main() == 0
    assert mock_run_push.call_args.kwargs["pulp_task_timeout"] == 7200


def test_main_passes_through_pulp_task_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`main` threads a custom PULP_TASK_TIMEOUT through to run_push."""
    result = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result))
    monkeypatch.setenv(
        "SNAPSHOT_JSON", json.dumps({"components": [{"staged": {"version": "1"}}]})
    )
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")
    monkeypatch.setenv("PULP_TASK_TIMEOUT", "300")

    with mock.patch.object(
        pulp_push_disk_images.pulp_push_disk_images, "run_push"
    ) as mock_run_push:
        assert pulp_push_disk_images.pulp_push_disk_images.main() == 0
    assert mock_run_push.call_args.kwargs["pulp_task_timeout"] == 300


def test_main_rejects_non_positive_pulp_task_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`main` exits with code 1 when PULP_TASK_TIMEOUT isn't a positive integer."""
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path / "result"))
    monkeypatch.setenv(
        "SNAPSHOT_JSON", json.dumps({"components": [{"staged": {"version": "1"}}]})
    )
    monkeypatch.setenv("EXODUS_GW_ENV", "pre")
    monkeypatch.setenv("CGW_HOSTNAME", "https://cgw.example.com")
    monkeypatch.setenv("PULP_TASK_TIMEOUT", "0")

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


def test_process_component_rejects_non_list_staged_files(tmp_path: Path) -> None:
    """A non-list ``staged.files`` value raises ValueError."""
    component = {
        "containerImage": "quay.io/org/image@sha256:abc",
        "staged": {"destination": "x86_64-isos", "files": "not-a-list"},
    }
    with pytest.raises(ValueError, match="staged.files must be a list"):
        pulp_push_disk_images.pulp_push_disk_images.process_component(
            component, tmp_path, stderr_path=tmp_path / "stderr.txt"
        )


def test_process_component_rejects_non_dict_staged_files_entry(tmp_path: Path) -> None:
    """A non-dict entry inside ``staged.files`` raises ValueError."""
    component = {
        "containerImage": "quay.io/org/image@sha256:abc",
        "staged": {"destination": "x86_64-isos", "files": ["not-a-dict"]},
    }
    with pytest.raises(ValueError, match="staged.files entries must be objects"):
        pulp_push_disk_images.pulp_push_disk_images.process_component(
            component, tmp_path, stderr_path=tmp_path / "stderr.txt"
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


def test_process_component_for_developer_portal_uses_squid_proxy_for_preprod_cgw(
    tmp_path: Path,
) -> None:
    """Preprod (developers.qa.redhat.com) CGW uploads route through the squid proxy."""
    calls: list[dict[str, object]] = []

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append({"cmd": cmd, "env": kwargs.get("env")})
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    pulp_push_disk_images.pulp_push_disk_images.process_component_for_developer_portal(
        _valid_component(),
        tmp_path,
        "https://developers.qa.redhat.com",
        stderr_path=tmp_path / "stderr.txt",
        run_cmd=fake_run_cmd,
    )

    assert len(calls) == 1
    env = calls[0]["env"]
    assert env["HTTP_PROXY"] == "http://squid.corp.redhat.com:3128"
    assert env["HTTPS_PROXY"] == "http://squid.corp.redhat.com:3128"


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


def test_process_component_rejects_path_traversal_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A staged.files[].source escaping the download directory is rejected."""

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(
        pulp_push_disk_images.pulp_push_disk_images.oras_utils.subprocess_cmd,
        "run_cmd",
        fake_run_cmd,
    )

    component = _valid_component()
    staged = component["staged"]
    assert isinstance(staged, dict)
    staged["files"] = [{"source": "../../etc/passwd", "filename": "amd.qcow2"}]

    with pytest.raises(ValueError, match=r"\.\."):
        pulp_push_disk_images.pulp_push_disk_images.process_component(
            component,
            tmp_path / "disk",
            stderr_path=tmp_path / "stderr.txt",
            run_cmd=fake_run_cmd,
        )


def test_process_component_falls_back_to_layered_image_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If `oras pull` doesn't produce the wanted file, fall back to skopeo+tar.

    This covers a normal (non-flat-artifact) image, e.g. a docker-build-oci-ta
    e2e test fixture, where the disk image lives inside a real tar layer rather
    than as a title-annotated ORAS blob.
    """
    stderr_path = tmp_path / "stderr.txt"
    extract_calls: list[tuple[str, list[str]]] = []

    def fake_extract(  # type: ignore[no-untyped-def]
        pull_spec, wanted_sources, destination, **kwargs
    ):
        extract_calls.append((pull_spec, list(wanted_sources)))
        (destination / "disk.qcow2").write_text("real bytes", encoding="utf-8")

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            # Simulates `oras pull` succeeding but not producing the wanted file
            # because the image is a normal layered image, not a flat artifact.
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(
        pulp_push_disk_images.pulp_push_disk_images.oras_utils.subprocess_cmd,
        "run_cmd",
        fake_run_cmd,
    )
    monkeypatch.setattr(
        pulp_push_disk_images.pulp_push_disk_images.oras_utils,
        "extract_disk_image_files",
        fake_extract,
    )

    component = _valid_component()
    pulp_push_disk_images.pulp_push_disk_images.process_component(
        component,
        tmp_path / "disk",
        stderr_path=stderr_path,
        run_cmd=fake_run_cmd,
    )

    assert extract_calls == [
        ("quay.io/org/image@sha256:abc", ["disk.qcow2"]),
    ]
    dest_file = tmp_path / "disk" / "x86_64-isos" / "FILES" / "amd.qcow2"
    assert dest_file.read_text(encoding="utf-8") == "real bytes"


def test_process_component_falls_back_when_oras_pull_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hard `oras pull` failure (not just a missing file) also triggers fallback."""
    stderr_path = tmp_path / "stderr.txt"
    extract_calls: list[str] = []

    def fake_extract(  # type: ignore[no-untyped-def]
        pull_spec, wanted_sources, destination, **kwargs
    ):
        extract_calls.append(pull_spec)
        (destination / "disk.qcow2").write_text("from fallback", encoding="utf-8")

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] == "oras":
            raise subprocess.CalledProcessError(1, cmd, stderr="not an oras artifact")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(
        pulp_push_disk_images.pulp_push_disk_images.oras_utils.subprocess_cmd,
        "run_cmd",
        fake_run_cmd,
    )
    monkeypatch.setattr(
        pulp_push_disk_images.pulp_push_disk_images.oras_utils,
        "extract_disk_image_files",
        fake_extract,
    )

    component = _valid_component()
    pulp_push_disk_images.pulp_push_disk_images.process_component(
        component,
        tmp_path / "disk",
        stderr_path=stderr_path,
        run_cmd=fake_run_cmd,
    )

    assert extract_calls == ["quay.io/org/image@sha256:abc"]
    dest_file = tmp_path / "disk" / "x86_64-isos" / "FILES" / "amd.qcow2"
    assert dest_file.read_text(encoding="utf-8") == "from fallback"


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
            pulp_task_timeout=7200,
            exodus_mount=exodus,
            pulp_mount=pulp,
            udcache_mount=udc,
            workloads_mount=workloads,
            cgw_mount=cgw,
            run_cmd=fake_run_cmd,
        )


@pytest.mark.parametrize("components", [[], "not-a-list", None])
def test_run_push_rejects_empty_or_invalid_components(
    components: object, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing, non-list, or empty `components` value fails fast."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus, pulp, udc, workloads, cgw = _setup_mount_secrets(tmp_path)

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    snapshot = {} if components is None else {"components": components}

    with pytest.raises(ValueError, match="snapshot must contain a non-empty components list"):
        pulp_push_disk_images.pulp_push_disk_images.run_push(
            snapshot,
            concurrent_limit=1,
            exodus_gw_env="pre",
            cgw_hostname="https://content-gateway.com",
            cert_warn_days=7,
            pulp_task_timeout=7200,
            exodus_mount=exodus,
            pulp_mount=pulp,
            udcache_mount=udc,
            workloads_mount=workloads,
            cgw_mount=cgw,
            run_cmd=fake_run_cmd,
        )


def test_run_push_skips_non_dict_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-dict entries in `components` are skipped rather than crashing the run."""
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

    monkeypatch.setattr(
        pulp_push_disk_images.pulp_push_disk_images.oras_utils.subprocess_cmd,
        "run_cmd",
        fake_run_cmd,
    )

    snapshot = {"components": [_valid_component(), "not-a-dict"]}

    pulp_push_disk_images.pulp_push_disk_images.run_push(
        snapshot,
        concurrent_limit=1,
        exodus_gw_env="pre",
        cgw_hostname="https://content-gateway.com",
        cert_warn_days=7,
        pulp_task_timeout=7200,
        exodus_mount=exodus,
        pulp_mount=pulp,
        udcache_mount=udc,
        workloads_mount=workloads,
        cgw_mount=cgw,
        run_cmd=fake_run_cmd,
    )

    assert len(portal_dirs) == 1


def test_run_push_oras_pull_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing oras pull AND a failing fallback extraction surface as an error.

    A failing `oras pull` alone now triggers the layered-image fallback (see
    `test_process_component_falls_back_when_oras_pull_fails`); this test covers
    the case where the underlying reference is unreachable/invalid altogether,
    so both the primary pull and the fallback's `skopeo copy` fail.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    _patch_cert_checks(monkeypatch)
    exodus, pulp, udc, workloads, cgw = _setup_mount_secrets(tmp_path)

    def fake_run_cmd(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")
        if cmd[0] in ("oras", "skopeo"):
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
            pulp_task_timeout=7200,
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
            pulp_task_timeout=7200,
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
            pulp_task_timeout=7200,
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


def test_main_writes_failure_result_for_unanticipated_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exception types outside the old fixed catch tuple still yield a Tekton result.

    Regression test: `run_push`'s layered-image fallback can raise e.g.
    `tarfile.ReadError` on a corrupt layer, which isn't a `ValueError`, `OSError`,
    or `subprocess.CalledProcessError`. `main` must still report it via
    RESULT_RESULT instead of letting it crash the step with a bare traceback.
    """
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
        side_effect=tarfile.ReadError("unexpected end of data"),
    ):
        assert pulp_push_disk_images.pulp_push_disk_images.main() == 0

    assert "unexpected end of data" in result.read_text(encoding="utf-8")


def test_dunder_main_block(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the ``if __name__ == "__main__"`` block."""
    monkeypatch.delenv("RESULT_RESULT", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        import runpy

        runpy.run_module("pulp_push_disk_images.pulp_push_disk_images", run_name="__main__")
    assert exc_info.value.code == 1
