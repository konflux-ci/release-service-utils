"""Unit tests for ``run_single_catalog_e2e_suite``."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import run_single_catalog_e2e_suite as rs

_SUITE_PY = Path(rs.__file__).resolve()


def _plr_json(
    *,
    completion_time: str | None = None,
    succeeded: bool | None = None,
    message: str = "done",
) -> str:
    status: dict = {}
    if completion_time is not None:
        status["completionTime"] = completion_time
    if succeeded is not None:
        status["conditions"] = [
            {
                "type": "Succeeded",
                "status": "True" if succeeded else "False",
                "message": message,
            }
        ]
    return json.dumps({"status": status})


def test_pipelinerun_finished_returns_none_while_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No completionTime means still running."""
    proc = subprocess.CompletedProcess(["kubectl"], 0, _plr_json(completion_time=None), "")
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    assert rs._pipelinerun_finished("pr1", "ns") is None


def test_pipelinerun_finished_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Succeeded=True yields (True, message)."""
    proc = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        _plr_json(completion_time="2024-01-01T00:00:00Z", succeeded=True, message="ok"),
        "",
    )
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    ok, msg = rs._pipelinerun_finished("pr1", "ns")
    assert ok is True
    assert msg == "ok"


def test_pipelinerun_finished_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Succeeded=False yields (False, message)."""
    proc = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        _plr_json(
            completion_time="2024-01-01T00:00:00Z",
            succeeded=False,
            message="task failed",
        ),
        "",
    )
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    ok, msg = rs._pipelinerun_finished("pr1", "ns")
    assert ok is False
    assert msg == "task failed"


def test_pipelinerun_finished_no_succeeded_condition(monkeypatch: pytest.MonkeyPatch) -> None:
    """Completed run without Succeeded condition is treated as failure tuple."""
    body = json.dumps(
        {
            "status": {
                "completionTime": "2024-01-01T00:00:00Z",
                "conditions": [{"type": "Other", "status": "True"}],
            }
        }
    )
    proc = subprocess.CompletedProcess(["kubectl"], 0, body, "")
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    ok, msg = rs._pipelinerun_finished("pr1", "ns")
    assert ok is False
    assert "no Succeeded" in msg


def test_pipelinerun_finished_kubectl_error_exits(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Non-zero kubectl exit terminates the process."""
    proc = subprocess.CompletedProcess(["kubectl"], 1, "", "kubectl: no")
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    with pytest.raises(SystemExit) as ei:
        rs._pipelinerun_finished("pr1", "ns")
    assert ei.value.code == 1
    assert "kubectl: no" in capsys.readouterr().err


def test_pipelinerun_finished_invalid_json_exits(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invalid JSON from kubectl terminates the process."""
    proc = subprocess.CompletedProcess(["kubectl"], 0, "not-json", "")
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    with pytest.raises(SystemExit) as ei:
        rs._pipelinerun_finished("pr1", "ns")
    assert ei.value.code == 1
    assert "invalid PipelineRun JSON" in capsys.readouterr().err


def test_wait_pipelinerun_terminal_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return True when first poll shows Succeeded=True."""
    monkeypatch.setattr(
        rs,
        "_pipelinerun_finished",
        lambda n, ns: (True, "ok"),
    )
    monkeypatch.setattr(rs.time, "sleep", lambda _: None)
    assert rs._wait_pipelinerun_terminal(name="x", ns="ns", timeout_seconds=60.0) is True


def test_wait_pipelinerun_terminal_timeout_exits_124(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 124 when monotonic time passes the deadline while still running."""
    tick = [0.0]

    def mono() -> float:
        tick[0] += 1000.0
        return tick[0]

    monkeypatch.setattr(rs, "_pipelinerun_finished", lambda n, ns: None)
    monkeypatch.setattr(rs.time, "monotonic", mono)
    monkeypatch.setattr(rs.time, "sleep", lambda _: None)
    with pytest.raises(SystemExit) as ei:
        rs._wait_pipelinerun_terminal(name="x", ns="ns", timeout_seconds=30.0)
    assert ei.value.code == 124
    assert "timeout" in capsys.readouterr().err


def test_wait_pipelinerun_terminal_heartbeat_and_sleep_while_running(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Emit heartbeat and sleep while polling until the run eventually succeeds."""
    poll_calls = [0]
    sleeps: list[float] = []

    def finished(_n: str, _ns: str):
        poll_calls[0] += 1
        if poll_calls[0] < 2:
            return None
        return (True, "ok")

    # 0,0: deadline + last_hb; 65: while guard; 65: now (heartbeat);
    # 65,65: second while iteration then success.
    monotonic_vals = iter([0.0, 0.0, 65.0, 65.0, 65.0])

    def mono() -> float:
        return next(monotonic_vals)

    monkeypatch.setattr(rs, "_pipelinerun_finished", finished)
    monkeypatch.setattr(rs.time, "monotonic", mono)
    monkeypatch.setattr(rs.time, "sleep", lambda s: sleeps.append(float(s)))

    assert rs._wait_pipelinerun_terminal(name="pr", ns="ns1", timeout_seconds=120.0) is True
    out = capsys.readouterr().out
    assert "Waiting on pipelinerun/pr" in out
    assert "remaining before timeout" in out
    assert sleeps == [10.0]


def test_wait_pipelinerun_terminal_failed_prints_to_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Failure path prints the PipelineRun message to stderr."""
    monkeypatch.setattr(
        rs,
        "_pipelinerun_finished",
        lambda n, ns: (False, "step foo died"),
    )
    monkeypatch.setattr(rs.time, "sleep", lambda _: None)
    assert rs._wait_pipelinerun_terminal(name="bad", ns="ns", timeout_seconds=30.0) is False
    assert "step foo died" in capsys.readouterr().err


def test_taskrun_name_for_pipeline_task_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve TaskRun name from childReferences."""
    body = json.dumps(
        {
            "status": {
                "childReferences": [
                    {"pipelineTaskName": "other", "name": "tr-other"},
                    {"pipelineTaskName": "run-test", "name": "tr-run-test-abc"},
                ]
            }
        }
    )
    proc = subprocess.CompletedProcess(["kubectl"], 0, body, "")
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    assert rs._taskrun_name_for_pipeline_task("pr1", "ns", "run-test") == "tr-run-test-abc"


def test_taskrun_name_for_pipeline_task_kubectl_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None when kubectl get pipelinerun fails."""
    proc = subprocess.CompletedProcess(["kubectl"], 1, "", "err")
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    assert rs._taskrun_name_for_pipeline_task("pr1", "ns", "run-test") is None


def test_taskrun_name_for_pipeline_task_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None when no childReference matches the pipeline task name."""
    body = json.dumps({"status": {"childReferences": []}})
    proc = subprocess.CompletedProcess(["kubectl"], 0, body, "")
    monkeypatch.setattr(rs.subprocess, "run", MagicMock(return_value=proc))
    assert rs._taskrun_name_for_pipeline_task("pr1", "ns", "run-test") is None


def test_fetch_run_test_task_output_json_parses_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Load TEST_OUTPUT JSON from the run-test TaskRun."""
    plr_body = json.dumps(
        {
            "status": {
                "childReferences": [
                    {"pipelineTaskName": "run-test", "name": "tr-1"},
                ]
            }
        }
    )
    payload = {"result": "SUCCESS", "foo": 1}
    tr_body = json.dumps(
        {
            "status": {
                "results": [
                    {"name": "TEST_OUTPUT", "value": json.dumps(payload)},
                ]
            }
        }
    )

    def fake_run(cmd: list, **kwargs):
        if "pipelinerun" in cmd:
            return subprocess.CompletedProcess(cmd, 0, plr_body, "")
        if "taskrun" in cmd:
            return subprocess.CompletedProcess(cmd, 0, tr_body, "")
        raise AssertionError(cmd)

    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    assert rs._fetch_run_test_task_output_json("pr1", "ns") == payload


def test_fetch_run_test_task_output_json_no_test_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return None when TaskRun has no TEST_OUTPUT result."""
    plr_body = json.dumps(
        {
            "status": {
                "childReferences": [
                    {"pipelineTaskName": "run-test", "name": "tr-1"},
                ]
            }
        }
    )
    tr_body = json.dumps({"status": {"results": [{"name": "OTHER", "value": "x"}]}})

    def fake_run(cmd: list, **kwargs):
        if "pipelinerun" in cmd:
            return subprocess.CompletedProcess(cmd, 0, plr_body, "")
        return subprocess.CompletedProcess(cmd, 0, tr_body, "")

    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    assert rs._fetch_run_test_task_output_json("pr1", "ns") is None
    assert "no TEST_OUTPUT" in capsys.readouterr().err


def test_fetch_run_test_task_output_json_when_taskrun_unresolved(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Print and return None when run-test TaskRun cannot be resolved."""
    monkeypatch.setattr(rs, "_taskrun_name_for_pipeline_task", lambda *a, **k: None)
    assert rs._fetch_run_test_task_output_json("pr-x", "ns-y") is None
    err = capsys.readouterr().err
    assert "could not resolve TaskRun name" in err
    assert "pr-x" in err and "ns-y" in err


def test_fetch_run_test_task_output_json_taskrun_kubectl_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return None when kubectl get taskrun fails."""
    plr_body = json.dumps(
        {
            "status": {
                "childReferences": [
                    {"pipelineTaskName": "run-test", "name": "tr-1"},
                ]
            }
        }
    )

    def fake_run(cmd: list, **kwargs):
        if "pipelinerun" in cmd:
            return subprocess.CompletedProcess(cmd, 0, plr_body, "")
        return subprocess.CompletedProcess(cmd, 1, "", "taskrun: not found")

    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    assert rs._fetch_run_test_task_output_json("pr1", "ns") is None
    assert "taskrun: not found" in capsys.readouterr().err


def test_fetch_run_test_task_output_json_empty_test_output_value(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return None when TEST_OUTPUT result value is empty."""
    plr_body = json.dumps(
        {
            "status": {
                "childReferences": [
                    {"pipelineTaskName": "run-test", "name": "tr-1"},
                ]
            }
        }
    )
    tr_body = json.dumps({"status": {"results": [{"name": "TEST_OUTPUT", "value": ""}]}})

    def fake_run(cmd: list, **kwargs):
        if "pipelinerun" in cmd:
            return subprocess.CompletedProcess(cmd, 0, plr_body, "")
        return subprocess.CompletedProcess(cmd, 0, tr_body, "")

    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    assert rs._fetch_run_test_task_output_json("pr1", "ns") is None
    assert "empty TEST_OUTPUT" in capsys.readouterr().err


def test_fetch_run_test_task_output_json_invalid_json_in_value(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return None when TEST_OUTPUT value is not valid JSON."""
    plr_body = json.dumps(
        {
            "status": {
                "childReferences": [
                    {"pipelineTaskName": "run-test", "name": "tr-1"},
                ]
            }
        }
    )
    tr_body = json.dumps(
        {"status": {"results": [{"name": "TEST_OUTPUT", "value": "not-json"}]}}
    )

    def fake_run(cmd: list, **kwargs):
        if "pipelinerun" in cmd:
            return subprocess.CompletedProcess(cmd, 0, plr_body, "")
        return subprocess.CompletedProcess(cmd, 0, tr_body, "")

    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    assert rs._fetch_run_test_task_output_json("pr1", "ns") is None
    assert "invalid TEST_OUTPUT JSON" in capsys.readouterr().err


def test_require_test_output_success_none_exits() -> None:
    with pytest.raises(SystemExit) as ei:
        rs._require_test_output_success(None)
    assert ei.value.code == 1


def test_require_test_output_success_failure_exits(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as ei:
        rs._require_test_output_success({"result": "FAILURE"})
    assert ei.value.code == 1
    assert "FAILURE" in capsys.readouterr().err


def test_require_test_output_success_ok() -> None:
    rs._require_test_output_success({"result": "SUCCESS"})


def test_require_test_output_success_skipped_prints(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rs._require_test_output_success({"result": "SKIPPED"})
    assert "SKIPPED" in capsys.readouterr().out


def test_require_test_output_success_unexpected_exits(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as ei:
        rs._require_test_output_success({"result": "WAT"})
    assert ei.value.code == 1
    assert "unexpected" in capsys.readouterr().err


def test_build_snapshot_shape() -> None:
    snap = rs._build_snapshot(
        runner="quay.io/img:v1",
        url="https://github.com/o/r.git",
        rev="abc",
    )
    assert snap["application"] == "utils-orchestrated-e2e"
    comp = snap["components"][0]
    assert comp["containerImage"] == "quay.io/img:v1"
    assert comp["source"]["git"]["url"] == "https://github.com/o/r.git"
    assert comp["source"]["git"]["revision"] == "abc"


def test_build_catalog_e2e_pipelinerun_shape() -> None:
    snap = rs._build_snapshot(runner="r", url="u", rev="v")
    m = rs._build_catalog_e2e_pipelinerun(
        ns="rhtap-release-2-tenant",
        child_plr_name="utils-e2e-catalog-uid1",
        parent="parent-plr",
        suite="my-suite",
        snap=snap,
        pipeline_used="pipe-a",
        vault_password_secret_name="vp",
        github_token_secret_name="gt",
        kubeconfig_secret_name="kc",
    )
    assert m["kind"] == "PipelineRun"
    assert m["metadata"]["name"] == "utils-e2e-catalog-uid1"
    assert m["metadata"]["labels"]["utils-e2e/suite"] == "my-suite"
    params = {p["name"]: p["value"] for p in m["spec"]["params"]}
    assert params["PIPELINE_TEST_SUITE"] == "my-suite"
    assert params["PIPELINE_USED"] == "pipe-a"
    assert params["VAULT_PASSWORD_SECRET_NAME"] == "vp"
    assert "SNAPSHOT" in params
    ref_params = {p["name"]: p["value"] for p in m["spec"]["pipelineRef"]["params"]}
    assert ref_params["url"] == "https://github.com/konflux-ci/release-service-catalog.git"
    assert ref_params["revision"] == "development"


def test_main_happy_path(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys) -> None:
    """Create PLR manifest, kubectl create, wait success, TEST_OUTPUT SUCCESS."""
    monkeypatch.setenv("KUBECONFIG", "/tmp/k")
    monkeypatch.setenv("CATALOG_GIT_URL", "https://github.com/o/c.git")
    monkeypatch.setenv("CATALOG_GIT_REVISION", "dev")
    monkeypatch.setenv("CATALOG_E2E_RUNNER_IMAGE", "quay.io/runner:v1")
    monkeypatch.setenv("PIPELINE_TEST_SUITE", "suite1")
    monkeypatch.setenv("PIPELINE_USED", "pipe1")
    monkeypatch.setenv("ORCHESTRATOR_PIPELINE_RUN_UID", "abc-123")
    monkeypatch.setenv("E2E_WAIT_TIMEOUT", "60")

    manifests: list[dict] = []

    def fake_mkstemp(suffix: str = "", **kw):
        path = tmp_path / "plr.json"
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        return (fd, str(path))

    monkeypatch.setattr(rs.tempfile, "mkstemp", fake_mkstemp)

    def fake_check_output(cmd: list, **kwargs):
        assert "-f" in cmd
        plr_path = cmd[cmd.index("-f") + 1]
        with open(plr_path, encoding="utf-8") as f:
            manifests.append(json.load(f))
        return "child-plr-name\n"

    monkeypatch.setattr(rs.subprocess, "check_output", fake_check_output)
    monkeypatch.setattr(
        rs,
        "_wait_pipelinerun_terminal",
        lambda **kw: True,
    )
    monkeypatch.setattr(
        rs,
        "_fetch_run_test_task_output_json",
        lambda pr, ns: {"result": "SUCCESS"},
    )

    rs.main()

    assert "child-plr-name" in capsys.readouterr().out
    assert len(manifests) == 1
    assert manifests[0]["metadata"]["name"] == "utils-e2e-catalog-abc-123"


def test_main_wait_failure_exits_1(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Exit 1 when _wait_pipelinerun_terminal returns False."""
    monkeypatch.setenv("KUBECONFIG", "/k")
    monkeypatch.setenv("CATALOG_GIT_URL", "u")
    monkeypatch.setenv("CATALOG_GIT_REVISION", "r")
    monkeypatch.setenv("CATALOG_E2E_RUNNER_IMAGE", "i")
    monkeypatch.setenv("PIPELINE_TEST_SUITE", "s")
    monkeypatch.setenv("PIPELINE_USED", "p")
    monkeypatch.setenv("ORCHESTRATOR_PIPELINE_RUN_UID", "uid")

    def fake_mkstemp(suffix: str = "", **kw):
        path = tmp_path / "plr2.json"
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        return (fd, str(path))

    monkeypatch.setattr(rs.tempfile, "mkstemp", fake_mkstemp)
    monkeypatch.setattr(
        rs.subprocess,
        "check_output",
        lambda cmd, **kw: "n\n",
    )
    monkeypatch.setattr(rs, "_wait_pipelinerun_terminal", lambda **kw: False)

    with pytest.raises(SystemExit) as ei:
        rs.main()
    assert ei.value.code == 1


def test_script_main_guard_exits_when_kubeconfig_missing() -> None:
    """Executing the file as the main program runs ``if __name__ == '__main__': main()``."""
    env = {"PATH": os.environ.get("PATH", "")}
    proc = subprocess.run(
        [sys.executable, str(_SUITE_PY)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 1
    assert "KUBECONFIG" in proc.stderr
