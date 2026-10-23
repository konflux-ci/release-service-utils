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


@pytest.fixture
def operational_logs(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    """Capture operational messages through the configured logging framework."""
    monkeypatch.setattr(rs.logger, "propagate", True)
    caplog.set_level("INFO", logger=rs.logger.name)
    return caplog


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
    monkeypatch.setattr(rs, "_start_run_test_log_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        rs,
        "_pipelinerun_finished",
        lambda n, ns: (True, "ok"),
    )
    monkeypatch.setattr(rs.time, "sleep", lambda _: None)
    assert rs._wait_pipelinerun_terminal(name="x", ns="ns", timeout_seconds=60.0) is True


def test_wait_pipelinerun_terminal_timeout_exits_124(
    monkeypatch: pytest.MonkeyPatch, operational_logs: pytest.LogCaptureFixture
) -> None:
    """Exit 124 when monotonic time passes the deadline while still running."""
    tick = [0.0]

    def mono() -> float:
        tick[0] += 1000.0
        return tick[0]

    monkeypatch.setattr(rs, "_start_run_test_log_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr(rs, "_pipelinerun_finished", lambda n, ns: None)
    monkeypatch.setattr(rs.time, "monotonic", mono)
    monkeypatch.setattr(rs.time, "sleep", lambda _: None)
    with pytest.raises(SystemExit) as ei:
        rs._wait_pipelinerun_terminal(name="x", ns="ns", timeout_seconds=30.0)
    assert ei.value.code == 124
    assert "timeout" in operational_logs.text


def test_wait_pipelinerun_terminal_heartbeat_and_sleep_while_running(
    monkeypatch: pytest.MonkeyPatch, operational_logs: pytest.LogCaptureFixture
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

    monkeypatch.setattr(rs, "_start_run_test_log_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr(rs, "_pipelinerun_finished", finished)
    monkeypatch.setattr(rs.time, "monotonic", mono)
    monkeypatch.setattr(rs.time, "sleep", lambda s: sleeps.append(float(s)))

    assert rs._wait_pipelinerun_terminal(name="pr", ns="ns1", timeout_seconds=120.0) is True
    out = operational_logs.text
    assert "Waiting on pipelinerun/pr" in out, out
    assert "remaining before timeout" in out
    assert sleeps == [10.0]


def test_wait_pipelinerun_terminal_failed_prints_to_stderr(
    monkeypatch: pytest.MonkeyPatch, operational_logs: pytest.LogCaptureFixture
) -> None:
    """Failure path prints the PipelineRun message to stderr."""
    monkeypatch.setattr(rs, "_start_run_test_log_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        rs,
        "_pipelinerun_finished",
        lambda n, ns: (False, "step foo died"),
    )
    monkeypatch.setattr(rs.time, "sleep", lambda _: None)
    assert rs._wait_pipelinerun_terminal(name="bad", ns="ns", timeout_seconds=30.0) is False
    assert "step foo died" in operational_logs.text


def test_print_run_test_step_logs_no_taskrun(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Skip log dump when run-test TaskRun cannot be resolved."""
    monkeypatch.setattr(rs, "_taskrun_name_for_pipeline_task", lambda *a, **k: None)
    rs._print_run_test_step_logs("pr1", "ns1")
    err = capsys.readouterr().err
    assert "skipping log dump" in err
    assert "end run-test logs" not in err


def test_print_run_test_step_logs_via_kubectl(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Stream kubectl logs directly to stderr."""

    def fake_run(cmd: list, **kwargs):
        if cmd[:3] == ["kubectl", "logs", "-n"]:
            assert kwargs.get("stdout") is sys.stderr
            sys.stderr.write("line1\nline2\n")
            return subprocess.CompletedProcess(cmd, 0, None, "")
        raise AssertionError(cmd)

    monkeypatch.setattr(rs, "_taskrun_name_for_pipeline_task", lambda *a, **k: "tr-run-test")
    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    rs._print_run_test_step_logs("pr1", "ns1")
    err = capsys.readouterr().err
    assert "run-test logs" in err
    assert "line1" in err
    assert "end run-test logs" in err


def test_print_run_test_step_logs_kubectl_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Print kubectl stderr when log fetch fails."""

    def fake_run(cmd: list, **kwargs):
        if cmd[:3] == ["kubectl", "logs", "-n"]:
            return subprocess.CompletedProcess(cmd, 1, None, "pods not found\n")
        raise AssertionError(cmd)

    monkeypatch.setattr(rs, "_taskrun_name_for_pipeline_task", lambda *a, **k: "tr-run-test")
    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    rs._print_run_test_step_logs("pr1", "ns1")
    err = capsys.readouterr().err
    assert "could not fetch run-test logs: pods not found" in err


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
    """Missing TEST_OUTPUT JSON exits with code 1."""
    with pytest.raises(SystemExit) as ei:
        rs._require_test_output_success(None)
    assert ei.value.code == 1


def test_require_test_output_success_failure_exits(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A FAILURE TEST_OUTPUT result exits with code 1."""
    with pytest.raises(SystemExit) as ei:
        rs._require_test_output_success({"result": "FAILURE"})
    assert ei.value.code == 1
    assert "FAILURE" in capsys.readouterr().err


def test_require_test_output_success_ok() -> None:
    """A SUCCESS TEST_OUTPUT result is accepted."""
    rs._require_test_output_success({"result": "SUCCESS"})


def test_require_test_output_success_skipped_prints(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A SKIPPED TEST_OUTPUT result is printed and accepted."""
    rs._require_test_output_success({"result": "SKIPPED"})
    assert "SKIPPED" in capsys.readouterr().out


def test_require_test_output_success_unexpected_exits(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An unexpected TEST_OUTPUT result exits with code 1."""
    with pytest.raises(SystemExit) as ei:
        rs._require_test_output_success({"result": "WAT"})
    assert ei.value.code == 1
    assert "unexpected" in capsys.readouterr().err


def test_build_snapshot_shape() -> None:
    """``_build_snapshot`` returns the expected application and component fields."""
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
    """``_build_catalog_e2e_pipelinerun`` wires snapshot and parent metadata."""
    snap = rs._build_snapshot(runner="r", url="u", rev="v")
    m = rs._build_catalog_e2e_pipelinerun(
        ns="konflux-release-service-tenant",
        child_plr_name="utils-e2e-catalog-uid1",
        parent="parent-plr",
        suite="my-suite",
        suite_vars='{"var":"val"}',
        snap=snap,
        pipeline_used="pipe-a",
        vault_password_secret_name="vp",
        github_token_secret_name="gt",
    )
    assert m["kind"] == "PipelineRun"
    assert m["metadata"]["name"] == "utils-e2e-catalog-uid1"
    assert m["metadata"]["labels"]["utils-e2e/suite"] == "my-suite"
    assert (
        m["spec"]["taskRunTemplate"]["serviceAccountName"] == rs.CHILD_PIPELINE_SERVICE_ACCOUNT
    )
    params = {p["name"]: p["value"] for p in m["spec"]["params"]}
    assert params["PIPELINE_TEST_SUITE"] == "my-suite"
    assert params["PIPELINE_TEST_SUITE_VARS"] == '{"var":"val"}'
    assert params["PIPELINE_USED"] == "pipe-a"
    assert params["VAULT_PASSWORD_SECRET_NAME"] == "vp"
    assert "KUBECONFIG_SECRET_NAME" not in params
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
    monkeypatch.setenv("PIPELINE_TEST_SUITE_VARS", '{"var":"val"}')
    monkeypatch.setenv("PIPELINE_USED", "pipe1")
    monkeypatch.setenv("ORCHESTRATOR_PIPELINE_RUN_UID", "abc-123")
    monkeypatch.setenv("E2E_WAIT_TIMEOUT", "60")

    manifests: list[dict] = []

    def fake_mkstemp(suffix: str = "", **kw):
        path = tmp_path / "plr.json"
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        return (fd, str(path))

    monkeypatch.setattr(rs.tempfile, "mkstemp", fake_mkstemp)

    def fake_run(cmd: list, **kwargs):
        assert "-f" in cmd
        plr_path = cmd[cmd.index("-f") + 1]
        with open(plr_path, encoding="utf-8") as f:
            manifests.append(json.load(f))
        return subprocess.CompletedProcess(
            cmd, returncode=0, stdout="child-plr-name\n", stderr=""
        )

    monkeypatch.setattr(rs.subprocess, "run", fake_run)
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


def test_main_wait_failure_exits_1(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys) -> None:
    """Exit 1 when _wait_pipelinerun_terminal returns False and dump run-test logs."""
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
        "run",
        lambda cmd, **kw: subprocess.CompletedProcess(
            cmd, returncode=0, stdout="n\n", stderr=""
        ),
    )
    monkeypatch.setattr(rs, "_wait_pipelinerun_terminal", lambda **kw: False)
    log_calls: list[tuple[str, str]] = []

    def fake_logs(pr: str, ns: str) -> None:
        log_calls.append((pr, ns))
        print("dumped run-test logs", file=sys.stderr)

    monkeypatch.setattr(rs, "_print_run_test_step_logs", fake_logs)

    with pytest.raises(SystemExit) as ei:
        rs.main()
    assert ei.value.code == 1
    assert log_calls == [("n", rs.DEFAULT_CATALOG_E2E_NAMESPACE)]
    assert "dumped run-test logs" in capsys.readouterr().err


@pytest.mark.parametrize(
    "test_output",
    [
        {"result": "FAILURE"},
        {"result": "WAT"},
        {},
    ],
)
def test_main_bad_test_output_dumps_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys,
    test_output: dict,
) -> None:
    """Dump run-test logs for any TEST_OUTPUT outcome that is not SUCCESS or SKIPPED."""
    monkeypatch.setenv("KUBECONFIG", "/tmp/k")
    monkeypatch.setenv("CATALOG_GIT_URL", "https://github.com/o/c.git")
    monkeypatch.setenv("CATALOG_GIT_REVISION", "dev")
    monkeypatch.setenv("CATALOG_E2E_RUNNER_IMAGE", "quay.io/runner:v1")
    monkeypatch.setenv("PIPELINE_TEST_SUITE", "suite1")
    monkeypatch.setenv("PIPELINE_USED", "pipe1")
    monkeypatch.setenv("ORCHESTRATOR_PIPELINE_RUN_UID", "abc-123")

    def fake_mkstemp(suffix: str = "", **kw):
        path = tmp_path / "plr3.json"
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        return (fd, str(path))

    monkeypatch.setattr(rs.tempfile, "mkstemp", fake_mkstemp)
    monkeypatch.setattr(
        rs.subprocess,
        "run",
        lambda cmd, **kw: subprocess.CompletedProcess(
            cmd, returncode=0, stdout="child-plr\n", stderr=""
        ),
    )
    monkeypatch.setattr(rs, "_wait_pipelinerun_terminal", lambda **kw: True)
    monkeypatch.setattr(
        rs,
        "_fetch_run_test_task_output_json",
        lambda pr, ns: test_output,
    )
    log_calls: list[tuple[str, str]] = []

    def fake_logs(pr: str, ns: str) -> None:
        log_calls.append((pr, ns))
        print("bad outcome log dump", file=sys.stderr)

    monkeypatch.setattr(rs, "_print_run_test_step_logs", fake_logs)

    with pytest.raises(SystemExit) as ei:
        rs.main()
    assert ei.value.code == 1
    assert log_calls == [("child-plr", rs.DEFAULT_CATALOG_E2E_NAMESPACE)]
    assert "bad outcome log dump" in capsys.readouterr().err


def test_main_missing_test_output_dumps_logs(
    monkeypatch: pytest.MonkeyPatch, tmp_path, capsys
) -> None:
    """Dump run-test logs when TEST_OUTPUT could not be loaded."""
    monkeypatch.setenv("KUBECONFIG", "/tmp/k")
    monkeypatch.setenv("CATALOG_GIT_URL", "https://github.com/o/c.git")
    monkeypatch.setenv("CATALOG_GIT_REVISION", "dev")
    monkeypatch.setenv("CATALOG_E2E_RUNNER_IMAGE", "quay.io/runner:v1")
    monkeypatch.setenv("PIPELINE_TEST_SUITE", "suite1")
    monkeypatch.setenv("PIPELINE_USED", "pipe1")
    monkeypatch.setenv("ORCHESTRATOR_PIPELINE_RUN_UID", "abc-123")

    def fake_mkstemp(suffix: str = "", **kw):
        path = tmp_path / "plr4.json"
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        return (fd, str(path))

    monkeypatch.setattr(rs.tempfile, "mkstemp", fake_mkstemp)
    monkeypatch.setattr(
        rs.subprocess,
        "run",
        lambda cmd, **kw: subprocess.CompletedProcess(
            cmd, returncode=0, stdout="child-plr\n", stderr=""
        ),
    )
    monkeypatch.setattr(rs, "_wait_pipelinerun_terminal", lambda **kw: True)
    monkeypatch.setattr(rs, "_fetch_run_test_task_output_json", lambda pr, ns: None)
    log_calls: list[tuple[str, str]] = []

    def fake_logs(pr: str, ns: str) -> None:
        log_calls.append((pr, ns))
        print("missing test output log dump", file=sys.stderr)

    monkeypatch.setattr(rs, "_print_run_test_step_logs", fake_logs)

    with pytest.raises(SystemExit) as ei:
        rs.main()
    assert ei.value.code == 1
    assert log_calls == [("child-plr", rs.DEFAULT_CATALOG_E2E_NAMESPACE)]
    assert "missing test output log dump" in capsys.readouterr().err


def test_catalog_e2e_namespace_prefers_env() -> None:
    """CATALOG_E2E_NAMESPACE wins over the serviceaccount file."""
    assert (
        rs.catalog_e2e_namespace(
            environ={"CATALOG_E2E_NAMESPACE": "konflux-release-service-tenant"},
            sa_namespace_path=Path("/does-not-exist"),
        )
        == "konflux-release-service-tenant"
    )


def test_catalog_e2e_namespace_uses_sa_file(tmp_path: Path) -> None:
    """In-cluster namespace file is used when the env var is unset."""
    sa_file = tmp_path / "namespace"
    sa_file.write_text("from-sa\n", encoding="utf-8")
    assert rs.catalog_e2e_namespace(environ={}, sa_namespace_path=sa_file) == "from-sa"


def test_catalog_e2e_namespace_defaults_when_unset() -> None:
    """Fall back to the staging tenant when env and sa file are absent."""
    assert (
        rs.catalog_e2e_namespace(
            environ={},
            sa_namespace_path=Path("/does-not-exist"),
        )
        == rs.DEFAULT_CATALOG_E2E_NAMESPACE
    )


def test_main_in_cluster_uses_env_namespace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """KUBECONFIG is optional; child PLR namespace comes from env."""
    monkeypatch.delenv("KUBECONFIG", raising=False)
    monkeypatch.setenv("CATALOG_E2E_NAMESPACE", "konflux-release-service-tenant")
    monkeypatch.setenv("CATALOG_GIT_URL", "https://github.com/o/c.git")
    monkeypatch.setenv("CATALOG_GIT_REVISION", "dev")
    monkeypatch.setenv("CATALOG_E2E_RUNNER_IMAGE", "quay.io/runner:v1")
    monkeypatch.setenv("PIPELINE_TEST_SUITE", "suite1")
    monkeypatch.setenv("PIPELINE_USED", "pipe1")
    monkeypatch.setenv("ORCHESTRATOR_PIPELINE_RUN_UID", "abc-123")

    manifests: list[dict] = []

    def fake_mkstemp(suffix: str = "", **kw: object) -> tuple[int, str]:
        path = tmp_path / "plr-stage.json"
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        return (fd, str(path))

    monkeypatch.setattr(rs.tempfile, "mkstemp", fake_mkstemp)

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        plr_path = cmd[cmd.index("-f") + 1]
        with open(plr_path, encoding="utf-8") as f:
            manifests.append(json.load(f))
        return subprocess.CompletedProcess(
            cmd, returncode=0, stdout="child-plr-name\n", stderr=""
        )

    monkeypatch.setattr(rs.subprocess, "run", fake_run)
    monkeypatch.setattr(rs, "_wait_pipelinerun_terminal", lambda **kw: True)
    monkeypatch.setattr(
        rs,
        "_fetch_run_test_task_output_json",
        lambda pr, ns: {"result": "SUCCESS"},
    )

    rs.main()

    assert "child-plr-name" in capsys.readouterr().out
    assert manifests[0]["metadata"]["namespace"] == "konflux-release-service-tenant"
    assert (
        manifests[0]["spec"]["taskRunTemplate"]["serviceAccountName"]
        == rs.CHILD_PIPELINE_SERVICE_ACCOUNT
    )


def test_script_main_guard_exits_when_required_env_missing() -> None:
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
    assert "required" in proc.stderr


@pytest.mark.parametrize("taskrun_name,pod_name", [(None, ""), ("tr", ""), ("tr", "pod")])
def test_start_run_test_log_stream(
    monkeypatch: pytest.MonkeyPatch, taskrun_name: str | None, pod_name: str
) -> None:
    """Wait for pod discovery and follow the test container without capturing its output."""
    monkeypatch.setattr(
        rs, "_taskrun_name_for_pipeline_task", lambda *args, **kwargs: taskrun_name
    )
    monkeypatch.setattr(
        rs.subprocess,
        "run",
        MagicMock(return_value=subprocess.CompletedProcess([], 0, pod_name, "")),
    )
    start_process = MagicMock()
    monkeypatch.setattr(rs.subprocess, "Popen", start_process)
    result = rs._start_run_test_log_stream("pr", "ns", deadline=rs.time.monotonic() + 60)
    if not pod_name:
        assert result is None
        start_process.assert_not_called()
    else:
        assert result is start_process.return_value
        command = start_process.call_args.args[0]
        assert "pod/pod" in command
        assert command[command.index("-c") + 1] == "step-run-test"
        assert "--follow" in command
        assert "--tail=-1" in command
        assert start_process.call_args.kwargs == {"stdout": sys.stderr, "stderr": sys.stderr}


@pytest.mark.parametrize("stream_exit", [None, 0, 1])
def test_wait_pipelinerun_stream_retries(
    monkeypatch: pytest.MonkeyPatch, stream_exit: int | None
) -> None:
    """Retry discovery and failed streams, but retain running or completed streams."""
    stream = MagicMock()
    stream.poll.return_value = stream_exit
    start_stream = MagicMock(side_effect=[None, stream, stream])
    monkeypatch.setattr(rs, "_start_run_test_log_stream", start_stream)
    monkeypatch.setattr(
        rs, "_pipelinerun_finished", MagicMock(side_effect=[None, None, None, (True, "ok")])
    )
    monkeypatch.setattr(rs.time, "sleep", lambda _: None)
    assert rs._wait_pipelinerun_terminal(name="pr", ns="ns", timeout_seconds=60)
    assert start_stream.call_count == (3 if stream_exit == 1 else 2)
    stream.wait.assert_called_once_with(timeout=5)


@pytest.mark.parametrize("failure", ["timeout", "kubectl"])
def test_wait_pipelinerun_cleans_up_log_stream(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """Reap the follower even when the status poll or wait exits early."""
    stream = MagicMock()
    monkeypatch.setattr(rs, "_start_run_test_log_stream", lambda *args, **kwargs: stream)
    monkeypatch.setattr(
        rs, "_pipelinerun_finished", MagicMock(side_effect=[None, SystemExit(1)])
    )
    current_time = [0.0]

    def sleep(_seconds: float) -> None:
        current_time[0] += 100 if failure == "timeout" else 10

    monkeypatch.setattr(rs.time, "monotonic", lambda: current_time[0])
    monkeypatch.setattr(rs.time, "sleep", sleep)
    with pytest.raises(SystemExit) as error:
        rs._wait_pipelinerun_terminal(name="pr", ns="ns", timeout_seconds=60)
    assert error.value.code == (124 if failure == "timeout" else 1)
    stream.wait.assert_called_once_with(timeout=5)


@pytest.mark.parametrize("timeouts", [0, 1, 2])
def test_stop_run_test_log_stream(timeouts: int) -> None:
    """Allow final output to drain, escalating to terminate and kill only if necessary."""
    stream = MagicMock()
    stream.wait.side_effect = [subprocess.TimeoutExpired("kubectl", 5)] * timeouts + [0]
    rs._stop_run_test_log_stream(stream)
    assert stream.terminate.call_count == int(timeouts >= 1)
    assert stream.kill.call_count == int(timeouts == 2)
    assert stream.wait.call_count == timeouts + 1


@pytest.mark.parametrize("success", [True, False])
def test_terminal_poll_does_not_start_follower(
    monkeypatch: pytest.MonkeyPatch, success: bool
) -> None:
    """Do not launch a follower when the PipelineRun has already finished."""
    monkeypatch.setattr(rs, "_pipelinerun_finished", lambda *args: (success, "done"))
    start_stream = MagicMock()
    monkeypatch.setattr(rs, "_start_run_test_log_stream", start_stream)
    assert rs._wait_pipelinerun_terminal(name="pr", ns="ns", timeout_seconds=60) == success
    start_stream.assert_not_called()


@pytest.mark.parametrize("timed_out_lookup", [1, 2])
def test_log_discovery_timeout_is_retryable(
    monkeypatch: pytest.MonkeyPatch, timed_out_lookup: int
) -> None:
    """Bound both discovery commands by the shrinking deadline and retry timeouts."""
    current_time = [0.0]
    lookup_timeouts = []

    def lookup(command, **kwargs):
        lookup_timeouts.append(kwargs["timeout"])
        if len(lookup_timeouts) == timed_out_lookup:
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        current_time[0] += 2
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "status": {
                        "childReferences": [{"pipelineTaskName": "run-test", "name": "tr"}]
                    }
                }
            ),
            "",
        )

    monkeypatch.setattr(rs.time, "monotonic", lambda: current_time[0])
    monkeypatch.setattr(rs.subprocess, "run", lookup)
    start_process = MagicMock()
    monkeypatch.setattr(rs.subprocess, "Popen", start_process)
    assert rs._start_run_test_log_stream("pr", "ns", deadline=5) is None
    assert lookup_timeouts == ([5] if timed_out_lookup == 1 else [5, 3])
    start_process.assert_not_called()


def test_expired_log_discovery_does_not_run_kubectl(monkeypatch) -> None:
    """Skip discovery entirely once the PipelineRun wait budget is exhausted."""
    monkeypatch.setattr(rs.time, "monotonic", lambda: 5)
    lookup = MagicMock()
    monkeypatch.setattr(rs.subprocess, "run", lookup)
    assert rs._start_run_test_log_stream("pr", "ns", deadline=5) is None
    lookup.assert_not_called()
