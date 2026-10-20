#!/usr/bin/env python3
"""Create one PipelineRun for catalog integration tests and wait for completion.

Usage: python3 run_single_catalog_e2e_suite.py

Invokes the pipeline defined in catalog at
``integration-tests/pipelines/e2e-tests-staging-pipeline.yaml``.

Used by ``utils-e2e-catalog-pipeline`` task ``run-catalog-e2e`` (this file:
``run_single_catalog_e2e_suite.py``). Expects a single suite pair in env.

Required env:
  CATALOG_GIT_URL, CATALOG_GIT_REVISION, CATALOG_E2E_RUNNER_IMAGE,
  PIPELINE_TEST_SUITE, PIPELINE_USED,
  VAULT_PASSWORD_SECRET_NAME, GITHUB_TOKEN_SECRET_NAME,
  ORCHESTRATOR_PIPELINE_RUN_UID — orchestrator PLR ``metadata.uid`` (pipeline sets
  ``$(context.pipelineRun.uid)``); child PLR name is ``utils-e2e-catalog-<uid>``
  (same suffix as the temp GitHub fork).

Optional env:
  PIPELINE_TEST_SUITE_VARS
  KUBECONFIG — path to kubeconfig for ``kubectl`` create/wait. The pipeline
  mounts orchestrationKubeconfigSecretName (optional) as a file and sets this
  when the file is non-empty. Unset uses in-cluster config (the PipelineRun
  service account).
  CATALOG_E2E_NAMESPACE — namespace for the child catalog PipelineRun. The
  pipeline sets this from the parent PipelineRun metadata.namespace. Unset
  uses the in-cluster serviceaccount namespace, else
  ``konflux-release-service-tenant``.

Optional:
  E2E_WAIT_TIMEOUT — max wait for the child catalog PipelineRun, in seconds
  (default 14400 = 4h).
  PARENT_PIPELINE_RUN

The catalog ``run-test`` task **always exits 0** and encodes outcome in task
result **TEST_OUTPUT** (JSON ``result``: SUCCESS | FAILURE | SKIPPED). This
script fails if **TEST_OUTPUT** is FAILURE, matching how Konflux reads
integration tests — **not** ``PipelineRun.status`` alone.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path

from catalog_e2e_helpers import require_env, opt_env
from release_service_utils.helpers.logger import logger

DEFAULT_CATALOG_E2E_NAMESPACE = "konflux-release-service-tenant"
CHILD_PIPELINE_SERVICE_ACCOUNT = "konflux-integration-runner"
_SA_NAMESPACE_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")


def catalog_e2e_namespace(
    environ: Mapping[str, str] | None = None,
    sa_namespace_path: Path = _SA_NAMESPACE_PATH,
) -> str:
    """Return the namespace for the child catalog e2e PipelineRun.

    Prefers CATALOG_E2E_NAMESPACE, then the in-cluster serviceaccount
    namespace, then DEFAULT_CATALOG_E2E_NAMESPACE.
    """
    env = os.environ if environ is None else environ
    from_env = env.get("CATALOG_E2E_NAMESPACE", "").strip()
    if from_env:
        return from_env
    try:
        from_sa = sa_namespace_path.read_text(encoding="utf-8").strip()
    except OSError:
        from_sa = ""
    return from_sa or DEFAULT_CATALOG_E2E_NAMESPACE


def _pipelinerun_finished(name: str, ns: str) -> tuple[bool, str] | None:
    """Return (succeeded, message) when done; None while still running."""
    plr = subprocess.run(
        [
            "kubectl",
            "get",
            f"pipelinerun/{name}",
            "-n",
            ns,
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if plr.returncode != 0:
        print(plr.stderr or plr.stdout, file=sys.stderr)
        sys.exit(1)
    try:
        plr_json = json.loads(plr.stdout)
    except json.JSONDecodeError as e:
        print(f"ERROR: invalid PipelineRun JSON from kubectl: {e}", file=sys.stderr)
        sys.exit(1)
    status = plr_json.get("status") or {}
    if not (status.get("completionTime") or "").strip():
        return None
    for cond in status.get("conditions") or []:
        if cond.get("type") == "Succeeded":
            ok = cond.get("status") == "True"
            msg = (cond.get("message") or "").strip() or "(no message)"
            return (ok, msg)
    return (False, "(no Succeeded condition)")


def _wait_pipelinerun_terminal(*, name: str, ns: str, timeout_seconds: float) -> bool:
    """Wait until completionTime is set; return True if Succeeded=True.

    Exits on timeout or kubectl error.
    """
    deadline = time.monotonic() + timeout_seconds
    interval = 10.0
    heartbeat = 60.0
    last_hb = time.monotonic()
    log_stream: subprocess.Popen | None = None
    try:
        while time.monotonic() < deadline:
            fin = _pipelinerun_finished(name, ns)
            if fin is not None:
                success, msg = fin
                if success:
                    return True
                logger.error("PipelineRun %s failed: %s", name, msg)
                return False
            # Retry startup or connection failures; reconnects may replay earlier logs.
            if log_stream is None or log_stream.poll() not in (None, 0):
                log_stream = _start_run_test_log_stream(name, ns, deadline=deadline)
            now = time.monotonic()
            if now - last_hb >= heartbeat:
                left = int(deadline - now)
                logger.info(
                    "Waiting on pipelinerun/%s in %s (%ss remaining before timeout)...",
                    name,
                    ns,
                    max(0, left),
                )
                last_hb = now
            time.sleep(min(interval, max(0, deadline - now)))
        logger.error(
            "timeout waiting for pipelinerun/%s in %s (%gs)", name, ns, timeout_seconds
        )
        sys.exit(124)
    finally:
        if log_stream is not None:
            _stop_run_test_log_stream(log_stream)


def _taskrun_name_for_pipeline_task(
    pr_name: str, ns: str, pipeline_task: str, *, deadline: float | None = None
) -> str | None:
    """Resolve the TaskRun name for a pipeline task from the PipelineRun's childReferences."""
    plr = subprocess.run(
        ["kubectl", "get", "pipelinerun", pr_name, "-n", ns, "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
        timeout=_log_discovery_timeout(deadline) if deadline is not None else None,
    )
    if plr.returncode != 0:
        print(plr.stderr or plr.stdout, file=sys.stderr)
        return None
    plr_json = json.loads(plr.stdout)
    for ref in plr_json.get("status", {}).get("childReferences", []) or []:
        if ref.get("pipelineTaskName") == pipeline_task:
            name = ref.get("name")
            if isinstance(name, str) and name:
                return name
    return None


def _log_discovery_timeout(deadline: float) -> float:
    """Limit each log lookup to ten seconds and the remaining wait budget."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired("log discovery", 0)
    return min(10.0, remaining)


def _start_run_test_log_stream(
    pr_name: str, ns: str, *, deadline: float
) -> subprocess.Popen | None:
    """Follow the catalog test container's logs on stderr once its TaskRun exists."""
    try:
        taskrun_name = _taskrun_name_for_pipeline_task(
            pr_name, ns, "run-test", deadline=deadline
        )
        if not taskrun_name:
            return None
        # A selector-based logs call can exit successfully before any pod exists.
        pod = subprocess.run(
            [
                "kubectl",
                "get",
                "taskrun",
                taskrun_name,
                "-n",
                ns,
                "-o",
                "jsonpath={.status.podName}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=_log_discovery_timeout(deadline),
        )
        if pod.returncode != 0 or not pod.stdout.strip():
            return None
        pod_name = pod.stdout.strip()
        return subprocess.Popen(
            [
                "kubectl",
                "logs",
                "-n",
                ns,
                f"pod/{pod_name}",
                "-c",
                "step-run-test",
                "--follow",
                "--tail=-1",
            ],
            stdout=sys.stderr,
            stderr=sys.stderr,
        )
    except subprocess.TimeoutExpired:
        logger.warning("Timed out discovering run-test logs for pipelinerun/%s", pr_name)
        return None


def _stop_run_test_log_stream(log_stream: subprocess.Popen) -> None:
    """Drain final log output briefly, then stop and reap the follower."""
    try:
        # Completion can arrive before kubectl has forwarded the last log lines.
        log_stream.wait(timeout=5)
    except subprocess.TimeoutExpired:
        log_stream.terminate()
        try:
            log_stream.wait(timeout=5)
        except subprocess.TimeoutExpired:
            log_stream.kill()
            log_stream.wait()


def _print_run_test_step_logs(pr_name: str, ns: str) -> None:
    """Print run-test TaskRun logs to stderr."""
    tr_name = _taskrun_name_for_pipeline_task(pr_name, ns, "run-test")
    if not tr_name:
        print(
            "NOTE: could not resolve run-test TaskRun for "
            f"pipelinerun/{pr_name} in {ns}; skipping log dump",
            file=sys.stderr,
        )
        return

    print(
        f"--- run-test logs (TaskRun {tr_name}, pipelinerun/{pr_name} in {ns}) ---",
        file=sys.stderr,
        flush=True,
    )

    result = subprocess.run(
        [
            "kubectl",
            "logs",
            "-n",
            ns,
            "-l",
            f"tekton.dev/taskRun={tr_name}",
            "--tail=-1",
            "--all-containers",
        ],
        stdout=sys.stderr,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        err = (result.stderr or "empty").rstrip("\n")
        print(f"NOTE: could not fetch run-test logs: {err}", file=sys.stderr)

    print("--- end run-test logs ---", file=sys.stderr, flush=True)


def _fetch_run_test_task_output_json(pr_name: str, ns: str) -> dict | None:
    """Load JSON from TaskRun ``run-test`` result TEST_OUTPUT."""
    tr_name = _taskrun_name_for_pipeline_task(pr_name, ns, "run-test")
    if not tr_name:
        print(
            "ERROR: could not resolve TaskRun name for pipeline task run-test "
            f"(pipelinerun/{pr_name} in {ns})",
            file=sys.stderr,
        )
        return None
    taskrun = subprocess.run(
        ["kubectl", "get", "taskrun", tr_name, "-n", ns, "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if taskrun.returncode != 0:
        print(taskrun.stderr or taskrun.stdout, file=sys.stderr)
        return None
    taskrun_json = json.loads(taskrun.stdout)
    for res in taskrun_json.get("status", {}).get("results", []) or []:
        if res.get("name") != "TEST_OUTPUT":
            continue
        raw = res.get("value")
        if raw is None or raw == "":
            print("ERROR: TaskRun run-test has empty TEST_OUTPUT result", file=sys.stderr)
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"ERROR: invalid TEST_OUTPUT JSON: {e}: {raw!r}", file=sys.stderr)
            return None
    print("ERROR: TaskRun run-test has no TEST_OUTPUT result", file=sys.stderr)
    return None


def _require_test_output_success(payload: dict | None) -> None:
    """Exit 1 unless TEST_OUTPUT documents SUCCESS or SKIPPED (not FAILURE)."""
    if payload is None:
        sys.exit(1)
    outcome = str(payload.get("result", "")).strip().upper()
    if outcome == "FAILURE":
        detail = json.dumps(payload)
        print(
            "ERROR: catalog e2e run-test reported FAILURE in task result TEST_OUTPUT "
            f"(IntegrationTest uses this; PipelineRun may still show Succeeded): {detail}",
            file=sys.stderr,
        )
        sys.exit(1)
    if outcome == "SUCCESS":
        return
    if outcome == "SKIPPED":
        print(
            f"NOTE: catalog e2e run-test reported SKIPPED: {json.dumps(payload)}",
            flush=True,
        )
        return
    print(
        "ERROR: unexpected TEST_OUTPUT result field "
        f"(expected SUCCESS, FAILURE, or SKIPPED): {payload!r}",
        file=sys.stderr,
    )
    sys.exit(1)


def _build_snapshot(*, runner: str, url: str, rev: str) -> dict[str, object]:
    """Build the SNAPSHOT JSON object passed into the catalog e2e PipelineRun."""
    return {
        "application": "utils-orchestrated-e2e",
        "artifacts": {},
        "components": [
            {
                "containerImage": runner,
                "name": "catalog-e2e",
                "source": {"git": {"revision": rev, "url": url}},
            }
        ],
    }


def _build_catalog_e2e_pipelinerun(
    *,
    ns: str,
    child_plr_name: str,
    parent: str,
    suite: str,
    suite_vars: str,
    snap: dict[str, object],
    pipeline_used: str,
    vault_password_secret_name: str,
    github_token_secret_name: str,
) -> dict[str, object]:
    """Build the child catalog e2e PipelineRun manifest (metadata, pipelineRef, params)."""
    return {
        "apiVersion": "tekton.dev/v1",
        "kind": "PipelineRun",
        "metadata": {
            "name": child_plr_name,
            "namespace": ns,
            "labels": {
                "app.kubernetes.io/managed-by": "utils-e2e-catalog-pipeline",
                "utils-e2e/parent": parent,
                "utils-e2e/suite": suite,
            },
        },
        "spec": {
            "taskRunTemplate": {
                "serviceAccountName": CHILD_PIPELINE_SERVICE_ACCOUNT,
            },
            "pipelineRef": {
                "resolver": "git",
                "params": [
                    {
                        "name": "url",
                        "value": "https://github.com/konflux-ci/release-service-catalog.git",
                    },
                    {"name": "revision", "value": "development"},
                    {
                        "name": "pathInRepo",
                        "value": "integration-tests/pipelines/e2e-tests-staging-pipeline.yaml",
                    },
                ],
            },
            "params": [
                {"name": "SNAPSHOT", "value": json.dumps(snap)},
                {"name": "PIPELINE_TEST_SUITE", "value": suite},
                {"name": "PIPELINE_TEST_SUITE_VARS", "value": suite_vars},
                {"name": "PIPELINE_USED", "value": pipeline_used},
                {"name": "VAULT_PASSWORD_SECRET_NAME", "value": vault_password_secret_name},
                {"name": "GITHUB_TOKEN_SECRET_NAME", "value": github_token_secret_name},
            ],
        },
    }


def main() -> None:
    """Create the catalog test PipelineRun from env, wait for it, and validate TEST_OUTPUT."""
    ns = catalog_e2e_namespace()
    url = require_env("CATALOG_GIT_URL")
    rev = require_env("CATALOG_GIT_REVISION")
    runner = require_env("CATALOG_E2E_RUNNER_IMAGE")
    suite = require_env("PIPELINE_TEST_SUITE")
    suite_vars = opt_env("PIPELINE_TEST_SUITE_VARS", "{}")
    used = require_env("PIPELINE_USED")

    parent = os.environ.get("PARENT_PIPELINE_RUN", "")
    wait_seconds = float(os.environ.get("E2E_WAIT_TIMEOUT") or "14400")
    orch_uid = require_env("ORCHESTRATOR_PIPELINE_RUN_UID")
    child_plr_name = f"utils-e2e-catalog-{orch_uid}"
    vault = os.environ.get("VAULT_PASSWORD_SECRET_NAME", "e2e-test-vault-password")
    gh = os.environ.get("GITHUB_TOKEN_SECRET_NAME", "e2e-test-github-token")

    snap = _build_snapshot(runner=runner, url=url, rev=rev)
    plr_manifest = _build_catalog_e2e_pipelinerun(
        ns=ns,
        child_plr_name=child_plr_name,
        parent=parent,
        suite=suite,
        suite_vars=suite_vars,
        snap=snap,
        pipeline_used=used,
        vault_password_secret_name=vault,
        github_token_secret_name=gh,
    )

    path: Path | None = None
    try:
        fd, path_str = tempfile.mkstemp(suffix=".json")
        path = Path(path_str)
        with os.fdopen(fd, "w") as f:
            json.dump(plr_manifest, f)

        out = subprocess.run(
            [
                "kubectl",
                "create",
                "-f",
                str(path),
                "-o",
                "jsonpath={.metadata.name}",
            ],
            text=True,
            capture_output=True,
        )
        if out.returncode != 0:
            sys.exit(
                f"kubectl create failed (exit {out.returncode}):\n"
                f"  stdout: {out.stdout.strip()}\n"
                f"  stderr: {out.stderr.strip()}"
            )

        name = out.stdout.strip()
        print(
            f"Created catalog test PipelineRun {name} in {ns} for suite {suite!r}", flush=True
        )
        print(f"Waiting for pipelinerun/{name} to finish...", flush=True)
        # Do not use kubectl wait --for=condition=Succeeded: on failure Succeeded
        # stays False, so the step would hang until timeout and delay finally cleanup.
        if not _wait_pipelinerun_terminal(name=name, ns=ns, timeout_seconds=wait_seconds):
            _print_run_test_step_logs(name, ns)
            sys.exit(1)
        # Catalog run-test exits 0 even on test failure; status is task TEST_OUTPUT.
        test_output = _fetch_run_test_task_output_json(name, ns)
        outcome = str((test_output or {}).get("result", "")).strip().upper()
        if test_output is None or outcome not in ("SUCCESS", "SKIPPED"):
            _print_run_test_step_logs(name, ns)
        _require_test_output_success(test_output)
        print(f"PipelineRun {name} succeeded (TEST_OUTPUT ok)", flush=True)
    finally:
        if path is not None:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
