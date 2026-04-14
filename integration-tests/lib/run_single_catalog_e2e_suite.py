#!/usr/bin/env python3
"""Create one PipelineRun for catalog integration tests and wait for completion.

Usage: python3 run_single_catalog_e2e_suite.py

Invokes the pipeline defined in catalog at
``integration-tests/pipelines/e2e-tests-staging-pipeline.yaml``.

Used by ``utils-e2e-catalog-pipeline`` task ``run-catalog-e2e`` (this file:
``run_single_catalog_e2e_suite.py``). Expects a single suite pair in env.

Required env:
  KUBECONFIG (path to kubeconfig for ``kubectl`` create/wait; from
  ``orchestrationKubeconfigSecretName``)
  CATALOG_GIT_URL, CATALOG_GIT_REVISION, CATALOG_E2E_RUNNER_IMAGE,
  PIPELINE_TEST_SUITE, PIPELINE_USED,
  VAULT_PASSWORD_SECRET_NAME, GITHUB_TOKEN_SECRET_NAME, KUBECONFIG_SECRET_NAME,
  ORCHESTRATOR_PIPELINE_RUN_UID — orchestrator PLR ``metadata.uid`` (pipeline sets
  ``$(context.pipelineRun.uid)``); child PLR name is ``utils-e2e-catalog-<uid>``
  (same suffix as the temp GitHub fork).

``KUBECONFIG_SECRET_NAME`` is the Secret **name** passed to the child catalog
``PipelineRun`` as pipeline param ``KUBECONFIG_SECRET_NAME`` (stage/test cluster
kubeconfig for ``e2e-tests-staging-pipeline`` tasks).

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
from pathlib import Path

from catalog_e2e_helpers import require_env

CATALOG_E2E_NAMESPACE = "rhtap-release-2-tenant"


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
    while time.monotonic() < deadline:
        fin = _pipelinerun_finished(name, ns)
        if fin is not None:
            success, msg = fin
            if success:
                return True
            print(f"PipelineRun {name} failed: {msg}", file=sys.stderr)
            return False
        now = time.monotonic()
        if now - last_hb >= heartbeat:
            left = int(deadline - now)
            print(
                f"Waiting on pipelinerun/{name} in {ns} ({left}s remaining before timeout)...",
                flush=True,
            )
            last_hb = now
        time.sleep(interval)
    print(
        f"ERROR: timeout waiting for pipelinerun/{name} in {ns} ({timeout_seconds:g}s)",
        file=sys.stderr,
    )
    sys.exit(124)


def _taskrun_name_for_pipeline_task(pr_name: str, ns: str, pipeline_task: str) -> str | None:
    """Resolve the TaskRun name for a pipeline task from the PipelineRun's childReferences."""
    plr = subprocess.run(
        ["kubectl", "get", "pipelinerun", pr_name, "-n", ns, "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
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
    snap: dict[str, object],
    pipeline_used: str,
    vault_password_secret_name: str,
    github_token_secret_name: str,
    kubeconfig_secret_name: str,
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
                {"name": "PIPELINE_USED", "value": pipeline_used},
                {"name": "VAULT_PASSWORD_SECRET_NAME", "value": vault_password_secret_name},
                {"name": "GITHUB_TOKEN_SECRET_NAME", "value": github_token_secret_name},
                {"name": "KUBECONFIG_SECRET_NAME", "value": kubeconfig_secret_name},
            ],
        },
    }


def main() -> None:
    """Create the catalog test PipelineRun from env, wait for it, and validate TEST_OUTPUT."""
    require_env("KUBECONFIG")
    ns = CATALOG_E2E_NAMESPACE
    url = require_env("CATALOG_GIT_URL")
    rev = require_env("CATALOG_GIT_REVISION")
    runner = require_env("CATALOG_E2E_RUNNER_IMAGE")
    suite = require_env("PIPELINE_TEST_SUITE")
    used = require_env("PIPELINE_USED")

    parent = os.environ.get("PARENT_PIPELINE_RUN", "")
    wait_seconds = float(os.environ.get("E2E_WAIT_TIMEOUT") or "14400")
    orch_uid = require_env("ORCHESTRATOR_PIPELINE_RUN_UID")
    child_plr_name = f"utils-e2e-catalog-{orch_uid}"
    vault = os.environ.get("VAULT_PASSWORD_SECRET_NAME", "e2e-test-vault-password")
    gh = os.environ.get("GITHUB_TOKEN_SECRET_NAME", "e2e-test-github-token")
    kc = os.environ.get("KUBECONFIG_SECRET_NAME", "e2e-test-service-account-kubeconfig")

    snap = _build_snapshot(runner=runner, url=url, rev=rev)
    plr_manifest = _build_catalog_e2e_pipelinerun(
        ns=ns,
        child_plr_name=child_plr_name,
        parent=parent,
        suite=suite,
        snap=snap,
        pipeline_used=used,
        vault_password_secret_name=vault,
        github_token_secret_name=gh,
        kubeconfig_secret_name=kc,
    )

    path: Path | None = None
    try:
        fd, path_str = tempfile.mkstemp(suffix=".json")
        path = Path(path_str)
        with os.fdopen(fd, "w") as f:
            json.dump(plr_manifest, f)

        out = subprocess.check_output(
            [
                "kubectl",
                "create",
                "-f",
                str(path),
                "-o",
                "jsonpath={.metadata.name}",
            ],
            text=True,
        )
        name = out.strip()
        print(
            f"Created catalog test PipelineRun {name} in {ns} for suite {suite!r}", flush=True
        )
        print(f"Waiting for pipelinerun/{name} to finish...", flush=True)
        # Do not use kubectl wait --for=condition=Succeeded: on failure Succeeded
        # stays False, so the step would hang until timeout and delay finally cleanup.
        if not _wait_pipelinerun_terminal(name=name, ns=ns, timeout_seconds=wait_seconds):
            sys.exit(1)
        # Catalog run-test exits 0 even on test failure; status is task TEST_OUTPUT.
        _require_test_output_success(_fetch_run_test_task_output_json(name, ns))
        print(f"PipelineRun {name} succeeded (TEST_OUTPUT ok)", flush=True)
    finally:
        if path is not None:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
