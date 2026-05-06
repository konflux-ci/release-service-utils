"""Tests for the internal-request utility script."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "internal-request"


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _run_internal_request(
    tmp_path: Path,
    labels: list[str] | None = None,
    existing_irs: list[str] | None = None,
    delete_fails: bool = False,
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    kubectl_log = tmp_path / "kubectl.log"
    sleep_log = tmp_path / "sleep.log"

    # Build a JSON array of IR objects for the mock get response.
    ir_names = existing_irs or []
    items_json = ", ".join(f'{{"metadata": {{"name": "{name}"}}}}' for name in ir_names)
    mock_get_json = f'{{"items": [{items_json}]}}'

    delete_exit = "1" if delete_fails else "0"

    _write_executable(
        bin_dir / "kubectl",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "$KUBECTL_LOG"
if [[ "${{1:-}}" == "get" && "${{2:-}}" == "internalrequest" ]]; then
    echo '{mock_get_json}'
    exit 0
fi
if [[ "${{1:-}}" == "delete" && "${{2:-}}" == "internalrequest" ]]; then
    exit {delete_exit}
fi
if [[ "${{1:-}}" == "create" ]]; then
    cat >/dev/null
    echo '{{"metadata":{{"name":"new-ir"}}}}'
    exit 0
fi
exit 1
""",
    )

    _write_executable(
        bin_dir / "sleep",
        """#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "$SLEEP_LOG"
""",
    )

    cmd = [
        "bash",
        str(SCRIPT_PATH),
        "--pipeline",
        "test-pipeline",
        "-p",
        "taskGitUrl=https://github.com/konflux-ci/release-service-catalog",
        "-p",
        "taskGitRevision=main",
        "-s",
        "false",
    ]
    for label in labels or []:
        cmd.extend(["-l", label])

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["KUBECTL_LOG"] = str(kubectl_log)
    env["SLEEP_LOG"] = str(sleep_log)

    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)

    kubectl_calls = kubectl_log.read_text(encoding="utf-8").splitlines()
    sleep_calls = (
        sleep_log.read_text(encoding="utf-8").splitlines() if sleep_log.exists() else []
    )
    return result, kubectl_calls, sleep_calls


def test_internal_request_cleans_up_existing_requests(tmp_path):
    result, kubectl_calls, sleep_calls = _run_internal_request(
        tmp_path=tmp_path,
        labels=["internal-services.appstudio.openshift.io/pipelinerun-uid=uid-123"],
        existing_irs=["old-ir-1", "old-ir-2"],
    )

    assert result.returncode == 0, result.stderr
    selector = (
        "get internalrequest -l "
        "internal-services.appstudio.openshift.io/pipelinerun-uid=uid-123"
    )
    assert any(call.startswith(selector) for call in kubectl_calls)
    assert any(c.startswith("delete internalrequest old-ir-1") for c in kubectl_calls)
    assert any(c.startswith("delete internalrequest old-ir-2") for c in kubectl_calls)
    assert "5" in sleep_calls
    assert any(call.startswith("create -f - -o json") for call in kubectl_calls)


def test_internal_request_skips_cleanup_when_no_existing_requests(tmp_path):
    result, kubectl_calls, sleep_calls = _run_internal_request(
        tmp_path=tmp_path,
        labels=["internal-services.appstudio.openshift.io/pipelinerun-uid=uid-123"],
        existing_irs=[],
    )

    assert result.returncode == 0, result.stderr
    assert any(call.startswith("get internalrequest -l") for call in kubectl_calls)
    assert not any(call.startswith("delete internalrequest") for call in kubectl_calls)
    assert sleep_calls == []
    assert any(call.startswith("create -f - -o json") for call in kubectl_calls)


def test_internal_request_skips_cleanup_without_pipelinerun_uid_label(tmp_path):
    result, kubectl_calls, sleep_calls = _run_internal_request(
        tmp_path=tmp_path,
        labels=["some-other-label=foo"],
        existing_irs=["old-ir-1"],
    )

    assert result.returncode == 0, result.stderr
    assert not any(call.startswith("get internalrequest -l") for call in kubectl_calls)
    assert not any(call.startswith("delete internalrequest") for call in kubectl_calls)
    assert sleep_calls == []
    assert any(call.startswith("create -f - -o json") for call in kubectl_calls)


def test_internal_request_fails_when_delete_fails(tmp_path):
    result, kubectl_calls, sleep_calls = _run_internal_request(
        tmp_path=tmp_path,
        labels=["internal-services.appstudio.openshift.io/pipelinerun-uid=uid-123"],
        existing_irs=["old-ir-1"],
        delete_fails=True,
    )

    assert result.returncode != 0, "Expected non-zero exit when delete fails"
    assert any(c.startswith("delete internalrequest old-ir-1") for c in kubectl_calls)
    assert not any(call.startswith("create -f - -o json") for call in kubectl_calls)
