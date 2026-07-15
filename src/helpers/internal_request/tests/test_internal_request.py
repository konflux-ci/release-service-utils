"""Test internal_request_results helpers."""

from __future__ import annotations

from pathlib import Path

from release_service_utils.helpers.internal_request import internal_request_results


def test_write_result_paths(tmp_path: Path) -> None:
    """Write pipeline and task run names to Tekton result files."""
    pr_path = tmp_path / "pr"
    tr_path = tmp_path / "tr"
    internal_request_results.write_result_paths(
        {"internal_pr_name": pr_path, "internal_task_run_name": tr_path},
        pipeline_run_name="pr-123",
        task_run_name="tr-456",
    )
    assert pr_path.read_text(encoding="utf-8") == "pr-123"
    assert tr_path.read_text(encoding="utf-8") == "tr-456"
