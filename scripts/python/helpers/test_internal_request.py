"""Tests for `internal_request`."""

from __future__ import annotations

from pathlib import Path

import internal_request


def test_write_result_paths(tmp_path: Path) -> None:
    pr_path = tmp_path / "pr"
    tr_path = tmp_path / "tr"
    internal_request.write_result_paths(
        {"internal_pr_name": pr_path, "internal_task_run_name": tr_path},
        pipeline_run_name="pr-123",
        task_run_name="tr-456",
    )
    assert pr_path.read_text(encoding="utf-8") == "pr-123"
    assert tr_path.read_text(encoding="utf-8") == "tr-456"
