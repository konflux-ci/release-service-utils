"""Helpers for interacting with InternalRequests."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path


def write_result_paths(
    result_paths: Mapping[str, Path],
    *,
    pipeline_run_name: str,
    task_run_name: str,
) -> None:
    """
    Write pipeline and task run names to the internal-request Tekton results.

    *result_paths* must include ``internal_pr_name`` and ``internal_task_run_name``
    keys mapping to the result file paths from the task step.
    """
    result_paths["internal_pr_name"].write_text(pipeline_run_name, encoding="utf-8")
    result_paths["internal_task_run_name"].write_text(task_run_name, encoding="utf-8")
