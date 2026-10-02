"""Persist retry-safety state to Tekton task result paths."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from release_service_utils.helpers.file.file import load_json_dict

DEFAULT_SAFE_SUMMARY = "No unsafe operations were started."
MAX_RESULT_BYTES = 4096
_STARTED_SUMMARY = "Unsafe operation started."
_COMPLETED_SUMMARY = "Unsafe operation completed."


@dataclass
class RetrySafetyReport:
    """Represent whether a failed task is still safe to retry."""

    is_safe_to_retry: bool = True
    unsafe_operation_in_progress: bool = False
    unsafe_operations_in_progress_count: int = 0
    unsafe_operations_completed: int = 0
    summary: str = DEFAULT_SAFE_SUMMARY
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize the derived in-progress flag from the outstanding count."""
        if self.unsafe_operation_in_progress and self.unsafe_operations_in_progress_count == 0:
            self.unsafe_operations_in_progress_count = 1
        self.unsafe_operation_in_progress = self.unsafe_operations_in_progress_count > 0

    def mark_unsafe_operation_started(
        self,
        summary: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        """Mark the report unsafe once execution crosses the unsafe retry boundary."""
        self.is_safe_to_retry = False
        self.unsafe_operations_in_progress_count += 1
        self.unsafe_operation_in_progress = True
        self.summary = _normalized_summary(summary, _STARTED_SUMMARY)
        self.merge_details(details)

    def mark_unsafe_operation_completed(
        self,
        summary: str,
        *,
        count: int = 1,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        """Record one or more completed unsafe operations."""
        if count < 1:
            raise ValueError("count must be greater than 0")

        self.is_safe_to_retry = False
        self.unsafe_operations_in_progress_count = max(
            0,
            self.unsafe_operations_in_progress_count - count,
        )
        self.unsafe_operation_in_progress = self.unsafe_operations_in_progress_count > 0
        self.unsafe_operations_completed += count
        self.summary = _normalized_summary(summary, _COMPLETED_SUMMARY)
        self.merge_details(details)

    def merge_details(self, details: Mapping[str, Any] | None) -> None:
        """Merge extra report fields into ``details``."""
        if details:
            self.details.update(dict(details))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary for the report."""
        return {
            "is_safe_to_retry": self.is_safe_to_retry,
            "unsafe_operation_in_progress": self.unsafe_operation_in_progress,
            "unsafe_operations_in_progress_count": self.unsafe_operations_in_progress_count,
            "unsafe_operations_completed": self.unsafe_operations_completed,
            "summary": self.summary,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RetrySafetyReport:
        """Build a report from previously persisted JSON data."""
        is_safe_to_retry = data["is_safe_to_retry"]
        unsafe_operation_in_progress = data.get("unsafe_operation_in_progress", False)
        unsafe_operations_in_progress_count = data.get(
            "unsafe_operations_in_progress_count",
            1 if unsafe_operation_in_progress else 0,
        )
        unsafe_operations_completed = data.get("unsafe_operations_completed", 0)
        summary = data.get("summary", DEFAULT_SAFE_SUMMARY)
        details = data.get("details", {})

        if not isinstance(is_safe_to_retry, bool):
            raise TypeError("is_safe_to_retry must be a bool")
        if not isinstance(unsafe_operation_in_progress, bool):
            raise TypeError("unsafe_operation_in_progress must be a bool")
        if not _is_exact_int(unsafe_operations_in_progress_count):
            raise TypeError("unsafe_operations_in_progress_count must be an int")
        if unsafe_operations_in_progress_count < 0:
            raise ValueError("unsafe_operations_in_progress_count must be non-negative")
        if not _is_exact_int(unsafe_operations_completed):
            raise TypeError("unsafe_operations_completed must be an int")
        if unsafe_operations_completed < 0:
            raise ValueError("unsafe_operations_completed must be non-negative")
        if not isinstance(summary, str):
            raise TypeError("summary must be a str")
        if not isinstance(details, dict):
            raise TypeError("details must be an object")
        if unsafe_operation_in_progress != (unsafe_operations_in_progress_count > 0):
            raise ValueError(
                "unsafe_operation_in_progress conflicts with outstanding operations"
            )
        if is_safe_to_retry and (
            unsafe_operations_in_progress_count > 0 or unsafe_operations_completed > 0
        ):
            raise ValueError("is_safe_to_retry conflicts with unsafe operation state")

        return cls(
            is_safe_to_retry=is_safe_to_retry,
            unsafe_operation_in_progress=unsafe_operations_in_progress_count > 0,
            unsafe_operations_in_progress_count=unsafe_operations_in_progress_count,
            unsafe_operations_completed=unsafe_operations_completed,
            summary=summary,
            details=dict(details),
        )


def initialize_result(
    result_path: Path,
    *,
    summary: str = DEFAULT_SAFE_SUMMARY,
) -> RetrySafetyReport:
    """Create and persist a default-safe retry-safety task result."""
    report = RetrySafetyReport(summary=_normalized_summary(summary, DEFAULT_SAFE_SUMMARY))
    write_result(result_path, report)
    return report


def load_result(result_path: Path) -> RetrySafetyReport:
    """Load retry-safety state from a Tekton task result path."""
    return RetrySafetyReport.from_dict(load_json_dict(result_path))


def write_result(result_path: Path, report: RetrySafetyReport) -> None:
    """Write retry-safety state to a Tekton task result path as compact JSON."""
    result_path.parent.mkdir(parents=True, exist_ok=True)
    serialized_report = _serialized_result(report)
    temp_file_descriptor, temp_file_path = tempfile.mkstemp(
        dir=result_path.parent,
        prefix=f"{result_path.name}.",
        suffix=".tmp",
    )

    try:
        # Replace atomically only after the new result is fully written.
        with os.fdopen(temp_file_descriptor, "w", encoding="utf-8") as handle:
            handle.write(serialized_report)
        os.replace(temp_file_path, result_path)
    except Exception:
        os.unlink(temp_file_path)
        raise


def _normalized_summary(summary: str, default: str) -> str:
    """Return a stripped summary or a fallback message."""
    value = summary.strip()
    return value if value else default


def _serialized_result(report: RetrySafetyReport) -> str:
    """Serialize a report while respecting the Tekton result size limit."""
    result_data = RetrySafetyReport.from_dict(report.to_dict()).to_dict()
    serialized_report = json.dumps(
        result_data,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    if len(serialized_report.encode("utf-8")) > MAX_RESULT_BYTES:
        raise ValueError(f"retry-safety result exceeds {MAX_RESULT_BYTES} bytes")
    return serialized_report


def _is_exact_int(value: Any) -> bool:
    """Return True when *value* is an int but not a bool."""
    return type(value) is int
