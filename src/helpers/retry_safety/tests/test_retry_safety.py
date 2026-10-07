"""Test the retry-safety helper."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.helpers import retry_safety
from release_service_utils.helpers.retry_safety import retry_safety as retry_safety_module


def _encoded_result(report: retry_safety.RetrySafetyReport) -> bytes:
    """Encode a report exactly as the helper writes it."""
    return json.dumps(
        report.to_dict(),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _report_with_ascii_summary_size(size_in_bytes: int) -> retry_safety.RetrySafetyReport:
    """Build a report whose compact JSON encoding is exactly *size_in_bytes* bytes."""
    report = retry_safety.RetrySafetyReport(is_safe_to_retry=False, summary="")
    summary_length = size_in_bytes - len(_encoded_result(report))
    if summary_length < 0:
        raise ValueError("size_in_bytes is too small for the report shape")
    report.summary = "x" * summary_length
    assert len(_encoded_result(report)) == size_in_bytes
    return report


def test_initialize_report_writes_default_safe_state(tmp_path: Path) -> None:
    """Create a persisted default-safe task result."""
    result_path = tmp_path / "results" / "retry-safety"

    report = retry_safety.initialize_result(result_path)

    assert report.is_safe_to_retry is True
    assert report.unsafe_operation_in_progress is False
    assert report.unsafe_operations_completed == 0
    assert report.summary == retry_safety.DEFAULT_SAFE_SUMMARY
    assert retry_safety.load_result(result_path).to_dict() == report.to_dict()


def test_initialize_report_uses_default_safe_summary_for_blank_input(
    tmp_path: Path,
) -> None:
    """Fall back to the default safe summary when initialization gets blank text."""
    result_path = tmp_path / "results" / "retry-safety"

    report = retry_safety.initialize_result(result_path, summary="   ")

    assert report.summary == retry_safety.DEFAULT_SAFE_SUMMARY


def test_mark_unsafe_operation_started_updates_state() -> None:
    """Mark the report unsafe and in progress after the unsafe boundary is crossed."""
    report = retry_safety.RetrySafetyReport()

    report.mark_unsafe_operation_started(
        "Started pushing images",
        details={"task": "push-snapshot", "images_pushed": 0},
    )

    assert report.is_safe_to_retry is False
    assert report.unsafe_operation_in_progress is True
    assert report.unsafe_operations_in_progress_count == 1
    assert report.unsafe_operations_completed == 0
    assert report.summary == "Started pushing images"
    assert report.details == {"task": "push-snapshot", "images_pushed": 0}


def test_mark_unsafe_operation_started_uses_default_summary_for_blank_input() -> None:
    """Fall back to the default started summary when the provided summary is blank."""
    report = retry_safety.RetrySafetyReport()

    report.mark_unsafe_operation_started("   ")

    assert report.summary == "Unsafe operation started."


def test_mark_unsafe_operation_completed_updates_counts() -> None:
    """Accumulate completed unsafe operations and clear the in-progress bit."""
    report = retry_safety.RetrySafetyReport()
    report.mark_unsafe_operation_started("Started pushing images")

    report.mark_unsafe_operation_completed(
        "Pushed images to registry",
        count=1,
        details={"images_pushed": 4},
    )

    assert report.is_safe_to_retry is False
    assert report.unsafe_operation_in_progress is False
    assert report.unsafe_operations_in_progress_count == 0
    assert report.unsafe_operations_completed == 1
    assert report.summary == "Pushed images to registry"
    assert report.details == {"images_pushed": 4}


def test_mark_unsafe_operation_completed_uses_default_summary_for_blank_input() -> None:
    """Fall back to the default completed summary when the provided summary is blank."""
    report = retry_safety.RetrySafetyReport()

    report.mark_unsafe_operation_completed("  ", count=1)

    assert report.summary == "Unsafe operation completed."


def test_mark_unsafe_operation_completed_keeps_in_progress_flag_for_overlapping_work() -> None:
    """Keep the report in progress until all overlapping unsafe work is completed."""
    report = retry_safety.RetrySafetyReport()

    report.mark_unsafe_operation_started("Started first push")
    report.mark_unsafe_operation_started("Started second push")
    report.mark_unsafe_operation_completed("Completed one push")

    assert report.unsafe_operation_in_progress is True
    assert report.unsafe_operations_in_progress_count == 1
    assert report.unsafe_operations_completed == 1


def test_mark_unsafe_operation_completed_handles_multiple_completions() -> None:
    """Complete multiple outstanding unsafe operations with one call."""
    report = retry_safety.RetrySafetyReport()

    report.mark_unsafe_operation_started("Started first push")
    report.mark_unsafe_operation_started("Started second push")
    report.mark_unsafe_operation_started("Started third push")
    report.mark_unsafe_operation_completed("Completed two pushes", count=2)

    assert report.unsafe_operation_in_progress is True
    assert report.unsafe_operations_in_progress_count == 1
    assert report.unsafe_operations_completed == 2
    assert report.summary == "Completed two pushes"


def test_mark_unsafe_operation_completed_rejects_non_positive_count() -> None:
    """Reject counts less than one when recording completed unsafe operations."""
    report = retry_safety.RetrySafetyReport()

    with pytest.raises(ValueError, match="greater than 0"):
        report.mark_unsafe_operation_completed("Completed nothing", count=0)


def test_write_report_round_trips_details(tmp_path: Path) -> None:
    """Persist and reload the full task-result structure without losing details."""
    result_path = tmp_path / "retry-safety"
    report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        unsafe_operation_in_progress=True,
        unsafe_operations_in_progress_count=1,
        unsafe_operations_completed=2,
        summary="Publish operation still in progress",
        details={"published_repositories": ["a", "b"]},
    )

    retry_safety.write_result(result_path, report)

    loaded = retry_safety.load_result(result_path)
    assert loaded.to_dict() == report.to_dict()


def test_write_result_accepts_payload_at_exact_tekton_limit(
    tmp_path: Path,
) -> None:
    """Persist a result whose compact JSON encoding is exactly 4096 bytes."""
    result_path = tmp_path / "retry-safety"
    report = _report_with_ascii_summary_size(retry_safety_module.MAX_RESULT_BYTES)

    retry_safety.write_result(result_path, report)

    loaded = retry_safety.load_result(result_path)
    assert len(result_path.read_bytes()) == retry_safety_module.MAX_RESULT_BYTES
    assert loaded.to_dict() == report.to_dict()


def test_write_result_rejects_payload_over_tekton_limit_and_preserves_existing_file(
    tmp_path: Path,
) -> None:
    """Reject a 4097-byte result and leave the previous task result untouched."""
    result_path = tmp_path / "retry-safety"
    original_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        unsafe_operations_completed=1,
        summary="Pushed one image",
    )
    retry_safety.write_result(result_path, original_report)
    oversized_report = _report_with_ascii_summary_size(
        retry_safety_module.MAX_RESULT_BYTES + 1
    )

    with pytest.raises(ValueError, match="exceeds"):
        retry_safety.write_result(result_path, oversized_report)

    loaded = retry_safety.load_result(result_path)
    assert loaded.to_dict() == original_report.to_dict()


def test_write_result_counts_utf8_bytes_for_limit(tmp_path: Path) -> None:
    """Measure the Tekton result boundary in UTF-8 bytes, not Python characters."""
    result_path = tmp_path / "retry-safety"
    original_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        unsafe_operations_completed=2,
        summary="Already unsafe",
    )
    retry_safety.write_result(result_path, original_report)

    report = retry_safety.RetrySafetyReport(is_safe_to_retry=False, summary="")
    while len(_encoded_result(report)) <= retry_safety_module.MAX_RESULT_BYTES:
        report.summary += "é"

    with pytest.raises(ValueError, match="exceeds"):
        retry_safety.write_result(result_path, report)

    assert retry_safety.load_result(result_path).to_dict() == original_report.to_dict()


def test_write_result_rejects_conflicting_safe_state_and_preserves_existing_file(
    tmp_path: Path,
) -> None:
    """Reject contradictory in-memory state before replacing an existing result."""
    result_path = tmp_path / "retry-safety"
    original_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        unsafe_operations_completed=3,
        summary="Pushed three images",
    )
    retry_safety.write_result(result_path, original_report)
    bad_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=True,
        unsafe_operation_in_progress=True,
    )

    with pytest.raises(ValueError, match="conflicts with unsafe operation state"):
        retry_safety.write_result(result_path, bad_report)

    assert retry_safety.load_result(result_path).to_dict() == original_report.to_dict()


def test_write_result_removes_temp_file_when_replace_fails(
    tmp_path: Path,
) -> None:
    """Clean up the temporary file when the final atomic replace fails."""
    result_path = tmp_path / "retry-safety"
    report = retry_safety.RetrySafetyReport(is_safe_to_retry=False, summary="unsafe")

    with patch.object(
        retry_safety_module.os,
        "replace",
        side_effect=RuntimeError("replace failed"),
    ):
        with pytest.raises(RuntimeError, match="replace failed"):
            retry_safety.write_result(result_path, report)

    assert list(result_path.parent.glob("retry-safety.*.tmp")) == []


def test_write_result_preserves_original_exception_when_unlink_fails(
    tmp_path: Path,
) -> None:
    """Keep the original write failure when temp-file cleanup also fails."""
    result_path = tmp_path / "retry-safety"
    report = retry_safety.RetrySafetyReport(is_safe_to_retry=False, summary="unsafe")

    with (
        patch.object(
            retry_safety_module.os,
            "replace",
            side_effect=RuntimeError("replace failed"),
        ),
        patch.object(
            retry_safety_module.os,
            "unlink",
            side_effect=OSError("unlink failed"),
        ),
    ):
        with pytest.raises(RuntimeError, match="replace failed"):
            retry_safety.write_result(result_path, report)


def test_write_report_preserves_existing_file_on_serialization_failure(
    tmp_path: Path,
) -> None:
    """Leave an existing task result intact when JSON serialization fails."""
    result_path = tmp_path / "retry-safety"
    original_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        unsafe_operations_completed=3,
        summary="Pushed three images",
    )
    retry_safety.write_result(result_path, original_report)

    bad_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        details={"not_json": {1, 2, 3}},
    )

    with pytest.raises(TypeError):
        retry_safety.write_result(result_path, bad_report)

    loaded = retry_safety.load_result(result_path)
    assert loaded.to_dict() == original_report.to_dict()


@pytest.mark.parametrize(
    "value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
    ],
)
def test_write_result_preserves_existing_file_on_non_finite_float(
    tmp_path: Path,
    value: float,
) -> None:
    """Leave an existing task result intact when details contain non-finite floats."""
    result_path = tmp_path / "retry-safety"
    original_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        unsafe_operations_completed=3,
        summary="Pushed three images",
    )
    retry_safety.write_result(result_path, original_report)

    bad_report = retry_safety.RetrySafetyReport(
        is_safe_to_retry=False,
        details={"invalid_float": value},
    )

    with pytest.raises(ValueError, match="Out of range float values"):
        retry_safety.write_result(result_path, bad_report)

    loaded = retry_safety.load_result(result_path)
    assert loaded.to_dict() == original_report.to_dict()


def test_from_dict_uses_defaults_for_optional_fields() -> None:
    """Apply default values when optional persisted fields are absent."""
    report = retry_safety.RetrySafetyReport.from_dict(
        {"is_safe_to_retry": True, "summary": "still safe"}
    )

    assert report.is_safe_to_retry is True
    assert report.unsafe_operation_in_progress is False
    assert report.unsafe_operations_in_progress_count == 0
    assert report.unsafe_operations_completed == 0
    assert report.summary == "still safe"
    assert report.details == {}


def test_from_dict_infers_outstanding_count_from_legacy_in_progress_flag() -> None:
    """Infer one outstanding operation from the legacy in-progress flag alone."""
    report = retry_safety.RetrySafetyReport.from_dict(
        {"is_safe_to_retry": False, "unsafe_operation_in_progress": True}
    )

    assert report.unsafe_operation_in_progress is True
    assert report.unsafe_operations_in_progress_count == 1


def test_from_dict_requires_explicit_safety_flag() -> None:
    """Reject persisted reports that omit the safety flag."""
    with pytest.raises(KeyError, match="is_safe_to_retry"):
        retry_safety.RetrySafetyReport.from_dict({"summary": "still safe"})


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"is_safe_to_retry": "yes"}, "is_safe_to_retry must be a bool"),
        (
            {"is_safe_to_retry": True, "unsafe_operation_in_progress": "yes"},
            "unsafe_operation_in_progress must be a bool",
        ),
        (
            {"is_safe_to_retry": True, "unsafe_operations_in_progress_count": False},
            "unsafe_operations_in_progress_count must be an int",
        ),
        ({"is_safe_to_retry": True, "summary": 1}, "summary must be a str"),
        ({"is_safe_to_retry": True, "details": []}, "details must be an object"),
    ],
)
def test_from_dict_rejects_invalid_types(
    payload: dict[str, object],
    message: str,
) -> None:
    """Reject invalid persisted field types during load."""
    with pytest.raises(TypeError, match=message):
        retry_safety.RetrySafetyReport.from_dict(payload)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        (
            "unsafe_operations_completed",
            False,
            "unsafe_operations_completed must be an int",
        ),
    ],
)
def test_from_dict_rejects_boolean_values_for_int_fields(
    field_name: str,
    value: bool,
    message: str,
) -> None:
    """Reject persisted booleans for integer-only fields."""
    with pytest.raises(TypeError, match=message):
        retry_safety.RetrySafetyReport.from_dict({"is_safe_to_retry": True, field_name: value})


def test_from_dict_rejects_negative_completed_count() -> None:
    """Reject persisted negative completed-operation counts."""
    with pytest.raises(ValueError, match="non-negative"):
        retry_safety.RetrySafetyReport.from_dict(
            {"is_safe_to_retry": False, "unsafe_operations_completed": -1}
        )


def test_from_dict_rejects_negative_in_progress_count() -> None:
    """Reject persisted negative outstanding-operation counts."""
    with pytest.raises(ValueError, match="non-negative"):
        retry_safety.RetrySafetyReport.from_dict(
            {"is_safe_to_retry": False, "unsafe_operations_in_progress_count": -1}
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"is_safe_to_retry": True, "unsafe_operation_in_progress": True},
        {"is_safe_to_retry": True, "unsafe_operations_completed": 1},
    ],
)
def test_from_dict_rejects_conflicting_safe_state(payload: dict[str, object]) -> None:
    """Reject persisted reports that claim safety despite unsafe activity."""
    with pytest.raises(ValueError, match="conflicts with unsafe operation state"):
        retry_safety.RetrySafetyReport.from_dict(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "is_safe_to_retry": False,
            "unsafe_operation_in_progress": True,
            "unsafe_operations_in_progress_count": 0,
        },
        {
            "is_safe_to_retry": False,
            "unsafe_operation_in_progress": False,
            "unsafe_operations_in_progress_count": 1,
        },
    ],
)
def test_from_dict_rejects_conflicting_in_progress_state(payload: dict[str, object]) -> None:
    """Reject persisted reports whose in-progress flag disagrees with the count."""
    with pytest.raises(
        ValueError,
        match="unsafe_operation_in_progress conflicts with outstanding operations",
    ):
        retry_safety.RetrySafetyReport.from_dict(payload)


def test_load_result_rejects_non_object_json_root(tmp_path: Path) -> None:
    """Reject persisted task results whose JSON root is not an object."""
    result_path = tmp_path / "retry-safety"
    result_path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")

    with pytest.raises(TypeError, match="JSON root must be an object"):
        retry_safety.load_result(result_path)
