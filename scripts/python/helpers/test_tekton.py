"""Tests for the ``tekton`` helper module."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
import tekton


def test_check_step_error() -> None:
    """``CheckStepError`` stores the human *action* and the underlying *cause*."""
    inner = KeyError("k")
    err = tekton.CheckStepError("step", inner)
    assert err.action == "step" and err.cause is inner


def test_result_text_from_exception_truncates() -> None:
    """Long exception text is cut to *max_len* and ends with an ellipsis."""
    e = KeyError("e" * 1000)
    s = tekton.result_text_from_exception(e, max_len=40)
    assert s.endswith("...")
    assert len(s) == 40


def test_result_text_for_check_step_error_includes_action_and_cause() -> None:
    """Result text includes the user-facing *action* and the cause string."""
    inner = ValueError("token request failed")
    s = tekton.result_text_for_check_step_error(
        "check_embargoed_cves.py",
        tekton.CheckStepError("getting an OSIDB access token (HTTP request)", inner),
    )
    assert "OSIDB access token" in s
    assert "token request failed" in s
    assert "Failed while" in s


def test_subprocess_cmd_preview_for_tekton_result() -> None:
    """A list *cmd* is space-joined, then trimmed to *max_len* for result lines."""
    long = "x" * 300
    s = tekton.subprocess_cmd_preview_for_tekton_result(
        ["kinit", "p@R", "-k", "-t", long], max_len=30
    )
    assert len(s) == 30
    assert s.startswith("kinit p@R -k -t ")


def test_subprocess_cmd_preview_for_string_cmd() -> None:
    """A non-list command uses its plain string form before trimming."""
    s = tekton.subprocess_cmd_preview_for_tekton_result("echo hi", max_len=20)
    assert s == "echo hi"


def test_result_paths_returns_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each set env var yields a ``Path`` in the same order as the arguments."""
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.write_text("x", encoding="utf-8")
    b.write_text("y", encoding="utf-8")
    monkeypatch.setenv("RESULT_RESULT", str(a))
    monkeypatch.setenv("RESULT_EMBARGOED_CVES", str(b))
    one, two = tekton.result_paths("RESULT_RESULT", "RESULT_EMBARGOED_CVES")
    assert (one, two) == (a, b)


def test_result_paths_missing_exits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset or empty env values print which names are missing and exit with code 1."""
    p = tmp_path / "a"
    p.write_text("x", encoding="utf-8")
    buf = io.StringIO()
    monkeypatch.setenv("A_ONLY", str(p))
    with pytest.raises(SystemExit) as ex:
        tekton.result_paths("A_ONLY", "B_MISSING", file=buf)
    assert ex.value.code == 1
    s = buf.getvalue()
    assert "B_MISSING" in s
    assert "must be set" in s


def test_result_paths_three_missing_names_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Three missing names are joined in the error with an Oxford-comma style list."""
    buf = io.StringIO()
    for name in ("A", "B", "C"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit) as ex:
        tekton.result_paths("A", "B", "C", file=buf)
    assert ex.value.code == 1
    assert buf.getvalue().strip() == "A, B, and C must be set"


def test_result_paths_no_names() -> None:
    """Calling ``result_paths`` with no var names is a programming error."""
    with pytest.raises(ValueError, match="at least one"):
        tekton.result_paths()
