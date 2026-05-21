"""Tests for the `tekton` helper module."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
import tekton


def test_check_step_error() -> None:
    """`CheckStepError` stores the human *action* and the underlying *cause*."""
    inner = KeyError("k")
    err = tekton.CheckStepError("step", inner)
    assert err.action == "step" and err.cause is inner


def test_result_text_from_exception_truncates() -> None:
    """Long exception text is cut to *max_len* and ends with an ellipsis."""
    e = KeyError("e" * 1000)
    s = tekton.result_text_from_exception(e, max_len=40)
    assert s.endswith("...")
    assert len(s) == 40


def test_write_failure_result_check_step_error(tmp_path: Path) -> None:
    """`CheckStepError` uses its *action* in the result summary."""
    inner = ValueError("token request failed")
    result = tmp_path / "result.txt"
    tekton.write_failure_result(
        result,
        "check_embargoed_cves.py",
        tekton.CheckStepError("getting an OSIDB access token (HTTP request)", inner),
    )
    text = result.read_text(encoding="utf-8")
    assert "OSIDB access token" in text
    assert "token request failed" in text
    assert "Failed while" in text


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


def test_require_env_returns_stripped_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-empty values are returned with surrounding whitespace removed."""
    monkeypatch.setenv("TASK_PARAM", "  value  ")
    assert tekton.require_env("TASK_PARAM") == "value"


def test_require_env_missing_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset or blank env values print the variable name and exit with code 1."""
    buf = io.StringIO()
    monkeypatch.delenv("MISSING_PARAM", raising=False)
    with pytest.raises(SystemExit) as ex:
        tekton.require_env("MISSING_PARAM", file=buf)
    assert ex.value.code == 1
    assert buf.getvalue().strip() == "MISSING_PARAM must be set"


def test_result_paths_returns_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each set env var yields a `Path` in the same order as the arguments."""
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.write_text("x", encoding="utf-8")
    b.write_text("y", encoding="utf-8")
    monkeypatch.setenv("RESULT_RESULT", str(a))
    monkeypatch.setenv("RESULT_EMBARGOED_CVES", str(b))
    one, two = tekton.result_paths("RESULT_RESULT", "RESULT_EMBARGOED_CVES")
    assert (one, two) == (a, b)


def test_result_paths_one_missing_name_message(monkeypatch: pytest.MonkeyPatch) -> None:
    buf = io.StringIO()
    monkeypatch.delenv("ONLY_MISSING", raising=False)
    with pytest.raises(SystemExit) as ex:
        tekton.result_paths("ONLY_MISSING", file=buf)
    assert ex.value.code == 1
    assert buf.getvalue().strip() == "ONLY_MISSING must be set"


def test_result_paths_two_missing_names_message(monkeypatch: pytest.MonkeyPatch) -> None:
    buf = io.StringIO()
    monkeypatch.delenv("A", raising=False)
    monkeypatch.delenv("B", raising=False)
    with pytest.raises(SystemExit) as ex:
        tekton.result_paths("A", "B", file=buf)
    assert ex.value.code == 1
    assert "A and B must be set" in buf.getvalue()


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
    """Calling `result_paths` with no var names is a programming error."""
    with pytest.raises(ValueError, match="at least one"):
        tekton.result_paths()


def test_write_failure_result_truncates_long_text(tmp_path: Path) -> None:
    result = tmp_path / "result.txt"
    tekton.write_failure_result(
        result,
        "create_advisory.py",
        ValueError("x"),
        max_total_len=20,
    )
    text = result.read_text(encoding="utf-8")
    assert len(text) == 20
    assert text.endswith("...")


def test_write_failure_result_appends_command_log(tmp_path: Path) -> None:
    log = tmp_path / "log.txt"
    log.write_text("line1\nline2\nline3\n", encoding="utf-8")
    result = tmp_path / "result.txt"
    tekton.write_failure_result(
        result,
        "create_advisory.py",
        ValueError("bad input"),
        command_log_path=log,
        max_log_lines=2,
    )
    text = result.read_text(encoding="utf-8")
    assert "bad input" in text
    assert "line2" in text and "line3" in text
    assert "line1" not in text
