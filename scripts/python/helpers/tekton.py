"""Shared helpers for Tekton ``result``-file and step-error conventions in task scripts.

The catalog passes result file locations via environment variables
(``env.value: $(results.foo.path)`` on the task step). Tasks that require those
paths should fail fast in the same way: print to stderr and exit 1, since
downstream can only interpret ``result`` bodies when the step was intended to
write them.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TextIO


class CheckStepError(Exception):
    """
    Pairs a short *human* description of what the task was doing (the first
    argument) with the real exception, for the Tekton one-line result file.
    The standard library and ``requests`` only know about the underlying failure
    (HTTP, I/O, exit code) — not which high-level action was running; attach
    that at each ``raise`` site.
    """

    def __init__(self, action: str, cause: BaseException) -> None:
        self.action = action
        self.cause = cause
        super().__init__(f"{action}: {cause}")


def result_text_from_exception(exc: BaseException, *, max_len: int = 500) -> str:
    """
    ``str(exc)`` in a form suitable for a Tekton ``result`` value.

    Reuses each exception’s normal string (no custom wording per type).
    Newlines are flattened and the text is cut at *max_len* so the value
    is not unbounded.
    """
    t = str(exc).replace("\n", " ").strip()
    if len(t) > max_len:
        t = t[: max_len - 3] + "..."
    return t


def result_text_for_check_step_error(program_name: str, e: CheckStepError) -> str:
    """
    Text to put in a Tekton ``result`` when a task step failed with
    ``CheckStepError`` — the *action* from the error plus
    ``result_text_from_exception`` on the underlying *cause*.

    *program_name* is usually the basename of ``sys.argv[0]`` for messages that
    look like a log line prefix.
    """
    why = result_text_from_exception(e.cause)
    return f"{program_name}: Failed while {e.action}: {why}."


def subprocess_cmd_preview_for_tekton_result(
    cmd: str | list[str] | tuple[object, ...] | object,
    *,
    max_len: int = 200,
) -> str:
    """
    One-line description of a subprocess *cmd* for Tekton ``result`` files.

    ``CalledProcessError`` (and some APIs) expose *cmd* as a string, a list of
    argv pieces, or another object. Result lines stay short, so the joined or
    string form is cut to *max_len* characters.
    """
    if isinstance(cmd, (list, tuple)) and cmd:
        t = " ".join(str(x) for x in cmd)
    else:
        t = str(cmd)
    return t[:max_len]


def _join_var_names(names: list[str]) -> str:
    """
    Build a small English list for a one-line error message, for example
    "A and B" or "A, B, and C".
    """
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def result_paths(*env_var_names: str, file: TextIO = sys.stderr) -> tuple[Path, ...]:
    """
    Return pathlib Paths for the given environment variable names, in order.

    In Tekton, each result file is often wired as an env var whose value is the
    path the step must write. For every name you pass, the value in ``os.environ``
    must be non-empty. If any are missing or empty, a single error line is
    printed to ``file`` and ``SystemExit(1)`` is raised, so the step can stop
    before it tries to write to unknown locations.

    Example names: ``"RESULT_RESULT"``, ``"RESULT_EMBARGOED_CVES"``.
    """
    if not env_var_names:
        raise ValueError("at least one environment variable name is required")
    out: list[Path] = []
    missing: list[str] = []
    for n in env_var_names:
        v = os.environ.get(n)
        if v is None or not str(v).strip():
            missing.append(n)
        else:
            out.append(Path(v))
    if missing:
        label = _join_var_names(missing)
        print(f"{label} must be set", file=file)
        raise SystemExit(1)
    return tuple(out)
