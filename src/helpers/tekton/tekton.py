"""Shared helpers for Tekton task scripts: results, step errors, and CLI parsing.

The catalog passes result file locations via environment variables
(`env.value: $(results.foo.path)` on the task step). Tasks that require those
paths should fail fast in the same way: print to stderr and exit 1, since
downstream can only interpret `result` bodies when the step was intended to
write them.

CLI entrypoints use the same contract: short usage on stderr and exit 1 for
``--help`` or missing required flags (not argparse's default stdout help or
exit codes).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import NoReturn, TextIO

from release_service_utils.helpers.redact import redact_secrets


class CheckStepError(Exception):
    """Attach a human task action to an underlying exception for Tekton results.

    Pairs a short *human* description of what the task was doing (the first
    argument) with the real exception, for the Tekton one-line result file.
    The standard library and `requests` only know about the underlying failure
    (HTTP, I/O, exit code) — not which high-level action was running; attach
    that at each `raise` site.
    """

    def __init__(self, action: str, cause: BaseException) -> None:
        """Store *action* and *cause* on the exception instance."""
        self.action = action
        self.cause = cause
        super().__init__(f"{action}: {cause}")


def result_text_from_exception(exc: BaseException, *, max_len: int = 500) -> str:
    """`str(exc)` in a form suitable for a Tekton `result` value.

    Reuses each exception’s normal string (no custom wording per type).
    Newlines are flattened and the text is cut at *max_len* so the value
    is not unbounded.
    """
    t = str(exc).replace("\n", " ").strip()
    if len(t) > max_len:
        t = t[: max_len - 3] + "..."
    return t


def write_failure_result(
    result_path: Path,
    program_name: str,
    exc: BaseException,
    *,
    command_log_path: Path | None = None,
    workflow_action: str = "running the workflow",
    max_log_lines: int = 20,
    max_total_len: int = 8192,
) -> None:
    """Write Tekton `result` text for a failed step.

    Includes a one-line summary (`CheckStepError` uses its *action*; other
    exceptions use *workflow_action*) and, when *command_log_path* is set, the
    last *max_log_lines* of subprocess/git output collected during the run.
    """
    if isinstance(exc, CheckStepError):
        why = redact_secrets(result_text_from_exception(exc.cause))
        summary = f"{program_name}: Failed while {exc.action}: {why}."
    else:
        why = redact_secrets(result_text_from_exception(exc))
        summary = f"{program_name}: Failed while {workflow_action}: " f"{why}"

    parts = [summary.strip()]
    if command_log_path is not None and command_log_path.is_file():
        lines = command_log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if lines:
            tail = redact_secrets("\n".join(lines[-max_log_lines:]).strip())
            parts.append(tail)

    text = redact_secrets("\n".join(parts))
    if len(text) > max_total_len:
        text = text[: max_total_len - 3] + "..."
    result_path.write_text(text, encoding="utf-8")


def subprocess_cmd_preview_for_tekton_result(
    cmd: str | list[str] | tuple[object, ...] | object,
    *,
    max_len: int = 200,
) -> str:
    """One-line description of a subprocess *cmd* for Tekton `result` files.

    `CalledProcessError` (and some APIs) expose *cmd* as a string, a list of
    argv pieces, or another object. Result lines stay short, so the joined or
    string form is cut to *max_len* characters.
    """
    if isinstance(cmd, (list, tuple)) and cmd:
        t = " ".join(str(x) for x in cmd)
    else:
        t = str(cmd)
    return t[:max_len]


def _join_var_names(names: list[str]) -> str:
    """Format names as an English list, for example ``A and B`` or ``A, B, and C``."""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def require_env(name: str, *, file: TextIO = sys.stderr) -> str:
    """Return the non-empty string value of an environment variable.

    If *name* is unset or only whitespace, print `{name} must be set` to
    *file* and raise `SystemExit(1)`.
    """
    value = os.environ.get(name)
    if value is None or not str(value).strip():
        print(f"{name} must be set", file=file)
        raise SystemExit(1)
    return str(value).strip()


def result_paths_from_env(*env_var_names: str, file: TextIO = sys.stderr) -> tuple[Path, ...]:
    """Return pathlib Paths for the given environment variable names, in order.

    In Tekton, each result file is often wired as an env var whose value is the
    path the step must write. For every name you pass, the value in `os.environ`
    must be non-empty. If any are missing or empty, a single error line is
    printed to *file* and `SystemExit(1)` is raised, so the step can stop
    before it tries to write to unknown locations.

    Example names: `RESULT_RESULT`, `RESULT_EMBARGOED_CVES`.
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


def tekton_argument_parser(prog: str) -> argparse.ArgumentParser:
    """Return an ``ArgumentParser`` for Tekton task entrypoints.

    Disables argparse's default help and usage strings so callers can print a
    short, task-specific usage summary to stderr and exit with code ``1``.
    Unknown or extra arguments still make ``parse_args`` exit with code ``2``.
    """
    return argparse.ArgumentParser(prog=prog, add_help=False, usage=argparse.SUPPRESS)


def exit_with_usage(usage: str, code: int = 1) -> NoReturn:
    """Print *usage* to stderr and terminate with *code*."""
    print(usage, file=sys.stderr, end="")
    raise SystemExit(code)


def missing_blank_option_values(*options: tuple[str, str | None]) -> list[str]:
    """Return flag names whose values are missing or whitespace-only."""
    return [name for name, val in options if not val or not str(val).strip()]
