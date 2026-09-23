"""Run subprocess commands with optional stderr log file."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from release_service_utils.helpers import tekton

RunCmd = Callable[..., str]


def _append_command_failure(stderr_path: Path, argv: list[str]) -> None:
    """Record a failed command on the stderr log and the process stderr stream."""
    failure = f"\ncommand exited with failure: {' '.join(argv)}\n"
    with open(stderr_path, "a", encoding="utf-8", errors="replace") as errf:
        errf.write(failure)
    sys.stderr.write(failure)
    sys.stderr.flush()


def run_cmd(
    cmd: Sequence[str | Path],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    stdin: str | bytes | None = None,
    stderr_path: Path | None = None,
    check: bool = True,
    stream_stdout: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run *cmd*; capture stdout as text; optionally tee stderr to *stderr_path*.

    By default stdout is captured (piped) and only becomes available once the
    child exits, matching the historical behavior callers rely on for parsing
    output (e.g. ``yq``/``jq``). Set *stream_stdout* to ``True`` for long-running,
    high-output commands (e.g. upload wrappers that print live progress) so
    stdout is inherited from this process instead of buffered, letting it
    stream straight to the Tekton step log in real time like the old bash
    tasks did. ``result.stdout`` is ``None`` when *stream_stdout* is set.

    When *stderr_path* is set, child stderr is appended to that file *and*
    copied to this process's stderr (bash ``2> >(tee -a file >&2)``). That keeps
    a copy for Tekton result failures without hiding live child progress from
    the step log. ``PYTHONUNBUFFERED=1`` is also set so child Python processes
    flush logs immediately when there is no TTY.
    """
    # Child must inherit pod env (PATH, KUBECONFIG, etc.); only overlay *env*.
    merged: dict[str, str] = {**os.environ, **dict(env or {})}
    merged.setdefault("PYTHONUNBUFFERED", "1")
    argv = [str(x) for x in cmd]
    err_f: Any = subprocess.PIPE
    tee_proc: subprocess.Popen[bytes] | None = None
    try:
        if stderr_path is not None:
            # tee copies stdin to the log file and to fd 2 (Tekton step stderr).
            tee_proc = subprocess.Popen(
                ["tee", "-a", str(stderr_path)],
                stdin=subprocess.PIPE,
                stdout=2,
            )
            err_f = tee_proc.stdin
        try:
            return subprocess.run(
                argv,
                cwd=cwd,
                env=merged,
                input=stdin,
                stdout=None if stream_stdout else subprocess.PIPE,
                stderr=err_f,
                text=True,
                check=check,
            )
        except subprocess.CalledProcessError:
            if stderr_path is not None:
                _append_command_failure(stderr_path, argv)
            raise
    finally:
        if tee_proc is not None:
            if tee_proc.stdin is not None:
                tee_proc.stdin.close()
            tee_proc.wait()


def run_cmd_text(
    cmd: Sequence[str | Path],
    *,
    cwd: Path | None = None,
) -> str:
    """Run *cmd*, return captured stdout as text, and raise on non-zero exit.

    Uses a Tekton-friendly command preview in ``CalledProcessError.cmd``.
    """
    argv = [str(x) for x in cmd]
    proc = subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        preview = tekton.subprocess_cmd_preview_for_tekton_result(argv)
        err = (proc.stderr or proc.stdout or "").strip()
        raise subprocess.CalledProcessError(
            proc.returncode,
            preview,
            output=err,
        )
    return proc.stdout or ""


def run_yq_json(
    path: Path,
    expression: str,
    *,
    run_cmd: RunCmd | None = None,
) -> Any:
    """Evaluate a ``yq`` expression against ``path`` and parse JSON output."""
    runner = run_cmd or run_cmd_text
    out = runner(["yq", "-o=json", expression, str(path)])
    if not str(out).strip():
        return []
    return json.loads(out)
