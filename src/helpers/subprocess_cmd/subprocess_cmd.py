"""Run subprocess commands with optional stderr log file."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from release_service_utils.helpers import tekton

RunCmd = Callable[..., str]


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
    """Run *cmd*; capture stdout as text; optionally append stderr to *stderr_path*.

    By default stdout is captured (piped) and only becomes available once the
    child exits, matching the historical behavior callers rely on for parsing
    output (e.g. ``yq``/``jq``). Set *stream_stdout* to ``True`` for long-running,
    high-output commands (e.g. upload wrappers that print live progress) so
    stdout is inherited from this process instead of buffered, letting it
    stream straight to the Tekton step log in real time like the old bash
    tasks did. ``result.stdout`` is ``None`` when *stream_stdout* is set.
    """
    # Child must inherit pod env (PATH, KUBECONFIG, etc.); only overlay *env*.
    merged: dict[str, str] = {**os.environ, **dict(env or {})}
    err_f: Any = subprocess.PIPE
    fh: Any = None
    try:
        if stderr_path is not None:
            fh = open(
                stderr_path,
                "a",
                encoding="utf-8",
                errors="replace",
            )
            err_f = fh
        argv = [str(x) for x in cmd]
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
                with open(
                    stderr_path,
                    "a",
                    encoding="utf-8",
                    errors="replace",
                ) as errf:
                    errf.write(f"\ncommand exited with failure: {' '.join(argv)}\n")
            raise
    finally:
        if fh is not None:
            fh.close()


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
