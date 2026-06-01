"""Run subprocess commands with optional stderr log file."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


def run_cmd(
    cmd: Sequence[str | Path],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    stdin: str | bytes | None = None,
    stderr_path: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run *cmd*; capture stdout as text; optionally append stderr to *stderr_path*."""
    # Child must inherit pod env (PATH, KUBECONFIG, etc.); only overlay *env*.
    merged: dict[str, str] = {**os.environ, **dict(env or {})}
    err_f: Any = subprocess.DEVNULL
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
                stdout=subprocess.PIPE,
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
