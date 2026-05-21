"""Tests for `subprocess_cmd`."""

from __future__ import annotations

import subprocess

import pytest
import subprocess_cmd


def test_run_cmd_captures_stdout() -> None:
    out = subprocess_cmd.run_cmd(["echo", "hi"], check=True).stdout.strip()
    assert out == "hi"


def test_run_cmd_check_false(tmp_path) -> None:
    r = subprocess_cmd.run_cmd(["false"], cwd=tmp_path, check=False)
    assert r.returncode != 0


def test_run_cmd_stderr_path_logs_failed_command(tmp_path) -> None:
    log = tmp_path / "log.txt"
    with pytest.raises(subprocess.CalledProcessError):
        subprocess_cmd.run_cmd(["false"], stderr_path=log, check=True)
    text = log.read_text(encoding="utf-8")
    assert "command exited with failure" in text
    assert "false" in text


def test_run_cmd_stderr_path_on_success(tmp_path) -> None:
    log = tmp_path / "log.txt"
    subprocess_cmd.run_cmd(["echo", "ok"], stderr_path=log, check=True)
    assert log.read_text(encoding="utf-8") == ""
