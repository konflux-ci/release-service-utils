"""Tests for `subprocess_cmd`."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from unittest import mock

import pytest
import subprocess_cmd


def test_run_cmd_captures_stdout() -> None:
    """``run_cmd`` returns captured stdout from a successful subprocess."""
    out = subprocess_cmd.run_cmd(["echo", "hi"], check=True).stdout.strip()
    assert out == "hi"


def test_run_cmd_check_false(tmp_path) -> None:
    """``run_cmd`` with ``check=False`` returns a non-zero exit code without raising."""
    r = subprocess_cmd.run_cmd(["false"], cwd=tmp_path, check=False)
    assert r.returncode != 0


def test_run_cmd_stderr_path_logs_failed_command(tmp_path) -> None:
    """Failed commands append a failure line to *stderr_path*."""
    log = tmp_path / "log.txt"
    with pytest.raises(subprocess.CalledProcessError):
        subprocess_cmd.run_cmd(["false"], stderr_path=log, check=True)
    text = log.read_text(encoding="utf-8")
    assert "command exited with failure" in text
    assert "false" in text


def test_run_cmd_stderr_path_on_success(tmp_path) -> None:
    """Successful commands leave *stderr_path* empty."""
    log = tmp_path / "log.txt"
    subprocess_cmd.run_cmd(["echo", "ok"], stderr_path=log, check=True)
    assert log.read_text(encoding="utf-8") == ""


def test_run_cmd_text_success() -> None:
    """``run_cmd_text`` returns stdout from a successful subprocess."""
    with mock.patch("subprocess_cmd.subprocess.run") as run:
        run.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
        assert subprocess_cmd.run_cmd_text(["echo", "ok"]) == "ok"


def test_run_cmd_text_failure() -> None:
    """``run_cmd_text`` raises ``CalledProcessError`` on non-zero exit."""
    with mock.patch("subprocess_cmd.subprocess.run") as run:
        run.return_value = mock.Mock(returncode=1, stdout="", stderr="bad")
        with pytest.raises(subprocess.CalledProcessError):
            subprocess_cmd.run_cmd_text(["false"])


def test_run_yq_json_empty_output(tmp_path: Path) -> None:
    """Blank ``yq`` output is treated as an empty list."""
    path = tmp_path / "advisory.yaml"
    path.write_text("x: 1\n", encoding="utf-8")
    assert (
        subprocess_cmd.run_yq_json(
            path, ".spec.content.images // []", run_cmd=lambda *_a, **_k: ""
        )
        == []
    )


def test_run_yq_json_uses_runner(tmp_path: Path) -> None:
    """Injected ``run_cmd`` is used instead of subprocess."""
    path = tmp_path / "advisory.yaml"
    path.write_text("x: 1\n", encoding="utf-8")

    def _runner(args: list[str], *, cwd: Path | None = None) -> str:
        assert args[0] == "yq"
        return json.dumps([{"containerImage": "a"}])

    assert subprocess_cmd.run_yq_json(path, ".spec.content.images // []", run_cmd=_runner) == [
        {"containerImage": "a"}
    ]


def test_run_yq_json_reads_yaml_with_yq(tmp_path: Path) -> None:
    """``run_yq_json`` runs ``yq -o=json`` against advisory YAML and parses JSON."""
    if shutil.which("yq") is None:
        pytest.skip("yq is not installed")

    path = tmp_path / "advisory.yaml"
    path.write_text(
        """\
spec:
  type: RHBA
  content:
    images:
      - containerImage: registry.example.com/app@sha256:abc
        tags: ["v1"]
        repository: registry.example.com/app
metadata:
  name: "2025:1"
""",
        encoding="utf-8",
    )

    assert subprocess_cmd.run_yq_json(path, ".spec.content.images // []") == [
        {
            "containerImage": "registry.example.com/app@sha256:abc",
            "tags": ["v1"],
            "repository": "registry.example.com/app",
        }
    ]
    assert subprocess_cmd.run_yq_json(path, ".spec.type") == "RHBA"
    assert subprocess_cmd.run_yq_json(path, ".metadata.name") == "2025:1"
