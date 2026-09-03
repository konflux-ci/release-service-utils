"""Verify every managed task subpackage is importable via its absolute path."""

from __future__ import annotations

import importlib
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

_TASKS_DIR = Path(__file__).parent
_REPO_ROOT = _TASKS_DIR.parent.parent.parent
_MODULE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_SKIP = {"__pycache__", "tests"}


def _discover_subpackages() -> list[str]:
    """Return managed task subpackage names that should be importable."""
    return sorted(
        d.name
        for d in _TASKS_DIR.iterdir()
        if d.is_dir()
        and d.name not in _SKIP
        and _MODULE_RE.match(d.name)
        and (d / "__init__.py").exists()
    )


@pytest.mark.parametrize("name", _discover_subpackages())
def test_managed_task_importable(name: str) -> None:
    """Import ``release_service_utils.tasks.managed.<name>`` without error."""
    importlib.import_module(f"release_service_utils.tasks.managed.{name}")


@pytest.mark.parametrize("name", _discover_subpackages())
def test_managed_task_runnable_as_module(name: str) -> None:
    """Run ``python -m release_service_utils.tasks.managed.<name> --help``.

    without import errors
    """
    module_name = f"release_service_utils.tasks.managed.{name}"

    # Set PYTHONPATH to include utils/ and other directories, matching the container setup
    env = os.environ.copy()
    pythonpath_parts = [
        str(_REPO_ROOT),
        str(_REPO_ROOT / "utils"),
        str(_REPO_ROOT / "pyxis"),
        str(_REPO_ROOT / "src" / "helpers"),
        str(_REPO_ROOT / "pubtools-pulp-wrapper"),
        str(_REPO_ROOT / "publish-to-cgw-wrapper"),
        str(_REPO_ROOT / "pubtools-marketplacesvm-wrapper"),
        str(_REPO_ROOT / "developer-portal-wrapper"),
    ]
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    result = subprocess.run(
        [sys.executable, "-m", module_name, "--help"],
        capture_output=True,
        text=True,
        env=env,
    )
    # Accept exit code 0 (success), 1 (custom error), or 2 (argparse error)
    # The key is that there should be no import/module errors
    assert (
        "ModuleNotFoundError" not in result.stderr
    ), f"Module import failed for {module_name}:\n{result.stderr}"
    assert (
        "ImportError" not in result.stderr
    ), f"Import failed for {module_name}:\n{result.stderr}"
    # Exit code 0 means --help worked, 1-2 means argument error (acceptable)
    assert result.returncode in (0, 1, 2), (
        f"Unexpected exit code {result.returncode} for {module_name}:\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
