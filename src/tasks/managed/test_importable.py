"""Verify every managed task subpackage is importable via its absolute path."""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

_TASKS_DIR = Path(__file__).parent
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
