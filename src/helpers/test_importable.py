"""Verify every helper subpackage is importable via its absolute path."""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

_HELPERS_DIR = Path(__file__).parent
_MODULE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_SKIP = {"__pycache__"}


def _discover_subpackages() -> list[str]:
    """Return helper subpackage names that should be importable."""
    return sorted(
        d.name
        for d in _HELPERS_DIR.iterdir()
        if d.is_dir()
        and d.name not in _SKIP
        and _MODULE_RE.match(d.name)
        and (d / "__init__.py").exists()
    )


@pytest.mark.parametrize("name", _discover_subpackages())
def test_helper_importable(name: str) -> None:
    """Import ``release_service_utils.helpers.<name>`` without error."""
    importlib.import_module(f"release_service_utils.helpers.{name}")
