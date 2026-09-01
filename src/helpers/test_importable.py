"""Verify every helper subpackage is importable via its absolute path."""

from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path

import pytest

_HELPERS_DIR = Path(__file__).parent.resolve()
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


def _discover_flat_modules() -> list[str]:
    """Return helper modules that live as ``helpers/<name>.py`` files."""
    return sorted(
        f.stem
        for f in _HELPERS_DIR.iterdir()
        if f.is_file()
        and f.suffix == ".py"
        and not f.name.startswith("test_")
        and f.name != "__init__.py"
        and _MODULE_RE.match(f.stem)
    )


@pytest.mark.parametrize("name", _discover_subpackages() + _discover_flat_modules())
def test_helper_importable(name: str) -> None:
    """Import ``release_service_utils.helpers.<name>`` without error."""
    importlib.import_module(f"release_service_utils.helpers.{name}")


@pytest.mark.parametrize("name", _discover_flat_modules())
def test_flat_helper_imports_without_helpers_on_sys_path(name: str) -> None:
    """Flat helpers must not rely on ``src/helpers`` being on PYTHONPATH.

    Pytest puts ``src/helpers`` on ``pythonpath``, so a leftover
    ``from subprocess_cmd import ...`` still works in unit tests. Catalog
    tasks run the installed package without that path and fail at import.
    """
    helper_top_level = set(_discover_subpackages() + _discover_flat_modules())
    filtered_path = [p for p in sys.path if Path(p).resolve() != _HELPERS_DIR]
    to_drop = [
        mod
        for mod in sys.modules
        if mod in helper_top_level
        or any(mod.startswith(f"{h}.") for h in helper_top_level)
        or mod == f"release_service_utils.helpers.{name}"
    ]
    old_path = sys.path[:]
    saved_modules = {mod: sys.modules[mod] for mod in to_drop}
    try:
        sys.path[:] = filtered_path
        for mod in to_drop:
            sys.modules.pop(mod, None)
        importlib.import_module(f"release_service_utils.helpers.{name}")
    finally:
        sys.path[:] = old_path
        sys.modules.update(saved_modules)
