#!/usr/bin/env python3
"""Map ``scripts/python/helpers/*.py`` to task scripts that import them.

Task scripts live under ``scripts/python/tasks/**/*.py``. Imports are resolved with
the ``ast`` module: bare imports like ``import file`` match ``helpers/file.py``;
``from helpers.foo import ...`` is also recognized when present.

Used by `find_catalog_suite_from_utils_diff` so a helper-only diff still
propagates to dependent ``tasks/`` paths for Dockerfile token generation.
"""

from __future__ import annotations

import ast
from pathlib import Path


def _helper_stems(helpers_root: Path) -> frozenset[str]:
    """Stem names for each ``*.py`` directly under ``helpers_root``."""
    # Only top-level modules (``helpers/foo.py``). Nested packages are out of scope
    # for this lightweight scanner.
    if not helpers_root.is_dir():
        return frozenset()
    return frozenset(p.stem for p in helpers_root.glob("*.py") if p.is_file())


def _is_task_script(path: Path) -> bool:
    """Skip unit tests next to tasks and anything under a ``tests`` directory."""
    # Task YAML references runnable entrypoints, not pytest modules or fixtures.
    if path.name.startswith("test_"):
        return False
    if "tests" in path.parts:
        return False
    return path.suffix == ".py"


def _collect_imported_helper_names(tree: ast.AST, helper_stems: frozenset[str]) -> set[str]:
    """Return helper module stems referenced by *tree* that exist under helpers."""
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            # ``import foo`` / ``import foo.bar`` — first segment must match a helper stem.
            for alias in node.names:
                stem = alias.name.split(".", 1)[0]
                if stem in helper_stems:
                    found.add(stem)
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            parts = node.module.split(".")
            # ``from helpers.foo import ...`` → ``foo.py`` under helpers/.
            if parts[0] == "helpers" and len(parts) >= 2:
                inner = parts[1]
                if inner in helper_stems:
                    found.add(inner)
            elif parts[0] in helper_stems:
                # Rare: ``from file import ...`` with ``file`` as the package prefix.
                found.add(parts[0])
    return found


def build_helper_to_task_paths(repo_root: Path) -> dict[str, set[str]]:
    """Build reverse map: helper stem -> repo-relative paths of importing tasks."""
    helpers_root = repo_root / "scripts" / "python" / "helpers"
    tasks_root = repo_root / "scripts" / "python" / "tasks"
    stems = _helper_stems(helpers_root)
    if not stems or not tasks_root.is_dir():
        return {}

    reverse: dict[str, set[str]] = {s: set() for s in stems}
    for py in tasks_root.rglob("*.py"):
        if not _is_task_script(py):
            continue
        try:
            text = py.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(text, filename=str(py))
        except (OSError, SyntaxError):
            # Broken files cannot be analyzed; omit rather than failing the pipeline.
            continue
        rel = py.relative_to(repo_root).as_posix()
        for stem in _collect_imported_helper_names(tree, stems):
            reverse[stem].add(rel)
    return reverse


def expand_changed_paths_for_helper_deps(
    repo_root: Path,
    changed_paths: list[str],
    *,
    _reverse: dict[str, set[str]] | None = None,
) -> list[str]:
    """Append task paths that import changed helpers; preserve order, dedupe.

    Paths under ``scripts/python/helpers/*.py`` add any task files that import that
    helper module. Other paths pass through unchanged.
    """
    reverse = _reverse if _reverse is not None else build_helper_to_task_paths(repo_root)
    if not reverse:
        return list(changed_paths)

    helper_prefix = "scripts/python/helpers/"
    seen: set[str] = set()
    out: list[str] = []

    def add(p: str) -> None:
        """Normalize repo-relative paths once and dedupe."""
        norm = p.strip().strip("./")
        if not norm or norm in seen:
            return
        seen.add(norm)
        out.append(norm)

    for raw in changed_paths:
        s = raw.strip().strip("./")
        if not s:
            continue
        add(s)
        if not s.startswith(helper_prefix) or not s.endswith(".py"):
            continue
        stem = Path(s).stem
        if stem == "__init__":
            # We only index plain modules; package ``__init__`` has no stem mapping.
            continue
        # Stable order so repeated runs diffs are deterministic.
        for task_rel in sorted(reverse.get(stem, ())):
            add(task_rel)

    return out
