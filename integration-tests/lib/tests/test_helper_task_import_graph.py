"""Tests for ``helper_task_import_graph``."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import patch

import helper_task_import_graph as ht


def _minimal_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "scripts" / "python" / "helpers").mkdir(parents=True)
    (root / "scripts" / "python" / "tasks" / "internal").mkdir(parents=True)
    return root


def test_is_task_script_requires_py_suffix() -> None:
    """Runnable tasks are ``*.py``; other suffixes are ignored."""
    assert ht._is_task_script(Path("scripts/python/tasks/internal/run.py")) is True
    assert ht._is_task_script(Path("scripts/python/tasks/internal/run.sh")) is False
    assert ht._is_task_script(Path("scripts/python/tasks/managed/test_run.py")) is False
    assert ht._is_task_script(Path("scripts/python/tasks/managed/tests/run.py")) is False


def test_collect_import_from_helpers_pkg_form() -> None:
    """``from helpers.foo import …`` maps to stem ``foo`` when it exists under helpers."""
    tree = ast.parse("from helpers.bar import thing\n")
    stems = frozenset({"bar"})
    assert ht._collect_imported_helper_names(tree, stems) == {"bar"}


def test_collect_import_from_top_level_pkg_prefix() -> None:
    """``from file import …`` counts when ``file`` is a helper module stem."""
    tree = ast.parse("from file import read_all\n")
    stems = frozenset({"file"})
    assert ht._collect_imported_helper_names(tree, stems) == {"file"}


def test_collect_import_from_relative_skipped() -> None:
    """``from . import …`` has ``module is None`` and does not match helpers."""
    tree = ast.parse("from . import sibling\n")
    stems = frozenset({"sibling"})
    assert ht._collect_imported_helper_names(tree, stems) == set()


def test_build_helper_to_task_paths_bare_import(tmp_path: Path) -> None:
    """``import foo`` maps helper stem ``foo`` to the task path."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "foo.py").write_text("# x\n", encoding="utf-8")
    task = root / "scripts" / "python" / "tasks" / "internal" / "runner.py"
    task.write_text("import foo\n", encoding="utf-8")

    rev = ht.build_helper_to_task_paths(root)
    assert rev.get("foo") == {"scripts/python/tasks/internal/runner.py"}


def test_build_helper_to_task_paths_skips_test_modules(tmp_path: Path) -> None:
    """Skip ``test_*.py`` next to tasks."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "foo.py").write_text("#\n", encoding="utf-8")
    (root / "scripts" / "python" / "tasks" / "internal" / "test_runner.py").write_text(
        "import foo\n", encoding="utf-8"
    )

    rev = ht.build_helper_to_task_paths(root)
    assert rev.get("foo") == set()


def test_build_helper_to_task_paths_skips_syntax_error(tmp_path: Path) -> None:
    """Invalid Python in a task file is skipped without failing (SyntaxError path)."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "foo.py").write_text("#\n", encoding="utf-8")
    bad = root / "scripts" / "python" / "tasks" / "internal" / "broken.py"
    bad.write_text("def not_closed(\n", encoding="utf-8")
    good = root / "scripts" / "python" / "tasks" / "internal" / "ok.py"
    good.write_text("import foo\n", encoding="utf-8")

    rev = ht.build_helper_to_task_paths(root)
    assert rev.get("foo") == {"scripts/python/tasks/internal/ok.py"}


def test_build_helper_to_task_paths_skips_oserror_on_read(tmp_path: Path) -> None:
    """Unreadable task files are omitted (OSError path)."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "foo.py").write_text("#\n", encoding="utf-8")
    task = root / "scripts" / "python" / "tasks" / "internal" / "blocked.py"
    task.write_text("import foo\n", encoding="utf-8")

    real_read = Path.read_text

    def read_text_wrapper(self: Path, *a, **kw):
        if self.name == "blocked.py":
            raise OSError("permission denied")
        return real_read(self, *a, **kw)

    with patch.object(Path, "read_text", read_text_wrapper):
        rev = ht.build_helper_to_task_paths(root)
    assert rev.get("foo") == set()


def test_expand_changed_paths_for_helper_deps_appends_tasks(tmp_path: Path) -> None:
    """Changing a helper path pulls in importing task paths."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "file.py").write_text("#\n", encoding="utf-8")
    task = root / "scripts" / "python" / "tasks" / "internal" / "step.py"
    task.write_text("import file\n", encoding="utf-8")

    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["scripts/python/helpers/file.py"],
        _reverse=reverse,
    )
    assert out == [
        "scripts/python/helpers/file.py",
        "scripts/python/tasks/internal/step.py",
    ]


def test_expand_changed_paths_for_helper_deps_preserves_order_dedupes(
    tmp_path: Path,
) -> None:
    """Dedupe paths and keep first occurrence order."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "a.py").write_text("#\n", encoding="utf-8")
    t1 = root / "scripts" / "python" / "tasks" / "internal" / "t1.py"
    t2 = root / "scripts" / "python" / "tasks" / "internal" / "t2.py"
    t1.write_text("import a\n", encoding="utf-8")
    t2.write_text("import a\n", encoding="utf-8")

    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["scripts/python/helpers/a.py", "scripts/python/tasks/internal/t1.py"],
        _reverse=reverse,
    )
    assert out[0] == "scripts/python/helpers/a.py"
    assert out[1] == "scripts/python/tasks/internal/t1.py"
    assert out[2] == "scripts/python/tasks/internal/t2.py"


def test_expand_changed_paths_for_helper_deps_no_helpers_dir(tmp_path: Path) -> None:
    """Missing helpers tree leaves paths unchanged."""
    root = tmp_path / "empty"
    root.mkdir()
    out = ht.expand_changed_paths_for_helper_deps(root, ["README.md"])
    assert out == ["README.md"]


def test_expand_skips_blank_changed_lines(tmp_path: Path) -> None:
    """Empty stdin lines are ignored."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "a.py").write_text("#\n", encoding="utf-8")
    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["", "   ", "\t"],
        _reverse=reverse,
    )
    assert out == []


def test_expand_skips_helpers_init_py(tmp_path: Path) -> None:
    """``helpers/__init__.py`` does not map to helper stems; no extra task paths."""
    root = _minimal_repo(tmp_path)
    (root / "scripts" / "python" / "helpers" / "__init__.py").write_text(
        "#\n", encoding="utf-8"
    )
    (root / "scripts" / "python" / "helpers" / "mod.py").write_text("#\n", encoding="utf-8")
    task = root / "scripts" / "python" / "tasks" / "internal" / "t.py"
    task.write_text("import mod\n", encoding="utf-8")

    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["scripts/python/helpers/__init__.py"],
        _reverse=reverse,
    )
    assert out == ["scripts/python/helpers/__init__.py"]
