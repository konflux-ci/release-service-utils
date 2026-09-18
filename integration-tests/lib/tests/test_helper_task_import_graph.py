"""Tests for ``helper_task_import_graph``."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import patch

import helper_task_import_graph as ht


def _minimal_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "src" / "helpers").mkdir(parents=True)
    (root / "src" / "tasks" / "internal").mkdir(parents=True)
    return root


def test_is_task_script_requires_py_suffix() -> None:
    """Runnable tasks are ``*.py``; other suffixes are ignored."""
    assert ht._is_task_script(Path("src/tasks/internal/run.py")) is True
    assert ht._is_task_script(Path("src/tasks/internal/run.sh")) is False
    assert ht._is_task_script(Path("src/tasks/managed/test_run.py")) is False
    assert ht._is_task_script(Path("src/tasks/managed/tests/run.py")) is False


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
    """``import foo`` maps helper stem ``foo`` to the task path (nested task package)."""
    root = _minimal_repo(tmp_path)
    (root / "src" / "helpers" / "foo.py").write_text("# x\n", encoding="utf-8")
    task_pkg = root / "src" / "tasks" / "internal" / "runner"
    task_pkg.mkdir()
    (task_pkg / "runner.py").write_text("import foo\n", encoding="utf-8")

    rev = ht.build_helper_to_task_paths(root)
    assert rev.get("foo") == {"src/tasks/internal/runner/runner.py"}


def test_build_helper_to_task_paths_skips_test_modules(tmp_path: Path) -> None:
    """Skip ``test_*.py`` next to tasks."""
    root = _minimal_repo(tmp_path)
    (root / "src" / "helpers" / "foo.py").write_text("#\n", encoding="utf-8")
    task_pkg = root / "src" / "tasks" / "internal" / "runner"
    task_pkg.mkdir()
    (task_pkg / "test_runner.py").write_text("import foo\n", encoding="utf-8")

    rev = ht.build_helper_to_task_paths(root)
    assert rev.get("foo") == set()


def test_build_helper_to_task_paths_skips_syntax_error(tmp_path: Path) -> None:
    """Invalid Python in a task file is skipped without failing (SyntaxError path)."""
    root = _minimal_repo(tmp_path)
    (root / "src" / "helpers" / "foo.py").write_text("#\n", encoding="utf-8")
    bad_pkg = root / "src" / "tasks" / "internal" / "broken"
    bad_pkg.mkdir()
    (bad_pkg / "broken.py").write_text("def not_closed(\n", encoding="utf-8")
    good_pkg = root / "src" / "tasks" / "internal" / "ok"
    good_pkg.mkdir()
    (good_pkg / "ok.py").write_text("import foo\n", encoding="utf-8")

    rev = ht.build_helper_to_task_paths(root)
    assert rev.get("foo") == {"src/tasks/internal/ok/ok.py"}


def test_build_helper_to_task_paths_skips_oserror_on_read(tmp_path: Path) -> None:
    """Unreadable task files are omitted (OSError path)."""
    root = _minimal_repo(tmp_path)
    (root / "src" / "helpers" / "foo.py").write_text("#\n", encoding="utf-8")
    task_pkg = root / "src" / "tasks" / "internal" / "blocked"
    task_pkg.mkdir()
    (task_pkg / "blocked.py").write_text("import foo\n", encoding="utf-8")

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
    (root / "src" / "helpers" / "file.py").write_text("#\n", encoding="utf-8")
    task_pkg = root / "src" / "tasks" / "internal" / "step"
    task_pkg.mkdir()
    (task_pkg / "step.py").write_text("import file\n", encoding="utf-8")

    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["src/helpers/file.py"],
        _reverse=reverse,
    )
    assert out == [
        "src/helpers/file.py",
        "src/tasks/internal/step/step.py",
    ]


def test_expand_changed_paths_for_helper_deps_preserves_order_dedupes(
    tmp_path: Path,
) -> None:
    """Dedupe paths and keep first occurrence order."""
    root = _minimal_repo(tmp_path)
    (root / "src" / "helpers" / "a.py").write_text("#\n", encoding="utf-8")
    t1_pkg = root / "src" / "tasks" / "internal" / "t1"
    t1_pkg.mkdir()
    (t1_pkg / "t1.py").write_text("import a\n", encoding="utf-8")
    t2_pkg = root / "src" / "tasks" / "internal" / "t2"
    t2_pkg.mkdir()
    (t2_pkg / "t2.py").write_text("import a\n", encoding="utf-8")

    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["src/helpers/a.py", "src/tasks/internal/t1/t1.py"],
        _reverse=reverse,
    )
    assert out[0] == "src/helpers/a.py"
    assert out[1] == "src/tasks/internal/t1/t1.py"
    assert out[2] == "src/tasks/internal/t2/t2.py"


def test_expand_changed_paths_for_helper_deps_no_helpers_dir(tmp_path: Path) -> None:
    """Missing helpers tree leaves paths unchanged."""
    root = tmp_path / "empty"
    root.mkdir()
    out = ht.expand_changed_paths_for_helper_deps(root, ["README.md"])
    assert out == ["README.md"]


def test_expand_skips_blank_changed_lines(tmp_path: Path) -> None:
    """Empty stdin lines are ignored."""
    root = _minimal_repo(tmp_path)
    (root / "src" / "helpers" / "a.py").write_text("#\n", encoding="utf-8")
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
    (root / "src" / "helpers" / "__init__.py").write_text("#\n", encoding="utf-8")
    (root / "src" / "helpers" / "mod.py").write_text("#\n", encoding="utf-8")
    task_pkg = root / "src" / "tasks" / "internal" / "t"
    task_pkg.mkdir()
    (task_pkg / "t.py").write_text("import mod\n", encoding="utf-8")

    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["src/helpers/__init__.py"],
        _reverse=reverse,
    )
    assert out == ["src/helpers/__init__.py"]


def test_nested_helper_package_detection(tmp_path: Path) -> None:
    """Nested helper package (helpers/name/name.py) is detected as stem."""
    root = _minimal_repo(tmp_path)
    helper_pkg = root / "src" / "helpers" / "sign_windows"
    helper_pkg.mkdir()
    (helper_pkg / "sign_windows.py").write_text("#\n", encoding="utf-8")

    stems = ht._helper_stems(root / "src" / "helpers")
    assert "sign_windows" in stems


def test_nested_task_package_import_detection(tmp_path: Path) -> None:
    """Task in nested package (tasks/internal/name/name.py) can import helpers."""
    root = _minimal_repo(tmp_path)

    # Create helper: helpers/myhelper/myhelper.py
    helper_pkg = root / "src" / "helpers" / "myhelper"
    helper_pkg.mkdir()
    (helper_pkg / "myhelper.py").write_text("#\n", encoding="utf-8")

    # Create task: tasks/internal/mytask/mytask.py
    task_pkg = root / "src" / "tasks" / "internal" / "mytask"
    task_pkg.mkdir()
    (task_pkg / "mytask.py").write_text(
        "from release_service_utils.helpers import myhelper\n", encoding="utf-8"
    )

    rev = ht.build_helper_to_task_paths(root)
    assert rev.get("myhelper") == {"src/tasks/internal/mytask/mytask.py"}


def test_expand_nested_helper_package(tmp_path: Path) -> None:
    """Changing helpers/name/name.py expands to importing nested task packages."""
    root = _minimal_repo(tmp_path)

    # Helper package
    helper_pkg = root / "src" / "helpers" / "http_client"
    helper_pkg.mkdir()
    (helper_pkg / "http_client.py").write_text("#\n", encoding="utf-8")

    # Task package that imports it
    task_pkg = root / "src" / "tasks" / "internal" / "fetch_data"
    task_pkg.mkdir()
    (task_pkg / "fetch_data.py").write_text(
        "from release_service_utils.helpers import http_client\n", encoding="utf-8"
    )

    reverse = ht.build_helper_to_task_paths(root)
    out = ht.expand_changed_paths_for_helper_deps(
        root,
        ["src/helpers/http_client/http_client.py"],
        _reverse=reverse,
    )
    assert out == [
        "src/helpers/http_client/http_client.py",
        "src/tasks/internal/fetch_data/fetch_data.py",
    ]
