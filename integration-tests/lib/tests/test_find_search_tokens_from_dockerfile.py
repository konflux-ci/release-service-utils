"""Tests for ``find_search_tokens_from_dockerfile``."""

from __future__ import annotations

from pathlib import Path

import find_search_tokens_from_dockerfile as fts

MINIMAL_UTILS_DOCKERFILE = """
COPY pyxis /home/pyxis
COPY scripts /home/scripts
ENV PATH="$PATH:/home/pyxis"
"""


def test_parse_dockerfile_home_layout_maps_copy_and_path() -> None:
    """``COPY`` to ``/home`` and ``ENV PATH`` augmentations are captured."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    assert layout.repo_segment_to_home == {
        "pyxis": "/home/pyxis",
        "scripts": "/home/scripts",
    }
    assert "/home/pyxis" in layout.path_home_dirs


def test_parse_dockerfile_ignores_pythonpath_env() -> None:
    """``ENV PYTHONPATH`` must not be treated as executable ``PATH``."""
    text = """
COPY scripts /home/scripts
ENV PATH="$PATH:/home/pyxis"
ENV PYTHONPATH="/home:/home/scripts/python/helpers:/home/scripts/python/tasks/internal"
"""
    layout = fts.parse_dockerfile_home_layout(text)
    assert "/home/pyxis" in layout.path_home_dirs
    assert "/home/scripts" not in layout.path_home_dirs


def test_search_tokens_scripts_py_no_stem_when_scripts_not_on_path() -> None:
    """``scripts/**/*.py`` only get a full path token when ``/home/scripts`` is not on PATH."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    tokens = fts.search_tokens_for_repo_path("scripts/python/helpers/tekton.py", layout)
    assert tokens == frozenset({"/home/scripts/python/helpers/tekton.py"})


def test_parse_skips_copy_from_stage() -> None:
    """``COPY --from=`` lines do not define repo layout."""
    text = """
COPY --from=oras /usr/bin/oras /usr/bin/oras
COPY utils /home/utils
"""
    layout = fts.parse_dockerfile_home_layout(text)
    assert layout.repo_segment_to_home == {"utils": "/home/utils"}


def test_parse_skips_multi_segment_copy_source() -> None:
    """Only single-segment ``COPY`` sources are mapped (see module docstring)."""
    text = """
COPY foo/bar /home/bar
COPY pyxis /home/pyxis
"""
    layout = fts.parse_dockerfile_home_layout(text)
    assert layout.repo_segment_to_home == {"pyxis": "/home/pyxis"}


def test_load_layout_from_dockerfile_reads_file(tmp_path: Path) -> None:
    """``load_layout_from_dockerfile`` reads path and parses layout."""
    df = tmp_path / "Dockerfile"
    df.write_text(MINIMAL_UTILS_DOCKERFILE, encoding="utf-8")
    layout = fts.load_layout_from_dockerfile(df)
    assert "pyxis" in layout.repo_segment_to_home


def test_search_tokens_pyxis_py_includes_path_and_command_stem() -> None:
    """Python files under a PATH dir get a basename search token for PATH invocation."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    n = fts.search_tokens_for_repo_path("pyxis/create_container_image.py", layout)
    assert "/home/pyxis/create_container_image.py" in n
    assert "create_container_image" in n


def test_search_tokens_scripts_sh_only_full_path() -> None:
    """Non-``.py`` files under ``scripts/`` only get the container path search token."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    n = fts.search_tokens_for_repo_path("scripts/foo.sh", layout)
    assert n == frozenset({"/home/scripts/foo.sh"})


def test_search_tokens_unknown_root_empty() -> None:
    """Paths outside mapped ``COPY`` trees yield no search tokens."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    assert fts.search_tokens_for_repo_path("unknown/x.py", layout) == frozenset()


def test_search_tokens_skips_directory_paths() -> None:
    """Trailing slash paths are treated as directories and skipped."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    assert fts.search_tokens_for_repo_path("pyxis/", layout) == frozenset()


def test_search_tokens_strip_dot_slash_prefix() -> None:
    """Paths with leading ``./`` normalize like git output."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    a = fts.search_tokens_for_repo_path("scripts/x.sh", layout)
    b = fts.search_tokens_for_repo_path("./scripts/x.sh", layout)
    assert a == b


def test_search_tokens_for_changed_paths_unions() -> None:
    """``search_tokens_for_changed_paths`` unions per-file search tokens."""
    layout = fts.parse_dockerfile_home_layout(MINIMAL_UTILS_DOCKERFILE)
    n = fts.search_tokens_for_changed_paths(
        ["pyxis/a.py", "scripts/b.sh"],
        layout,
    )
    assert "/home/pyxis/a.py" in n
    assert "/home/scripts/b.sh" in n


# --- module-path token tests ---

_PKG_MAPPING = {"src": "release_service_utils"}


def test_parse_pyproject_package_dirs_extracts_mapping(tmp_path: Path) -> None:
    """Read ``[tool.setuptools] package-dir`` and invert to source→package."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.setuptools]\npackage-dir = {"release_service_utils" = "src"}\n',
        encoding="utf-8",
    )
    assert fts.parse_pyproject_package_dirs(pyproject) == {"src": "release_service_utils"}


def test_parse_pyproject_package_dirs_missing_key(tmp_path: Path) -> None:
    """Return empty mapping when ``[tool.setuptools]`` has no ``package-dir``."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'x'\n", encoding="utf-8")
    assert fts.parse_pyproject_package_dirs(pyproject) == {}


def test_module_tokens_task_py_file() -> None:
    """Generate parent-package module token for a task ``.py`` file."""
    tokens = fts.module_tokens_for_repo_path(
        "src/tasks/managed/check_labels/check_labels.py", _PKG_MAPPING
    )
    assert tokens == frozenset({"release_service_utils.tasks.managed.check_labels"})


def test_module_tokens_init_py() -> None:
    """Generate package module token for ``__init__.py``."""
    tokens = fts.module_tokens_for_repo_path(
        "src/tasks/managed/check_labels/__init__.py", _PKG_MAPPING
    )
    assert tokens == frozenset({"release_service_utils.tasks.managed.check_labels"})


def test_module_tokens_main_py() -> None:
    """Generate package module token for ``__main__.py``."""
    tokens = fts.module_tokens_for_repo_path(
        "src/tasks/managed/check_labels/__main__.py", _PKG_MAPPING
    )
    assert tokens == frozenset({"release_service_utils.tasks.managed.check_labels"})


def test_module_tokens_non_py_returns_empty() -> None:
    """Non-``.py`` files yield no module tokens."""
    assert fts.module_tokens_for_repo_path("src/tasks/README.md", _PKG_MAPPING) == frozenset()


def test_module_tokens_unknown_root_returns_empty() -> None:
    """Paths outside mapped source dirs yield no module tokens."""
    assert fts.module_tokens_for_repo_path("pyxis/foo.py", _PKG_MAPPING) == frozenset()


def test_module_tokens_bare_package_skipped() -> None:
    """Skip bare package name from ``src/__init__.py``."""
    assert fts.module_tokens_for_repo_path("src/__init__.py", _PKG_MAPPING) == frozenset()


def test_module_tokens_directory_path_returns_empty() -> None:
    """Directory paths (trailing slash) yield no module tokens."""
    assert fts.module_tokens_for_repo_path("src/tasks/", _PKG_MAPPING) == frozenset()


def test_module_tokens_dot_slash_prefix() -> None:
    """Paths with ``./`` prefix normalize correctly."""
    tokens = fts.module_tokens_for_repo_path("./src/helpers/tekton/__init__.py", _PKG_MAPPING)
    assert tokens == frozenset({"release_service_utils.helpers.tekton"})


def test_module_tokens_for_changed_paths_unions() -> None:
    """Union module tokens across multiple changed paths."""
    tokens = fts.module_tokens_for_changed_paths(
        [
            "src/tasks/managed/check_labels/check_labels.py",
            "src/helpers/tekton/__init__.py",
        ],
        _PKG_MAPPING,
    )
    assert "release_service_utils.tasks.managed.check_labels" in tokens
    assert "release_service_utils.helpers.tekton" in tokens
