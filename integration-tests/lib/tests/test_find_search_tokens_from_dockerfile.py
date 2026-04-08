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
