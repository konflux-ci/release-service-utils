"""Find catalog Task search tokens from utils repo paths using the image Dockerfile.

release-service-catalog Tekton Tasks reference files and commands that exist inside the
``release-service-utils`` container. Those paths are defined by ``COPY <src> <dest>`` lines
(targeting ``/home/...``) and by ``ENV PATH`` entries. This module parses a Dockerfile and
maps changed repository paths (e.g. from ``git diff --name-only``) to strings that can be
searched for in Task YAML — without a hand-maintained table.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

# ``COPY --from=...`` installs into paths other than our repo layout; skip those lines.
_COPY_HOME = re.compile(
    r"^COPY\s+(?!--from=)(\S+)\s+(/home/\S+)\s*(?:#.*)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class UtilsImageHomeLayout:
    """Layout of repo trees copied into ``/home`` and dirs appended to ``PATH``."""

    #: First path segment in the repo (e.g. ``pyxis``) -> container dir (e.g. ``/home/pyxis``).
    repo_segment_to_home: dict[str, str]
    #: Dirs on ``PATH`` (e.g. ``/home/pyxis``) for basename search tokens.
    path_home_dirs: frozenset[str]


def parse_dockerfile_home_layout(dockerfile_text: str) -> UtilsImageHomeLayout:
    """Parse ``COPY <src> /home/...`` and ``ENV PATH=`` lines from Dockerfile text.

    Only ``COPY`` lines without ``--from=`` are considered. ``<src>`` must be a single path
    segment (no ``/``); multi-segment sources (e.g. ``data/certs``) are skipped so only
    top-level repo directories mapped into ``/home`` are indexed.
    """
    # First pass: COPY lines map repo roots (pyxis, scripts, …) under /home.
    segment_to_home: dict[str, str] = {}
    for line in dockerfile_text.splitlines():
        raw = line.split("#", 1)[0].strip()
        m = _COPY_HOME.match(raw)
        if not m:
            continue
        src, dest = m.group(1), m.group(2)
        # Skip COPY foo/bar … — one top-level source dir per line (matches Dockerfile).
        if "/" in src:
            continue
        segment_to_home[src] = dest.rstrip("/")

    # Second pass: PATH adds e.g. /home/pyxis for basename search tokens.
    path_dirs: set[str] = set()
    for line in dockerfile_text.splitlines():
        raw = line.split("#", 1)[0].strip()
        if not raw.upper().startswith("ENV "):
            continue
        if "PATH" not in raw:
            continue
        for hm in re.finditer(r"/home/[a-zA-Z0-9_.-]+", raw):
            path_dirs.add(hm.group(0))

    return UtilsImageHomeLayout(
        repo_segment_to_home=segment_to_home,
        path_home_dirs=frozenset(path_dirs),
    )


def load_layout_from_dockerfile(dockerfile: Path) -> UtilsImageHomeLayout:
    """Read and parse the utils ``Dockerfile`` at ``dockerfile``."""
    text = dockerfile.read_text(encoding="utf-8", errors="replace")
    return parse_dockerfile_home_layout(text)


def search_tokens_for_repo_path(rel_path: str, layout: UtilsImageHomeLayout) -> frozenset[str]:
    """Return search tokens for one repo-relative path (POSIX, e.g. ``pyxis/foo.py``).

    Includes the in-container file path under ``/home/...``. For ``*.py`` files whose
    directory is on ``PATH`` (or the file lies directly under that tree root), also adds the
    module stem (e.g. ``create_container_image``) so Tasks that invoke the command without a
    full path still match.

    Directory-only paths (empty or trailing ``/``) yield no search tokens. Paths whose first
    segment is not part of the image layout yield an empty set.
    """
    raw = rel_path.strip()
    # Trailing slash: do not emit a search token for the tree root alone.
    if not raw or raw.endswith("/"):
        return frozenset()
    rel = raw.strip("./")
    if not rel:
        return frozenset()

    parts = rel.split("/", 1)
    root = parts[0]
    rest = parts[1] if len(parts) > 1 else ""

    home = layout.repo_segment_to_home.get(root)
    # First segment must be COPY'd to /home; else not in the image layout.
    if not home:
        return frozenset()

    if rest:
        full_path = f"{home}/{rest}"
    else:
        full_path = home

    # Always search for the full in-container path; Task YAML may reference it explicitly.
    out: set[str] = {full_path}

    if rest.endswith(".py"):
        parent_container_dir: str
        if "/" in rest:
            parent_container_dir = f"{home}/{PurePosixPath(rest).parent.as_posix()}"
        else:
            parent_container_dir = home
        # Tasks often run `create_container_image` without a path; dir must be on PATH.
        if parent_container_dir in layout.path_home_dirs or home in layout.path_home_dirs:
            stem = PurePosixPath(rest).stem
            if stem:
                out.add(stem)

    return frozenset(out)


def search_tokens_for_changed_paths(
    changed_paths: list[str],
    layout: UtilsImageHomeLayout,
) -> frozenset[str]:
    """Union of :func:`search_tokens_for_repo_path` over all changed paths."""
    acc: set[str] = set()
    for line in changed_paths:
        acc.update(search_tokens_for_repo_path(line, layout))
    return frozenset(acc)
