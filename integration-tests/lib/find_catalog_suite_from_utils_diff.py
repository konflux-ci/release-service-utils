#!/usr/bin/env python3
"""
Map a release-service-utils diff to catalog ``PIPELINE_TEST_SUITE`` /
``PIPELINE_USED`` strings.

1. Build search tokens from changed paths using the utils ``Dockerfile``
   (``COPY`` into ``/home``, ``PATH``) via :mod:`find_search_tokens_from_dockerfile`
   (the process cwd must be the utils repo root so ``./Dockerfile`` exists).
2. Search catalog ``tasks/**/*.yaml`` for those search tokens (skips
   ``tasks/**/tests/`` fixture YAML).
3. Source catalog's ``find_release_pipelines_from_pr.sh`` and run
   ``_catalog_stdin_task_paths_to_testcase_tokens`` (same mapping as catalog PR
   tooling).
4. Changes under **utils** ``integration-tests/`` (except ``run-test.sh`` and
   any ``*.md`` file), or changes to the repo-root ``Dockerfile``, force all catalog
   RPA suites—plumbing and image layout are not reliably reflected in Task YAML
   substring search alone.
5. Changes under ``scripts/python/helpers/*.py`` also add repo paths for task
   scripts that import those modules (see `helper_task_import_graph`) so
   catalog Tasks that invoke ``tasks/…/*.py`` still match when only helpers move.

**Stdin** (one changed path per line): print one JSON object
``{"pipelineTestSuite": <string|null>, "pipelineUsed": <string|null>}``.
Use JSON ``null`` when no suite/pipeline pair is resolved. Non-null values may be
space-separated tokens when several suites apply.

**``--print-all-pairs``:** print ``suite<TAB>pipeline`` per line from the catalog
clone (no stdin).

When multiple suites apply, ``pipelineTestSuite`` lists ``run-test.sh`` directory
names and ``pipelineUsed`` lists pipeline names (same length, word-aligned).
Pairings are taken from ``integration-tests/*/resources/managed/rpa.yaml``
(``pipelines/managed/<name>/`` references)—no hardcoded suite maps in this repo.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import find_search_tokens_from_dockerfile as fts
import helper_task_import_graph as htig

_MANAGED_PIPELINE_PATH = re.compile(r"pipelines/managed/([^/]+)/")


def all_suite_catalog_pairs(catalog: Path) -> list[tuple[str, str]]:
    """List one (suite directory, pipeline name) pair per integration test suite.

    Builds on ``_suite_pipeline_pairs_from_catalog_rpa``. A suite may reference
    several managed pipelines in its RPA; we keep a single pipeline per suite
    (the first in sorted ``(suite, pipe)`` order) so each suite appears once. Used
    for ``--print-all-pairs`` and for tooling that needs a representative
    pipeline name per suite.
    """
    raw = _suite_pipeline_pairs_from_catalog_rpa(catalog)
    chosen: dict[str, str] = {}
    for suite, pipe in sorted(raw):
        if suite not in chosen:
            chosen[suite] = pipe
    return [(s, chosen[s]) for s in sorted(chosen)]


def _suite_pipeline_pairs_from_catalog_rpa(catalog: Path) -> list[tuple[str, str]]:
    """Collect (integration suite directory, managed pipeline name) pairs.

    Walks ``integration-tests/*/resources/managed/rpa.yaml``. Each RPA file can
    mention release pipelines under ``pipelines/managed/<name>/``; we record one
    pair per distinct path: the parent suite directory (the first segment under
    ``integration-tests/``) and ``<name>`` as the pipeline name.
    """
    it = catalog / "integration-tests"
    if not it.is_dir():
        return []
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for rpa in sorted(it.glob("*/resources/managed/rpa.yaml")):
        try:
            text = rpa.read_text(errors="replace")
        except OSError:
            continue
        suite_dir = rpa.relative_to(it).parts[0]
        for m in _MANAGED_PIPELINE_PATH.finditer(text):
            pn = m.group(1)
            key = (suite_dir, pn)
            if key not in seen:
                seen.add(key)
                out.append(key)
    return out


def _suite_pipeline_strings_for_tokens(
    catalog: Path, pipeline_tokens: set[str]
) -> tuple[str, str]:
    """Build ``PIPELINE_TEST_SUITE`` and ``PIPELINE_USED`` space-separated strings.

    ``pipeline_tokens`` are pipeline testcase names (as returned by catalog's
    ``_catalog_stdin_task_paths_to_testcase_tokens``, usually managed pipeline
    basenames). We look up, in the RPA-derived index, which integration suite
    directories pair with each name. The result is two strings of tokens in
    matching order: suite directory names, then pipeline names. Tokens that do
    not appear under ``pipelines/managed/<token>/`` in any ``rpa.yaml`` are
    dropped—there is no integration test suite binding for them in this catalog
    checkout.
    """
    if not pipeline_tokens:
        return "", ""
    index = _suite_pipeline_pairs_from_catalog_rpa(catalog)
    by_pipeline: dict[str, list[str]] = {}
    for suite_dir, pipeline_name in index:
        by_pipeline.setdefault(pipeline_name, []).append(suite_dir)
    suite_parts: list[str] = []
    pipeline_name_parts: list[str] = []
    for token in sorted(pipeline_tokens):
        suite_dirs = sorted(set(by_pipeline.get(token, [])))
        if not suite_dirs:
            continue
        for sd in suite_dirs:
            suite_parts.append(sd)
            pipeline_name_parts.append(token)
    return " ".join(suite_parts), " ".join(pipeline_name_parts)


def _all_managed_pipeline_tokens_from_rpa(catalog: Path) -> set[str]:
    """Distinct managed pipeline basenames from every catalog ``rpa.yaml``."""
    return {pn for _, pn in _suite_pipeline_pairs_from_catalog_rpa(catalog)}


def _changed_paths_trigger_global_catalog_run(changed: list[str]) -> bool:
    """True if changed paths should force union of all catalog RPA pipeline tokens.

    Triggers when the repo-root ``Dockerfile`` changes, or when any path under
    ``integration-tests/`` changes except ``integration-tests/run-test.sh`` and
    except Markdown files (``*.md``, case-insensitive).
    """
    for raw in changed:
        s = raw.strip()
        if not s or s.endswith("/"):
            continue
        p = s.strip("./")
        if not p:
            continue
        if p == "Dockerfile":
            return True
        if Path(p).suffix.lower() == ".md":
            continue
        if p.startswith("integration-tests/") and p != "integration-tests/run-test.sh":
            return True
    return False


def _is_under_task_tests_dir(path: Path, tasks_root: Path) -> bool:
    """Return whether ``path`` is under a ``tasks/.../tests/`` directory.

    Those directories hold Task test fixtures; we skip them so we only match
    real Task definitions.
    """
    try:
        rel = path.relative_to(tasks_root)
    except ValueError:
        return False
    return "tests" in rel.parts


def _find_tasks_referencing_search_tokens(catalog: Path, search_tokens: set[str]) -> set[str]:
    """Find Task YAML files whose content mentions any search token.

    Search tokens are in-container paths (e.g. ``/home/pyxis/foo.py``) or command
    tokens (e.g. ``create_container_image``) from
    :mod:`find_search_tokens_from_dockerfile`. We walk ``catalog/tasks``, skip
    ``tasks/**/tests/**``, require ``kind: Task``, and return paths relative to
    ``catalog`` for any substring match.
    """
    tasks_root = catalog / "tasks"
    if not tasks_root.is_dir():
        return set()
    found: set[str] = set()
    for task_yaml in tasks_root.rglob("*.yaml"):
        if _is_under_task_tests_dir(task_yaml, tasks_root):
            continue
        try:
            text = task_yaml.read_text(errors="replace")
        except OSError:
            continue
        # Ignore non-Task files (e.g. Pipeline snippets) that might live under tasks/.
        if not re.search(r"kind:\s*Task\b", text):
            continue
        for token in search_tokens:
            if token in text:
                found.add(task_yaml.relative_to(catalog).as_posix())
                break
    return found


def _suites_from_catalog_script(catalog: Path, task_relpaths: set[str]) -> set[str]:
    """Map catalog Task paths to integration testcase names.

    Uses ``find_release_pipelines_from_pr.sh``: sources that file from the catalog
    checkout and runs ``_catalog_stdin_task_paths_to_testcase_tokens`` with one
    Task path per line on stdin. Returns testcase names from stdout (possibly
    multiple space-separated names).
    """
    if not task_relpaths:
        return set()
    catalog_script = (
        catalog / "integration-tests" / "scripts" / "find_release_pipelines_from_pr.sh"
    )
    if not catalog_script.is_file():
        print(
            f"find_catalog_suite_from_utils_diff: missing {catalog_script}",
            file=sys.stderr,
        )
        return set()
    proc = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1" || exit 1\n' '_catalog_stdin_task_paths_to_testcase_tokens "$2"\n',
            "_",
            str(catalog_script),
            str(catalog),
        ],
        input="\n".join(sorted(task_relpaths)) + "\n",
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr, end="")
        return set()
    out = proc.stdout.strip()
    if not out or out == "no-test-case":
        return set()
    return {w for w in out.split() if w}


def _collect_task_search_tokens(changed: list[str]) -> set[str]:
    """Build substring search tokens for catalog Task YAML from changed repo paths."""
    resolved = Path("Dockerfile").resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Dockerfile required (run from utils repo root): {resolved}")
    layout = fts.load_layout_from_dockerfile(resolved)
    return set(fts.search_tokens_for_changed_paths(changed, layout))


def resolve(catalog: Path, changed_lines: list[str]) -> dict[str, str | None]:
    """Map changed utils paths to ``pipelineTestSuite`` and ``pipelineUsed``.

    Input is typically ``git diff --name-only`` output. Paths are mapped with the
    utils ``Dockerfile`` (see :mod:`find_search_tokens_from_dockerfile`) to search
    tokens, matched to catalog Task YAMLs, then handed to
    ``_suites_from_catalog_script`` to get pipeline testcase names. When
    :func:`_changed_paths_trigger_global_catalog_run` is true, every managed
    pipeline token from catalog RPA is unioned in.
    Those names are expanded via ``_suite_pipeline_strings_for_tokens`` (names
    with no RPA entry are omitted). Returns ``null`` for both JSON keys when
    nothing resolves, including when every testcase name is dropped because no
    ``rpa.yaml`` references it.

    Reads ``./Dockerfile`` relative to the process working directory. Raises
    ``FileNotFoundError`` if it is missing.
    """
    # changed_lines typically come from `git diff --name-only` in the utils repo.
    changed = [c.strip() for c in changed_lines if c.strip()]
    if not changed:
        return {"pipelineTestSuite": None, "pipelineUsed": None}

    repo_root = Path.cwd().resolve()
    changed = htig.expand_changed_paths_for_helper_deps(repo_root, changed)

    suites: set[str] = set()

    search_tokens_set = _collect_task_search_tokens(changed)
    if search_tokens_set:
        tasks = _find_tasks_referencing_search_tokens(catalog, search_tokens_set)
        # Delegate pipeline→suite mapping to catalog (same logic as catalog PR scripts).
        suites |= _suites_from_catalog_script(catalog, tasks)

    if _changed_paths_trigger_global_catalog_run(changed):
        suites |= _all_managed_pipeline_tokens_from_rpa(catalog)

    if not suites:
        return {"pipelineTestSuite": None, "pipelineUsed": None}
    pt, pu = _suite_pipeline_strings_for_tokens(catalog, suites)
    if not pt and not pu:
        return {"pipelineTestSuite": None, "pipelineUsed": None}
    return {"pipelineTestSuite": pt, "pipelineUsed": pu}


def main() -> None:
    """Parse CLI; stdin path lines or ``--print-all-pairs``; print JSON or TSV."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--catalog",
        type=Path,
        required=True,
        help=("Path to a clone of release-service-catalog " "(branch already checked out)"),
    )
    parser.add_argument(
        "--print-all-pairs",
        action="store_true",
        help="Print suite<TAB>pipeline per line for every integration suite (no stdin).",
    )
    args = parser.parse_args()
    catalog = args.catalog.resolve()
    if not catalog.is_dir():
        print(
            f"find_catalog_suite_from_utils_diff: missing catalog dir {catalog}",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.print_all_pairs:
        for s, p in all_suite_catalog_pairs(catalog):
            print(f"{s}\t{p}")
        return

    changed = [ln for ln in sys.stdin.read().splitlines()]
    try:
        payload = resolve(catalog, changed)
    except FileNotFoundError as err:
        print(f"find_catalog_suite_from_utils_diff: {err}", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
