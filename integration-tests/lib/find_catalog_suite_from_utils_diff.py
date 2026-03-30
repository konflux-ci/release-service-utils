#!/usr/bin/env python3
"""
Map a release-service-utils diff to catalog ``PIPELINE_TEST_SUITE`` and
``PIPELINE_USED`` strings.

1. Map changed ``scripts/`` paths to image paths (``/home/scripts/...``).
2. Search catalog ``tasks/**/*.yaml`` for those paths (skips
   ``tasks/**/tests/`` fixture YAML).
3. Source catalog's ``find_release_pipelines_from_pr.sh`` and run
   ``_catalog_stdin_task_paths_to_testcase_tokens`` (same mapping as catalog PR
   tooling).

**Stdin** (one changed path per line): print one JSON object
``{"pipelineTestSuite": <string|null>, "pipelineUsed": <string|null>}``.
Use JSON ``null`` when no suite/pipeline pair is resolved. Non-null values may be
space-separated tokens when several suites apply.

**``--print-all-pairs``:** print ``suite<TAB>pipeline`` per line from the catalog
clone (no stdin).

When multiple suites apply, ``pipelineTestSuite`` lists ``run-test.sh``
directory names and ``pipelineUsed`` lists pipeline names (same length,
word-aligned). Pairings are taken from
``integration-tests/*/resources/managed/rpa.yaml``
(``pipelines/managed/<name>/`` references)—no hardcoded suite maps in this repo.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

_MANAGED_PIPELINE_PATH = re.compile(r"pipelines/managed/([^/]+)/")


def all_suite_catalog_pairs(catalog: Path) -> list[tuple[str, str]]:
    """List one (suite directory, pipeline name) pair per integration test suite.

    Builds on ``_suite_pipeline_pairs_from_catalog_rpa``. A suite may reference
    several managed pipelines in its RPA; we keep a single pipeline per suite
    (the first in sorted ``(suite, pipe)`` order) so each suite appears once.
    Used for ``--print-all-pairs`` and for tooling that needs a representative
    pipeline name per suite.
    """
    raw = _suite_pipeline_pairs_from_catalog_rpa(catalog)
    chosen: dict[str, str] = {}
    for suite, pipe in sorted(raw):
        if suite not in chosen:
            chosen[suite] = pipe
    return [(s, chosen[s]) for s in sorted(chosen)]


def _suite_pipeline_pairs_from_catalog_rpa(catalog: Path) -> list[tuple[str, str]]:
    """Collect (suite directory, managed pipeline name) pairs from the catalog clone.

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
    """Build space-separated strings for ``PIPELINE_TEST_SUITE`` and ``PIPELINE_USED``.

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


def _is_under_task_tests_dir(path: Path, tasks_root: Path) -> bool:
    """Return whether ``path`` is under a ``tasks/.../tests/`` directory.

    Those directories hold Task test fixtures; we skip them so we only match real
    Task definitions.
    """
    try:
        rel = path.relative_to(tasks_root)
    except ValueError:
        return False
    return "tests" in rel.parts


def _find_tasks_referencing_image_paths(
    catalog: Path, image_script_paths: set[str]
) -> set[str]:
    """Find Task YAML files whose content mentions any given container script path.

    ``image_script_paths`` entries look like ``/home/scripts/foo.sh``. We walk
    ``catalog/tasks``, skip ``tasks/**/tests/**``, require ``kind: Task``, and
    return paths relative to ``catalog`` for every file that contains any
    substring match.
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
        # Any substring match is enough: Tasks embed paths like /home/scripts/foo.sh in steps.
        for script_path in image_script_paths:
            if script_path in text:
                found.add(task_yaml.relative_to(catalog).as_posix())
                break
    return found


def _suites_from_catalog_script(catalog: Path, task_relpaths: set[str]) -> set[str]:
    """Map catalog Task paths to integration testcase names via catalog's shell script.

    Sources ``find_release_pipelines_from_pr.sh`` from the catalog checkout and
    runs ``_catalog_stdin_task_paths_to_testcase_tokens`` with one Task path per
    line on stdin. Returns the set of testcase names printed on stdout (stdout
    may list multiple space-separated names).
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


def resolve(catalog: Path, changed_lines: list[str]) -> dict[str, str | None]:
    """Map changed utils paths to ``pipelineTestSuite`` and ``pipelineUsed`` values.

    Input is typically ``git diff --name-only`` output. Only ``scripts/`` paths
    participate: they are turned into ``/home/...`` paths, matched to catalog
    Task YAMLs, then handed to ``_suites_from_catalog_script`` to get pipeline
    testcase names. Those names are expanded to aligned suite and pipeline
    strings via ``_suite_pipeline_strings_for_tokens`` (names with no RPA entry
    are omitted). Returns ``null`` for both JSON keys when nothing resolves,
    including when every testcase name is dropped because no ``rpa.yaml``
    references it.
    """
    # changed_lines typically come from `git diff --name-only` in the utils repo.
    changed = [c.strip() for c in changed_lines if c.strip()]
    if not changed:
        return {"pipelineTestSuite": None, "pipelineUsed": None}

    script_files = [c for c in changed if c.startswith("scripts/")]

    suites: set[str] = set()

    # Repo paths scripts/foo.sh → paths as referenced inside the running container.
    image_script_paths: set[str] = set()
    for path in script_files:
        if path.endswith("/"):
            continue
        image_script_paths.add("/home/" + path)

    if image_script_paths:
        tasks = _find_tasks_referencing_image_paths(catalog, image_script_paths)
        # Delegate pipeline→suite mapping to catalog (same logic as catalog PR scripts).
        suites |= _suites_from_catalog_script(catalog, tasks)

    if not suites:
        return {"pipelineTestSuite": None, "pipelineUsed": None}
    pt, pu = _suite_pipeline_strings_for_tokens(catalog, suites)
    if not pt and not pu:
        return {"pipelineTestSuite": None, "pipelineUsed": None}
    return {"pipelineTestSuite": pt, "pipelineUsed": pu}


def main() -> None:
    """Parse CLI; read stdin path lines or emit ``--print-all-pairs``; print JSON or TSV."""
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
        help=("Print suite<TAB>pipeline per line for every integration suite " "(no stdin)."),
    )
    args = parser.parse_args()
    catalog = args.catalog.resolve()
    if not catalog.is_dir():
        if args.print_all_pairs:
            print(
                f"find_catalog_suite_from_utils_diff: missing catalog dir {catalog}",
                file=sys.stderr,
            )
            sys.exit(1)
        print(json.dumps({"pipelineTestSuite": None, "pipelineUsed": None}))
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
    print(json.dumps(resolve(catalog, changed)))


if __name__ == "__main__":
    main()
