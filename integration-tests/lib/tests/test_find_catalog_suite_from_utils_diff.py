"""Unit tests for ``find_catalog_suite_from_utils_diff``."""

from __future__ import annotations

import json
from io import StringIO
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import find_catalog_suite_from_utils_diff as fc

# ``resolve()`` reads ``./Dockerfile`` from cwd; tests chdir into a temp tree with it.
_MINIMAL_UTILS_DOCKERFILE = """
COPY pyxis /home/pyxis
COPY scripts /home/scripts
ENV PATH="$PATH:/home/pyxis"
"""


@pytest.fixture
def utils_repo_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Chdir to a temp tree containing ``Dockerfile``."""
    root = tmp_path / "utils-repo"
    root.mkdir()
    (root / "Dockerfile").write_text(
        _MINIMAL_UTILS_DOCKERFILE.strip() + "\n", encoding="utf-8"
    )
    monkeypatch.chdir(root)
    return root


def _write_rpa(catalog: Path, suite: str, *snippets: str) -> None:
    base = catalog / "integration-tests" / suite / "resources" / "managed"
    base.mkdir(parents=True)
    rpa = base / "rpa.yaml"
    rpa.write_text("\n".join(snippets), encoding="utf-8")


def test_all_suite_catalog_pairs_first_sorted_pipeline_per_suite(tmp_path: Path) -> None:
    """Pick the first pipeline in sorted order for a suite."""
    _write_rpa(
        tmp_path,
        "e2e",
        "a: pipelines/managed/zebra/",
        "b: pipelines/managed/alpha/",
    )
    pairs = fc.all_suite_catalog_pairs(tmp_path)
    assert pairs == [("e2e", "alpha")]


def test_all_suite_catalog_pairs_two_suites(tmp_path: Path) -> None:
    """Return one pair per suite in suite-sorted order."""
    _write_rpa(tmp_path, "a-suite", "x: pipelines/managed/p-common/")
    _write_rpa(tmp_path, "z-suite", "y: pipelines/managed/p-other/")
    pairs = fc.all_suite_catalog_pairs(tmp_path)
    assert pairs == [("a-suite", "p-common"), ("z-suite", "p-other")]


def test_suite_pipeline_pairs_from_catalog_rpa_missing_integration_tests(
    tmp_path: Path,
) -> None:
    """Return no pairs when integration-tests directory is absent."""
    assert fc._suite_pipeline_pairs_from_catalog_rpa(tmp_path) == []


def test_suite_pipeline_pairs_from_catalog_rpa_extracts_managed_names(tmp_path: Path) -> None:
    """Extract suite/pipeline pair from managed pipeline path in RPA."""
    _write_rpa(
        tmp_path,
        "e2e",
        "spec:",
        "  pipeline: pipelines/managed/e2e/e2e.yaml",
    )
    pairs = fc._suite_pipeline_pairs_from_catalog_rpa(tmp_path)
    assert ("e2e", "e2e") in pairs


def test_suite_pipeline_pairs_from_catalog_rpa_dedupes_same_pair(tmp_path: Path) -> None:
    """De-duplicate repeated references to the same suite/pipeline pair."""
    text = "pipelines/managed/foo/\nref: pipelines/managed/foo/\n"
    _write_rpa(tmp_path, "e2e", text)
    pairs = fc._suite_pipeline_pairs_from_catalog_rpa(tmp_path)
    assert pairs.count(("e2e", "foo")) == 1


def test_suite_pipeline_pairs_from_catalog_rpa_skips_unreadable_rpa(tmp_path: Path) -> None:
    """Skip unreadable RPA files and continue scanning."""
    _write_rpa(tmp_path, "e2e", "x: pipelines/managed/fbc-release/")
    with patch.object(Path, "read_text", side_effect=OSError("boom")):
        assert fc._suite_pipeline_pairs_from_catalog_rpa(tmp_path) == []


def test_suite_pipeline_strings_for_tokens_empty() -> None:
    """Return two empty strings when no pipeline tokens are provided."""
    assert fc._suite_pipeline_strings_for_tokens(Path("/nonexistent"), set()) == ("", "")


def test_suite_pipeline_strings_for_tokens_aligns_and_sorts_tokens(tmp_path: Path) -> None:
    """Emit aligned suite/pipeline token strings in sorted order."""
    _write_rpa(tmp_path, "suite-b", "p: pipelines/managed/y-pipe/")
    _write_rpa(tmp_path, "suite-a", "p: pipelines/managed/z-pipe/")
    pt, pu = fc._suite_pipeline_strings_for_tokens(tmp_path, {"y-pipe", "z-pipe"})
    assert pt == "suite-b suite-a"
    assert pu == "y-pipe z-pipe"


def test_suite_pipeline_strings_for_tokens_drops_unknown_pipeline(tmp_path: Path) -> None:
    """Keep mapped token ``known`` and drop unmapped token ``not-in-rpa``."""
    _write_rpa(tmp_path, "e2e", "p: pipelines/managed/known/")
    pt, pu = fc._suite_pipeline_strings_for_tokens(tmp_path, {"known", "not-in-rpa"})
    assert pt == "e2e"
    assert pu == "known"


def test_all_managed_pipeline_tokens_from_rpa(tmp_path: Path) -> None:
    """RPA helper collects distinct managed pipeline basenames from RPA files."""
    _write_rpa(
        tmp_path,
        "suite-a",
        "x: pipelines/managed/p-one/",
        "y: pipelines/managed/p-two/",
    )
    assert fc._all_managed_pipeline_tokens_from_rpa(tmp_path) == {"p-one", "p-two"}


def test_changed_paths_trigger_global_catalog_run() -> None:
    """Integration-tests plumbing (with exclusions) and repo-root Dockerfile."""
    g = fc._changed_paths_trigger_global_catalog_run
    assert g(["integration-tests/lib/x.py"]) is True
    assert g(["integration-tests/run-test.sh"]) is False
    assert g(["README.md"]) is False
    assert g(["integration-tests/README.md"]) is False
    assert g(["integration-tests/guide.md"]) is False
    assert g(["CONTRIBUTING.MD"]) is False
    assert g(["README.md", "integration-tests/lib/x.py"]) is True
    assert g(["Dockerfile"]) is True
    assert g(["./Dockerfile"]) is True
    assert g(["  Dockerfile  "]) is True
    assert g(["subdir/Dockerfile"]) is False
    assert g(["Dockerfile/"]) is False


def test_is_under_task_tests_dir(tmp_path: Path) -> None:
    """Identify files under a tasks/.../tests subtree."""
    tasks = tmp_path / "tasks"
    under = tasks / "managed" / "foo" / "tests" / "data.yaml"
    normal = tasks / "managed" / "bar" / "task.yaml"
    under.parent.mkdir(parents=True, exist_ok=True)
    normal.parent.mkdir(parents=True, exist_ok=True)
    assert fc._is_under_task_tests_dir(under, tasks) is True
    assert fc._is_under_task_tests_dir(normal, tasks) is False


def test_find_tasks_referencing_search_tokens_skips_tests_and_non_task(
    tmp_path: Path,
) -> None:
    """Keep real Task files and ignore fixture and non-Task YAML files."""
    tasks = tmp_path / "tasks"
    good = tasks / "managed" / "t" / "task.yaml"
    fixture = tasks / "managed" / "t" / "tests" / "fix.yaml"
    not_task = tasks / "snippet.yaml"
    good.parent.mkdir(parents=True, exist_ok=True)
    fixture.parent.mkdir(parents=True, exist_ok=True)
    not_task.parent.mkdir(parents=True, exist_ok=True)
    path = "/home/scripts/utils/foo.sh"
    good.write_text(f"kind: Task\nscript: {path}\n", encoding="utf-8")
    fixture.write_text(f"kind: Task\n{path}\n", encoding="utf-8")
    not_task.write_text(f"kind: Pipeline\n{path}\n", encoding="utf-8")
    found = fc._find_tasks_referencing_search_tokens(tmp_path, {path})
    assert found == {"tasks/managed/t/task.yaml"}


def test_find_tasks_referencing_search_tokens_returns_empty_without_tasks_root(
    tmp_path: Path,
) -> None:
    """Return an empty set when catalog/tasks does not exist."""
    found = fc._find_tasks_referencing_search_tokens(tmp_path, {"/home/scripts/x.sh"})
    assert found == set()


def test_find_tasks_referencing_search_tokens_skips_unreadable_yaml(tmp_path: Path) -> None:
    """Skip a task YAML file when read_text raises OSError."""
    task_yaml = tmp_path / "tasks" / "managed" / "t" / "task.yaml"
    task_yaml.parent.mkdir(parents=True, exist_ok=True)
    task_yaml.write_text("kind: Task\nscript: /home/scripts/x.sh\n", encoding="utf-8")
    with patch.object(Path, "read_text", side_effect=OSError("boom")):
        found = fc._find_tasks_referencing_search_tokens(tmp_path, {"/home/scripts/x.sh"})
    assert found == set()


def _write_catalog_script(catalog: Path) -> Path:
    script = catalog / "integration-tests" / "scripts" / "find_release_pipelines_from_pr.sh"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    return script


def test_suites_from_catalog_script_empty_task_paths_short_circuits(tmp_path: Path) -> None:
    """Return empty set immediately when no task paths are provided."""
    assert fc._suites_from_catalog_script(tmp_path, set()) == set()


def test_suites_from_catalog_script_missing_script_returns_empty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return empty set and print an error if catalog script is missing."""
    out = fc._suites_from_catalog_script(tmp_path, {"tasks/managed/x/task.yaml"})
    assert out == set()
    assert "missing" in capsys.readouterr().err


def test_suites_from_catalog_script_subprocess_error_returns_empty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return empty set and propagate stderr when subprocess fails."""
    _write_catalog_script(tmp_path)
    failed = SimpleNamespace(returncode=1, stdout="", stderr="boom\n")
    with patch.object(fc.subprocess, "run", return_value=failed):
        out = fc._suites_from_catalog_script(tmp_path, {"tasks/managed/x/task.yaml"})
    assert out == set()
    assert "boom" in capsys.readouterr().err


def test_suites_from_catalog_script_handles_no_test_case_token(tmp_path: Path) -> None:
    """Return empty set when catalog script prints no-test-case."""
    _write_catalog_script(tmp_path)
    proc = SimpleNamespace(returncode=0, stdout="no-test-case\n", stderr="")
    with patch.object(fc.subprocess, "run", return_value=proc):
        out = fc._suites_from_catalog_script(tmp_path, {"tasks/managed/x/task.yaml"})
    assert out == set()


def test_suites_from_catalog_script_splits_stdout_tokens(tmp_path: Path) -> None:
    """Split whitespace-delimited tokens from stdout into a set."""
    _write_catalog_script(tmp_path)
    proc = SimpleNamespace(returncode=0, stdout="fbc-release e2e\n", stderr="")
    with patch.object(fc.subprocess, "run", return_value=proc):
        out = fc._suites_from_catalog_script(tmp_path, {"tasks/managed/x/task.yaml"})
    assert out == {"fbc-release", "e2e"}


@pytest.mark.usefixtures("utils_repo_root")
def test_collect_task_search_tokens_maps_paths_via_dockerfile(tmp_path: Path) -> None:
    """Maps paths using cwd ``Dockerfile`` (same as ``search_tokens_for_changed_paths``)."""
    tokens = fc._collect_task_search_tokens(["pyxis/foo.py"])
    assert "/home/pyxis/foo.py" in tokens


@pytest.mark.usefixtures("utils_repo_root")
def test_collect_task_search_tokens_unions_multiple_paths(tmp_path: Path) -> None:
    """Unions search tokens across changed paths."""
    tokens = fc._collect_task_search_tokens(["scripts/a.sh", "pyxis/b.py"])
    assert "/home/scripts/a.sh" in tokens
    assert "/home/pyxis/b.py" in tokens


def test_collect_task_search_tokens_raises_without_dockerfile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Requires resolved ``./Dockerfile`` under cwd (utils repo root)."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match="Dockerfile required"):
        fc._collect_task_search_tokens(["scripts/x.sh"])


def test_resolve_empty_changed(tmp_path: Path) -> None:
    """Return null payload when changed input lines are empty."""
    assert fc.resolve(tmp_path, []) == {
        "pipelineTestSuite": None,
        "pipelineUsed": None,
    }
    assert fc.resolve(tmp_path, ["", "  "]) == {
        "pipelineTestSuite": None,
        "pipelineUsed": None,
    }


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_ignores_non_scripts(tmp_path: Path) -> None:
    """Paths that yield no search tokens and no global plumbing rules return null."""
    assert fc.resolve(tmp_path, ["README.md", "LICENSE"]) == {
        "pipelineTestSuite": None,
        "pipelineUsed": None,
    }


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_returns_null_when_no_suites_from_script(tmp_path: Path) -> None:
    """Return null payload when catalog mapping yields no suite tokens."""
    with patch.object(fc, "_suites_from_catalog_script", return_value=set()):
        out = fc.resolve(tmp_path, ["scripts/foo.sh"])
    assert out == {"pipelineTestSuite": None, "pipelineUsed": None}


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_maps_suites_when_script_returns_tokens(tmp_path: Path) -> None:
    """Map pipeline tokens to suite/pipeline output strings."""
    _write_rpa(tmp_path, "e2e", "p: pipelines/managed/fbc-release/")
    with patch.object(fc, "_suites_from_catalog_script", return_value={"fbc-release"}):
        out = fc.resolve(tmp_path, ["scripts/whatever.sh"])
    assert out == {
        "pipelineTestSuite": "e2e",
        "pipelineUsed": "fbc-release",
    }


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_returns_null_when_tokens_not_in_rpa(tmp_path: Path) -> None:
    """Return null payload when tokens exist but no RPA binding exists."""
    with patch.object(fc, "_suites_from_catalog_script", return_value={"orphan-pipeline"}):
        out = fc.resolve(tmp_path, ["scripts/x.sh"])
    assert out == {"pipelineTestSuite": None, "pipelineUsed": None}


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_ignores_directory_like_scripts_path(tmp_path: Path) -> None:
    """Skip scripts paths ending with slash and return null payload."""
    out = fc.resolve(tmp_path, ["scripts/somedir/"])
    assert out == {"pipelineTestSuite": None, "pipelineUsed": None}


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_dockerfile_only_unions_all_rpa_tokens(tmp_path: Path) -> None:
    """Changes to repo-root ``Dockerfile`` union every managed pipeline token from RPA."""
    _write_rpa(tmp_path, "suite-a", "p: pipelines/managed/alpha-pipe/")
    _write_rpa(tmp_path, "suite-b", "p: pipelines/managed/zebra-pipe/")
    with patch.object(fc, "_suites_from_catalog_script", return_value=set()):
        out = fc.resolve(tmp_path, ["Dockerfile"])
    assert out == {
        "pipelineTestSuite": "suite-a suite-b",
        "pipelineUsed": "alpha-pipe zebra-pipe",
    }


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_integration_tests_change_unions_all_rpa_tokens(tmp_path: Path) -> None:
    """Changes under ``integration-tests/`` union every managed pipeline token from RPA."""
    _write_rpa(tmp_path, "suite-a", "p: pipelines/managed/alpha-pipe/")
    _write_rpa(tmp_path, "suite-b", "p: pipelines/managed/zebra-pipe/")
    with patch.object(fc, "_suites_from_catalog_script", return_value=set()):
        out = fc.resolve(tmp_path, ["integration-tests/lib/foo.py"])
    assert out == {
        "pipelineTestSuite": "suite-a suite-b",
        "pipelineUsed": "alpha-pipe zebra-pipe",
    }


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_integration_tests_run_test_sh_only_no_global(tmp_path: Path) -> None:
    """``integration-tests/run-test.sh`` alone does not trigger the plumbing global rule."""
    _write_rpa(tmp_path, "e2e", "p: pipelines/managed/e2e/")
    out = fc.resolve(tmp_path, ["integration-tests/run-test.sh"])
    assert out == {"pipelineTestSuite": None, "pipelineUsed": None}


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_readme_only_no_plumbing(tmp_path: Path) -> None:
    """Repo-root ``README.md`` alone does not trigger plumbing or search-token mapping."""
    _write_rpa(tmp_path, "e2e", "p: pipelines/managed/e2e/")
    out = fc.resolve(tmp_path, ["README.md"])
    assert out == {"pipelineTestSuite": None, "pipelineUsed": None}


@pytest.mark.usefixtures("utils_repo_root")
def test_resolve_integration_tests_markdown_only_no_global(tmp_path: Path) -> None:
    """``*.md`` under ``integration-tests/`` does not trigger the global RPA union."""
    _write_rpa(tmp_path, "e2e", "p: pipelines/managed/e2e/")
    out = fc.resolve(tmp_path, ["integration-tests/README.md"])
    assert out == {"pipelineTestSuite": None, "pipelineUsed": None}


@pytest.mark.parametrize("print_pairs", [False, True])
def test_main_missing_catalog_exits_with_error(
    print_pairs: bool,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Missing ``--catalog`` dir exits 1; stderr explains; no stdout."""
    missing = tmp_path / "nope"
    argv = ["prog", "--catalog", str(missing)]
    if print_pairs:
        argv.append("--print-all-pairs")
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit) as ei:
            fc.main()
        assert ei.value.code == 1
        out = capsys.readouterr()
        assert "missing catalog dir" in out.err
        assert out.out == ""


def test_main_print_all_pairs(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    """Print all suite/pipeline pairs as tab-separated lines."""
    _write_rpa(tmp_path, "my-suite", "p: pipelines/managed/pl/")
    with patch.object(sys, "argv", ["prog", "--catalog", str(tmp_path), "--print-all-pairs"]):
        fc.main()
    line = capsys.readouterr().out.strip().splitlines()
    assert line == ["my-suite\tpl"]


def test_main_reads_stdin_and_prints_resolve_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Read stdin path lines, call resolve, and print JSON output."""
    stdin_data = "scripts/a.sh\nscripts/b.sh\n"
    payload = {"pipelineTestSuite": "e2e", "pipelineUsed": "fbc-release"}
    with (
        patch.object(sys, "argv", ["prog", "--catalog", str(tmp_path)]),
        patch.object(sys, "stdin", StringIO(stdin_data)),
        patch.object(fc, "resolve", return_value=payload) as mock_resolve,
    ):
        fc.main()

    mock_resolve.assert_called_once_with(tmp_path.resolve(), ["scripts/a.sh", "scripts/b.sh"])
    assert json.loads(capsys.readouterr().out.strip()) == payload
