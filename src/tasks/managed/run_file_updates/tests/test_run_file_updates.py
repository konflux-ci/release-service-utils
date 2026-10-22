"""Tests for run_file_updates."""

from __future__ import annotations

import json
import runpy
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.helpers.internal_request import (
    SPAWN_OVERHEAD_SECONDS,
    duration_to_seconds,
)
from release_service_utils.tasks.managed.run_file_updates import run_file_updates

TASK = "release_service_utils.tasks.managed.run_file_updates.run_file_updates"


def _write_json(path: Path, data: object) -> None:
    """Write data to path as JSON, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


@pytest.fixture
def sample_snapshot() -> dict:
    """Return a sample snapshot with componentGroup."""
    return {
        "componentGroup": "test-app",
        "components": [
            {
                "containerImage": "quay.io/test/image:v1.0",
                "name": "test-component",
                "repository": "test/repo",
            }
        ],
    }


@pytest.fixture
def sample_rpa_data() -> dict:
    """Return sample RPA data with fileUpdates."""
    return {
        "spec": {
            "data": {
                "fileUpdates": [
                    {
                        "repo": "https://gitlab.cee.redhat.com/foo",
                        "upstream_repo": "https://github.com/foo/bar",
                        "ref": "main",
                        "paths": [
                            {
                                "path": "config.yaml",
                                "replacements": [{"key": "image", "replacement": "new-value"}],
                            }
                        ],
                    }
                ]
            }
        }
    }


class TestParseFileUpdateItem:
    """Tests for _parse_file_update_item."""

    def test_parse_complete_item(self) -> None:
        """Parse a complete file update item."""
        item_data = {
            "repo": "https://gitlab.example.com/repo",
            "upstream_repo": "https://github.com/org/repo",
            "ref": "development",
            "paths": [{"path": "file.yaml", "replacements": []}],
            "file_updates_secret": "my-secret",
        }
        item = run_file_updates._parse_file_update_item(item_data)
        assert item.repo == "https://gitlab.example.com/repo"
        assert item.upstream_repo == "https://github.com/org/repo"
        assert item.ref == "development"
        assert item.paths == [{"path": "file.yaml", "replacements": []}]
        assert item.file_updates_secret == "my-secret"

    def test_parse_with_defaults(self) -> None:
        """Parse an item with missing optional fields uses defaults."""
        item_data = {"repo": "https://gitlab.example.com/repo"}
        item = run_file_updates._parse_file_update_item(item_data)
        assert item.ref == "main"
        assert item.file_updates_secret == "file-updates-secret"
        assert item.paths == []

    def test_parse_null_paths_uses_empty_list(self) -> None:
        """Explicit null paths fall back to an empty list."""
        item_data = {"repo": "https://gitlab.example.com/repo", "paths": None}
        item = run_file_updates._parse_file_update_item(item_data)
        assert item.paths == []

    def test_parse_empty_ref_is_preserved(self) -> None:
        """An empty ref string is kept; jq // does not replace it."""
        item_data = {"repo": "https://gitlab.example.com/repo", "ref": ""}
        item = run_file_updates._parse_file_update_item(item_data)
        assert item.ref == ""

    def test_json_booleans_match_jq_raw(self, tmp_path: Path) -> None:
        """JSON booleans from the data file render as true and false."""
        data_file = tmp_path / "rpa.json"
        data_file.write_text(
            """
            {
              "spec": {
                "data": {
                  "fileUpdates": [
                    {
                      "repo": true,
                      "upstream_repo": false,
                      "ref": true,
                      "file_updates_secret": true
                    }
                  ]
                }
              }
            }
            """,
            encoding="utf-8",
        )
        loaded = json.loads(data_file.read_text(encoding="utf-8"))
        item = run_file_updates._parse_file_update_item(
            loaded["spec"]["data"]["fileUpdates"][0]
        )
        assert item.repo == "true"
        assert item.upstream_repo == "false"
        assert item.ref == "true"
        assert item.file_updates_secret == "true"

        snapshot_file = tmp_path / "snapshot.json"
        snapshot_file.write_text('{"componentGroup": true}', encoding="utf-8")
        snapshot = json.loads(snapshot_file.read_text(encoding="utf-8"))
        assert run_file_updates._jq_raw(snapshot.get("componentGroup")) == "true"
        snapshot_file.write_text('{"componentGroup": false}', encoding="utf-8")
        snapshot = json.loads(snapshot_file.read_text(encoding="utf-8"))
        assert run_file_updates._jq_raw(snapshot.get("componentGroup")) == "false"

    def test_json_false_uses_jq_alternative(self, tmp_path: Path) -> None:
        """JSON false triggers jq // defaults; the string false does not."""
        data_file = tmp_path / "rpa.json"
        data_file.write_text(
            """
            {
              "ref": false,
              "file_updates_secret": false,
              "string_ref": "false"
            }
            """,
            encoding="utf-8",
        )
        loaded = json.loads(data_file.read_text(encoding="utf-8"))
        false_item = run_file_updates._parse_file_update_item(loaded)
        assert false_item.ref == "main"
        assert false_item.file_updates_secret == "file-updates-secret"
        string_item = run_file_updates._parse_file_update_item({"ref": loaded["string_ref"]})
        assert string_item.ref == "false"


class TestEvaluatePathExpressions:
    """Tests for _evaluate_path_expressions."""

    def test_no_expressions(self, tmp_path: Path) -> None:
        """Paths without {{ }} expressions are returned unchanged."""
        snapshot = {"components": [{"name": "test"}]}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "replacements": [{"key": "k", "replacement": "plain-value"}],
            }
        ]
        result = run_file_updates._evaluate_path_expressions(paths, tmp_path / "snapshot.json")
        assert result[0]["replacements"][0]["replacement"] == "plain-value"

    def test_expression_evaluation(self, tmp_path: Path) -> None:
        """{{ }} expressions are evaluated with yq."""
        snapshot = {"components": [{"containerImage": "my-image:v1.0"}]}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "replacements": [
                    {
                        "key": "image",
                        "replacement": "|image:.*|image: {{ .components[0].containerImage }}|",
                    }
                ],
            }
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(stdout="my-image:v1.0\n", returncode=0)
            result = run_file_updates._evaluate_path_expressions(
                paths, tmp_path / "snapshot.json"
            )

        expected = "|image:.*|image: my-image:v1.0|"
        assert result[0]["replacements"][0]["replacement"] == expected

    def test_leading_whitespace_survives_substitution(self, tmp_path: Path) -> None:
        """Leading spaces in a snapshot scalar survive substitution."""
        snapshot = {"indent": "  indented"}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "replacements": [{"key": "indent", "replacement": "value: {{ .indent }}"}],
            }
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(stdout="  indented\n", returncode=0)
            result = run_file_updates._evaluate_path_expressions(
                paths, tmp_path / "snapshot.json"
            )

        assert result[0]["replacements"][0]["replacement"] == "value:   indented"

    def test_distinct_expressions_in_one_replacement(self, tmp_path: Path) -> None:
        """Each different expression in one replacement gets its own result."""
        snapshot = {"image": "my-image", "tag": "v1"}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "replacements": [
                    {
                        "key": "image",
                        "replacement": "image: {{ .image }} tag: {{ .tag }}",
                    }
                ],
            }
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                mock.Mock(stdout="my-image\n", returncode=0),
                mock.Mock(stdout="v1\n", returncode=0),
            ]
            result = run_file_updates._evaluate_path_expressions(
                paths, tmp_path / "snapshot.json"
            )

        assert result[0]["replacements"][0]["replacement"] == "image: my-image tag: v1"
        expressions = [call.args[0][3] for call in mock_run.call_args_list]
        assert expressions == [".image", ".tag"]

    def test_expression_evaluation_inserts_backslashes_literally(self, tmp_path: Path) -> None:
        """Backslashes in evaluated snapshot data are inserted literally."""
        snapshot = {"image": r"team\image\v1"}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "replacements": [{"key": "image", "replacement": "image: {{ .image }}"}],
            }
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(stdout="team\\image\\v1\n", returncode=0)
            result = run_file_updates._evaluate_path_expressions(
                paths, tmp_path / "snapshot.json"
            )

        assert result[0]["replacements"][0]["replacement"] == r"image: team\image\v1"

    def test_expression_evaluation_preserves_other_fields(self, tmp_path: Path) -> None:
        """Expression evaluation preserves other fields in the paths."""
        snapshot = {"value": "replaced"}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "extra_field": "preserved",
                "replacements": [{"key": "k", "replacement": "{{ .value }}", "other": "data"}],
            }
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(stdout="replaced\n", returncode=0)
            result = run_file_updates._evaluate_path_expressions(
                paths, tmp_path / "snapshot.json"
            )

        assert result[0]["extra_field"] == "preserved"
        assert result[0]["replacements"][0]["other"] == "data"

    def test_yq_failure_raises_error(self, tmp_path: Path) -> None:
        """Yq failure raises FileUpdatesError."""
        snapshot = {}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "replacements": [{"key": "k", "replacement": "{{ .invalid }}"}],
            }
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.side_effect = run_file_updates.subprocess.CalledProcessError(
                1, "yq", stderr="yq error"
            )
            with pytest.raises(run_file_updates.FileUpdatesError, match="Failed to evaluate"):
                run_file_updates._evaluate_path_expressions(paths, tmp_path / "snapshot.json")

    def test_multiple_replacements(self, tmp_path: Path) -> None:
        """Multiple replacements in a path are all evaluated."""
        snapshot = {"val1": "a", "val2": "b"}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {
                "path": "file.yaml",
                "replacements": [
                    {"key": "k1", "replacement": "{{ .val1 }}"},
                    {"key": "k2", "replacement": "{{ .val2 }}"},
                ],
            }
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                mock.Mock(stdout="a\n", returncode=0),
                mock.Mock(stdout="b\n", returncode=0),
            ]
            result = run_file_updates._evaluate_path_expressions(
                paths, tmp_path / "snapshot.json"
            )

        assert result[0]["replacements"][0]["replacement"] == "a"
        assert result[0]["replacements"][1]["replacement"] == "b"

    def test_null_replacements_are_treated_as_empty(self, tmp_path: Path) -> None:
        """A null replacements value is skipped and a later path is still evaluated."""
        snapshot = {"value": "replaced"}
        _write_json(tmp_path / "snapshot.json", snapshot)

        paths = [
            {"path": "untouched.yaml", "replacements": None},
            {
                "path": "file.yaml",
                "replacements": [{"key": "k", "replacement": "{{ .value }}"}],
            },
        ]

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(stdout="replaced\n", returncode=0)
            result = run_file_updates._evaluate_path_expressions(
                paths, tmp_path / "snapshot.json"
            )

        assert result[0]["replacements"] is None
        assert result[1]["replacements"][0]["replacement"] == "replaced"
        mock_run.assert_called_once()


class TestProcessIRResults:
    """Tests for _process_ir_results."""

    def test_success_returns_mr_url(self) -> None:
        """Successful IR returns the merge request URL."""
        results = {
            "buildState": "Succeeded",
            "jsonBuildInfo": json.dumps({"merge_request": "https://gitlab.com/mr/123"}),
        }
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            mr_url = run_file_updates._process_ir_results("test-ir")
        assert mr_url == "https://gitlab.com/mr/123"

    def test_success_state_also_works(self) -> None:
        """BuildState 'Success' is also accepted."""
        results = {
            "buildState": "Success",
            "jsonBuildInfo": json.dumps({"merge_request": "https://gitlab.com/mr/456"}),
        }
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            mr_url = run_file_updates._process_ir_results("test-ir")
        assert mr_url == "https://gitlab.com/mr/456"

    def test_empty_results_raises_error(self) -> None:
        """Empty results raise FileUpdatesError."""
        with mock.patch(f"{TASK}.fetch_results", return_value={}):
            with pytest.raises(run_file_updates.FileUpdatesError, match="empty results"):
                run_file_updates._process_ir_results("test-ir")

    def test_failed_state_raises_error(self) -> None:
        """Failed buildState raises FileUpdatesError with details."""
        results = {
            "buildState": "Failed",
            "jsonBuildInfo": json.dumps({"error": "merge conflict", "str": "diff content"}),
        }
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            with pytest.raises(run_file_updates.FileUpdatesError, match="merge conflict"):
                run_file_updates._process_ir_results("test-ir")

    def test_existing_mr_embedded_in_text(self) -> None:
        """An existing merge request embedded in text is extracted."""
        url = "https://gitlab.com/org/up/-/merge_requests/99"
        build_info = (
            "There is an existing MR with the same updates in the repo\n"
            + json.dumps({"merge_request": url})
            + "\n"
        )
        results = {"buildState": "Succeeded", "jsonBuildInfo": build_info}
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            mr_url = run_file_updates._process_ir_results("test-ir")
        assert mr_url == url

    def test_no_change_returns_no_url(self) -> None:
        """A no-change success does not invent a merge request URL."""
        results = {"buildState": "Success", "jsonBuildInfo": "nothing needs change\n"}
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            mr_url = run_file_updates._process_ir_results("test-ir")
        assert mr_url is None

    def test_malformed_json_build_info(self) -> None:
        """Unrecognized success text raises instead of looking like no change."""
        results = {
            "buildState": "Succeeded",
            "jsonBuildInfo": "not-json",
        }
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            with pytest.raises(
                run_file_updates.FileUpdatesError,
                match="succeeded without a merge request URL",
            ):
                run_file_updates._process_ir_results("test-ir")

    def test_skips_invalid_json_before_merge_request(self) -> None:
        """A broken brace before the merge request object is skipped."""
        url = "https://gitlab.com/org/up/-/merge_requests/7"
        build_info = "note {not-json} " + json.dumps({"merge_request": url})
        results = {"buildState": "Succeeded", "jsonBuildInfo": build_info}
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            mr_url = run_file_updates._process_ir_results("test-ir")
        assert mr_url == url

    def test_skips_object_without_merge_request(self) -> None:
        """An earlier object without a URL does not hide a later merge request."""
        url = "https://gitlab.com/org/up/-/merge_requests/8"
        build_info = json.dumps({"other": 1}) + " " + json.dumps({"merge_request": url})
        results = {"buildState": "Succeeded", "jsonBuildInfo": build_info}
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            mr_url = run_file_updates._process_ir_results("test-ir")
        assert mr_url == url

    def test_failed_state_unparseable_build_info(self) -> None:
        """Failed build info that is not JSON still raises FileUpdatesError."""
        results = {"buildState": "Failed", "jsonBuildInfo": "not-json"}
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            with pytest.raises(
                run_file_updates.FileUpdatesError, match="Failed to parse build info"
            ):
                run_file_updates._process_ir_results("test-ir")

    def test_non_string_json_build_info(self) -> None:
        """A non-string success payload is stringified before parsing."""
        results = {
            "buildState": "Succeeded",
            "jsonBuildInfo": {"merge_request": "https://gitlab.com/mr/9"},
        }
        with mock.patch(f"{TASK}.fetch_results", return_value=results):
            with pytest.raises(
                run_file_updates.FileUpdatesError,
                match="succeeded without a merge request URL",
            ):
                run_file_updates._process_ir_results("test-ir")


class TestCreateFileUpdateRequest:
    """Tests for _create_file_update_request()."""

    def test_wait_timeout_includes_spawn_overhead(self) -> None:
        """Sync wait outlives the PipelineRun timeout by the spawn overhead."""
        item = run_file_updates.FileUpdateItem(
            repo="repo",
            upstream_repo="https://github.com/foo/bar",
            ref="main",
            paths=[],
            file_updates_secret="file-updates-secret",
        )
        with mock.patch(f"{TASK}.create", return_value="ir-1") as mock_create:
            run_file_updates._create_file_update_request(
                item=item,
                component_group="group",
                updated_paths=[],
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                request_id="req-1",
                pipeline_name="process-file-updates",
                sync=True,
            )

        call_kwargs = mock_create.call_args.kwargs
        assert call_kwargs["timeout"] == (
            duration_to_seconds(call_kwargs["pipeline_timeout"]) + SPAWN_OVERHEAD_SECONDS
        )


class TestRun:
    """Tests for run()."""

    def test_single_file_update_success(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """Single file update completes successfully."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", sample_rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions") as mock_eval,
            mock.patch(f"{TASK}.create", return_value="ir-12345"),
            mock.patch(
                f"{TASK}._process_ir_results",
                return_value="https://gitlab.com/mr/1",
            ),
        ):
            mock_eval.return_value = sample_rpa_data["spec"]["data"]["fileUpdates"][0]["paths"]
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key=".spec.data.fileUpdates",
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

        assert result_path.read_text().strip() == "https://gitlab.com/mr/1"
        results = json.loads((data_dir / "results" / "file-updates-results.json").read_text())
        assert results["merge_requests"][0]["url"] == "https://gitlab.com/mr/1"

    def test_multiple_file_updates(self, tmp_path: Path, sample_snapshot: dict) -> None:
        """Multiple file updates are processed sequentially."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)

        rpa_data = {
            "spec": {
                "data": {
                    "fileUpdates": [
                        {"repo": "repo1", "paths": []},
                        {"repo": "repo2", "paths": []},
                    ]
                }
            }
        }
        _write_json(data_dir / "rpa.json", rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create") as mock_create,
            mock.patch(f"{TASK}._process_ir_results") as mock_process,
        ):
            mock_create.side_effect = ["ir-1", "ir-2"]
            mock_process.side_effect = [
                "https://gitlab.com/mr/1",
                "https://gitlab.com/mr/2",
            ]

            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key=".spec.data.fileUpdates",
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

        assert mock_create.call_count == 2
        mr_results = result_path.read_text().strip().split("\n")
        assert len(mr_results) == 2

    def test_empty_file_updates(self, tmp_path: Path, sample_snapshot: dict) -> None:
        """Empty fileUpdates list produces empty results."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", {"spec": {"data": {"fileUpdates": []}}})
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        run_file_updates.run(
            data_dir=data_dir,
            file_updates_path="rpa.json",
            snapshot_path="snapshot.json",
            json_key=".spec.data.fileUpdates",
            results_dir_path="results",
            request_timeout=900,
            task_git_url="https://github.com/org/catalog",
            task_git_revision="main",
            pipelinerun_uid="pr-123",
            task_uid="task-456",
            pipeline_name="process-file-updates",
            sync=True,
            merge_request_url_result_path=result_path,
        )

        assert result_path.read_text() == ""
        results = json.loads((data_dir / "results" / "file-updates-results.json").read_text())
        assert results["merge_requests"] == []

    def test_missing_file_updates_key_is_zero_updates(
        self, tmp_path: Path, sample_snapshot: dict
    ) -> None:
        """A missing fileUpdates key selects null and produces no updates."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", {"spec": {"data": {}}})
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        run_file_updates.run(
            data_dir=data_dir,
            file_updates_path="rpa.json",
            snapshot_path="snapshot.json",
            json_key=".spec.data.fileUpdates",
            results_dir_path="results",
            request_timeout=900,
            task_git_url="https://github.com/org/catalog",
            task_git_revision="main",
            pipelinerun_uid="pr-123",
            task_uid="task-456",
            pipeline_name="process-file-updates",
            sync=True,
            merge_request_url_result_path=result_path,
        )

        assert result_path.read_text() == ""
        results = json.loads((data_dir / "results" / "file-updates-results.json").read_text())
        assert results["merge_requests"] == []

    def test_json_key_index_error_raises(self, tmp_path: Path, sample_snapshot: dict) -> None:
        """A jq expression that cannot be evaluated raises FileUpdatesError."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", {"spec": "nope"})
        result_path = tmp_path / "mr_result.txt"

        with pytest.raises(
            run_file_updates.FileUpdatesError, match="Failed to evaluate jsonKey"
        ):
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key=".spec.data.fileUpdates",
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

    def test_non_list_json_key_raises(self, tmp_path: Path, sample_snapshot: dict) -> None:
        """A jsonKey result that is not a list raises FileUpdatesError."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(
            data_dir / "rpa.json",
            {"spec": {"data": {"fileUpdates": {"repo": "not-a-list"}}}},
        )
        result_path = tmp_path / "mr_result.txt"

        with pytest.raises(
            run_file_updates.FileUpdatesError, match="did not return an update list"
        ):
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key=".spec.data.fileUpdates",
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

    def test_json_key_without_json_output_raises(self, tmp_path: Path) -> None:
        """A jq filter that prints no JSON is rejected."""
        path = tmp_path / "rpa.json"
        _write_json(path, {"spec": {}})
        with pytest.raises(
            run_file_updates.FileUpdatesError, match="did not return an update list"
        ):
            run_file_updates._file_updates_from_json_key(path, "empty")

    def test_json_key_operator_selects_matching_updates(
        self, tmp_path: Path, sample_snapshot: dict
    ) -> None:
        """A jq operator in jsonKey selects updates instead of matching no key."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(
            data_dir / "rpa.json",
            {
                "spec": {
                    "data": {
                        "fileUpdates": [
                            {"repo": "keep", "paths": []},
                            {"repo": "drop", "paths": []},
                        ]
                    }
                }
            },
        )
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1") as mock_create,
            mock.patch(f"{TASK}._process_ir_results", return_value="https://gitlab.com/mr/1"),
        ):
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key='.spec.data.fileUpdates | map(select(.repo == "keep"))',
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

        assert mock_create.call_args.kwargs["params"]["repo"] == "keep"
        mock_create.assert_called_once()

    def test_json_key_quoted_key_selects_updates(
        self, tmp_path: Path, sample_snapshot: dict
    ) -> None:
        """A quoted jsonKey selects a key that dotted splitting cannot name."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(
            data_dir / "rpa.json",
            {"spec": {"file-updates": [{"repo": "quoted", "paths": []}]}},
        )
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1") as mock_create,
            mock.patch(f"{TASK}._process_ir_results", return_value="https://gitlab.com/mr/1"),
        ):
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key='.spec["file-updates"]',
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

        assert mock_create.call_args.kwargs["params"]["repo"] == "quoted"
        mock_create.assert_called_once()

    def test_no_change_success_omits_url(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """A no-change success leaves the merge request results empty."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", sample_rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"
        ir_results = {"buildState": "Succeeded", "jsonBuildInfo": "nothing needs change\n"}

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1"),
            mock.patch(f"{TASK}.fetch_results", return_value=ir_results),
        ):
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key=".spec.data.fileUpdates",
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

        assert result_path.read_text() == ""
        written = json.loads((data_dir / "results" / "file-updates-results.json").read_text())
        assert written["merge_requests"] == []

    def test_async_mode_still_processes_results(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """Async mode waits for the InternalRequest before reading results."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", sample_rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        order: list[str] = []

        def _wait(**_kwargs: object) -> None:
            order.append("wait")

        def _process(_ir_name: str) -> str:
            order.append("process")
            return "https://gitlab.com/mr/1"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-12345"),
            mock.patch(f"{TASK}.wait_for_completion", side_effect=_wait) as mock_wait,
            mock.patch(f"{TASK}._process_ir_results", side_effect=_process) as mock_process,
        ):
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key=".spec.data.fileUpdates",
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=False,
                merge_request_url_result_path=result_path,
            )

        mock_wait.assert_called_once_with(
            name="ir-12345",
            timeout=900 + 300 + SPAWN_OVERHEAD_SECONDS,
        )
        mock_process.assert_called_once_with("ir-12345")
        assert order == ["wait", "process"]
        assert result_path.read_text().strip() == "https://gitlab.com/mr/1"

    def _run_sample(
        self,
        tmp_path: Path,
        sample_snapshot: dict,
        sample_rpa_data: dict,
        *,
        sync: bool,
    ) -> None:
        """Run one sample file update."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", sample_rpa_data)
        (data_dir / "results").mkdir()
        run_file_updates.run(
            data_dir=data_dir,
            file_updates_path="rpa.json",
            snapshot_path="snapshot.json",
            json_key=".spec.data.fileUpdates",
            results_dir_path="results",
            request_timeout=900,
            task_git_url="https://github.com/org/catalog",
            task_git_revision="main",
            pipelinerun_uid="pr-123",
            task_uid="task-456",
            pipeline_name="process-file-updates",
            sync=sync,
            merge_request_url_result_path=tmp_path / "mr_result.txt",
        )

    def test_ir_failure_raises_error(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """A wait error without an InternalRequest name preserves that error."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)
        _write_json(data_dir / "rpa.json", sample_rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(
                f"{TASK}.create",
                side_effect=run_file_updates.InternalRequestWaitError("IR failed", 21),
            ),
        ):
            with pytest.raises(run_file_updates.FileUpdatesError, match="IR failed"):
                run_file_updates.run(
                    data_dir=data_dir,
                    file_updates_path="rpa.json",
                    snapshot_path="snapshot.json",
                    json_key=".spec.data.fileUpdates",
                    results_dir_path="results",
                    request_timeout=900,
                    task_git_url="https://github.com/org/catalog",
                    task_git_revision="main",
                    pipelinerun_uid="pr-123",
                    task_uid="task-456",
                    pipeline_name="process-file-updates",
                    sync=True,
                    merge_request_url_result_path=result_path,
                )

    def test_wait_failure_reports_task_error_and_diff(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """A failed wait reports the task error and diff recorded on the request."""
        failure = {
            "buildState": "Failed",
            "jsonBuildInfo": json.dumps({"error": "merge conflict", "str": "diff content"}),
        }
        wait_error = run_file_updates.InternalRequestWaitError(
            "At least one InternalRequest failed",
            21,
            name="ir-1",
        )
        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1"),
            mock.patch(f"{TASK}.wait_for_completion", side_effect=wait_error),
            mock.patch(f"{TASK}.fetch_results", return_value=failure),
            pytest.raises(run_file_updates.FileUpdatesError) as exc_info,
        ):
            self._run_sample(tmp_path, sample_snapshot, sample_rpa_data, sync=False)

        message = str(exc_info.value)
        assert "merge conflict" in message
        assert "diff content" in message
        assert exc_info.value.__cause__ is wait_error

    def test_sync_wait_failure_reports_task_error_and_diff(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """Sync mode still reports task error and diff when create's wait fails."""
        failure = {
            "buildState": "Failed",
            "jsonBuildInfo": json.dumps({"error": "merge conflict", "str": "diff content"}),
        }
        wait_error = run_file_updates.InternalRequestWaitError(
            "At least one InternalRequest failed",
            21,
            name="ir-sync",
        )
        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", side_effect=wait_error),
            mock.patch(f"{TASK}.fetch_results", return_value=failure) as mock_fetch,
            pytest.raises(run_file_updates.FileUpdatesError) as exc_info,
        ):
            self._run_sample(tmp_path, sample_snapshot, sample_rpa_data, sync=True)

        assert "merge conflict" in str(exc_info.value)
        assert "diff content" in str(exc_info.value)
        mock_fetch.assert_called_once_with("ir-sync")
        assert exc_info.value.__cause__ is wait_error

    def test_wait_timeout_without_results_preserves_wait_error(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """A timeout with no results keeps the original wait error."""
        wait_error = run_file_updates.InternalRequestWaitError(
            "Timeout while waiting for the InternalRequests to complete",
            124,
            name="ir-1",
        )
        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1"),
            mock.patch(f"{TASK}.wait_for_completion", side_effect=wait_error),
            mock.patch(f"{TASK}.fetch_results", return_value={}),
            pytest.raises(
                run_file_updates.FileUpdatesError,
                match="Timeout while waiting for the InternalRequests to complete",
            ) as exc_info,
        ):
            self._run_sample(tmp_path, sample_snapshot, sample_rpa_data, sync=False)

        assert exc_info.value.__cause__ is wait_error

    def test_wait_failure_without_readable_results_preserves_wait_error(
        self, tmp_path: Path, sample_snapshot: dict, sample_rpa_data: dict
    ) -> None:
        """A wait error is preserved when result lookup itself fails."""
        wait_error = run_file_updates.InternalRequestWaitError(
            "Timeout while waiting for the InternalRequests to complete",
            124,
            name="ir-1",
        )
        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1"),
            mock.patch(f"{TASK}.wait_for_completion", side_effect=wait_error),
            mock.patch(f"{TASK}.fetch_results", side_effect=RuntimeError("api down")),
            pytest.raises(
                run_file_updates.FileUpdatesError,
                match="Timeout while waiting for the InternalRequests to complete",
            ) as exc_info,
        ):
            self._run_sample(tmp_path, sample_snapshot, sample_rpa_data, sync=False)

        assert exc_info.value.__cause__ is wait_error

    def test_multiple_irs_first_fails_stops_processing(
        self, tmp_path: Path, sample_snapshot: dict
    ) -> None:
        """When first IR fails, processing stops and no MRs are recorded."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)

        rpa_data = {
            "spec": {
                "data": {
                    "fileUpdates": [
                        {"repo": "repo1", "ref": "fail", "paths": []},
                        {"repo": "repo2", "paths": []},
                    ]
                }
            }
        }
        _write_json(data_dir / "rpa.json", rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1"),
            mock.patch(
                f"{TASK}._process_ir_results",
                side_effect=run_file_updates.FileUpdatesError("First IR failed"),
            ),
        ):
            with pytest.raises(run_file_updates.FileUpdatesError, match="First IR failed"):
                run_file_updates.run(
                    data_dir=data_dir,
                    file_updates_path="rpa.json",
                    snapshot_path="snapshot.json",
                    json_key=".spec.data.fileUpdates",
                    results_dir_path="results",
                    request_timeout=900,
                    task_git_url="https://github.com/org/catalog",
                    task_git_revision="main",
                    pipelinerun_uid="pr-123",
                    task_uid="task-456",
                    pipeline_name="process-file-updates",
                    sync=True,
                    merge_request_url_result_path=result_path,
                )

        # No MRs should be recorded since first one failed
        assert result_path.read_text() == ""

    def test_multiple_irs_last_fails_records_successful_ones(
        self, tmp_path: Path, sample_snapshot: dict
    ) -> None:
        """When last IR fails, successful MRs are recorded before failure."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)

        rpa_data = {
            "spec": {
                "data": {
                    "fileUpdates": [
                        {"repo": "repo1", "paths": []},
                        {"repo": "repo2", "ref": "fail", "paths": []},
                    ]
                }
            }
        }
        _write_json(data_dir / "rpa.json", rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create") as mock_create,
            mock.patch(f"{TASK}._process_ir_results") as mock_process,
        ):
            mock_create.side_effect = ["ir-1", "ir-2"]
            mock_process.side_effect = [
                "https://gitlab.com/mr/1",
                run_file_updates.FileUpdatesError("Second IR failed"),
            ]

            with pytest.raises(run_file_updates.FileUpdatesError, match="Second IR failed"):
                run_file_updates.run(
                    data_dir=data_dir,
                    file_updates_path="rpa.json",
                    snapshot_path="snapshot.json",
                    json_key=".spec.data.fileUpdates",
                    results_dir_path="results",
                    request_timeout=900,
                    task_git_url="https://github.com/org/catalog",
                    task_git_revision="main",
                    pipelinerun_uid="pr-123",
                    task_uid="task-456",
                    pipeline_name="process-file-updates",
                    sync=True,
                    merge_request_url_result_path=result_path,
                )

        # First successful MR should be recorded before the later failure.
        assert "https://gitlab.com/mr/1" in result_path.read_text()
        written = json.loads((data_dir / "results" / "file-updates-results.json").read_text())
        assert written["merge_requests"] == [{"url": "https://gitlab.com/mr/1"}]

    def test_file_updates_secret_passed_to_ir(
        self, tmp_path: Path, sample_snapshot: dict
    ) -> None:
        """Custom file_updates_secret is passed to InternalRequest."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", sample_snapshot)

        rpa_data = {
            "spec": {
                "data": {
                    "fileUpdates": [
                        {
                            "repo": "repo1",
                            "file_updates_secret": "mysecret",
                            "paths": [],
                        }
                    ]
                }
            }
        }
        _write_json(data_dir / "rpa.json", rpa_data)
        (data_dir / "results").mkdir()
        result_path = tmp_path / "mr_result.txt"

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1") as mock_create,
            mock.patch(
                f"{TASK}._process_ir_results",
                return_value="https://gitlab.com/mr/1",
            ),
        ):
            run_file_updates.run(
                data_dir=data_dir,
                file_updates_path="rpa.json",
                snapshot_path="snapshot.json",
                json_key=".spec.data.fileUpdates",
                results_dir_path="results",
                request_timeout=900,
                task_git_url="https://github.com/org/catalog",
                task_git_revision="main",
                pipelinerun_uid="pr-123",
                task_uid="task-456",
                pipeline_name="process-file-updates",
                sync=True,
                merge_request_url_result_path=result_path,
            )

        # Verify the secret was passed in params
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["params"]["file_updates_secret"] == "mysecret"


def test_package_provides_entrypoints() -> None:
    """The managed task package exposes main and run."""
    from release_service_utils.tasks.managed.run_file_updates import main, run

    assert callable(main)
    assert callable(run)


class TestMain:
    """Tests for main()."""

    def test_main_reads_env_and_calls_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """main() reads env vars and calls run()."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_path = tmp_path / "result.txt"

        monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
        monkeypatch.setenv("PARAM_FILE_UPDATES_PATH", "rpa.json")
        monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
        monkeypatch.setenv("PARAM_JSON_KEY", ".spec.data.fileUpdates")
        monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("PARAM_REQUEST_TIMEOUT", "600")
        monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://github.com/org/cat")
        monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "dev")
        monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pr-uid")
        monkeypatch.setenv("PARAM_TASK_RUN_UID", "task-uid")
        monkeypatch.setenv("PARAM_REQUEST", "process-file-updates")
        monkeypatch.setenv("PARAM_SYNCHRONOUSLY", "true")
        monkeypatch.setenv("RESULT_MERGE_REQUEST_URL", str(result_path))

        with mock.patch(f"{TASK}.run") as mock_run:
            assert run_file_updates.main() == 0
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs["data_dir"] == data_dir
            assert call_kwargs["file_updates_path"] == "rpa.json"
            assert call_kwargs["snapshot_path"] == "snap.json"
            assert call_kwargs["request_timeout"] == 600
            assert call_kwargs["sync"] is True
            assert call_kwargs["merge_request_url_result_path"] == result_path
            assert call_kwargs["pipelinerun_uid"] == "pr-uid"
            assert call_kwargs["task_uid"] == "task-uid"

    def test_main_defaults(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() uses defaults for optional parameters."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_path = tmp_path / "result.txt"

        monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
        monkeypatch.setenv("PARAM_FILE_UPDATES_PATH", "rpa.json")
        monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
        monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://github.com/org/cat")
        monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "dev")
        monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pr-uid")
        monkeypatch.setenv("PARAM_TASK_RUN_UID", "task-uid")
        monkeypatch.setenv("RESULT_MERGE_REQUEST_URL", str(result_path))
        # Don't set optional env vars

        with mock.patch(f"{TASK}.run") as mock_run:
            run_file_updates.main()
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs["json_key"] == ".spec.data.fileUpdates"
            assert call_kwargs["request_timeout"] == 900
            assert call_kwargs["pipeline_name"] == "process-file-updates"
            assert call_kwargs["sync"] is True

    def test_main_sync_false(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() parses PARAM_SYNCHRONOUSLY=false correctly."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_path = tmp_path / "result.txt"

        monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
        monkeypatch.setenv("PARAM_FILE_UPDATES_PATH", "rpa.json")
        monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
        monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://github.com/org/cat")
        monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "dev")
        monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pr-uid")
        monkeypatch.setenv("PARAM_TASK_RUN_UID", "task-uid")
        monkeypatch.setenv("PARAM_SYNCHRONOUSLY", "false")
        monkeypatch.setenv("RESULT_MERGE_REQUEST_URL", str(result_path))

        with mock.patch(f"{TASK}.run") as mock_run:
            run_file_updates.main()
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs["sync"] is False

    def test_main_wires_uid_env_to_labels(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TaskRun UID is the group-id label; PipelineRun UID is the other label."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_json(data_dir / "snapshot.json", {"componentGroup": "app"})
        _write_json(
            data_dir / "rpa.json",
            {"spec": {"data": {"fileUpdates": [{"repo": "repo", "paths": []}]}}},
        )
        result_path = tmp_path / "result.txt"

        monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
        monkeypatch.setenv("PARAM_FILE_UPDATES_PATH", "rpa.json")
        monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")
        monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://github.com/org/cat")
        monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "dev")
        monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pipeline-run-uid")
        monkeypatch.setenv("PARAM_TASK_RUN_UID", "task-run-uid")
        monkeypatch.setenv("RESULT_MERGE_REQUEST_URL", str(result_path))

        with (
            mock.patch(f"{TASK}._evaluate_path_expressions", return_value=[]),
            mock.patch(f"{TASK}.create", return_value="ir-1") as mock_create,
            mock.patch(
                f"{TASK}._process_ir_results",
                return_value="https://gitlab.com/mr/1",
            ),
        ):
            assert run_file_updates.main() == 0

        labels = mock_create.call_args.kwargs["labels"]
        assert labels[run_file_updates.TASK_LABEL] == "task-run-uid"
        assert labels[run_file_updates.PIPELINERUN_LABEL] == "pipeline-run-uid"

    def _set_entrypoint_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set the env vars required to enter main()."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
        monkeypatch.setenv("PARAM_FILE_UPDATES_PATH", "rpa.json")
        monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
        monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://github.com/org/cat")
        monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "dev")
        monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pr-uid")
        monkeypatch.setenv("PARAM_TASK_RUN_UID", "task-uid")
        monkeypatch.setenv("RESULT_MERGE_REQUEST_URL", str(tmp_path / "result.txt"))

    def test_module_main_guard(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Run the task module guard in its namespace so patched names are used."""
        self._set_entrypoint_env(tmp_path, monkeypatch)
        data_dir = tmp_path / "data"
        _write_json(data_dir / "snap.json", {"componentGroup": "app"})
        _write_json(
            data_dir / "rpa.json",
            {"spec": {"data": {"fileUpdates": [{"repo": "repo", "paths": []}]}}},
        )
        ir_results = {
            "buildState": "Succeeded",
            "jsonBuildInfo": json.dumps({"merge_request": "https://gitlab.com/mr/1"}),
        }
        source_path = Path(run_file_updates.__file__)
        source = source_path.read_text(encoding="utf-8")
        guard_at = source.index('if __name__ == "__main__":')
        entrypoint = compile(
            "\n" * source[:guard_at].count("\n") + source[guard_at:],
            str(source_path),
            "exec",
        )
        module_globals = run_file_updates.__dict__
        original_name = module_globals["__name__"]
        with (
            mock.patch(f"{TASK}.create", return_value="ir-1") as mock_create,
            mock.patch(f"{TASK}.fetch_results", return_value=ir_results) as mock_fetch,
        ):
            module_globals["__name__"] = "__main__"
            try:
                with pytest.raises(SystemExit) as exc_info:
                    exec(entrypoint, module_globals)
            finally:
                module_globals["__name__"] = original_name
        assert exc_info.value.code == 0
        mock_create.assert_called_once()
        mock_fetch.assert_called_once()

    def test_package_main(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Executing the package as __main__ calls main()."""
        self._set_entrypoint_env(tmp_path, monkeypatch)
        with mock.patch(f"{TASK}.run"):
            with pytest.raises(SystemExit) as exc_info:
                runpy.run_module(
                    "release_service_utils.tasks.managed.run_file_updates",
                    run_name="__main__",
                )
        assert exc_info.value.code == 0
