"""Test add-fbc-contribution task for adding FBC contributions to index images."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

import add_fbc_contribution
import iib
import pytest
from add_fbc_contribution import (
    AddFBCContributionConfig,
    BatchResult,
    OCPGroup,
    calculate_timeouts,
    deduplicate_results,
    get_batch_fragments,
    get_ocp_versions,
    group_components_by_ocp_version,
    process_batch_results,
    validate_snapshot,
)


def make_config(
    tmp_path: Path,
    **overrides: Any,
) -> AddFBCContributionConfig:
    """Create a test configuration."""
    defaults = {
        "snapshot_path": tmp_path / "snapshot.json",
        "data_path": tmp_path / "data.json",
        "data_dir": tmp_path,
        "results_dir_path": tmp_path / "results",
        "pipeline_run_uid": "test-pipeline-uid",
        "task_run_uid": "test-task-uid",
        "max_batch_size": 5,
        "must_publish_index_image": True,
        "must_overwrite_from_index_image": True,
        "iib_service_account_secret": "test-iib-secret",
        "max_retries": 3,
        "batch_retry_delay_seconds": 1,
        "task_git_url": "http://localhost",
        "task_git_revision": "main",
    }
    defaults.update(overrides)
    return AddFBCContributionConfig(**defaults)


def make_snapshot(components: list[dict[str, Any]]) -> dict[str, Any]:
    """Create a test snapshot."""
    return {
        "application": "test-app",
        "components": components,
    }


def make_component(
    name: str,
    ocp_version: str,
    container_image: str | None = None,
    from_index: str | None = None,
    target_index: str | None = None,
) -> dict[str, Any]:
    """Create a test component."""
    return {
        "name": name,
        "containerImage": container_image or f"registry.io/{name}@sha256:0000",
        "ocpVersion": ocp_version,
        "updatedFromIndex": from_index or f"quay.io/fbc-index:v{ocp_version}",
        "targetIndex": target_index or f"quay.io/fbc-target:v{ocp_version}",
    }


def make_data(fbc_config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Create test data."""
    if fbc_config is None:
        fbc_config = {
            "buildTimeoutSeconds": 420,
            "requestTimeoutSeconds": 120,
        }
    return {"fbc": fbc_config}


class TestValidateSnapshot:
    """Tests for validate_snapshot function."""

    def test_valid_snapshot(self) -> None:
        """Valid snapshot passes validation."""
        snapshot = make_snapshot([make_component("comp1", "4.12")])
        validate_snapshot(snapshot)

    def test_missing_components(self) -> None:
        """ValueError is raised for missing components array."""
        snapshot = {"application": "test"}

        with pytest.raises(ValueError, match="missing required 'components' array"):
            validate_snapshot(snapshot)

    def test_empty_components(self) -> None:
        """ValueError is raised for empty components array."""
        snapshot = make_snapshot([])

        with pytest.raises(ValueError, match="No components found"):
            validate_snapshot(snapshot)

    def test_components_not_array(self) -> None:
        """ValueError is raised when components is not an array."""
        snapshot = {"components": "not an array"}

        with pytest.raises(ValueError, match="missing required 'components' array"):
            validate_snapshot(snapshot)


class TestGetOcpVersions:
    """Tests for get_ocp_versions function."""

    def test_extracts_unique_versions(self) -> None:
        """Unique OCP versions are extracted and sorted."""
        snapshot = make_snapshot(
            [
                make_component("comp1", "4.12"),
                make_component("comp2", "4.14"),
                make_component("comp3", "4.12"),
                make_component("comp4", "4.13"),
            ]
        )

        result = get_ocp_versions(snapshot)

        assert result == ["4.12", "4.13", "4.14"]

    def test_empty_components(self) -> None:
        """Empty list is returned for empty components."""
        snapshot = {"components": []}

        result = get_ocp_versions(snapshot)

        assert result == []

    def test_missing_ocp_version(self) -> None:
        """Components without ocpVersion are skipped."""
        snapshot = {
            "components": [
                {"name": "comp1", "ocpVersion": "4.12"},
                {"name": "comp2"},
            ]
        }

        result = get_ocp_versions(snapshot)

        assert result == ["4.12"]


class TestGroupComponentsByOcpVersion:
    """Tests for group_components_by_ocp_version function."""

    def test_groups_by_ocp_version(self) -> None:
        """Components are grouped by OCP version."""
        snapshot = make_snapshot(
            [
                make_component("comp1", "4.12"),
                make_component("comp2", "4.12"),
                make_component("comp3", "4.14"),
            ]
        )
        ocp_versions = ["4.12", "4.14"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, [])

        assert len(groups) == 2
        assert groups[0].ocp_version == "4.12"
        assert len(groups[0].components) == 2
        assert groups[1].ocp_version == "4.14"
        assert len(groups[1].components) == 1

    def test_extracts_from_index_and_target_index(self) -> None:
        """from_index and target_index are extracted from first component."""
        component = make_component(
            "comp1",
            "4.12",
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        snapshot = make_snapshot([component])
        ocp_versions = ["4.12"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, [])

        assert groups[0].from_index == "quay.io/from:latest"
        assert groups[0].target_index == "quay.io/target:v4.12"

    def test_adds_target_tag_to_build_tags(self) -> None:
        """Target index tag is added to build tags."""
        component = make_component(
            "comp1",
            "4.12",
            target_index="quay.io/target:v4.12",
        )
        snapshot = make_snapshot([component])
        ocp_versions = ["4.12"]
        global_tags = ["global-tag"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, global_tags)

        assert "global-tag" in groups[0].build_tags
        assert "v4.12" in groups[0].build_tags

    def test_empty_target_index_no_tag_added(self) -> None:
        """No tag is added when target_index is empty."""
        component = {
            "name": "comp1",
            "containerImage": "registry.io/comp1@sha256:0000",
            "ocpVersion": "4.12",
            "updatedFromIndex": "quay.io/fbc-index:v4.12",
            "targetIndex": "",
        }
        snapshot = make_snapshot([component])
        ocp_versions = ["4.12"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, ["tag1"])

        assert groups[0].build_tags == ["tag1"]


class TestGetBatchFragments:
    """Tests for get_batch_fragments function."""

    def test_gets_batch_fragments(self) -> None:
        """Correct fragments are returned for a batch."""
        components = [
            make_component(f"comp{i}", "4.12", container_image=f"img{i}") for i in range(10)
        ]

        result = get_batch_fragments(components, batch_num=0, max_batch_size=3)
        assert result == ["img0", "img1", "img2"]

        result = get_batch_fragments(components, batch_num=1, max_batch_size=3)
        assert result == ["img3", "img4", "img5"]

        result = get_batch_fragments(components, batch_num=3, max_batch_size=3)
        assert result == ["img9"]

    def test_handles_exact_batch_size(self) -> None:
        """Handles components that divide evenly by batch size."""
        components = [
            make_component(f"comp{i}", "4.12", container_image=f"img{i}") for i in range(6)
        ]

        result = get_batch_fragments(components, batch_num=1, max_batch_size=3)
        assert result == ["img3", "img4", "img5"]


class TestCalculateTimeouts:
    """Tests for calculate_timeouts function."""

    def test_calculates_timeouts(self) -> None:
        """Timeouts are calculated correctly."""
        pipeline_timeout, task_timeout = calculate_timeouts(3600)

        assert task_timeout == "1h0m0s"
        assert pipeline_timeout == "1h5m0s"

    def test_handles_smaller_timeout(self) -> None:
        """Smaller timeouts are calculated correctly."""
        pipeline_timeout, task_timeout = calculate_timeouts(120)

        assert task_timeout == "0h2m0s"
        assert pipeline_timeout == "0h7m0s"

    def test_handles_complex_timeout(self) -> None:
        """Complex timeouts with hours, minutes, and seconds."""
        pipeline_timeout, task_timeout = calculate_timeouts(3661)

        assert task_timeout == "1h1m1s"
        assert pipeline_timeout == "1h6m1s"


class TestDeduplicateResults:
    """Tests for deduplicate_results function."""

    def test_no_deduplication_needed(self) -> None:
        """No deduplication when components match unique targets."""
        results = {
            "components": [
                {"target_index": "idx1", "ocp_version": "4.12"},
                {"target_index": "idx2", "ocp_version": "4.14"},
            ]
        }

        result = deduplicate_results(results, is_staged=False)

        assert len(result["components"]) == 2

    def test_deduplicates_by_target_index(self) -> None:
        """Keeps last component per target_index."""
        results = {
            "components": [
                {"target_index": "idx1", "ocp_version": "4.12", "order": 1},
                {"target_index": "idx1", "ocp_version": "4.12", "order": 2},
                {"target_index": "idx2", "ocp_version": "4.14", "order": 3},
            ]
        }

        result = deduplicate_results(results, is_staged=False)

        assert len(result["components"]) == 2
        idx1_components = [c for c in result["components"] if c["target_index"] == "idx1"]
        assert len(idx1_components) == 1
        assert idx1_components[0]["order"] == 2

    def test_deduplicates_by_ocp_version_when_staged(self) -> None:
        """Keeps last component per ocp_version when staged."""
        results = {
            "components": [
                {"target_index": "", "ocp_version": "4.12", "order": 1},
                {"target_index": "", "ocp_version": "4.12", "order": 2},
                {"target_index": "", "ocp_version": "4.14", "order": 3},
            ]
        }

        result = deduplicate_results(results, is_staged=True)

        assert len(result["components"]) == 2
        v412_components = [c for c in result["components"] if c["ocp_version"] == "4.12"]
        assert len(v412_components) == 1
        assert v412_components[0]["order"] == 2

    def test_empty_components(self) -> None:
        """Empty components list is handled."""
        results: dict[str, Any] = {"components": []}

        result = deduplicate_results(results, is_staged=False)

        assert result["components"] == []


class TestProcessBatchResults:
    """Tests for process_batch_results function."""

    def test_processes_successful_batch(self, tmp_path: Path) -> None:
        """Successful batch results are processed correctly."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        build_info = {
            "updated": "2024-03-06T16:39:11.314092Z",
            "index_image": "quay.io/iib:01",
            "index_image_resolved": "quay.io/iib@sha256:abc",
        }
        batch_result = BatchResult(
            batch_num=0,
            success=True,
            index_image="quay.io/iib:01",
            results={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a sha256:b",
                "iibLog": "Test log",
            },
        )
        results_data: dict[str, Any] = {"components": []}

        process_batch_results(batch_result, group, config, "%s", results_data)

        assert len(results_data["components"]) == 1
        component = results_data["components"][0]
        assert component["fbc_fragment"] == "img1"
        assert component["target_index"] == "quay.io/target:v4.12"
        assert component["ocp_version"] == "4.12"
        assert component["index_image"] == "quay.io/iib:01"
        assert component["image_digests"] == ["sha256:a", "sha256:b"]

    def test_skips_failed_batch(self, tmp_path: Path) -> None:
        """Failed batch results are not processed."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        batch_result = BatchResult(batch_num=0, success=False)
        results_data: dict[str, Any] = {"components": []}

        process_batch_results(batch_result, group, config, "%s", results_data)

        assert len(results_data["components"]) == 0


class TestCreateInternalRequest:
    """Tests for create_internal_request function."""

    def test_creates_request_successfully(self, tmp_path: Path) -> None:
        """InternalRequest is created and name is extracted."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
            build_tags=["tag1"],
        )
        data = make_data()

        mock_result = mock.MagicMock()
        mock_result.stdout = "InternalRequest 'test-ir-123' created\n"
        mock_runner = mock.MagicMock(return_value=mock_result)

        result = add_fbc_contribution.create_internal_request(
            "quay.io/from:latest",
            ["img1", "img2"],
            config,
            group,
            data,
            "1h5m0s",
            "1h0m0s",
            run_command=mock_runner,
        )

        assert result == "test-ir-123"
        mock_runner.assert_called_once()
        call_args = mock_runner.call_args[0][0]
        assert "internal-request" in call_args
        assert "update-fbc-catalog" in call_args

    def test_raises_when_no_request_name(self, tmp_path: Path) -> None:
        """ValueError is raised when request name cannot be extracted."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        mock_result = mock.MagicMock()
        mock_result.stdout = "Some other output without request name"
        mock_runner = mock.MagicMock(return_value=mock_result)

        with pytest.raises(ValueError, match="Failed to extract InternalRequest name"):
            add_fbc_contribution.create_internal_request(
                "quay.io/from:latest",
                ["img1"],
                config,
                group,
                data,
                "1h5m0s",
                "1h0m0s",
                run_command=mock_runner,
            )


class TestGetInternalRequestStatus:
    """Tests for get_internal_request_status function."""

    def test_returns_succeeded_status(self) -> None:
        """Returns success when condition is True."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(
            {
                "status": "True",
                "reason": "Succeeded",
                "message": "",
            }
        )
        mock_runner = mock.MagicMock(return_value=mock_result)

        succeeded, reason, message = add_fbc_contribution.get_internal_request_status(
            "test-ir",
            run_command=mock_runner,
        )

        assert succeeded is True
        assert reason == "Succeeded"

    def test_returns_failed_status(self) -> None:
        """Returns failure when condition is False."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(
            {
                "status": "False",
                "reason": "Failed",
                "message": "Some error",
            }
        )
        mock_runner = mock.MagicMock(return_value=mock_result)

        succeeded, reason, message = add_fbc_contribution.get_internal_request_status(
            "test-ir",
            run_command=mock_runner,
        )

        assert succeeded is False
        assert reason == "Failed"
        assert message == "Some error"

    def test_handles_kubectl_error(self) -> None:
        """Handles kubectl command failure."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Not found"
        mock_runner = mock.MagicMock(return_value=mock_result)

        succeeded, reason, message = add_fbc_contribution.get_internal_request_status(
            "test-ir",
            run_command=mock_runner,
        )

        assert succeeded is False
        assert reason == "Error"


class TestExecuteBatch:
    """Tests for execute_batch function."""

    def test_successful_batch_execution(self, tmp_path: Path) -> None:
        """Batch executes successfully and returns result."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        build_info = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:01",
        }

        call_count = [0]

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            call_count[0] += 1
            result = mock.MagicMock()

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.returncode = 0
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.returncode = 0
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.returncode = 0
                result.stdout = ""

            return result

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
            run_command=mock_runner,
        )

        assert batch_result.success is True
        assert batch_result.index_image == "quay.io/iib:01"

    def test_failed_batch_execution(self, tmp_path: Path) -> None:
        """Batch failure is handled correctly."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.returncode = 0
                result.stdout = json.dumps(
                    {
                        "status": "False",
                        "reason": "Failed",
                        "message": "IIB error",
                    }
                )
            else:
                result.returncode = 0
                result.stdout = ""

            return result

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
            run_command=mock_runner,
        )

        assert batch_result.success is False
        assert "Failed" in batch_result.error_message


class TestProcessOcpGroup:
    """Tests for process_ocp_group function."""

    def test_processes_all_batches_successfully(self, tmp_path: Path) -> None:
        """All batches are processed successfully."""
        config = make_config(tmp_path, max_batch_size=2)
        group = OCPGroup(
            ocp_version="4.12",
            components=[
                make_component(f"comp{i}", "4.12", container_image=f"img{i}") for i in range(3)
            ],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        build_info = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:01",
            "index_image_resolved": "quay.io/iib@sha256:abc",
        }

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                        "iibLog": "Log",
                    }
                )
            else:
                result.stdout = ""

            return result

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            timestamp_format="%s",
            results_data=results_data,
            run_command=mock_runner,
            sleep_fn=lambda x: None,
        )

        assert success is True
        assert len(results_data["components"]) == 3

    def test_retries_failed_batches(self, tmp_path: Path) -> None:
        """Failed batches are retried."""
        config = make_config(tmp_path, max_batch_size=1, max_retries=2)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        call_count = [0]
        build_info = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:01",
        }

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                call_count[0] += 1
                if call_count[0] == 1:
                    result.stdout = json.dumps(
                        {
                            "status": "False",
                            "reason": "Failed",
                        }
                    )
                else:
                    result.stdout = json.dumps(
                        {
                            "status": "True",
                            "reason": "Succeeded",
                        }
                    )
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            timestamp_format="%s",
            results_data=results_data,
            run_command=mock_runner,
            sleep_fn=lambda x: None,
        )

        assert success is True
        assert call_count[0] == 2


class TestRun:
    """Tests for run function."""

    def test_full_workflow(self, tmp_path: Path) -> None:
        """Full workflow executes successfully."""
        snapshot = make_snapshot(
            [
                make_component("comp1", "4.12", container_image="img1"),
            ]
        )
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        data_path.write_text(json.dumps(data), encoding="utf-8")

        config = make_config(
            tmp_path,
            snapshot_path=snapshot_path,
            data_path=data_path,
        )

        build_info = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:01",
            "index_image_resolved": "quay.io/iib@sha256:abc",
        }

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                        "iibLog": "Log",
                    }
                )
            else:
                result.stdout = ""

            return result

        from datetime import datetime, timezone

        fixed_now = datetime(2024, 3, 6, 12, 0, 0, tzinfo=timezone.utc)

        results_data, timestamp = add_fbc_contribution.run(
            config,
            run_command=mock_runner,
            sleep_fn=lambda x: None,
            now_fn=lambda: fixed_now,
        )

        assert "components" in results_data
        assert len(results_data["components"]) == 1

    def test_raises_for_missing_snapshot(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised for missing snapshot."""
        data_path = tmp_path / "data.json"
        data_path.write_text(json.dumps(make_data()), encoding="utf-8")

        config = make_config(
            tmp_path,
            snapshot_path=tmp_path / "missing.json",
            data_path=data_path,
        )

        with pytest.raises(FileNotFoundError):
            add_fbc_contribution.run(config)


class TestMain:
    """Tests for main entry point."""

    def test_successful_execution(self, tmp_path: Path) -> None:
        """Main returns 0 on success."""
        snapshot = make_snapshot(
            [
                make_component("comp1", "4.12", container_image="img1"),
            ]
        )
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        results_dir = tmp_path / "results"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        data_path.write_text(json.dumps(data), encoding="utf-8")

        build_info = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:01",
        }

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        with mock.patch("add_fbc_contribution.run_cmd", mock_runner):
            result = add_fbc_contribution.main(
                [
                    "--snapshot-path",
                    "snapshot.json",
                    "--data-path",
                    "data.json",
                    "--data-dir",
                    str(tmp_path),
                    "--results-dir-path",
                    "results",
                    "--pipeline-run-uid",
                    "test-plr",
                    "--task-run-uid",
                    "test-tr",
                    "--iib-service-account-secret",
                    "test-secret",
                    "--task-git-url",
                    "http://localhost",
                    "--task-git-revision",
                    "main",
                    "--must-publish-index-image",
                    "true",
                    "--must-overwrite-from-index-image",
                    "true",
                ]
            )

        assert result == 0
        assert results_dir.exists()
        assert (results_dir / "internal-requests-results.json").exists()

    def test_returns_1_on_failure(self, tmp_path: Path) -> None:
        """Main returns 1 on failure."""
        result = add_fbc_contribution.main(
            [
                "--snapshot-path",
                "missing.json",
                "--data-path",
                "missing.json",
                "--data-dir",
                str(tmp_path),
                "--results-dir-path",
                "results",
                "--pipeline-run-uid",
                "test-plr",
                "--task-run-uid",
                "test-tr",
                "--iib-service-account-secret",
                "test-secret",
                "--task-git-url",
                "http://localhost",
                "--task-git-revision",
                "main",
            ]
        )

        assert result == 1


class TestParseArgs:
    """Tests for argument parsing."""

    def test_all_required_args(self) -> None:
        """All required arguments are parsed correctly."""
        parser = add_fbc_contribution.setup_argparser()
        args = parser.parse_args(
            [
                "--snapshot-path",
                "/path/to/snapshot.json",
                "--data-path",
                "/path/to/data.json",
                "--data-dir",
                "/var/workdir",
                "--results-dir-path",
                "/results",
                "--pipeline-run-uid",
                "plr-123",
                "--task-run-uid",
                "tr-456",
                "--iib-service-account-secret",
                "my-secret",
                "--task-git-url",
                "http://example.com",
                "--task-git-revision",
                "v1.0",
            ]
        )

        assert args.snapshot_path == Path("/path/to/snapshot.json")
        assert args.data_path == Path("/path/to/data.json")
        assert args.data_dir == Path("/var/workdir")
        assert args.pipeline_run_uid == "plr-123"
        assert args.iib_service_account_secret == "my-secret"

    def test_default_values(self) -> None:
        """Default values are applied correctly."""
        parser = add_fbc_contribution.setup_argparser()
        args = parser.parse_args(
            [
                "--snapshot-path",
                "snapshot.json",
                "--data-path",
                "data.json",
                "--data-dir",
                "/var/workdir",
                "--results-dir-path",
                "/results",
                "--pipeline-run-uid",
                "plr",
                "--task-run-uid",
                "tr",
                "--iib-service-account-secret",
                "secret",
                "--task-git-url",
                "http://example.com",
                "--task-git-revision",
                "main",
            ]
        )

        assert args.max_batch_size == 5
        assert args.max_retries == 3
        assert args.batch_retry_delay_seconds == 60
        assert args.must_publish_index_image == "false"
        assert args.must_overwrite_from_index_image == "false"

    def test_missing_required_args(self) -> None:
        """SystemExit is raised for missing required arguments."""
        parser = add_fbc_contribution.setup_argparser()

        with pytest.raises(SystemExit):
            parser.parse_args([])


class TestEdgeCasesAndRaceConditions:
    """Tests for edge cases, race conditions, and error handling."""

    def test_skips_empty_ocp_groups(self) -> None:
        """OCP versions with no components are skipped."""
        snapshot = make_snapshot([make_component("comp1", "4.12")])
        ocp_versions = ["4.12", "4.14"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, [])

        assert len(groups) == 1
        assert groups[0].ocp_version == "4.12"

    def test_invalid_json_in_status_response(self) -> None:
        """Invalid JSON in status response is handled gracefully."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not valid json {"
        mock_runner = mock.MagicMock(return_value=mock_result)

        succeeded, reason, message = add_fbc_contribution.get_internal_request_status(
            "test-ir",
            run_command=mock_runner,
        )

        assert succeeded is False
        assert reason == "Error"
        assert "Invalid JSON" in message

    def test_empty_status_response(self) -> None:
        """Empty status response is handled as not succeeded."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_runner = mock.MagicMock(return_value=mock_result)

        succeeded, reason, message = add_fbc_contribution.get_internal_request_status(
            "test-ir",
            run_command=mock_runner,
        )

        assert succeeded is False

    def test_get_internal_request_results_empty(self) -> None:
        """Empty results response returns empty dict."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_runner = mock.MagicMock(return_value=mock_result)

        results = add_fbc_contribution.get_internal_request_results(
            "test-ir",
            run_command=mock_runner,
        )

        assert results == {}

    def test_get_internal_request_results_invalid_json(self) -> None:
        """Invalid JSON in results returns empty dict."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not json"
        mock_runner = mock.MagicMock(return_value=mock_result)

        results = add_fbc_contribution.get_internal_request_results(
            "test-ir",
            run_command=mock_runner,
        )

        assert results == {}

    def test_get_internal_request_results_kubectl_error(self) -> None:
        """Kubectl error in results returns empty dict."""
        mock_result = mock.MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_runner = mock.MagicMock(return_value=mock_result)

        results = add_fbc_contribution.get_internal_request_results(
            "test-ir",
            run_command=mock_runner,
        )

        assert results == {}

    def test_execute_batch_empty_results(self, tmp_path: Path) -> None:
        """Batch with empty results returns failure."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = "{}"
            else:
                result.stdout = ""

            return result

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
            run_command=mock_runner,
        )

        assert batch_result.success is False
        assert "Empty results" in batch_result.error_message

    def test_execute_batch_missing_json_build_info(self, tmp_path: Path) -> None:
        """Batch with missing jsonBuildInfo returns failure."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps({"indexImageDigests": "sha256:a"})
            else:
                result.stdout = ""

            return result

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
            run_command=mock_runner,
        )

        assert batch_result.success is False
        assert "Missing jsonBuildInfo" in batch_result.error_message

    def test_execute_batch_invalid_json_build_info(self, tmp_path: Path) -> None:
        """Batch with invalid jsonBuildInfo returns failure."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps({"jsonBuildInfo": "not-valid-base64!!"})
            else:
                result.stdout = ""

            return result

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
            run_command=mock_runner,
        )

        assert batch_result.success is False
        assert "Failed to decompress" in batch_result.error_message

    def test_execute_batch_create_request_failure(self, tmp_path: Path) -> None:
        """Batch fails when InternalRequest creation fails."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0
            result.stdout = "Some error output without request name"
            return result

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
            run_command=mock_runner,
        )

        assert batch_result.success is False
        assert "Failed to extract" in batch_result.error_message

    def test_index_image_chaining_without_overwrite(self, tmp_path: Path) -> None:
        """Index images are chained when must_overwrite is False."""
        config = make_config(tmp_path, max_batch_size=1, must_overwrite_from_index_image=False)
        group = OCPGroup(
            ocp_version="4.12",
            components=[
                make_component("comp1", "4.12", container_image="img1"),
                make_component("comp2", "4.12", container_image="img2"),
            ],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        batch_counter = [0]
        from_indices_used: list[str] = []

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                for i, arg in enumerate(cmd):
                    if arg.startswith("fromIndex="):
                        from_indices_used.append(arg.split("=", 1)[1])
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                batch_counter[0] += 1
                build_info = {
                    "updated": "2024-03-06T16:39:11Z",
                    "index_image": f"quay.io/iib:batch{batch_counter[0]}",
                }
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            timestamp_format="%s",
            results_data=results_data,
            run_command=mock_runner,
            sleep_fn=lambda x: None,
        )

        assert success is True
        assert len(from_indices_used) == 2
        assert from_indices_used[0] == "quay.io/from:latest"
        assert from_indices_used[1] == "quay.io/iib:batch1"

    def test_all_batches_fail_after_retries(self, tmp_path: Path) -> None:
        """Returns False when all batches fail after max retries."""
        config = make_config(tmp_path, max_batch_size=1, max_retries=2)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "False", "reason": "Failed"})
            else:
                result.stdout = ""

            return result

        sleep_calls: list[int] = []

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            timestamp_format="%s",
            results_data=results_data,
            run_command=mock_runner,
            sleep_fn=lambda x: sleep_calls.append(x),
        )

        assert success is False
        assert len(sleep_calls) == 1

    def test_retry_delay_between_attempts(self, tmp_path: Path) -> None:
        """Retry delay is applied between failed retry attempts."""
        config = make_config(
            tmp_path, max_batch_size=1, max_retries=3, batch_retry_delay_seconds=30
        )
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        call_count = [0]

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                call_count[0] += 1
                if call_count[0] <= 3:
                    result.stdout = json.dumps({"status": "False", "reason": "Failed"})
                else:
                    result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                build_info = {
                    "updated": "2024-03-06T16:39:11Z",
                    "index_image": "quay.io/iib:01",
                }
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        sleep_calls: list[int] = []

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            timestamp_format="%s",
            results_data=results_data,
            run_command=mock_runner,
            sleep_fn=lambda x: sleep_calls.append(x),
        )

        assert success is True
        assert sleep_calls == [30, 30]

    def test_deduplicate_fallback_to_ocp_version(self) -> None:
        """Deduplication falls back to ocp_version when target_index is empty."""
        results = {
            "components": [
                {"target_index": "", "ocp_version": "4.12", "order": 1},
                {"target_index": "", "ocp_version": "4.12", "order": 2},
            ]
        }

        result = deduplicate_results(results, is_staged=False)

        assert len(result["components"]) == 1
        assert result["components"][0]["order"] == 2

    def test_process_batch_results_missing_results(self, tmp_path: Path) -> None:
        """Skips processing when batch results are empty."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        batch_result = BatchResult(batch_num=0, success=True, results={})
        results_data: dict[str, Any] = {"components": []}

        add_fbc_contribution.process_batch_results(
            batch_result, group, config, "%s", results_data
        )

        assert len(results_data["components"]) == 0

    def test_process_batch_results_invalid_build_info(self, tmp_path: Path) -> None:
        """Skips processing when build info decompression fails."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        batch_result = BatchResult(
            batch_num=0,
            success=True,
            results={"jsonBuildInfo": "invalid-data"},
        )
        results_data: dict[str, Any] = {"components": []}

        add_fbc_contribution.process_batch_results(
            batch_result, group, config, "%s", results_data
        )

        assert len(results_data["components"]) == 0

    def test_run_raises_on_group_failure(self, tmp_path: Path) -> None:
        """RuntimeError is raised when OCP group processing fails."""
        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        data_path.write_text(json.dumps(data), encoding="utf-8")

        config = make_config(
            tmp_path,
            snapshot_path=snapshot_path,
            data_path=data_path,
            max_retries=1,
        )

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "False", "reason": "Failed"})
            else:
                result.stdout = ""

            return result

        with pytest.raises(RuntimeError, match="One or more OCP groups failed"):
            add_fbc_contribution.run(
                config,
                run_command=mock_runner,
                sleep_fn=lambda x: None,
            )

    def test_multiple_ocp_groups_processed_sequentially(self, tmp_path: Path) -> None:
        """Multiple OCP groups are processed in sequence."""
        snapshot = make_snapshot(
            [
                make_component("comp1", "4.12", container_image="img1"),
                make_component("comp2", "4.14", container_image="img2"),
            ]
        )
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        data_path.write_text(json.dumps(data), encoding="utf-8")

        config = make_config(
            tmp_path,
            snapshot_path=snapshot_path,
            data_path=data_path,
        )

        ocp_versions_processed: list[str] = []

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                for arg in cmd:
                    if arg.startswith("fromIndex="):
                        idx = arg.split("=", 1)[1]
                        if "4.12" in idx:
                            ocp_versions_processed.append("4.12")
                        elif "4.14" in idx:
                            ocp_versions_processed.append("4.14")
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                build_info = {
                    "updated": "2024-03-06T16:39:11Z",
                    "index_image": "quay.io/iib:01",
                }
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        from datetime import datetime, timezone

        fixed_now = datetime(2024, 3, 6, 12, 0, 0, tzinfo=timezone.utc)

        results_data, timestamp = add_fbc_contribution.run(
            config,
            run_command=mock_runner,
            sleep_fn=lambda x: None,
            now_fn=lambda: fixed_now,
        )

        assert "components" in results_data
        assert len(results_data["components"]) == 2

    def test_process_batch_results_empty_completion_time(self, tmp_path: Path) -> None:
        """Handles empty completion_time_raw gracefully."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        build_info = {
            "updated": "",
            "index_image": "quay.io/iib:01",
            "index_image_resolved": "quay.io/iib@sha256:abc",
        }
        batch_result = BatchResult(
            batch_num=0,
            success=True,
            index_image="quay.io/iib:01",
            results={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            },
        )
        results_data: dict[str, Any] = {"components": []}

        add_fbc_contribution.process_batch_results(
            batch_result, group, config, "%s", results_data
        )

        assert len(results_data["components"]) == 1
        assert results_data["components"][0]["completion_time"] == ""

    def test_process_batch_results_invalid_timestamp(self, tmp_path: Path) -> None:
        """Handles invalid timestamp format gracefully."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        build_info = {
            "updated": "not-a-valid-timestamp",
            "index_image": "quay.io/iib:01",
        }
        batch_result = BatchResult(
            batch_num=0,
            success=True,
            index_image="quay.io/iib:01",
            results={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            },
        )
        results_data: dict[str, Any] = {"components": []}

        add_fbc_contribution.process_batch_results(
            batch_result, group, config, "%s", results_data
        )

        assert len(results_data["components"]) == 1
        assert results_data["components"][0]["completion_time"] == "not-a-valid-timestamp"

    def test_process_batch_results_missing_json_build_info_in_results(
        self, tmp_path: Path
    ) -> None:
        """Skips processing when jsonBuildInfo is missing from results."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        batch_result = BatchResult(
            batch_num=0,
            success=True,
            index_image="quay.io/iib:01",
            results={"indexImageDigests": "sha256:a"},
        )
        results_data: dict[str, Any] = {"components": []}

        add_fbc_contribution.process_batch_results(
            batch_result, group, config, "%s", results_data
        )

        assert len(results_data["components"]) == 0

    def test_retry_success_updates_index_image_without_overwrite(self, tmp_path: Path) -> None:
        """Index image is updated on retry success when must_overwrite is False."""
        config = make_config(
            tmp_path,
            max_batch_size=1,
            max_retries=2,
            must_overwrite_from_index_image=False,
        )
        group = OCPGroup(
            ocp_version="4.12",
            components=[
                make_component("comp1", "4.12", container_image="img1"),
                make_component("comp2", "4.12", container_image="img2"),
            ],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        call_count = [0]
        from_indices_used: list[str] = []

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                for arg in cmd:
                    if arg.startswith("fromIndex="):
                        from_indices_used.append(arg.split("=", 1)[1])
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                call_count[0] += 1
                if call_count[0] == 1:
                    result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
                elif call_count[0] == 2:
                    result.stdout = json.dumps({"status": "False", "reason": "Failed"})
                else:
                    result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                build_info = {
                    "updated": "2024-03-06T16:39:11Z",
                    "index_image": f"quay.io/iib:batch{call_count[0]}",
                }
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            timestamp_format="%s",
            results_data=results_data,
            run_command=mock_runner,
            sleep_fn=lambda x: None,
        )

        assert success is True
        assert len(from_indices_used) == 3
        assert from_indices_used[2] == "quay.io/iib:batch1"

    def test_run_with_non_list_build_tags(self, tmp_path: Path) -> None:
        """Handles non-list buildTags gracefully."""
        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = {"fbc": {"buildTags": "not-a-list", "requestTimeoutSeconds": 120}}

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        data_path.write_text(json.dumps(data), encoding="utf-8")

        config = make_config(
            tmp_path,
            snapshot_path=snapshot_path,
            data_path=data_path,
        )

        build_info = {"updated": "2024-03-06T16:39:11Z", "index_image": "quay.io/iib:01"}

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        from datetime import datetime, timezone

        fixed_now = datetime(2024, 3, 6, 12, 0, 0, tzinfo=timezone.utc)

        results_data, timestamp = add_fbc_contribution.run(
            config,
            run_command=mock_runner,
            sleep_fn=lambda x: None,
            now_fn=lambda: fixed_now,
        )

        assert "components" in results_data

    def test_main_writes_result_files(self, tmp_path: Path) -> None:
        """Main writes all result files when paths are provided."""
        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        data_path.write_text(json.dumps(data), encoding="utf-8")

        build_timestamp_result = tmp_path / "build_timestamp"
        request_results_result = tmp_path / "request_results"
        ir_results_result = tmp_path / "ir_results"

        build_info = {"updated": "2024-03-06T16:39:11Z", "index_image": "quay.io/iib:01"}

        def mock_runner(cmd: list[str], check: bool = True) -> mock.MagicMock:
            result = mock.MagicMock()
            result.returncode = 0

            if "internal-request" in cmd:
                result.stdout = "InternalRequest 'test-ir' created\n"
            elif "jsonpath={.status.conditions" in str(cmd):
                result.stdout = json.dumps({"status": "True", "reason": "Succeeded"})
            elif "jsonpath={.status.results}" in str(cmd):
                result.stdout = json.dumps(
                    {
                        "jsonBuildInfo": iib.compress_build_info(build_info),
                        "indexImageDigests": "sha256:a",
                    }
                )
            else:
                result.stdout = ""

            return result

        with mock.patch("add_fbc_contribution.run_cmd", mock_runner):
            result = add_fbc_contribution.main(
                [
                    "--snapshot-path",
                    "snapshot.json",
                    "--data-path",
                    "data.json",
                    "--data-dir",
                    str(tmp_path),
                    "--results-dir-path",
                    "results",
                    "--pipeline-run-uid",
                    "test-plr",
                    "--task-run-uid",
                    "test-tr",
                    "--iib-service-account-secret",
                    "test-secret",
                    "--task-git-url",
                    "http://localhost",
                    "--task-git-revision",
                    "main",
                    "--must-publish-index-image",
                    "true",
                    "--must-overwrite-from-index-image",
                    "true",
                    "--build-timestamp-result",
                    str(build_timestamp_result),
                    "--request-results-file-result",
                    str(request_results_result),
                    "--internal-request-results-file-result",
                    str(ir_results_result),
                ]
            )

        assert result == 0
        assert build_timestamp_result.exists()
        assert request_results_result.exists()
        assert ir_results_result.exists()
