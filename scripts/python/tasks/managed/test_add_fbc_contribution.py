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
    compute_target_index_with_timestamp,
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
    ocp_version: str | list[str],
    container_image: str | None = None,
    from_index: str | None = None,
    target_index: str | None = None,
) -> dict[str, Any]:
    """Create a test component."""
    if isinstance(ocp_version, list):
        return {
            "name": name,
            "containerImage": container_image or f"registry.io/{name}@sha256:0000",
            "ocpVersion": ocp_version,
            "updatedFromIndex": from_index or f"quay.io/fbc-index:v{ocp_version[0]}",
            "targetIndex": target_index or f"quay.io/fbc-target:v{ocp_version[0]}",
        }
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

    def test_multi_ocp_versions_array(self) -> None:
        """OCP versions as JSON array are extracted correctly."""
        snapshot = {
            "components": [
                {"name": "comp1", "ocpVersion": ["v4.17", "v4.18", "v4.19"]},
                {"name": "comp2", "ocpVersion": ["v4.18", "v4.19"]},
            ]
        }

        result = get_ocp_versions(snapshot)

        assert result == ["v4.17", "v4.18", "v4.19"]

    def test_mixed_string_and_array_ocp_versions(self) -> None:
        """Handles mixed string and array ocpVersion formats."""
        snapshot = {
            "components": [
                {"name": "comp1", "ocpVersion": "4.12"},
                {"name": "comp2", "ocpVersion": ["4.13", "4.14"]},
            ]
        }

        result = get_ocp_versions(snapshot)

        assert result == ["4.12", "4.13", "4.14"]


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

    def test_multi_ocp_with_version_metadata(self) -> None:
        """Components with multiple OCP versions use ocpVersionMetadata."""
        snapshot = {
            "components": [
                {
                    "name": "comp1",
                    "containerImage": "registry.io/comp1@sha256:0001",
                    "ocpVersion": ["v4.17", "v4.18"],
                    "ocpVersionMetadata": [
                        {
                            "version": "v4.17",
                            "updatedFromIndex": "quay.io/from:v4.17",
                            "targetIndex": "quay.io/target:v4.17",
                        },
                        {
                            "version": "v4.18",
                            "updatedFromIndex": "quay.io/from:v4.18",
                            "targetIndex": "quay.io/target:v4.18",
                        },
                    ],
                }
            ]
        }
        ocp_versions = ["v4.17", "v4.18"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, [])

        assert len(groups) == 2
        assert groups[0].ocp_version == "v4.17"
        assert groups[0].from_index == "quay.io/from:v4.17"
        assert groups[0].target_index == "quay.io/target:v4.17"
        assert groups[0].components[0]["name"] == "comp1-v4.17"
        assert groups[1].ocp_version == "v4.18"
        assert groups[1].from_index == "quay.io/from:v4.18"
        assert groups[1].target_index == "quay.io/target:v4.18"
        assert groups[1].components[0]["name"] == "comp1-v4.18"

    def test_multi_ocp_preserves_original_name(self) -> None:
        """Original component name is preserved when expanding multi-OCP."""
        snapshot = {
            "components": [
                {
                    "name": "comp1",
                    "containerImage": "registry.io/comp1@sha256:0001",
                    "ocpVersion": ["v4.17", "v4.18"],
                    "ocpVersionMetadata": [
                        {"version": "v4.17", "updatedFromIndex": "idx1", "targetIndex": "t1"},
                        {"version": "v4.18", "updatedFromIndex": "idx2", "targetIndex": "t2"},
                    ],
                }
            ]
        }
        ocp_versions = ["v4.17"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, [])

        assert groups[0].components[0]["originalName"] == "comp1"


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


class TestComputeTargetIndexWithTimestamp:
    """Tests for compute_target_index_with_timestamp function."""

    def test_empty_target_index_returns_empty(self) -> None:
        """Empty target_index returns empty string."""
        result = compute_target_index_with_timestamp("", "1709746751")

        assert result == ""

    def test_target_index_with_existing_timestamp(self) -> None:
        """Target index with existing 10-digit timestamp is returned as-is."""
        result = compute_target_index_with_timestamp(
            "quay.io/target:v4.17-1709746751", "9999999999"
        )

        assert result == "quay.io/target:v4.17-1709746751"

    def test_target_index_without_timestamp_gets_appended(self) -> None:
        """Completion time is appended to target index without existing timestamp."""
        result = compute_target_index_with_timestamp("quay.io/target:v4.17", "1709746751")

        assert result == "quay.io/target:v4.17-1709746751"

    def test_target_index_with_digits_but_no_hyphen_prefix(self) -> None:
        """Target index ending with digits but no hyphen still gets timestamp appended."""
        result = compute_target_index_with_timestamp(
            "quay.io/target:1234567890", "1709746751"
        )

        assert result == "quay.io/target:1234567890-1709746751"


class TestCalculateTimeouts:
    """Tests for calculate_timeouts function."""

    def test_calculates_timeouts(self) -> None:
        """Timeouts are calculated correctly."""
        pipeline_timeout, task_timeout, finally_timeout = calculate_timeouts(3600)

        assert pipeline_timeout == "1h5m0s"
        assert task_timeout == "1h0m0s"
        assert finally_timeout == "0h5m0s"

    def test_handles_smaller_timeout(self) -> None:
        """Smaller timeouts are calculated correctly."""
        pipeline_timeout, task_timeout, finally_timeout = calculate_timeouts(120)

        assert pipeline_timeout == "0h7m0s"
        assert task_timeout == "0h2m0s"
        assert finally_timeout == "0h5m0s"

    def test_handles_complex_timeout(self) -> None:
        """Complex timeouts with hours, minutes, and seconds."""
        pipeline_timeout, task_timeout, finally_timeout = calculate_timeouts(3661)

        assert pipeline_timeout == "1h6m1s"
        assert task_timeout == "1h1m1s"
        assert finally_timeout == "0h5m0s"


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

        process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 1
        component = results_data["components"][0]
        assert component["fbc_fragment"] == "img1"
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

        process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 0


class TestDeduplicateResults:
    """Tests for deduplicate_results function."""

    def test_deduplicates_by_target_index(self) -> None:
        """Results are deduplicated by target_index."""
        results_data = {
            "components": [
                {"target_index": "target1", "ocp_version": "4.12", "name": "first"},
                {"target_index": "target1", "ocp_version": "4.12", "name": "second"},
                {"target_index": "target2", "ocp_version": "4.14", "name": "third"},
            ]
        }

        result = deduplicate_results(results_data, is_staged=False)

        assert len(result["components"]) == 2
        names = [c["name"] for c in result["components"]]
        assert "second" in names
        assert "third" in names

    def test_deduplicates_by_ocp_version_for_staged(self) -> None:
        """Results are deduplicated by ocp_version for staged releases."""
        results_data = {
            "components": [
                {"target_index": "", "ocp_version": "4.12", "name": "first"},
                {"target_index": "", "ocp_version": "4.12", "name": "second"},
                {"target_index": "", "ocp_version": "4.14", "name": "third"},
            ]
        }

        result = deduplicate_results(results_data, is_staged=True)

        assert len(result["components"]) == 2

    def test_no_deduplication_needed(self) -> None:
        """No deduplication when all targets are unique."""
        results_data = {
            "components": [
                {"target_index": "target1", "ocp_version": "4.12"},
                {"target_index": "target2", "ocp_version": "4.14"},
            ]
        }

        result = deduplicate_results(results_data, is_staged=False)

        assert len(result["components"]) == 2


class TestExecuteBatch:
    """Tests for execute_batch function using monkeypatch."""

    def test_successful_batch_execution(self, tmp_path: Path, monkeypatch: Any) -> None:
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

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
        )

        assert batch_result.success is True
        assert batch_result.index_image == "quay.io/iib:01"
        mock_create.assert_called_once()
        mock_fetch.assert_called_once_with("test-ir")

    def test_failed_batch_execution(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Batch failure is handled correctly."""
        from internal_request import InternalRequestWaitError

        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        mock_create = mock.MagicMock(side_effect=InternalRequestWaitError("IIB error", 21))

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
        )

        assert batch_result.success is False
        assert "IIB error" in batch_result.error_message


class TestProcessOcpGroup:
    """Tests for process_ocp_group function using monkeypatch."""

    def test_processes_all_batches_successfully(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
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

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
                "iibLog": "Log",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            results_data=results_data,
        )

        assert success is True
        assert mock_create.call_count == 2

    def test_handles_batch_failure_with_retry(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Failed batches are retried."""
        from internal_request import InternalRequestWaitError

        config = make_config(tmp_path, max_batch_size=1, max_retries=2)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
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

        call_count = [0]

        def mock_create_side_effect(*args: Any, **kwargs: Any) -> str:
            call_count[0] += 1
            if call_count[0] == 1:
                raise InternalRequestWaitError("Temporary failure", 21)
            return "test-ir"

        mock_create = mock.MagicMock(side_effect=mock_create_side_effect)
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            results_data=results_data,
        )

        assert success is True
        assert call_count[0] == 2


class TestRun:
    """Tests for run function using monkeypatch."""

    def test_run_success(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Run completes successfully."""
        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot))
        data_path.write_text(json.dumps(data))

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

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        results_data, timestamp = add_fbc_contribution.run(config)

        assert len(results_data["components"]) == 1
        assert timestamp


class TestMain:
    """Tests for main function."""

    def test_missing_required_args(self) -> None:
        """Missing required arguments cause parse error."""
        parser = add_fbc_contribution.setup_argparser()

        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_main_writes_result_files(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Main writes all result files correctly."""
        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        results_dir = tmp_path / "results"
        results_dir.mkdir()

        snapshot_path.write_text(json.dumps(snapshot))
        data_path.write_text(json.dumps(data))

        build_timestamp_result = tmp_path / "build_timestamp"
        request_results_result = tmp_path / "request_results"
        ir_results_result = tmp_path / "ir_results"

        build_info = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:01",
            "index_image_resolved": "quay.io/iib@sha256:abc",
        }

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

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


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_results_from_internal_request(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """Handles empty results from InternalRequest."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(return_value={})

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
        )

        assert batch_result.success is False
        assert "Empty results" in batch_result.error_message

    def test_missing_json_build_info(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Handles missing jsonBuildInfo in results."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(return_value={"indexImageDigests": "sha256:a"})

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
        )

        assert batch_result.success is False
        assert "Missing jsonBuildInfo" in batch_result.error_message

    def test_process_batch_results_empty_completion_time(self, tmp_path: Path) -> None:
        """Skips processing when completion_time is empty."""
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

        add_fbc_contribution.process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 0

    def test_process_batch_results_invalid_timestamp(self, tmp_path: Path) -> None:
        """Skips processing when timestamp format is invalid."""
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

        add_fbc_contribution.process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 0

    def test_group_components_with_non_matching_ocp_version(self) -> None:
        """Components are skipped when OCP version doesn't match."""
        snapshot = make_snapshot([make_component("comp1", "4.12")])
        ocp_versions = ["4.14"]

        groups = group_components_by_ocp_version(snapshot, ocp_versions, [])

        assert len(groups) == 0

    def test_execute_batch_generic_exception(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Generic exceptions during request creation are handled."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        mock_create = mock.MagicMock(side_effect=Exception("Generic error"))

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
        )

        assert batch_result.success is False
        assert "Generic error" in batch_result.error_message

    def test_execute_batch_invalid_gzip_data(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Invalid gzip data in jsonBuildInfo is handled."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": "not-valid-base64-gzip",
                "indexImageDigests": "sha256:a",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        batch_result = add_fbc_contribution.execute_batch(
            batch_num=0,
            from_index="quay.io/from:latest",
            group=group,
            config=config,
            data=data,
        )

        assert batch_result.success is False
        assert "Failed to decompress" in batch_result.error_message

    def test_process_batch_results_completion_time_null_string(self, tmp_path: Path) -> None:
        """Skips processing when completion_time is 'null' string."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        build_info = {
            "updated": "null",
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

        add_fbc_contribution.process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 0

    def test_process_batch_results_missing_json_build_info(self, tmp_path: Path) -> None:
        """Skips processing when jsonBuildInfo is missing."""
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

        add_fbc_contribution.process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 0

    def test_process_batch_results_invalid_json_build_info(self, tmp_path: Path) -> None:
        """Skips processing when jsonBuildInfo fails to decompress."""
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
            results={
                "jsonBuildInfo": "invalid-gzip-data",
                "indexImageDigests": "sha256:a",
            },
        )
        results_data: dict[str, Any] = {"components": []}

        add_fbc_contribution.process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 0

    def test_ocp_group_all_batches_fail_after_retries(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """Returns False when all batches fail after all retries."""
        from internal_request import InternalRequestWaitError

        config = make_config(tmp_path, max_batch_size=1, max_retries=1)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        mock_create = mock.MagicMock(
            side_effect=InternalRequestWaitError("Permanent failure", 21)
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            results_data=results_data,
        )

        assert success is False
        assert mock_create.call_count == 2

    def test_ocp_group_updates_from_index_when_not_overwrite(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """From index is updated between batches when not in overwrite mode."""
        config = make_config(
            tmp_path,
            max_batch_size=1,
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
        from_indexes: list[str] = []

        def mock_create_side_effect(*args: Any, **kwargs: Any) -> str:
            from_index = kwargs.get("params", {}).get("fromIndex", "")
            from_indexes.append(from_index)
            call_count[0] += 1
            return f"test-ir-{call_count[0]}"

        build_info_1 = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:batch1",
            "index_image_resolved": "quay.io/iib@sha256:abc1",
        }
        build_info_2 = {
            "updated": "2024-03-06T16:40:11Z",
            "index_image": "quay.io/iib:batch2",
            "index_image_resolved": "quay.io/iib@sha256:abc2",
        }

        fetch_results = [
            {
                "jsonBuildInfo": iib.compress_build_info(build_info_1),
                "indexImageDigests": "sha256:a",
            },
            {
                "jsonBuildInfo": iib.compress_build_info(build_info_2),
                "indexImageDigests": "sha256:b",
            },
        ]
        fetch_call_count = [0]

        def mock_fetch_side_effect(name: str) -> dict[str, Any]:
            result = fetch_results[fetch_call_count[0]]
            fetch_call_count[0] += 1
            return result

        mock_create = mock.MagicMock(side_effect=mock_create_side_effect)
        mock_fetch = mock.MagicMock(side_effect=mock_fetch_side_effect)

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            results_data=results_data,
        )

        assert success is True
        assert mock_create.call_count == 2
        assert from_indexes[0] == "quay.io/from:latest"
        assert from_indexes[1] == "quay.io/iib:batch1"

    def test_run_failure_returns_error(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Run raises RuntimeError when OCP group fails."""
        from internal_request import InternalRequestWaitError

        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot))
        data_path.write_text(json.dumps(data))

        config = make_config(
            tmp_path,
            snapshot_path=snapshot_path,
            data_path=data_path,
            max_retries=0,
        )

        mock_create = mock.MagicMock(side_effect=InternalRequestWaitError("IIB failure", 21))

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)

        with pytest.raises(RuntimeError, match="One or more OCP groups failed"):
            add_fbc_contribution.run(config)

    def test_main_raises_on_failure(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Main raises exception when run fails (managed task pattern)."""
        from internal_request import InternalRequestWaitError

        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = make_data()

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        results_dir = tmp_path / "results"
        results_dir.mkdir()

        snapshot_path.write_text(json.dumps(snapshot))
        data_path.write_text(json.dumps(data))

        mock_create = mock.MagicMock(side_effect=InternalRequestWaitError("IIB failure", 21))

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)

        with pytest.raises(RuntimeError, match="One or more OCP groups failed"):
            add_fbc_contribution.main(
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
                    "--max-retries",
                    "0",
                ]
            )

    def test_ocp_group_retry_with_exponential_backoff(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """Retry uses exponential backoff from the shared helper."""
        from internal_request import InternalRequestWaitError

        config = make_config(
            tmp_path,
            max_batch_size=1,
            max_retries=2,
            batch_retry_delay_seconds=10,
        )
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
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

        call_count = [0]

        def mock_create_side_effect(*args: Any, **kwargs: Any) -> str:
            call_count[0] += 1
            if call_count[0] <= 2:
                raise InternalRequestWaitError("Temporary failure", 21)
            return "test-ir"

        sleep_calls: list[float] = []

        def mock_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)

        mock_create = mock.MagicMock(side_effect=mock_create_side_effect)
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)
        monkeypatch.setattr("retry.time.sleep", mock_sleep)

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            results_data=results_data,
        )

        assert success is True
        assert call_count[0] == 3
        assert len(sleep_calls) == 2
        assert sleep_calls[0] == 10
        assert sleep_calls[1] == 20

    def test_ocp_group_batch_fails_after_retries(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """Returns False when a batch fails after all retry attempts."""
        from internal_request import InternalRequestWaitError

        config = make_config(
            tmp_path,
            max_batch_size=1,
            max_retries=2,
            batch_retry_delay_seconds=1,
        )
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        data = make_data()
        results_data: dict[str, Any] = {"components": []}

        mock_create = mock.MagicMock(
            side_effect=InternalRequestWaitError("Permanent failure", 21)
        )

        def mock_sleep(seconds: float) -> None:
            pass

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("retry.time.sleep", mock_sleep)

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            results_data=results_data,
        )

        assert success is False
        assert mock_create.call_count == 3

    def test_process_batch_results_completion_time_short(self, tmp_path: Path) -> None:
        """Skips processing when completion_time is not 10 digits."""
        config = make_config(tmp_path)
        group = OCPGroup(
            ocp_version="4.12",
            components=[make_component("comp1", "4.12", container_image="img1")],
            from_index="quay.io/from:latest",
            target_index="quay.io/target:v4.12",
        )
        build_info = {
            "updated": "1970-01-01T00:00:01Z",
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

        add_fbc_contribution.process_batch_results(batch_result, group, config, results_data)

        assert len(results_data["components"]) == 0

    def test_deduplicate_results_empty_components(self) -> None:
        """Returns unchanged when components list is empty."""
        results_data: dict[str, Any] = {"components": []}

        result = deduplicate_results(results_data, is_staged=False)

        assert result == {"components": []}

    def test_deduplicate_results_empty_target_indexes(self) -> None:
        """Falls back to ocp_version grouping when all target_indexes are empty."""
        results_data = {
            "components": [
                {"target_index": "", "ocp_version": "4.12", "name": "first"},
                {"target_index": "", "ocp_version": "4.12", "name": "second"},
            ]
        }

        result = deduplicate_results(results_data, is_staged=False)

        assert len(result["components"]) == 1

    def test_run_with_nonlist_build_tags(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Run handles non-list buildTags config."""
        snapshot = make_snapshot([make_component("comp1", "4.12", container_image="img1")])
        data = {
            "fbc": {
                "buildTimeoutSeconds": 420,
                "requestTimeoutSeconds": 120,
                "buildTags": "not-a-list",
            }
        }

        snapshot_path = tmp_path / "snapshot.json"
        data_path = tmp_path / "data.json"
        snapshot_path.write_text(json.dumps(snapshot))
        data_path.write_text(json.dumps(data))

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

        mock_create = mock.MagicMock(return_value="test-ir")
        mock_fetch = mock.MagicMock(
            return_value={
                "jsonBuildInfo": iib.compress_build_info(build_info),
                "indexImageDigests": "sha256:a",
            }
        )

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)

        results_data, timestamp = add_fbc_contribution.run(config)

        assert len(results_data["components"]) == 1

    def test_ocp_group_updates_from_index_after_successful_batch(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """From index is updated after a successful batch in non-overwrite mode."""
        config = make_config(
            tmp_path,
            max_batch_size=1,
            max_retries=2,
            must_overwrite_from_index_image=False,
            batch_retry_delay_seconds=1,
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

        build_info_1 = {
            "updated": "2024-03-06T16:39:11Z",
            "index_image": "quay.io/iib:batch1",
            "index_image_resolved": "quay.io/iib@sha256:batch1",
        }
        build_info_2 = {
            "updated": "2024-03-06T16:40:11Z",
            "index_image": "quay.io/iib:batch2",
            "index_image_resolved": "quay.io/iib@sha256:batch2",
        }

        call_count = [0]
        from_indexes: list[str] = []

        def mock_create_side_effect(*args: Any, **kwargs: Any) -> str:
            from_index = kwargs.get("params", {}).get("fromIndex", "")
            from_indexes.append(from_index)
            call_count[0] += 1
            return "test-ir"

        fetch_results_list = [
            {
                "jsonBuildInfo": iib.compress_build_info(build_info_1),
                "indexImageDigests": "sha256:a",
            },
            {
                "jsonBuildInfo": iib.compress_build_info(build_info_2),
                "indexImageDigests": "sha256:b",
            },
        ]
        fetch_call_count = [0]

        def mock_fetch_side_effect(name: str) -> dict[str, Any]:
            result = fetch_results_list[fetch_call_count[0]]
            fetch_call_count[0] += 1
            return result

        def mock_sleep(seconds: float) -> None:
            pass

        mock_create = mock.MagicMock(side_effect=mock_create_side_effect)
        mock_fetch = mock.MagicMock(side_effect=mock_fetch_side_effect)

        monkeypatch.setattr("add_fbc_contribution.create", mock_create)
        monkeypatch.setattr("add_fbc_contribution.fetch_results", mock_fetch)
        monkeypatch.setattr("retry.time.sleep", mock_sleep)

        success = add_fbc_contribution.process_ocp_group(
            group=group,
            config=config,
            data=data,
            results_data=results_data,
        )

        assert success is True
        assert len(from_indexes) == 2
        assert from_indexes[0] == "quay.io/from:latest"
        assert from_indexes[1] == "quay.io/iib:batch1"
