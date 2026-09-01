"""Tests for collect_data module."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.collect_data import (
    CollectDataResult,
    check_data_key_sources,
    collect,
    deep_merge,
    flatten_collectors,
    main,
    resolve_pipeline_ref,
    run,
    transform_snapshot_spec,
    write_outputs,
)


def _resource_side_effect(
    resource_type: str,
    namespace: str,
    name: str,
) -> dict:
    """Shared mock side effect returning test resources by type."""
    resources = {
        "release": {
            "metadata": {"name": "my-release"},
            "spec": {"data": {"foo": "shouldGetOverwritten"}},
            "status": {
                "collectors": {
                    "managed": {"c1": {"releaseNotes": {"cves": [{"key": "CVE-1"}]}}},
                    "tenant": {},
                }
            },
        },
        "releaseplan": {
            "metadata": {"name": "my-rp"},
            "spec": {"data": {"foo": "bar"}},
        },
        "releaseplanadmission": {
            "metadata": {"name": "my-rpa"},
            "spec": {
                "data": {"one": {"two": "three"}},
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "cluster",
                        "params": [{"name": "name", "value": "p1"}],
                    }
                },
            },
        },
        "releaseserviceconfig": {"metadata": {"name": "my-rsc"}, "spec": {}},
        "snapshot": {
            "metadata": {
                "name": "my-snap",
                "labels": {"appstudio.openshift.io/build-pipelinerun": "build-123"},
            },
            "spec": {
                "application": "myapp",
                "components": [{"name": "comp1"}],
            },
        },
    }
    return resources.get(resource_type, {})


class TestDeepMerge:
    """Tests for the deep_merge function."""

    def test_merge_dicts(self) -> None:
        """Test merge dicts."""
        assert deep_merge({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}

    def test_override_scalar(self) -> None:
        """Test override scalar."""
        assert deep_merge({"a": 1}, {"a": 2}) == {"a": 2}

    def test_nested_dict_merge(self) -> None:
        """Test nested dict merge."""
        base = {"one": {"two": "three"}}
        override = {"one": {"four": ["five", "six"]}}
        result = deep_merge(base, override)
        assert result == {"one": {"two": "three", "four": ["five", "six"]}}

    def test_array_concat_deduplicate(self) -> None:
        """Test array concat deduplicate."""
        base = {"tags": ["a", "b"]}
        override = {"tags": ["b", "c"]}
        assert deep_merge(base, override) == {"tags": ["a", "b", "c"]}

    def test_boolean_false_preserved(self) -> None:
        """Test boolean false preserved."""
        assert deep_merge({"flag": True}, {"flag": False}) == {"flag": False}

    def test_none_override_preserves_base(self) -> None:
        """Test none override preserves base."""
        assert deep_merge({"a": 1}, {"a": None}) == {"a": 1}

    def test_none_base_with_override(self) -> None:
        """Test none base with override."""
        assert deep_merge({"a": None}, {"a": 42}) == {"a": 42}

    def test_empty_dicts(self) -> None:
        """Test empty dicts."""
        assert deep_merge({}, {}) == {}

    def test_scalar_override_dict(self) -> None:
        """Test scalar override dict."""
        assert deep_merge("old", "new") == "new"

    def test_nested_arrays_in_dicts(self) -> None:
        """Test nested arrays in dicts."""
        base = {"issues": {"fixed": [{"id": "1"}]}}
        override = {"issues": {"fixed": [{"id": "2"}]}}
        result = deep_merge(base, override)
        assert result == {"issues": {"fixed": [{"id": "1"}, {"id": "2"}]}}

    def test_unsortable_list_items(self) -> None:
        """Test list merge falls back to insertion order for non-serializable items."""
        a, b = object(), object()
        result = deep_merge([a], [b])
        assert result == [a, b]


class TestFlattenCollectors:
    """Tests for the flatten_collectors function."""

    def test_managed_and_tenant(self) -> None:
        """Test managed and tenant."""
        status = {
            "managed": {
                "foo": {"cves": [{"key": "CVE-1"}]},
                "bar": {"issues": [{"id": "ISSUE-1"}]},
            },
            "tenant": {
                "baz": {"extra": [{"key": "E-1"}]},
            },
        }
        result = flatten_collectors(status)
        assert result == {
            "cves": [{"key": "CVE-1"}],
            "issues": [{"id": "ISSUE-1"}],
            "extra": [{"key": "E-1"}],
        }

    def test_empty_collectors(self) -> None:
        """Test empty collectors."""
        assert flatten_collectors({}) == {}

    def test_none_collectors(self) -> None:
        """Test none collectors."""
        assert flatten_collectors(None) == {}

    def test_missing_sections(self) -> None:
        """Test missing sections."""
        status = {"managed": {"a": {"x": 1}}}
        assert flatten_collectors(status) == {"x": 1}

    def test_overlapping_keys_merged(self) -> None:
        """Test overlapping keys merged."""
        status = {
            "managed": {
                "a": {"data": {"val": 1}},
                "b": {"data": {"val": 2}},
            },
        }
        result = flatten_collectors(status)
        assert result["data"]["val"] == 2


class TestTransformSnapshotSpec:
    """Tests for the transform_snapshot_spec function."""

    def test_empty_component_group_fallback(self) -> None:
        """Test empty component group fallback."""
        spec = {
            "componentGroup": "",
            "application": "myapp",
            "components": [],
        }
        result = transform_snapshot_spec(spec)
        assert result["componentGroup"] == "myapp"
        assert "application" not in result

    def test_null_component_group_fallback(self) -> None:
        """Test null component group fallback."""
        spec = {"componentGroup": None, "application": "myapp"}
        result = transform_snapshot_spec(spec)
        assert result["componentGroup"] == "myapp"

    def test_set_component_group_kept(self) -> None:
        """Test set component group kept."""
        spec = {"componentGroup": "group1", "application": "myapp"}
        result = transform_snapshot_spec(spec)
        assert result["componentGroup"] == "group1"
        assert "application" not in result

    def test_no_application_key(self) -> None:
        """Test no application key."""
        spec = {"componentGroup": ""}
        result = transform_snapshot_spec(spec)
        assert result["componentGroup"] is None

    def test_does_not_mutate_input(self) -> None:
        """Test does not mutate input."""
        spec = {
            "componentGroup": "",
            "application": "myapp",
            "components": [{"name": "c1"}],
        }
        original = spec.copy()
        transform_snapshot_spec(spec)
        assert spec == original


class TestResolvePipelineRef:
    """Tests for the resolve_pipeline_ref function."""

    @patch(
        "release_service_utils.tasks.managed.collect_data.collect_data.http_client.get_text"
    )
    def test_git_resolver(self, mock_get_text: MagicMock) -> None:
        """Test git resolver."""
        mock_get_text.return_value = '{"sha": "abc123"}'
        rpa = {
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {
                                "name": "url",
                                "value": "https://github.com/org1/repo1.git",
                            },
                            {"name": "revision", "value": "main"},
                            {
                                "name": "pathInRepo",
                                "value": "pipelines/release.yaml",
                            },
                        ],
                    }
                }
            }
        }
        result = resolve_pipeline_ref(rpa)
        assert result["org"] == "org1"
        assert result["repo"] == "repo1"
        assert result["revision"] == "main"
        assert result["pathinrepo"] == "pipelines/release.yaml"
        assert result["sha"] == "abc123"

    def test_non_git_resolver(self) -> None:
        """Test non git resolver."""
        rpa = {
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "cluster",
                        "params": [{"name": "name", "value": "my-pipeline"}],
                    }
                }
            }
        }
        result = resolve_pipeline_ref(rpa)
        assert result == {
            "org": "unknown",
            "repo": "unknown",
            "revision": "unknown",
            "pathinrepo": "unknown",
            "sha": "unknown",
        }

    def test_missing_pipeline_ref(self) -> None:
        """Test missing pipeline ref."""
        result = resolve_pipeline_ref({"spec": {}})
        assert all(v == "unknown" for v in result.values())

    @patch(
        "release_service_utils.tasks.managed.collect_data.collect_data.http_client.get_text"
    )
    def test_github_api_failure(self, mock_get_text: MagicMock) -> None:
        """Test github api failure."""
        mock_get_text.side_effect = Exception("network error")
        rpa = {
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {
                                "name": "url",
                                "value": "https://github.com/o/r",
                            },
                            {"name": "revision", "value": "v1"},
                            {
                                "name": "pathInRepo",
                                "value": "p.yaml",
                            },
                        ],
                    }
                }
            }
        }
        result = resolve_pipeline_ref(rpa)
        assert result["sha"] == "unknown"
        assert result["org"] == "o"
        assert result["repo"] == "r"

    @patch(
        "release_service_utils.tasks.managed.collect_data.collect_data.http_client.get_text"
    )
    def test_url_without_git_suffix(self, mock_get_text: MagicMock) -> None:
        """Test url without git suffix."""
        mock_get_text.return_value = '{"sha": "def456"}'
        rpa = {
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {
                                "name": "url",
                                "value": "https://github.com/myorg/myrepo",
                            },
                            {"name": "revision", "value": "dev"},
                            {"name": "pathInRepo", "value": "path"},
                        ],
                    }
                }
            }
        }
        result = resolve_pipeline_ref(rpa)
        assert result["org"] == "myorg"
        assert result["repo"] == "myrepo"

    @patch(
        "release_service_utils.tasks.managed.collect_data.collect_data.http_client.get_text"
    )
    def test_empty_url_returns_unknowns(self, mock_get_text: MagicMock) -> None:
        """Test git resolver with empty url defaults org and repo to unknown."""
        mock_get_text.return_value = '{"sha": "abc"}'
        rpa = {
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {"name": "url", "value": ""},
                            {"name": "revision", "value": "main"},
                            {"name": "pathInRepo", "value": "p.yaml"},
                        ],
                    }
                }
            }
        }
        result = resolve_pipeline_ref(rpa)
        assert result["org"] == "unknown"
        assert result["repo"] == "unknown"
        assert result["revision"] == "main"

    @patch(
        "release_service_utils.tasks.managed.collect_data.collect_data.http_client.get_text"
    )
    def test_empty_sha_in_response(self, mock_get_text: MagicMock) -> None:
        """Test empty sha in response."""
        mock_get_text.return_value = '{"sha": ""}'
        rpa = {
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {
                                "name": "url",
                                "value": "https://github.com/o/r",
                            },
                            {"name": "revision", "value": "v1"},
                            {"name": "pathInRepo", "value": "p"},
                        ],
                    }
                }
            }
        }
        result = resolve_pipeline_ref(rpa)
        assert result["sha"] == "unknown"

    @patch(
        "release_service_utils.tasks.managed.collect_data.collect_data.http_client.get_text"
    )
    def test_empty_sha_in_response_logs_warning(
        self,
        mock_get_text: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Log a warning when the GitHub API response has no commit SHA.

        Regression test: previously this fallback to "unknown" happened with
        no logging at all, making a real provenance-data gap invisible.
        """
        mock_get_text.return_value = '{"sha": ""}'
        rpa = {
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {"name": "url", "value": "https://github.com/o/r"},
                            {"name": "revision", "value": "v1"},
                            {"name": "pathInRepo", "value": "p"},
                        ],
                    }
                }
            }
        }
        release_logger = logging.getLogger("release")
        original_propagate = release_logger.propagate
        release_logger.propagate = True
        try:
            with caplog.at_level(logging.WARNING, logger="release"):
                result = resolve_pipeline_ref(rpa)
        finally:
            release_logger.propagate = original_propagate
        assert result["sha"] == "unknown"
        assert "did not include a commit SHA" in caplog.text


class TestCheckDataKeySources:
    """Tests for the check_data_key_sources function."""

    def test_clean_resources(self) -> None:
        """Test clean resources."""
        check_data_key_sources(
            {"spec": {"data": {"foo": "bar"}}},
            {"spec": {"data": {"foo": "bar"}}},
        )

    def test_disallowed_key_in_release(self) -> None:
        """Test disallowed key in release."""
        with pytest.raises(ValueError, match="product_id"):
            check_data_key_sources(
                {"spec": {"data": {"releaseNotes": {"product_id": 123}}}},
                {"spec": {}},
            )

    def test_disallowed_key_in_release_plan(self) -> None:
        """Test disallowed key in release plan."""
        with pytest.raises(ValueError, match="allow_custom_live_id"):
            check_data_key_sources(
                {"spec": {}},
                {"spec": {"data": {"releaseNotes": {"allow_custom_live_id": True}}}},
            )

    def test_rpa_keys_not_checked(self) -> None:
        """Test that RPA data is not subject to disallowed-key validation."""
        check_data_key_sources(
            {"spec": {}},
            {"spec": {}},
        )

    def test_no_spec_data(self) -> None:
        """Test no spec data."""
        check_data_key_sources(
            {"metadata": {"name": "x"}},
            {"metadata": {"name": "x"}},
        )

    def test_multiple_violations(self) -> None:
        """Test multiple violations."""
        with pytest.raises(ValueError, match=r"(?s)product_id.*product_name"):
            check_data_key_sources(
                {
                    "spec": {
                        "data": {
                            "releaseNotes": {
                                "product_id": 1,
                                "product_name": "x",
                            }
                        }
                    }
                },
                {"spec": {}},
            )


class TestCollect:
    """Tests for the collect function returning a CollectDataResult."""

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.get_resource_dict")
    def test_returns_result_dataclass(self, mock_get: MagicMock) -> None:
        """Test returns result dataclass."""
        mock_get.side_effect = _resource_side_effect

        result = collect(
            release="default/my-release",
            release_plan="default/my-rp",
            release_plan_admission="default/my-rpa",
            release_service_config="default/my-rsc",
            snapshot="default/my-snap",
            subdirectory="uid123",
        )

        assert isinstance(result, CollectDataResult)
        assert result.subdirectory == "uid123"
        assert result.snapshot_name == "my-snap"
        assert result.snapshot_namespace == "default"
        assert result.snapshot_build_id == "build-123"
        assert result.single_component_mode == "false"
        assert result.merged_data["foo"] == "bar"
        assert result.merged_data["one"]["two"] == "three"
        assert result.merged_data["releaseNotes"]["cves"] == [{"key": "CVE-1"}]
        assert result.snapshot_spec["componentGroup"] == "myapp"
        assert "application" not in result.snapshot_spec
        assert result.pipeline_metadata["org"] == "unknown"

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.get_resource_dict")
    def test_disallowed_key_raises(self, mock_get: MagicMock) -> None:
        """Test disallowed key raises."""

        def side_effect(
            resource_type: str,
            namespace: str,
            name: str,
        ) -> dict:
            if resource_type == "release":
                return {
                    "metadata": {"name": "r1"},
                    "spec": {"data": {"releaseNotes": {"product_id": 123}}},
                    "status": {"collectors": {}},
                }
            return _resource_side_effect(resource_type, namespace, name)

        mock_get.side_effect = side_effect

        with pytest.raises(ValueError, match="product_id"):
            collect(
                release="default/r1",
                release_plan="default/my-rp",
                release_plan_admission="default/my-rpa",
                release_service_config="default/my-rsc",
                snapshot="default/my-snap",
                subdirectory="sub",
            )

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.get_resource_dict")
    def test_single_component_mode_true(self, mock_get: MagicMock) -> None:
        """Test single component mode true."""

        def side_effect(
            resource_type: str,
            namespace: str,
            name: str,
        ) -> dict:
            if resource_type == "release":
                return {
                    "metadata": {"name": "r1"},
                    "spec": {"data": {"singleComponentMode": True}},
                    "status": {"collectors": {}},
                }
            return _resource_side_effect(resource_type, namespace, name)

        mock_get.side_effect = side_effect

        result = collect(
            release="default/r1",
            release_plan="default/my-rp",
            release_plan_admission="default/my-rpa",
            release_service_config="default/my-rsc",
            snapshot="default/my-snap",
            subdirectory="sub",
        )
        assert result.single_component_mode == "true"

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.get_resource_dict")
    def test_single_component_mode_null(self, mock_get: MagicMock) -> None:
        """Test single component mode null."""

        def side_effect(
            resource_type: str,
            namespace: str,
            name: str,
        ) -> dict:
            if resource_type == "release":
                return {
                    "metadata": {"name": "r1"},
                    "spec": {"data": {"singleComponentMode": None}},
                    "status": {"collectors": {}},
                }
            return _resource_side_effect(resource_type, namespace, name)

        mock_get.side_effect = side_effect

        result = collect(
            release="default/r1",
            release_plan="default/my-rp",
            release_plan_admission="default/my-rpa",
            release_service_config="default/my-rsc",
            snapshot="default/my-snap",
            subdirectory="sub",
        )
        assert result.single_component_mode == "false"


class TestWriteOutputs:
    """Tests for the write_outputs function."""

    def _make_result_paths(self, tmp_path: Path) -> dict[str, Path]:
        results = tmp_path / "results"
        results.mkdir()
        keys = [
            "release",
            "releasePlan",
            "releasePlanAdmission",
            "releaseServiceConfig",
            "snapshotSpec",
            "data",
            "resultsDir",
            "singleComponentMode",
            "snapshotName",
            "snapshotNamespace",
            "snapshotBuildId",
            "releasePipelineMetadata",
            "subdirectory",
        ]
        return {k: results / k for k in keys}

    def test_writes_all_files(self, tmp_path: Path) -> None:
        """Test writes all files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_paths = self._make_result_paths(tmp_path)

        result = CollectDataResult(
            subdirectory="uid123",
            release={"metadata": {"name": "r1"}, "spec": {}},
            release_plan={"metadata": {"name": "rp1"}, "spec": {}},
            release_plan_admission={"metadata": {"name": "rpa1"}, "spec": {}},
            release_service_config={"metadata": {"name": "rsc1"}, "spec": {}},
            snapshot_spec={
                "componentGroup": "myapp",
                "components": [],
            },
            merged_data={"key": "value"},
            pipeline_metadata={
                "org": "o",
                "repo": "r",
                "revision": "v",
                "pathinrepo": "p",
                "sha": "s",
            },
            single_component_mode="false",
            snapshot_name="my-snap",
            snapshot_namespace="default",
            snapshot_build_id="build-123",
        )

        write_outputs(result, data_dir, result_paths)

        assert result_paths["subdirectory"].read_text() == "uid123"
        assert result_paths["resultsDir"].read_text() == "uid123/results"
        assert result_paths["snapshotName"].read_text() == "my-snap"
        assert result_paths["snapshotNamespace"].read_text() == "default"
        assert result_paths["snapshotBuildId"].read_text() == "build-123"
        assert result_paths["singleComponentMode"].read_text() == "false"

        assert (data_dir / "uid123" / "release.json").exists()
        assert (data_dir / "uid123" / "data.json").exists()
        assert (data_dir / "uid123" / "snapshot_spec.json").exists()
        assert (data_dir / "uid123" / "results").is_dir()

        data = json.loads((data_dir / "uid123" / "data.json").read_text())
        assert data == {"key": "value"}

    def test_absolute_subdirectory_rejected(self, tmp_path: Path) -> None:
        """Test that an absolute subdirectory path is rejected."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_paths = self._make_result_paths(tmp_path)

        result = CollectDataResult(
            subdirectory="/etc/evil",
            release={"spec": {}},
            release_plan={"spec": {}},
            release_plan_admission={"spec": {}},
            release_service_config={"spec": {}},
            snapshot_spec={},
            merged_data={},
            pipeline_metadata={
                "org": "u",
                "repo": "u",
                "revision": "u",
                "pathinrepo": "u",
                "sha": "u",
            },
            single_component_mode="false",
            snapshot_name="s",
            snapshot_namespace="ns",
            snapshot_build_id="",
        )

        with pytest.raises(ValueError, match="must be relative"):
            write_outputs(result, data_dir, result_paths)

    def test_traversal_subdirectory_rejected(self, tmp_path: Path) -> None:
        """Test that a subdirectory with '..' traversal is rejected."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_paths = self._make_result_paths(tmp_path)

        result = CollectDataResult(
            subdirectory="../../etc",
            release={"spec": {}},
            release_plan={"spec": {}},
            release_plan_admission={"spec": {}},
            release_service_config={"spec": {}},
            snapshot_spec={},
            merged_data={},
            pipeline_metadata={
                "org": "u",
                "repo": "u",
                "revision": "u",
                "pathinrepo": "u",
                "sha": "u",
            },
            single_component_mode="false",
            snapshot_name="s",
            snapshot_namespace="ns",
            snapshot_build_id="",
        )

        with pytest.raises(ValueError, match="must stay under"):
            write_outputs(result, data_dir, result_paths)

    def test_empty_subdirectory(self, tmp_path: Path) -> None:
        """Test empty subdirectory."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_paths = self._make_result_paths(tmp_path)

        result = CollectDataResult(
            subdirectory="",
            release={"spec": {}},
            release_plan={"spec": {}},
            release_plan_admission={"spec": {}},
            release_service_config={"spec": {}},
            snapshot_spec={},
            merged_data={},
            pipeline_metadata={
                "org": "u",
                "repo": "u",
                "revision": "u",
                "pathinrepo": "u",
                "sha": "u",
            },
            single_component_mode="false",
            snapshot_name="s",
            snapshot_namespace="ns",
            snapshot_build_id="",
        )

        write_outputs(result, data_dir, result_paths)

        assert result_paths["resultsDir"].read_text() == "results"
        assert (data_dir / "release.json").exists()
        assert (data_dir / "data.json").exists()


class TestRun:
    """Tests for the run function orchestrating the full workflow."""

    def _make_result_paths(self, tmp_path: Path) -> dict[str, Path]:
        results = tmp_path / "results"
        results.mkdir()
        keys = [
            "release",
            "releasePlan",
            "releasePlanAdmission",
            "releaseServiceConfig",
            "snapshotSpec",
            "data",
            "resultsDir",
            "singleComponentMode",
            "snapshotName",
            "snapshotNamespace",
            "snapshotBuildId",
            "releasePipelineMetadata",
            "subdirectory",
        ]
        return {k: results / k for k in keys}

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.get_resource_dict")
    def test_full_run(self, mock_get: MagicMock, tmp_path: Path) -> None:
        """Test full run."""
        mock_get.side_effect = _resource_side_effect
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_paths = self._make_result_paths(tmp_path)

        run(
            release="default/my-release",
            release_plan="default/my-rp",
            release_plan_admission="default/my-rpa",
            release_service_config="default/my-rsc",
            snapshot="default/my-snap",
            subdirectory="uid123",
            data_dir=data_dir,
            result_paths=result_paths,
        )

        assert result_paths["subdirectory"].read_text() == "uid123"
        assert result_paths["resultsDir"].read_text() == "uid123/results"
        assert result_paths["snapshotName"].read_text() == "my-snap"
        assert result_paths["snapshotNamespace"].read_text() == "default"
        assert result_paths["snapshotBuildId"].read_text() == "build-123"

        data = json.loads((data_dir / "uid123" / "data.json").read_text())
        assert data["foo"] == "bar"
        assert data["one"]["two"] == "three"
        assert data["releaseNotes"]["cves"] == [{"key": "CVE-1"}]

        snap = json.loads((data_dir / "uid123" / "snapshot_spec.json").read_text())
        assert snap["componentGroup"] == "myapp"
        assert "application" not in snap

        metadata = json.loads(result_paths["releasePipelineMetadata"].read_text())
        assert metadata["org"] == "unknown"

        assert result_paths["singleComponentMode"].read_text() == "false"

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.get_resource_dict")
    def test_run_empty_subdirectory(self, mock_get: MagicMock, tmp_path: Path) -> None:
        """Test run empty subdirectory."""
        mock_get.side_effect = _resource_side_effect
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_paths = self._make_result_paths(tmp_path)

        run(
            release="default/my-release",
            release_plan="default/my-rp",
            release_plan_admission="default/my-rpa",
            release_service_config="default/my-rsc",
            snapshot="default/my-snap",
            subdirectory="",
            data_dir=data_dir,
            result_paths=result_paths,
        )

        assert result_paths["resultsDir"].read_text() == "results"
        assert (data_dir / "release.json").exists()
        assert (data_dir / "data.json").exists()

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.get_resource_dict")
    @patch("release_service_utils.tasks.managed.collect_data.collect_data.setup_ca_cert")
    def test_run_calls_setup_ca_cert(
        self, mock_ca: MagicMock, mock_get: MagicMock, tmp_path: Path
    ) -> None:
        """Test run calls setup ca cert."""
        mock_get.side_effect = _resource_side_effect
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_paths = self._make_result_paths(tmp_path)

        run(
            release="default/my-release",
            release_plan="default/my-rp",
            release_plan_admission="default/my-rpa",
            release_service_config="default/my-rsc",
            snapshot="default/my-snap",
            subdirectory="sub",
            data_dir=data_dir,
            result_paths=result_paths,
        )
        mock_ca.assert_called_once()


class TestMain:
    """Tests for the main() entrypoint."""

    @staticmethod
    def _set_env(
        monkeypatch: pytest.MonkeyPatch,
        env: dict[str, str],
    ) -> None:
        """Clear all env vars and set *env* via monkeypatch."""
        for key in list(os.environ):
            monkeypatch.delenv(key)
        for key, value in env.items():
            monkeypatch.setenv(key, value)

    def _env_vars(self, tmp_path: Path) -> dict[str, str]:
        results = tmp_path / "results"
        results.mkdir()
        env = {
            "RELEASE": "default/my-release",
            "RELEASE_PLAN": "default/my-rp",
            "RELEASE_PLAN_ADMISSION": "default/my-rpa",
            "RELEASE_SERVICE_CONFIG": "default/my-rsc",
            "SNAPSHOT": "default/my-snap",
            "PARAM_SUBDIRECTORY": "sub",
            "PARAM_DATA_DIR": str(tmp_path / "data"),
        }
        result_names = [
            "RESULT_RELEASE",
            "RESULT_RELEASE_PLAN",
            "RESULT_RELEASE_PLAN_ADMISSION",
            "RESULT_RELEASE_SERVICE_CONFIG",
            "RESULT_SNAPSHOT_SPEC",
            "RESULT_DATA",
            "RESULT_RESULTS_DIR",
            "RESULT_SINGLE_COMPONENT_MODE",
            "RESULT_SNAPSHOT_NAME",
            "RESULT_SNAPSHOT_NAMESPACE",
            "RESULT_SNAPSHOT_BUILD_ID",
            "RESULT_RELEASE_PIPELINE_METADATA",
            "RESULT_SUBDIRECTORY",
        ]
        for name in result_names:
            path = results / name
            env[name] = str(path)
        (tmp_path / "data").mkdir()
        return env

    @patch("release_service_utils.tasks.managed.collect_data.collect_data.run")
    def test_main_calls_run(
        self,
        mock_run: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test main calls run."""
        self._set_env(monkeypatch, self._env_vars(tmp_path))
        result = main()
        assert result == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["release"] == "default/my-release"
        assert call_kwargs["subdirectory"] == "sub"

    def test_main_missing_env_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test main missing env raises."""
        self._set_env(monkeypatch, {})
        with pytest.raises(SystemExit):
            main()

    def test_dunder_main_block(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exercise the ``if __name__ == "__main__"`` block."""
        self._set_env(monkeypatch, self._env_vars(tmp_path))
        with patch("get_resource.get_resource_dict", return_value={}):
            with pytest.raises(SystemExit) as exc_info:
                import runpy

                runpy.run_module(
                    "release_service_utils.tasks.managed.collect_data.collect_data",
                    run_name="__main__",
                )
            assert exc_info.value.code == 0
