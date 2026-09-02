"""Test advisory image filtering and its InternalRequest orchestration."""

from __future__ import annotations

import base64
import gzip
import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

from filter_already_released_advisory_images_managed import (
    filter_already_released_advisory_images_managed as task,
)
import pytest


def _component(
    name: str,
    image: str = "reg.io/img@sha256:abc",
    repositories: list[dict] | None = None,
) -> dict:
    """Build a snapshot component dict."""
    c: dict = {"name": name, "containerImage": image}
    if repositories is not None:
        c["repositories"] = repositories
    return c


def _snapshot(components: list[dict]) -> dict:
    """Build a minimal snapshot dict."""
    return {"application": "test", "components": components}


def _config(tmp_path: Path, **overrides: Any) -> task.FilterConfig:
    """Build a FilterConfig with sensible test defaults."""
    defaults = dict(
        snapshot_file=tmp_path / "snapshot.json",
        rpa_file=tmp_path / "rpa.json",
        data_file=tmp_path / "data.json",
        results_file=tmp_path / "results.json",
        pipeline_run_uid="uid-1",
        task_git_url="http://localhost",
        task_git_revision="main",
        synchronously=True,
    )
    defaults.update(overrides)
    return task.FilterConfig(**defaults)


def _result_paths(tmp_path: Path) -> task.ResultPaths:
    """Build ResultPaths pointing at fresh files under tmp_path."""
    return task.ResultPaths(
        result=tmp_path / "result",
        skip_release=tmp_path / "skip_release",
        environment=tmp_path / "environment",
        latest_advisory_url=tmp_path / "latest_advisory_url",
        latest_advisory_internal_url=tmp_path / "latest_advisory_internal_url",
    )


def _gzip_b64(obj: Any) -> str:
    """Gzip+base64 encode a JSON-serializable object, matching IR result encoding."""
    return base64.b64encode(gzip.compress(json.dumps(obj).encode("utf-8"))).decode("ascii")


class TestSyncFromParam:
    """Test _sync_from_param Tekton bool-string parsing."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("true", True),
            ("True", True),
            (" true ", True),
            ("false", False),
            ("False", False),
            ("", False),
        ],
    )
    def test_parses_tekton_bool_string(self, value: str, expected: bool) -> None:
        """Case/whitespace-insensitive parsing of Tekton's string-typed bool param."""
        assert task._sync_from_param(value) is expected


class TestRepoUrlCategory:
    """Test _repo_url_category classification."""

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("quay.io/redhat-pending/foo", "pending"),
            ("quay.io/rh-flatpaks-stage/foo", "pending"),
            ("quay.io/redhat-prod/foo", "prod"),
            ("quay.io/rh-flatpaks-prod/foo", "prod"),
            ("quay.io/some-other-org/foo", "orphan"),
        ],
    )
    def test_classification(self, url: str, expected: str) -> None:
        """Each URL pattern maps to the expected category."""
        assert task._repo_url_category(url) == expected


class TestDetermineEnvironment:
    """Test determine_environment across repository classification scenarios."""

    def test_pending_only(self) -> None:
        """All-pending repositories select the stage secret."""
        snapshot = _snapshot(
            [_component("c1", repositories=[{"url": "quay.io/redhat-pending/foo"}])]
        )
        assert task.determine_environment(snapshot) == (
            "stage",
            "create-advisory-stage-secret",
        )

    def test_prod_only(self) -> None:
        """All-production repositories select the production secret."""
        snapshot = _snapshot(
            [_component("c1", repositories=[{"url": "quay.io/redhat-prod/foo"}])]
        )
        assert task.determine_environment(snapshot) == (
            "production",
            "create-advisory-prod-secret",
        )

    def test_mixed_pending_and_prod_raises(self) -> None:
        """Mixing pending and production repositories raises ValueError."""
        snapshot = _snapshot(
            [
                _component(
                    "c1",
                    repositories=[
                        {"url": "quay.io/redhat-pending/foo"},
                        {"url": "quay.io/redhat-prod/bar"},
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="cannot publish to both"):
            task.determine_environment(snapshot)

    def test_orphan_raises(self) -> None:
        """An orphaned repository URL raises ValueError."""
        snapshot = _snapshot(
            [
                _component(
                    "c1",
                    repositories=[
                        {"url": "quay.io/redhat-pending/foo"},
                        {"url": "quay.io/some-other-org/bar"},
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="must publish to either"):
            task.determine_environment(snapshot)

    def test_no_repositories_raises(self) -> None:
        """No mapped repositories at all raises ValueError."""
        snapshot = _snapshot([_component("c1")])
        with pytest.raises(ValueError, match="must publish to either"):
            task.determine_environment(snapshot)

    def test_empty_and_missing_urls_ignored(self) -> None:
        """Empty-string and missing url fields are ignored, not treated as orphan."""
        snapshot = _snapshot(
            [
                _component(
                    "c1",
                    repositories=[
                        {"url": ""},
                        {},
                        {"url": "quay.io/redhat-prod/foo"},
                    ],
                )
            ]
        )
        assert task.determine_environment(snapshot) == (
            "production",
            "create-advisory-prod-secret",
        )


class TestResolveImageArchitectures:
    """Test _resolve_image_architectures output parsing."""

    def test_parses_multiple_lines(self) -> None:
        """Each non-blank output line is parsed as a separate JSON object."""
        output = (
            json.dumps({"digest": "sha256:a", "platform": {"architecture": "amd64"}})
            + "\n"
            + json.dumps({"digest": "sha256:b", "platform": {"architecture": "ppc64le"}})
        )
        with patch.object(
            task.subprocess_cmd,
            "run_cmd_text",
            return_value=output,
        ):
            result = task._resolve_image_architectures("reg.io/img@sha256:abc")
        assert result == [
            {"digest": "sha256:a", "platform": {"architecture": "amd64"}},
            {"digest": "sha256:b", "platform": {"architecture": "ppc64le"}},
        ]

    def test_propagates_subprocess_error(self) -> None:
        """A failing get-image-architectures call propagates CalledProcessError."""
        with patch.object(
            task.subprocess_cmd,
            "run_cmd_text",
            side_effect=subprocess.CalledProcessError(1, "get-image-architectures"),
        ):
            with pytest.raises(subprocess.CalledProcessError):
                task._resolve_image_architectures("reg.io/img@sha256:abc")


class TestTransformComponent:
    """Test transform_component entry expansion."""

    def test_one_entry_per_repo_and_arch(self) -> None:
        """A component with 2 repos and 2 architectures yields 4 entries."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            repositories=[
                {"rh-registry-repo": "registry.io/repo-a", "tags": ["v1"]},
                {"rh-registry-repo": "registry.io/repo-b", "tags": ["v2"]},
            ],
        )
        with patch.object(
            task,
            "_resolve_image_architectures",
            return_value=[{"digest": "sha256:d1"}, {"digest": "sha256:d2"}],
        ):
            entries = task.transform_component(comp)
        assert len(entries) == 4
        assert entries[0] == {
            "name": "c1",
            "containerImage": "registry.io/repo-a@sha256:d1",
            "tags": ["v1"],
            "repository": "registry.io/repo-a",
        }

    def test_no_repositories_yields_no_entries(self) -> None:
        """A component without repositories yields an empty list."""
        comp = _component("c1")
        with patch.object(
            task,
            "_resolve_image_architectures",
            return_value=[{"digest": "sha256:d1"}],
        ):
            assert task.transform_component(comp) == []


class TestTransformSnapshot:
    """Test transform_snapshot aggregation and failure handling."""

    def test_all_succeed(self) -> None:
        """Every component's entries are aggregated; no failures recorded."""
        snapshot = _snapshot(
            [
                _component("c1", repositories=[{"rh-registry-repo": "r.io/a"}]),
                _component("c2", repositories=[{"rh-registry-repo": "r.io/b"}]),
            ]
        )
        with patch.object(
            task,
            "transform_component",
            side_effect=[[{"name": "c1"}], [{"name": "c2"}]],
        ):
            entries, failed = task.transform_snapshot(snapshot)
        assert entries == [{"name": "c1"}, {"name": "c2"}]
        assert failed == []

    def test_failure_is_recorded_not_raised(self) -> None:
        """A component whose transform raises is recorded as failed, not fatal."""
        snapshot = _snapshot(
            [
                _component("c1", repositories=[{"rh-registry-repo": "r.io/a"}]),
                _component("c2", repositories=[{"rh-registry-repo": "r.io/b"}]),
            ]
        )
        with patch.object(
            task,
            "transform_component",
            side_effect=[
                subprocess.CalledProcessError(1, "get-image-architectures"),
                [{"name": "c2"}],
            ],
        ):
            entries, failed = task.transform_snapshot(snapshot)
        assert entries == [{"name": "c2"}]
        assert failed == ["c1"]


class TestCheckSkipFilter:
    """Test check_skip_filter parsing of data.json."""

    def test_missing_file_returns_false(self, tmp_path: Path) -> None:
        """A missing data file means skipFilter is not set."""
        assert task.check_skip_filter(tmp_path / "missing.json") is False

    def test_boolean_true(self, tmp_path: Path) -> None:
        """Boolean true triggers skip."""
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps({"skipFilter": True}), encoding="utf-8")
        assert task.check_skip_filter(data_file) is True

    def test_string_true(self, tmp_path: Path) -> None:
        """String "true" also triggers skip (matches jq -r string coercion)."""
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps({"skipFilter": "true"}), encoding="utf-8")
        assert task.check_skip_filter(data_file) is True

    def test_false_does_not_skip(self, tmp_path: Path) -> None:
        """Boolean false does not trigger skip."""
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps({"skipFilter": False}), encoding="utf-8")
        assert task.check_skip_filter(data_file) is False

    def test_missing_key_does_not_skip(self, tmp_path: Path) -> None:
        """A data.json without skipFilter does not trigger skip."""
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps({}), encoding="utf-8")
        assert task.check_skip_filter(data_file) is False


class TestRunFilterRequest:
    """Test run_filter_request InternalRequest submission, wait, and result fetch."""

    def test_calls_create_with_expected_params(self, tmp_path: Path) -> None:
        """internal_request.create is called with expected pipeline/params/labels."""
        config = _config(tmp_path)
        with (
            patch.object(
                task.internal_request,
                "create",
                return_value="ir-1",
            ) as mock_create,
            patch.object(
                task.internal_request,
                "fetch_results",
                return_value={"result": "Success", "advisory_url": "http://x"},
            ),
        ):
            result = task.run_filter_request(
                [{"name": "c1"}],
                origin="my-origin",
                advisory_secret_name="create-advisory-stage-secret",
                config=config,
            )
        assert result == {"result": "Success", "advisory_url": "http://x"}
        mock_create.assert_called_once()
        call = mock_create.call_args
        assert call.args[0] == "filter-already-released-advisory-images"
        assert call.kwargs["params"]["origin"] == "my-origin"
        assert call.kwargs["params"]["advisory_secret_name"] == "create-advisory-stage-secret"
        assert call.kwargs["params"]["internalRequestPipelineRunName"] == "uid-1"
        assert call.kwargs["labels"] == {
            "internal-services.appstudio.openshift.io/pipelinerun-uid": "uid-1"
        }
        assert call.kwargs["sync"] is True

    def test_passes_through_synchronously_false(self, tmp_path: Path) -> None:
        """config.synchronously=False is forwarded as internal_request.create(sync=False)."""
        config = _config(tmp_path, synchronously=False)
        with (
            patch.object(
                task.internal_request,
                "create",
                return_value="ir-1",
            ) as mock_create,
            patch.object(
                task.internal_request,
                "fetch_results",
                return_value={"result": "Success"},
            ),
        ):
            task.run_filter_request(
                [{"name": "c1"}], origin="o", advisory_secret_name="s", config=config
            )
        assert mock_create.call_args.kwargs["sync"] is False

    def test_encodes_entries_as_gzip_base64(self, tmp_path: Path) -> None:
        """Verify transformedSnapshot param is the gzip+base64 of the entries list."""
        config = _config(tmp_path)
        entries = [{"name": "c1"}]
        with (
            patch.object(
                task.internal_request,
                "create",
                return_value="ir-1",
            ) as mock_create,
            patch.object(
                task.internal_request,
                "fetch_results",
                return_value={"result": "Success"},
            ),
        ):
            task.run_filter_request(
                entries, origin="o", advisory_secret_name="s", config=config
            )
        transformed = mock_create.call_args.kwargs["params"]["transformedSnapshot"]
        decoded = json.loads(gzip.decompress(base64.b64decode(transformed)))
        assert decoded == entries

    def test_non_success_result_raises(self, tmp_path: Path) -> None:
        """A non-Success result field raises RuntimeError."""
        config = _config(tmp_path)
        with (
            patch.object(
                task.internal_request,
                "create",
                return_value="ir-1",
            ),
            patch.object(
                task.internal_request,
                "fetch_results",
                return_value={"result": "Failed"},
            ),
        ):
            with pytest.raises(RuntimeError, match="Filtering failed"):
                task.run_filter_request(
                    [], origin="o", advisory_secret_name="s", config=config
                )


class TestDecodeUnreleasedComponents:
    """Test decode_unreleased_components gzip+base64 decoding."""

    def test_round_trip(self) -> None:
        """A gzip+base64-encoded list round-trips correctly."""
        raw = _gzip_b64(["c1", "c2"])
        assert task.decode_unreleased_components(raw) == ["c1", "c2"]

    def test_empty_raises(self) -> None:
        """An empty string raises RuntimeError."""
        with pytest.raises(RuntimeError, match="No unreleased components"):
            task.decode_unreleased_components("")


class TestFilterSnapshot:
    """Test filter_snapshot component retention."""

    def test_keeps_only_unreleased_names(self) -> None:
        """Only components whose name is in the unreleased set are kept."""
        snapshot = _snapshot([_component("c1"), _component("c2"), _component("c3")])
        filtered = task.filter_snapshot(snapshot, {"c1", "c3"})
        assert [c["name"] for c in filtered["components"]] == ["c1", "c3"]

    def test_preserves_non_component_keys(self) -> None:
        """Non-component snapshot keys survive filtering."""
        snapshot = _snapshot([_component("c1")])
        snapshot["application"] = "my-app"
        filtered = task.filter_snapshot(snapshot, {"c1"})
        assert filtered["application"] == "my-app"

    def test_empty_unreleased_set_empties_components(self) -> None:
        """An empty unreleased set filters out every component."""
        snapshot = _snapshot([_component("c1"), _component("c2")])
        filtered = task.filter_snapshot(snapshot, set())
        assert filtered["components"] == []


class TestRun:
    """Test the run() orchestration end to end, mocking IR and arch resolution."""

    @pytest.fixture
    def config(self, tmp_path: Path) -> task.FilterConfig:
        """Build a fresh FilterConfig pointing at tmp_path, for every test."""
        return _config(tmp_path)

    @pytest.fixture
    def results(self, tmp_path: Path) -> task.ResultPaths:
        """Build a fresh ResultPaths pointing at tmp_path, for every test."""
        return _result_paths(tmp_path)

    def _write_inputs(
        self,
        config: task.FilterConfig,
        *,
        components: list[dict],
        origin: str = "my-origin",
        data: dict | None = None,
    ) -> None:
        """Write snapshot, RPA, and (optionally) data.json fixture files."""
        config.snapshot_file.write_text(json.dumps(_snapshot(components)), encoding="utf-8")
        config.rpa_file.write_text(json.dumps({"spec": {"origin": origin}}), encoding="utf-8")
        if data is not None:
            config.data_file.write_text(json.dumps(data), encoding="utf-8")

    def test_skip_filter_short_circuits(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """skipFilter=true writes result/skip_release and returns before any IR call."""
        self._write_inputs(
            config,
            components=[_component("c1", repositories=[{"url": "quay.io/redhat-prod/foo"}])],
            data={"skipFilter": True},
        )
        with (
            patch.object(
                task,
                "transform_snapshot",
                return_value=([{"name": "c1"}], []),
            ),
            patch.object(task, "run_filter_request") as mock_run_filter_request,
        ):
            task.run(config, results)
        mock_run_filter_request.assert_not_called()
        assert results.result.read_text(encoding="utf-8") == "Success"
        assert results.skip_release.read_text(encoding="utf-8") == "false"
        assert results.environment.read_text(encoding="utf-8") == "production"

    def test_all_components_released(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """All components released: empty snapshot, skip_release=true, results file written."""
        self._write_inputs(
            config,
            components=[_component("c1", repositories=[{"url": "quay.io/redhat-prod/foo"}])],
        )
        with (
            patch.object(
                task,
                "transform_snapshot",
                return_value=([{"name": "c1"}], []),
            ),
            patch.object(
                task,
                "run_filter_request",
                return_value={
                    "advisory_url": "http://advisory",
                    "advisory_internal_url": "http://internal",
                    "unreleased_components": "",
                },
            ),
            patch.object(
                task,
                "decode_unreleased_components",
                return_value=[],
            ),
        ):
            task.run(config, results)

        assert results.result.read_text(encoding="utf-8") == "Success"
        assert results.skip_release.read_text(encoding="utf-8") == "true"
        assert results.latest_advisory_url.read_text(encoding="utf-8") == "http://advisory"
        assert (
            results.latest_advisory_internal_url.read_text(encoding="utf-8")
            == "http://internal"
        )
        written_snapshot = json.loads(config.snapshot_file.read_text(encoding="utf-8"))
        assert written_snapshot["components"] == []
        results_file = json.loads(config.results_file.read_text(encoding="utf-8"))
        assert results_file == {
            "advisory": {"url": "http://advisory", "internal_url": "http://internal"}
        }

    def test_all_components_released_missing_advisory_url_raises(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """A Success result missing advisory_url fails loudly instead of writing "" ."""
        self._write_inputs(
            config,
            components=[_component("c1", repositories=[{"url": "quay.io/redhat-prod/foo"}])],
        )
        with (
            patch.object(
                task,
                "transform_snapshot",
                return_value=([{"name": "c1"}], []),
            ),
            patch.object(
                task,
                "run_filter_request",
                return_value={"unreleased_components": ""},
            ),
            patch.object(
                task,
                "decode_unreleased_components",
                return_value=[],
            ),
        ):
            with pytest.raises(KeyError, match="advisory_url"):
                task.run(config, results)

    def test_partial_components_released(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """Some components still need release: filtered snapshot, skip_release=false."""
        self._write_inputs(
            config,
            components=[
                _component("released", repositories=[{"url": "quay.io/redhat-prod/foo"}]),
                _component("kept", repositories=[{"url": "quay.io/redhat-prod/bar"}]),
            ],
        )
        with (
            patch.object(
                task,
                "transform_snapshot",
                return_value=([{"name": "released"}, {"name": "kept"}], []),
            ),
            patch.object(
                task,
                "run_filter_request",
                return_value={
                    "advisory_url": "http://advisory",
                    "advisory_internal_url": "http://internal",
                    "unreleased_components": "encoded",
                },
            ),
            patch.object(
                task,
                "decode_unreleased_components",
                return_value=["kept"],
            ),
        ):
            task.run(config, results)

        assert results.skip_release.read_text(encoding="utf-8") == "false"
        assert results.latest_advisory_url.read_text(encoding="utf-8") == ""
        assert results.latest_advisory_internal_url.read_text(encoding="utf-8") == ""
        written_snapshot = json.loads(config.snapshot_file.read_text(encoding="utf-8"))
        assert [c["name"] for c in written_snapshot["components"]] == ["kept"]
        assert not config.results_file.exists()

    def test_arch_resolution_failures_folded_into_unreleased(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """Components whose arch resolution failed are kept even if IR omits them."""
        self._write_inputs(
            config,
            components=[
                _component("failed-arch", repositories=[{"url": "quay.io/redhat-prod/foo"}]),
                _component("normal", repositories=[{"url": "quay.io/redhat-prod/bar"}]),
            ],
        )
        with (
            patch.object(
                task,
                "transform_snapshot",
                return_value=([{"name": "normal"}], ["failed-arch"]),
            ),
            patch.object(
                task,
                "run_filter_request",
                return_value={"unreleased_components": "encoded"},
            ),
            patch.object(
                task,
                "decode_unreleased_components",
                return_value=[],
            ),
        ):
            task.run(config, results)

        written_snapshot = json.loads(config.snapshot_file.read_text(encoding="utf-8"))
        assert [c["name"] for c in written_snapshot["components"]] == ["failed-arch"]

    def test_empty_origin_raises(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """An empty origin in the ReleasePlanAdmission raises ValueError."""
        self._write_inputs(
            config,
            components=[_component("c1", repositories=[{"url": "quay.io/redhat-prod/foo"}])],
            origin="",
        )
        with patch.object(
            task,
            "transform_snapshot",
            return_value=([{"name": "c1"}], []),
        ):
            with pytest.raises(ValueError, match="origin"):
                task.run(config, results)

    def test_missing_origin_key_raises(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """A ReleasePlanAdmission missing spec.origin raises KeyError naturally."""
        config.snapshot_file.write_text(
            json.dumps(_snapshot([_component("c1")])), encoding="utf-8"
        )
        config.rpa_file.write_text(json.dumps({"spec": {}}), encoding="utf-8")
        with patch.object(
            task,
            "transform_snapshot",
            return_value=([], []),
        ):
            with pytest.raises(KeyError):
                task.run(config, results)

    def test_missing_snapshot_raises(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """A missing snapshot file raises FileNotFoundError naturally."""
        with pytest.raises(FileNotFoundError):
            task.run(config, results)

    def test_missing_rpa_raises(
        self, config: task.FilterConfig, results: task.ResultPaths
    ) -> None:
        """A missing ReleasePlanAdmission file raises FileNotFoundError naturally."""
        config.snapshot_file.write_text(
            json.dumps(_snapshot([_component("c1")])), encoding="utf-8"
        )
        with (
            patch.object(
                task,
                "transform_snapshot",
                return_value=([], []),
            ),
            pytest.raises(FileNotFoundError),
        ):
            task.run(config, results)


class TestMain:
    """Test the main() entry point's environment variable wiring."""

    def _set_env(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Set every environment variable main() requires."""
        env = {
            "RESULT_RESULT": str(tmp_path / "result"),
            "RESULT_SKIP_RELEASE": str(tmp_path / "skip_release"),
            "RESULT_ENVIRONMENT": str(tmp_path / "environment"),
            "RESULT_LATEST_ADVISORY_URL": str(tmp_path / "latest_advisory_url"),
            "RESULT_LATEST_ADVISORY_INTERNAL_URL": str(
                tmp_path / "latest_advisory_internal_url"
            ),
            "SNAPSHOT_FILE": str(tmp_path / "snapshot.json"),
            "RPA_FILE": str(tmp_path / "rpa.json"),
            "DATA_FILE": str(tmp_path / "data.json"),
            "RESULTS_FILE": str(tmp_path / "results.json"),
            "PIPELINE_RUN_UID": "uid-1",
            "TASK_GIT_URL": "http://localhost",
            "TASK_GIT_REVISION": "main",
            "SYNCHRONOUSLY": "true",
        }
        for k, v in env.items():
            monkeypatch.setenv(k, v)

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() wires config/results from env vars and returns 0."""
        self._set_env(monkeypatch, tmp_path)
        with patch.object(task, "run") as mock_run:
            assert task.main() == 0
        config, results = mock_run.call_args.args
        assert config.snapshot_file == tmp_path / "snapshot.json"
        assert config.pipeline_run_uid == "uid-1"
        assert config.synchronously is True
        assert results.result == tmp_path / "result"

    def test_synchronously_false(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SYNCHRONOUSLY=false is parsed to config.synchronously=False."""
        self._set_env(monkeypatch, tmp_path)
        monkeypatch.setenv("SYNCHRONOUSLY", "false")
        with patch.object(task, "run") as mock_run:
            assert task.main() == 0
        config, _results = mock_run.call_args.args
        assert config.synchronously is False

    def test_missing_env_var_exits(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A missing required env var raises SystemExit."""
        self._set_env(monkeypatch, tmp_path)
        monkeypatch.delenv("PIPELINE_RUN_UID", raising=False)
        with pytest.raises(SystemExit):
            task.main()
