"""Tests for `set_advisory_severity`."""

from __future__ import annotations

import json
import runpy
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from release_service_utils.helpers import file as file_helper
from release_service_utils.helpers.internal_request import (
    PIPELINERUN_UID_LABEL,
    SPAWN_OVERHEAD_SECONDS,
    InternalRequestWaitError,
    seconds_to_duration,
)
from release_service_utils.tasks.managed import set_advisory_severity

TASK = "release_service_utils.tasks.managed.set_advisory_severity"


def _write_data(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _image(cves: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a release-notes image row."""
    row: dict[str, Any] = {"containerImage": "foo"}
    if cves is not None:
        row["cves"] = {"fixed": cves}
    return row


def _rhsa_images(*cve_ids: str) -> dict[str, Any]:
    """Build RHSA data with one image per CVE id."""
    images = [_image({cve: {"components": ["pkg:rpm/foo"]}}) for cve in cve_ids]
    return {"releaseNotes": {"type": "RHSA", "content": {"images": images}}}


def _run(
    data_file: Path,
    *,
    pipeline_run_uid: str = "uid",
    request_timeout: int = 7200,
    task_git_url: str = "https://example.test/catalog",
    task_git_revision: str = "main",
) -> None:
    """Call ``run`` with default InternalRequest parameters."""
    set_advisory_severity.run(
        data_file,
        pipeline_run_uid=pipeline_run_uid,
        request_timeout=request_timeout,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
    )


class TestCountFixedCves:
    """Test count_fixed_cves."""

    def test_counts_images_and_artifacts(self) -> None:
        """Sum fixed CVE map sizes from images and artifacts."""
        data = {
            "releaseNotes": {
                "content": {
                    "images": [_image({"CVE-1": {}, "CVE-2": {}})],
                    "artifacts": [_image({"CVE-3": {}})],
                },
            },
        }
        assert set_advisory_severity.count_fixed_cves(data) == 3

    def test_counts_list_fixed_cves(self) -> None:
        """Count list-shaped ``cves.fixed`` values."""
        data = {
            "releaseNotes": {
                "content": {"images": [{"cves": {"fixed": ["CVE-1", "CVE-2"]}}]},
            },
        }
        assert set_advisory_severity.count_fixed_cves(data) == 2

    def test_returns_zero_when_missing_or_malformed(self) -> None:
        """Return 0 when notes, content, or rows cannot yield CVEs."""
        assert set_advisory_severity.count_fixed_cves({}) == 0
        assert set_advisory_severity.count_fixed_cves({"releaseNotes": []}) == 0
        assert (
            set_advisory_severity.count_fixed_cves({"releaseNotes": {"content": "nope"}}) == 0
        )
        assert (
            set_advisory_severity.count_fixed_cves(
                {"releaseNotes": {"content": {"images": "nope"}}}
            )
            == 0
        )
        assert (
            set_advisory_severity.count_fixed_cves(
                {"releaseNotes": {"content": {"images": ["skip", {}, {"cves": "x"}]}}}
            )
            == 0
        )
        assert (
            set_advisory_severity.count_fixed_cves(
                {
                    "releaseNotes": {
                        "content": {
                            "images": [{"cves": {}}, {"cves": {"fixed": "nope"}}],
                        },
                    },
                }
            )
            == 0
        )


class TestRun:
    """Test the run() orchestrator."""

    def test_missing_data_file(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when data file is missing."""
        with pytest.raises(FileNotFoundError):
            _run(tmp_path / "missing.json")

    def test_no_release_notes(self, tmp_path: Path) -> None:
        """Succeed without calling InternalRequest when releaseNotes is absent."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, {"foo": "bar"})
        with mock.patch(f"{TASK}.set_advisory_severity.create") as mock_create:
            _run(data_file)
        mock_create.assert_not_called()
        assert json.loads(data_file.read_text()) == {"foo": "bar"}

    def test_not_rhsa_noop(self, tmp_path: Path) -> None:
        """Succeed without rewriting when type is not RHSA and no severity."""
        data_file = tmp_path / "data.json"
        original = {"releaseNotes": {"type": "RHBA"}}
        _write_data(data_file, original)
        with mock.patch(f"{TASK}.set_advisory_severity.create") as mock_create:
            _run(data_file)
        mock_create.assert_not_called()
        assert json.loads(data_file.read_text()) == original

    def test_not_rhsa_strips_user_severity(self, tmp_path: Path) -> None:
        """Remove a user-supplied severity key on non-RHSA advisories."""
        data_file = tmp_path / "data.json"
        _write_data(
            data_file,
            {"releaseNotes": {"type": "RHBA", "severity": "Moderate"}},
        )
        with mock.patch(f"{TASK}.set_advisory_severity.create") as mock_create:
            _run(data_file)
        mock_create.assert_not_called()
        written = json.loads(data_file.read_text())
        assert "severity" not in written["releaseNotes"]

    def test_rhsa_no_cves_fails(self, tmp_path: Path) -> None:
        """Fail when type is RHSA but no fixed CVEs are listed."""
        data_file = tmp_path / "data.json"
        _write_data(
            data_file,
            {"releaseNotes": {"type": "RHSA", "content": {"images": [_image()]}}},
        )
        with pytest.raises(RuntimeError, match="no fixed CVEs"):
            _run(data_file)

    def test_generic_artifacts_skip_ir(self, tmp_path: Path) -> None:
        """Skip InternalRequest when generic artifacts are present."""
        data_file = tmp_path / "data.json"
        _write_data(
            data_file,
            {
                "releaseNotes": {
                    "type": "RHSA",
                    "content": {
                        "artifacts": [_image({"CVE-123": {"components": ["pkg:rpm/foo"]}})],
                    },
                },
            },
        )
        with mock.patch(f"{TASK}.set_advisory_severity.create") as mock_create:
            _run(data_file)
        mock_create.assert_not_called()
        assert "severity" not in json.loads(data_file.read_text())["releaseNotes"]

    @mock.patch(f"{TASK}.set_advisory_severity.fetch_results")
    @mock.patch(f"{TASK}.set_advisory_severity.create", return_value="success-ir")
    def test_sets_severity_from_ir(
        self,
        mock_create: mock.MagicMock,
        mock_fetch: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Write InternalRequest severity onto releaseNotes."""
        data_file = tmp_path / "data.json"
        data = _rhsa_images("CVE-123", "CVE-555")
        _write_data(data_file, data)
        mock_fetch.return_value = {
            "result": "Success",
            "severity": "IMPORTANT",
            "internalRequestPipelineRunName": "pr-1",
            "internalRequestTaskRunName": "tr-1",
        }

        _run(
            data_file,
            pipeline_run_uid="uid-123",
            request_timeout=7200,
            task_git_url="https://example.test/catalog",
            task_git_revision="main",
        )

        expected_images = data["releaseNotes"]["content"]["images"]
        mock_create.assert_called_once_with(
            "get-advisory-severity",
            params={
                "releaseNotesImages": file_helper.encode_json_gzip_b64(expected_images),
                "taskGitUrl": "https://example.test/catalog",
                "taskGitRevision": "main",
            },
            labels={PIPELINERUN_UID_LABEL: "uid-123"},
            sync=True,
            timeout=7200 + SPAWN_OVERHEAD_SECONDS,
            pipeline_timeout=seconds_to_duration(7200 + SPAWN_OVERHEAD_SECONDS),
            task_timeout=seconds_to_duration(7200),
        )
        mock_fetch.assert_called_once_with("success-ir")
        written = json.loads(data_file.read_text())
        assert written["releaseNotes"]["severity"] == "IMPORTANT"

    @mock.patch(f"{TASK}.set_advisory_severity.create")
    def test_ir_wait_error_propagates(
        self,
        mock_create: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Propagate InternalRequestWaitError from create()."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _rhsa_images("CVE-123"))
        mock_create.side_effect = InternalRequestWaitError("timeout", 124)
        with pytest.raises(InternalRequestWaitError, match="timeout"):
            _run(data_file)

    @mock.patch(f"{TASK}.set_advisory_severity.fetch_results")
    @mock.patch(f"{TASK}.set_advisory_severity.create", return_value="failure-ir")
    def test_ir_result_not_success(
        self,
        mock_create: mock.MagicMock,
        mock_fetch: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Fail when InternalRequest results are not Success."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _rhsa_images("CVE-999"))
        mock_fetch.return_value = {}
        with pytest.raises(RuntimeError, match="unsuccessful"):
            _run(data_file)

    @mock.patch(f"{TASK}.set_advisory_severity.fetch_results")
    @mock.patch(f"{TASK}.set_advisory_severity.create", return_value="success-ir")
    def test_ir_success_missing_severity(
        self,
        mock_create: mock.MagicMock,
        mock_fetch: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Fail when Success results omit a severity string."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _rhsa_images("CVE-123"))
        mock_fetch.return_value = {"result": "Success"}
        with pytest.raises(RuntimeError, match="did not return a severity"):
            _run(data_file)

    @mock.patch(f"{TASK}.set_advisory_severity.fetch_results")
    @mock.patch(f"{TASK}.set_advisory_severity.create", return_value="success-ir")
    def test_ir_success_blank_severity(
        self,
        mock_create: mock.MagicMock,
        mock_fetch: mock.MagicMock,
        tmp_path: Path,
    ) -> None:
        """Fail when Success results include an empty severity."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _rhsa_images("CVE-123"))
        mock_fetch.return_value = {"result": "Success", "severity": "  "}
        with pytest.raises(RuntimeError, match="did not return a severity"):
            _run(data_file)


class TestMain:
    """Test the main() entry point."""

    def _set_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
        data_file: Path,
        *,
        request_timeout: str | None = "7200",
    ) -> None:
        """Set required Tekton environment variables."""
        monkeypatch.setenv("DATA_FILE", str(data_file))
        monkeypatch.setenv("PIPELINE_RUN_UID", "uid-123")
        monkeypatch.setenv("TASK_GIT_URL", "https://example.test/catalog")
        monkeypatch.setenv("TASK_GIT_REVISION", "main")
        if request_timeout is None:
            monkeypatch.delenv("REQUEST_TIMEOUT", raising=False)
        else:
            monkeypatch.setenv("REQUEST_TIMEOUT", request_timeout)

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Exit zero on success."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _rhsa_images("CVE-123"))
        self._set_env(monkeypatch, data_file)
        with mock.patch(f"{TASK}.set_advisory_severity.run") as mock_run:
            assert set_advisory_severity.set_advisory_severity.main() == 0
        mock_run.assert_called_once_with(
            data_file,
            pipeline_run_uid="uid-123",
            request_timeout=7200,
            task_git_url="https://example.test/catalog",
            task_git_revision="main",
        )

    def test_default_request_timeout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Use 7200 seconds when REQUEST_TIMEOUT is unset."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _rhsa_images("CVE-123"))
        self._set_env(monkeypatch, data_file, request_timeout=None)
        with mock.patch(f"{TASK}.set_advisory_severity.run") as mock_run:
            assert set_advisory_severity.main() == 0
        assert mock_run.call_args.kwargs["request_timeout"] == 7200

    def test_missing_required_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Exit non-zero when required env vars are missing."""
        monkeypatch.delenv("DATA_FILE", raising=False)
        with pytest.raises(SystemExit):
            set_advisory_severity.set_advisory_severity.main()

    def test_module_main_guard(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Executing the task module as __main__ calls main()."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, {"releaseNotes": {"type": "RHBA"}})
        self._set_env(monkeypatch, data_file)
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module(
                f"{TASK}.set_advisory_severity",
                run_name="__main__",
            )
        assert exc_info.value.code == 0

    def test_package_main(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Executing the package as __main__ calls main()."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, {"releaseNotes": {"type": "RHBA"}})
        self._set_env(monkeypatch, data_file)
        with mock.patch(f"{TASK}.set_advisory_severity.run"):
            with pytest.raises(SystemExit) as exc_info:
                runpy.run_module(TASK, run_name="__main__")
            assert exc_info.value.code == 0

    def test_package_main_not_run_on_import(self) -> None:
        """Importing the package __main__ module does not call main()."""
        import importlib
        import sys

        module_name = f"{TASK}.__main__"
        sys.modules.pop(module_name, None)
        with mock.patch(f"{TASK}.set_advisory_severity.main") as mock_main:
            importlib.import_module(module_name)
        mock_main.assert_not_called()
