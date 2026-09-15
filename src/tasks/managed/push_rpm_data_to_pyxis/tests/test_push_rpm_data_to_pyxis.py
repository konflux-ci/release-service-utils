"""Tests for the ``push_rpm_data_to_pyxis`` task implementation."""

from __future__ import annotations

import json
import runpy
import subprocess
from pathlib import Path
from unittest.mock import call, patch

import pytest

from release_service_utils.tasks.managed.push_rpm_data_to_pyxis import (
    push_rpm_data_to_pyxis as task,
)

TASK = "release_service_utils.tasks.managed.push_rpm_data_to_pyxis.push_rpm_data_to_pyxis"


def _job(
    image_id: str = "abcdef",
    *,
    platform: str | None = None,
    container_image: str = "quay.io/example/image@sha256:index",
) -> task.ImageJob:
    return task.ImageJob(
        image_id=image_id,
        container_image=container_image,
        platform=platform,
    )


def _completed(
    command: list[str] | None = None,
    *,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command or [], 0, stdout=stdout, stderr=stderr)


class TestImageCollection:
    """Test parsing, validation, and deduplication of image jobs."""

    def test_platform_for_single_and_multi_arch_images(self) -> None:
        """Store no platform for single-arch and OS/arch for multi-arch images."""
        data = {
            "components": [
                {
                    "containerImage": "quay.io/example/single",
                    "pyxisImages": [
                        {
                            "imageId": "aaaaaa",
                            "digest": "sha256:index",
                            "arch_digest": "sha256:index",
                        }
                    ],
                },
                {
                    "containerImage": "quay.io/example/multi",
                    "pyxisImages": [
                        {
                            "imageId": "bbbbbb",
                            "digest": "sha256:index",
                            "arch_digest": "sha256:arch",
                            "os": "linux",
                            "arch": "amd64",
                        }
                    ],
                },
            ]
        }
        jobs = task.collect_image_jobs(data)
        assert [job.platform for job in jobs] == [None, "linux/amd64"]

    def test_collects_jobs_and_ignores_components_without_images(self) -> None:
        """Flatten Pyxis images and retain their parent component reference."""
        data = {
            "components": [
                {"containerImage": "unused"},
                {
                    "containerImage": "quay.io/example/image",
                    "pyxisImages": [
                        {
                            "imageId": "abc123",
                            "digest": "sha256:index",
                            "arch_digest": "sha256:arch",
                            "os": "linux",
                            "arch": "arm64",
                        }
                    ],
                },
            ]
        }
        jobs = task.collect_image_jobs(data)
        assert jobs == [
            task.ImageJob(
                image_id="abc123",
                container_image="quay.io/example/image",
                platform="linux/arm64",
            )
        ]

    @pytest.mark.parametrize(
        ("data", "message"),
        [
            ({"components": {}}, "components.*array"),
            ({"components": ["bad"]}, "component must be an object"),
            (
                {"components": [{"pyxisImages": {}}]},
                "pyxisImages.*array",
            ),
            (
                {"components": [{"pyxisImages": ["bad"]}]},
                "image must be an object",
            ),
        ],
    )
    def test_rejects_invalid_collection_shapes(self, data: dict, message: str) -> None:
        """Reject non-array and non-object values in the Pyxis data structure."""
        with pytest.raises(TypeError, match=message):
            task.collect_image_jobs(data)

    def test_treats_null_pyxis_images_as_empty(self) -> None:
        """Ignore a component whose Pyxis image list is explicitly null."""
        assert task.collect_image_jobs({"components": [{"pyxisImages": None}]}) == []

    def test_rejects_missing_required_field(self) -> None:
        """Report the required field missing from a Pyxis image."""
        data = {
            "components": [
                {
                    "containerImage": "quay.io/example/image",
                    "pyxisImages": [
                        {
                            "imageId": "abc123",
                            "digest": "sha256:index",
                        }
                    ],
                }
            ]
        }
        with pytest.raises(ValueError, match="arch_digest"):
            task.collect_image_jobs(data)

    @pytest.mark.parametrize("image_id", ["", "not-hex"])
    def test_rejects_invalid_image_id(self, image_id: str) -> None:
        """Require non-empty hexadecimal Pyxis image IDs."""
        data = {
            "components": [
                {
                    "containerImage": "quay.io/example/image",
                    "pyxisImages": [
                        {
                            "imageId": image_id,
                            "digest": "sha256:index",
                            "arch_digest": "sha256:index",
                        }
                    ],
                }
            ]
        }
        with pytest.raises(ValueError, match="imageId is invalid"):
            task.collect_image_jobs(data)

    def test_rejects_multi_arch_image_without_platform(self) -> None:
        """Validate multi-arch platform metadata while constructing jobs."""
        data = {
            "components": [
                {
                    "containerImage": "quay.io/example/image",
                    "pyxisImages": [
                        {
                            "imageId": "abc123",
                            "digest": "sha256:index",
                            "arch_digest": "sha256:arch",
                        }
                    ],
                }
            ]
        }
        with pytest.raises(ValueError, match="no complete platform"):
            task.collect_image_jobs(data)

    def test_deduplicates_by_image_id_using_first_job(self) -> None:
        """Keep the first occurrence of every image ID in input order."""
        first = _job("aaaaaa", container_image="first")
        duplicate = _job("aaaaaa", container_image="second")
        other = _job("bbbbbb")
        assert task.unique_download_jobs([first, duplicate, other]) == [first, other]


class TestDownloadSbom:
    """Test authenticated and retried SBOM downloads."""

    @pytest.mark.parametrize(
        ("job", "expected_tail"),
        [
            (_job(), ["quay.io/example/image@sha256:index"]),
            (
                _job(platform="linux/amd64"),
                ["--platform", "linux/amd64", "quay.io/example/image@sha256:index"],
            ),
        ],
    )
    def test_downloads_single_and_multi_arch_sboms(
        self, tmp_path: Path, job: task.ImageJob, expected_tail: list[str]
    ) -> None:
        """Build the correct cosign command and temporary Docker configuration."""
        commands: list[list[str]] = []
        docker_configs: list[str] = []

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            commands.append(command)
            if command[0] == "select-oci-auth":
                return _completed(command, stdout='{"auths": {}}')
            config_dir = Path(str(kwargs["env"]["DOCKER_CONFIG"]))  # type: ignore[index]
            docker_configs.append((config_dir / "config.json").read_text())
            (tmp_path / f"{job.image_id}.json").write_text("{}", encoding="utf-8")
            return _completed(command, stdout="downloaded", stderr="cosign status")

        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run):
                task.download_sbom(job, retries=3)

        assert commands[0] == ["select-oci-auth", job.container_image]
        assert commands[1][:5] == [
            "cosign",
            "download",
            "sbom",
            "--output-file",
            str(tmp_path / f"{job.image_id}.json"),
        ]
        assert commands[1][-len(expected_tail) :] == expected_tail
        assert docker_configs == ['{"auths": {}}']

    def test_uses_empty_config_for_empty_auth_output(self, tmp_path: Path) -> None:
        """Write an empty JSON object when select-oci-auth prints nothing."""

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            if command[0] == "select-oci-auth":
                return _completed(command)
            config_dir = Path(str(kwargs["env"]["DOCKER_CONFIG"]))  # type: ignore[index]
            assert (config_dir / "config.json").read_text() == "{}"
            (tmp_path / "abcdef.json").write_text("{}", encoding="utf-8")
            return _completed(command)

        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run):
                task.download_sbom(_job(), retries=0)

    def test_retries_cosign_failure(self, tmp_path: Path) -> None:
        """Retry a failed cosign process and succeed on the next attempt."""
        cosign_calls = 0

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            nonlocal cosign_calls
            if command[0] == "select-oci-auth":
                return _completed(command)
            cosign_calls += 1
            if cosign_calls == 1:
                raise subprocess.CalledProcessError(1, command)
            (tmp_path / "abcdef.json").write_text("{}", encoding="utf-8")
            return _completed(command)

        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run):
                with patch("release_service_utils.helpers.retry.retry.time.sleep") as sleep:
                    task.download_sbom(_job(), retries=1)

        assert cosign_calls == 2
        sleep.assert_called_once_with(2)

    def test_propagates_authentication_failure(self, tmp_path: Path) -> None:
        """Log safe diagnostics and fail when registry credential selection fails."""
        error = subprocess.CalledProcessError(
            1,
            ["select-oci-auth"],
            output='{"auths": {"registry": "secret"}}',
            stderr="credential selection failed",
        )
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=error):
                with patch(f"{TASK}.logger.info") as log:
                    with pytest.raises(subprocess.CalledProcessError):
                        task.download_sbom(_job(), retries=3)

        assert call("credential selection failed") in log.call_args_list
        assert all("secret" not in str(log_call) for log_call in log.call_args_list)

    def test_raises_when_cosign_exhausts_retries(self, tmp_path: Path) -> None:
        """Log every failed cosign attempt and propagate the final error."""

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            if command[0] == "select-oci-auth":
                return _completed(command)
            raise subprocess.CalledProcessError(
                1, command, output="cosign output", stderr="cosign error"
            )

        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run):
                with patch("release_service_utils.helpers.retry.retry.time.sleep"):
                    with patch(f"{TASK}.logger.info") as log:
                        with pytest.raises(subprocess.CalledProcessError):
                            task.download_sbom(_job(), retries=1)

        assert log.call_args_list.count(call("cosign output")) == 2
        assert log.call_args_list.count(call("cosign error")) == 2

    def test_requires_cosign_to_create_output(self, tmp_path: Path) -> None:
        """Reject a successful cosign process that produced no SBOM file."""
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(
                f"{TASK}.subprocess_cmd.run_cmd",
                side_effect=[_completed(stdout="{}"), _completed()],
            ):
                with pytest.raises(RuntimeError, match="was not created"):
                    task.download_sbom(_job(), retries=0)


class TestUploadRpmData:
    """Test SPDX validation and RPM upload command execution."""

    def test_requires_existing_sbom(self, tmp_path: Path) -> None:
        """Fail when an image's downloaded SBOM cannot be found."""
        with patch.object(task, "SBOM_DIR", tmp_path):
            with pytest.raises(RuntimeError, match="not found"):
                task.upload_rpm_data(_job())

    @pytest.mark.parametrize("data", [{}, {"spdxVersion": ""}])
    def test_rejects_non_spdx_sbom(self, tmp_path: Path, data: dict) -> None:
        """Require a non-empty SPDX version in the downloaded JSON."""
        (tmp_path / "abcdef.json").write_text(json.dumps(data), encoding="utf-8")
        with patch.object(task, "SBOM_DIR", tmp_path):
            with pytest.raises(ValueError, match="not a valid SPDX SBOM"):
                task.upload_rpm_data(_job())

    @pytest.mark.parametrize(
        ("content", "error"),
        [("not-json", json.JSONDecodeError), ("[]", TypeError)],
    )
    def test_rejects_invalid_sbom_json(
        self, tmp_path: Path, content: str, error: type[Exception]
    ) -> None:
        """Reject malformed JSON and JSON whose root is not an object."""
        (tmp_path / "abcdef.json").write_text(content, encoding="utf-8")
        with patch.object(task, "SBOM_DIR", tmp_path):
            with pytest.raises(error):
                task.upload_rpm_data(_job())

    def test_invokes_existing_upload_command(self, tmp_path: Path) -> None:
        """Run the existing upload command with retry, image ID, and SBOM path."""
        sbom = tmp_path / "abcdef.json"
        sbom.write_text('{"spdxVersion": "SPDX-2.3"}', encoding="utf-8")
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(
                f"{TASK}.subprocess_cmd.run_cmd",
                return_value=_completed(stdout="uploaded", stderr="upload status"),
            ) as run_cmd:
                task.upload_rpm_data(_job())

        run_cmd.assert_called_once_with(
            [
                "upload_rpm_data",
                "--retry",
                "--image-id",
                "abcdef",
                "--sbom-path",
                str(sbom),
                "--verbose",
            ],
            check=True,
        )

    def test_logs_upload_output_on_failure(self, tmp_path: Path) -> None:
        """Preserve upload command diagnostics when the process fails."""
        (tmp_path / "abcdef.json").write_text('{"spdxVersion": "SPDX-2.3"}', encoding="utf-8")
        error = subprocess.CalledProcessError(
            1,
            ["upload_rpm_data"],
            output="upload output",
            stderr="upload error",
        )
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=error):
                with patch(f"{TASK}.logger.info") as log:
                    with pytest.raises(subprocess.CalledProcessError):
                        task.upload_rpm_data(_job())

        assert call("upload output") in log.call_args_list
        assert call("upload error") in log.call_args_list


class TestConcurrentPhases:
    """Test throttled concurrent phase execution."""

    def test_runs_all_jobs_and_applies_throttling(self) -> None:
        """Submit every job, throttle each submission, and pause after five jobs."""
        jobs = [_job(f"abcde{i}") for i in range(6)]
        seen: list[str] = []
        with patch(f"{TASK}.memory_throttle.wait_for_memory") as wait:
            with patch(f"{TASK}.time.sleep") as sleep:
                task._run_phase(jobs, lambda job: seen.append(job.image_id), 2, "test")

        assert sorted(seen) == sorted(job.image_id for job in jobs)
        assert wait.call_args_list == [call(task.MEMORY_THRESHOLD)] * 6
        sleep.assert_called_once_with(task.STABILIZATION_DELAY)

    def test_collects_all_failures_before_raising(self) -> None:
        """Complete every submitted job and include each failure in the final error."""
        jobs = [_job("aaaaaa"), _job("bbbbbb"), _job("cccccc")]
        seen: list[str] = []

        def operation(job: task.ImageJob) -> None:
            seen.append(job.image_id)
            if job.image_id != "bbbbbb":
                raise ValueError(f"bad {job.image_id}")

        with patch(f"{TASK}.memory_throttle.wait_for_memory"):
            with patch(f"{TASK}.logger.error") as error:
                with pytest.raises(RuntimeError, match="aaaaaa.*\ncccccc|cccccc.*\naaaaaa"):
                    task._run_phase(jobs, operation, 2, "test")

        assert sorted(seen) == sorted(job.image_id for job in jobs)
        assert error.call_count == 2
        assert all(call.kwargs.get("exc_info") is True for call in error.call_args_list)


class TestRun:
    """Test orchestration and phase ordering."""

    def test_validates_inputs(self, tmp_path: Path) -> None:
        """Reject invalid concurrency or retry values."""
        pyxis_file = tmp_path / "pyxis.json"
        pyxis_file.write_text("{}", encoding="utf-8")
        with pytest.raises(ValueError, match="CONCURRENT_LIMIT"):
            task.run(pyxis_file, 0, 3)
        with pytest.raises(ValueError, match="RETRIES"):
            task.run(pyxis_file, 1, -1)

    @pytest.mark.parametrize(
        "payload",
        [
            "{}",
            '{"components": []}',
            '{"components": [{"pyxisImages": null}]}',
            '{"components": [{"containerImage": "quay.io/example/image"}]}',
        ],
    )
    def test_empty_workload_fails(self, tmp_path: Path, payload: str) -> None:
        """Fail when the Pyxis file lists no images to download or upload."""
        pyxis_file = tmp_path / "pyxis.json"
        pyxis_file.write_text(payload, encoding="utf-8")
        sbom_dir = tmp_path / "sboms"
        with patch.object(task, "SBOM_DIR", sbom_dir):
            with pytest.raises(RuntimeError, match="No Pyxis images found"):
                task.run(pyxis_file, 1, 3)
        assert not sbom_dir.exists()

    def test_runs_download_then_upload_phases(self, tmp_path: Path) -> None:
        """Deduplicate downloads while retaining every upload job."""
        pyxis_file = tmp_path / "pyxis.json"
        pyxis_file.write_text("{}", encoding="utf-8")
        first = _job("aaaaaa", container_image="first")
        duplicate = _job("aaaaaa", container_image="second")
        other = _job("bbbbbb")
        phase_calls: list[tuple[list[task.ImageJob], str]] = []

        def fake_phase(
            jobs: list[task.ImageJob],
            operation: object,
            concurrent_limit: int,
            phase_name: str,
        ) -> None:
            assert concurrent_limit == 4
            phase_calls.append((jobs, phase_name))

        with patch.object(task, "SBOM_DIR", tmp_path / "sboms"):
            with patch(f"{TASK}.collect_image_jobs", return_value=[first, duplicate, other]):
                with patch(f"{TASK}._run_phase", side_effect=fake_phase):
                    with patch(f"{TASK}.memory_throttle.log_memory_throttle_status") as log:
                        task.run(pyxis_file, 4, 3)

        assert phase_calls == [
            ([first, other], "SBOM download"),
            ([first, duplicate, other], "RPM data upload"),
        ]
        log.assert_called_once_with(task.MEMORY_THRESHOLD)

    def test_download_failure_prevents_upload_phase(self, tmp_path: Path) -> None:
        """Stop before uploads when the download phase fails."""
        pyxis_file = tmp_path / "pyxis.json"
        pyxis_file.write_text("{}", encoding="utf-8")
        with patch.object(task, "SBOM_DIR", tmp_path / "sboms"):
            with patch(f"{TASK}.collect_image_jobs", return_value=[_job()]):
                with patch(
                    f"{TASK}._run_phase", side_effect=RuntimeError("download")
                ) as phase:
                    with pytest.raises(RuntimeError, match="download"):
                        task.run(pyxis_file, 1, 3)
        assert phase.call_count == 1


class TestMain:
    """Test Tekton environment parsing and task setup."""

    def test_missing_required_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Exit non-zero when a required Tekton environment variable is missing."""
        monkeypatch.delenv("PYXIS_FILE", raising=False)
        with pytest.raises(SystemExit):
            task.main()

    def test_configures_pyxis_and_runs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Translate Tekton environment values into Pyxis and run configuration."""
        monkeypatch.setenv("PYXIS_FILE", "/data/pyxis.json")
        monkeypatch.setenv("PYXIS_SECRET_PATH", "/secrets")
        monkeypatch.setenv("PYXIS_SERVER", "stage")
        monkeypatch.setenv("CONCURRENT_LIMIT", "7")
        monkeypatch.setenv("RETRIES", "4")

        with patch(f"{TASK}.authentication.setup_ca_cert") as setup_ca:
            with patch(f"{TASK}.run") as run:
                assert task.main() == 0

        assert task.os.environ["PYXIS_CERT_PATH"] == "/secrets/cert"
        assert task.os.environ["PYXIS_KEY_PATH"] == "/secrets/key"
        assert (
            task.os.environ["PYXIS_GRAPHQL_API"]
            == "https://graphql-pyxis.preprod.api.redhat.com/graphql/"
        )
        setup_ca.assert_called_once_with()
        run.assert_called_once_with(Path("/data/pyxis.json"), 7, 4)

    def test_uses_default_retry_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use three retries when Tekton does not override RETRIES."""
        monkeypatch.setenv("PYXIS_FILE", "/data/pyxis.json")
        monkeypatch.setenv("PYXIS_SECRET_PATH", "/secrets")
        monkeypatch.setenv("PYXIS_SERVER", "production")
        monkeypatch.setenv("CONCURRENT_LIMIT", "1")
        monkeypatch.delenv("RETRIES", raising=False)
        with patch(f"{TASK}.authentication.setup_ca_cert"):
            with patch(f"{TASK}.run") as run:
                task.main()
        run.assert_called_once_with(Path("/data/pyxis.json"), 1, 3)

    def test_invalid_server_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Propagate validation from the shared Pyxis server mapping helper."""
        monkeypatch.setenv("PYXIS_FILE", "/data/pyxis.json")
        monkeypatch.setenv("PYXIS_SECRET_PATH", "/secrets")
        monkeypatch.setenv("PYXIS_SERVER", "invalid")
        monkeypatch.setenv("CONCURRENT_LIMIT", "1")
        with pytest.raises(ValueError, match="Invalid server parameter"):
            task.main()

    def test_package_module_entrypoint(self) -> None:
        """Delegate package execution to the implementation main function."""
        module = "release_service_utils.tasks.managed.push_rpm_data_to_pyxis.__main__"
        with patch(f"{TASK}.main", return_value=0) as main:
            with pytest.raises(SystemExit) as exc:
                runpy.run_module(module, run_name="__main__")
        assert exc.value.code == 0
        main.assert_called_once_with()
