"""Tests for the ``push_rpm_data_to_pyxis`` task implementation."""

from __future__ import annotations

import base64
import json
import runpy
import subprocess
from pathlib import Path
from unittest.mock import call, patch

import pytest
from release_service_utils.helpers import cosign

from release_service_utils.tasks.managed.push_rpm_data_to_pyxis import (
    push_rpm_data_to_pyxis as task,
)

TASK = "release_service_utils.tasks.managed.push_rpm_data_to_pyxis.push_rpm_data_to_pyxis"
ARCH_DIGEST = "sha256:" + "a" * 64
SPDX_TYPE = "https://spdx.dev/Document"
ECP = json.dumps({"spec": {"publicKey": "k8s://ns/key"}})


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
        arch_digest=ARCH_DIGEST,
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
                arch_digest="sha256:arch",
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
    """Test that SBOM downloads delegate to the shared cosign helper."""

    @pytest.mark.parametrize(
        ("job", "retries", "expected_attempts"),
        [
            (_job(), 3, 4),
            (_job(platform="linux/amd64"), 0, 1),
        ],
    )
    def test_delegates_to_cosign_helper(
        self,
        tmp_path: Path,
        job: task.ImageJob,
        retries: int,
        expected_attempts: int,
    ) -> None:
        """Map ImageJob fields and retry count onto cosign.download."""
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.cosign.download") as download:
                task.download_sbom(job, retries=retries, enterprise_contract_policy=None)

        download.assert_called_once_with(
            "sbom",
            job.container_image,
            tmp_path / f"{job.image_id}.json",
            max_attempts=expected_attempts,
            platform=job.platform,
        )


def _envelope(predicate_type: str, digest: str, predicate: dict) -> str:
    statement = {
        "_type": "https://in-toto.io/Statement/v0.1",
        "predicateType": predicate_type,
        "subject": [{"name": "image", "digest": {"sha256": digest.split(":")[1]}}],
        "predicate": predicate,
    }
    payload = base64.b64encode(json.dumps(statement).encode()).decode()
    return json.dumps({"payloadType": "application/vnd.in-toto+json", "payload": payload})


def _missing_sbom() -> cosign.SbomNotFoundError:
    """Model the shared helper's error when a legacy SBOM is missing."""
    return cosign.SbomNotFoundError("No SBOM attached to image: no sbom attached to reference")


class TestSpdxAttestationFallback:
    """Test the SPDX attestation fallback of download_sbom."""

    SPDX = {"spdxVersion": "SPDX-2.3", "packages": []}

    @pytest.mark.parametrize("bundle", [False, True])
    @pytest.mark.parametrize("platform", [None, "linux/arm64"])
    def test_uses_verified_attestation_when_sbom_is_missing(
        self, tmp_path: Path, bundle: bool, platform: str | None
    ) -> None:
        """Extract SPDX from verified envelopes and referrer bundles per architecture."""
        matching = _envelope(SPDX_TYPE, ARCH_DIGEST, self.SPDX)
        if bundle:
            matching = json.dumps(
                {
                    "mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
                    "dsseEnvelope": json.loads(matching),
                }
            )
        output = "\n\n".join(
            [
                # Intentionally wrong digest: skip this attestation and use the matching one.
                _envelope(SPDX_TYPE, "sha256:" + "b" * 64, {"other": 1}),
                matching,
            ]
        )
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.cosign.download", side_effect=_missing_sbom()):
                with patch(
                    f"{TASK}.cosign.verify_spdx_attestation", return_value=output
                ) as verify:
                    task.download_sbom(
                        _job(platform=platform), retries=2, enterprise_contract_policy=ECP
                    )

        verify.assert_called_once_with(
            f"quay.io/example/image@{ARCH_DIGEST}", key="k8s://ns/key", max_attempts=3
        )
        assert json.loads((tmp_path / "abcdef.json").read_text()) == self.SPDX

    def test_legacy_sbom_success_skips_the_fallback(self, tmp_path: Path) -> None:
        """Do not verify attestations when the SBOM download succeeds."""
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.cosign.download"):
                with patch(f"{TASK}.cosign.verify_spdx_attestation") as verify:
                    task.download_sbom(_job(), retries=0, enterprise_contract_policy=None)
        verify.assert_not_called()

    @pytest.mark.parametrize(
        ("policy", "output"),
        [
            (ECP, ""),
            (ECP, _envelope(SPDX_TYPE, "sha256:" + "b" * 64, {})),
            (ECP, "not json"),
        ],
    )
    def test_fallback_failures_are_reported(
        self, tmp_path: Path, policy: str, output: str
    ) -> None:
        """Reject empty, non-matching, and malformed verified output."""
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.cosign.download", side_effect=_missing_sbom()):
                with patch(f"{TASK}.cosign.verify_spdx_attestation", return_value=output):
                    with pytest.raises(RuntimeError, match="fallback failed too") as caught:
                        task.download_sbom(
                            _job(), retries=0, enterprise_contract_policy=policy
                        )
        assert isinstance(caught.value.__cause__, ValueError)
        assert not (tmp_path / "abcdef.json").exists()

    @pytest.mark.parametrize(
        ("policy", "message"),
        [
            ("{}", "needed for the referrers API SPDX attestation fallback"),
            (None, "No EnterpriseContractPolicy provided"),
            ("", "No EnterpriseContractPolicy provided"),
            ("not json", "not valid JSON"),
            ("[1]", "must be a JSON object"),
            (json.dumps({"spec": None}), "'spec' must be a JSON object"),
            (json.dumps({"spec": {}}), "needed for referrers API SPDX fallback"),
        ],
    )
    def test_missing_public_key_is_explained(
        self, tmp_path: Path, policy: str | None, message: str
    ) -> None:
        """Distinguish an unprovided policy from a policy without a publicKey."""
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.cosign.download", side_effect=_missing_sbom()):
                with patch(f"{TASK}.cosign.verify_spdx_attestation") as verify:
                    with pytest.raises(RuntimeError, match="fallback failed too") as caught:
                        task.download_sbom(
                            _job(), retries=0, enterprise_contract_policy=policy
                        )
        assert message in str(caught.value.__cause__)
        verify.assert_not_called()

    def test_verification_failure_is_reported(self, tmp_path: Path) -> None:
        """Do not write an SBOM when cosign verification fails."""
        with patch.object(task, "SBOM_DIR", tmp_path):
            with patch(f"{TASK}.cosign.download", side_effect=_missing_sbom()):
                with patch(
                    f"{TASK}.cosign.verify_spdx_attestation",
                    side_effect=RuntimeError("bad sig"),
                ):
                    with pytest.raises(RuntimeError, match="bad sig") as caught:
                        task.download_sbom(_job(), retries=0, enterprise_contract_policy=ECP)
        assert "no sbom attached to reference" in str(caught.value)
        assert not (tmp_path / "abcdef.json").exists()

    @pytest.mark.parametrize(
        "line",
        [
            "not json",
            json.dumps({"dsseEnvelope": {}}),
            json.dumps({"payload": "!!!"}),
            json.dumps({"payload": base64.b64encode(b"nope").decode()}),
            json.dumps({"payload": base64.b64encode(b"[1]").decode()}),
            "[1]",
        ],
    )
    def test_malformed_envelope_names_reference_and_line(
        self, tmp_path: Path, line: str
    ) -> None:
        """Wrap parse failures with the image reference and line index."""
        output = _envelope(SPDX_TYPE, "sha256:" + "b" * 64, {}) + "\n" + line
        with pytest.raises(ValueError, match=r"quay.io/x@sha256:\w+ \(line 2\)") as caught:
            task._write_spdx_from_attestations(
                output, f"quay.io/x@{ARCH_DIGEST}", ARCH_DIGEST, tmp_path / "o.json"
            )
        assert caught.value.__cause__ is not None or "not a JSON object" in str(caught.value)

    def test_malformed_envelope_line_number_includes_blank_lines(self, tmp_path: Path) -> None:
        """Count skipped blank lines when reporting malformed output."""
        output = _envelope(SPDX_TYPE, "sha256:" + "b" * 64, {}) + "\n \nnot json"
        with pytest.raises(ValueError, match=r"quay.io/x@sha256:\w+ \(line 3\)"):
            task._write_spdx_from_attestations(
                output, f"quay.io/x@{ARCH_DIGEST}", ARCH_DIGEST, tmp_path / "o.json"
            )

    @pytest.mark.parametrize(
        "error",
        [
            RuntimeError("Failed to download sbom for image after 1 attempts."),
            RuntimeError("output file was not created"),
            subprocess.CalledProcessError(1, ["select-oci-auth"], stderr="no credentials"),
        ],
    )
    def test_other_download_failures_do_not_fall_back(self, error: Exception) -> None:
        """Preserve network, authentication, and output-file failures."""
        with patch(f"{TASK}.cosign.download", side_effect=error):
            with patch(f"{TASK}.cosign.verify_spdx_attestation") as verify:
                with pytest.raises(type(error)) as exc:
                    task.download_sbom(_job(), retries=0, enterprise_contract_policy=ECP)
        assert exc.value is error
        verify.assert_not_called()


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
            task.run(pyxis_file, 0, 3, enterprise_contract_policy=None)
        with pytest.raises(ValueError, match="RETRIES"):
            task.run(pyxis_file, 1, -1, enterprise_contract_policy=None)

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
                task.run(pyxis_file, 1, 3, enterprise_contract_policy=None)
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
                        task.run(pyxis_file, 4, 3, enterprise_contract_policy=None)

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
                        task.run(pyxis_file, 1, 3, enterprise_contract_policy=None)
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
        monkeypatch.setenv("ENTERPRISE_CONTRACT_POLICY", ECP)

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
        run.assert_called_once_with(
            Path("/data/pyxis.json"), 7, 4, enterprise_contract_policy=ECP
        )

    def test_uses_default_retry_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use three retries when Tekton does not override RETRIES."""
        monkeypatch.setenv("PYXIS_FILE", "/data/pyxis.json")
        monkeypatch.setenv("PYXIS_SECRET_PATH", "/secrets")
        monkeypatch.setenv("PYXIS_SERVER", "production")
        monkeypatch.setenv("CONCURRENT_LIMIT", "1")
        monkeypatch.delenv("RETRIES", raising=False)
        monkeypatch.delenv("ENTERPRISE_CONTRACT_POLICY", raising=False)
        with patch(f"{TASK}.authentication.setup_ca_cert"):
            with patch(f"{TASK}.run") as run:
                task.main()
        run.assert_called_once_with(
            Path("/data/pyxis.json"), 1, 3, enterprise_contract_policy=None
        )

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
