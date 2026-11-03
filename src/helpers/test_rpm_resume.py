"""Tests for signed RPM checkpoint discovery and validation."""

from __future__ import annotations

import base64
import gzip
import io
import json
import subprocess
import tarfile
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.helpers import rpm_resume

INPUTS = {"one-1-1.x86_64.rpm": "sha-one", "two-1-1.x86_64.rpm": "sha-two"}
REF = "oci:quay.io/signed@sha256:" + "a" * 64


def _request(inputs: dict[str, str] = INPUTS, alias: str = "release4") -> dict:
    artifact_json = {"artifacts": {name: {"sha256": sha} for name, sha in inputs.items()}}
    encoded = base64.b64encode(gzip.compress(json.dumps(artifact_json).encode())).decode()
    return {
        "metadata": {"name": "signing-1", "creationTimestamp": "2026-11-02T12:00:00Z"},
        "spec": {
            "params": {
                "signing_alias": alias,
                "artifact_json": encoded,
                "create_oci_artifact": "true",
            },
            "pipeline": {
                "pipelineRef": {
                    "params": [
                        {
                            "name": "pathInRepo",
                            "value": rpm_resume.SIGNING_PIPELINE_PATH,
                        }
                    ]
                }
            },
        },
        "status": {
            "conditions": [{"type": "Succeeded", "status": "True"}],
            "results": {"signed_rpms_oci_artifact": REF},
        },
    }


def test_fingerprint_matches_snapshot_and_artifact_json() -> None:
    """Use the same fingerprint before and after the unsigned Pulp push."""
    snapshot = {
        "components": [
            {
                "rpmsToPublish": [
                    {"rpm": "two-1-1.x86_64.rpm", "sha256": "sha-two"},
                    {"rpm": "one-1-1.x86_64.rpm", "sha256": "sha-one"},
                    {"rpm": "one-1-1.x86_64.rpm", "sha256": "sha-one"},
                ]
            }
        ]
    }
    from_snapshot = rpm_resume.inputs_from_snapshot(snapshot)
    from_artifact = rpm_resume.inputs_from_artifact_json(
        {"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}}
    )
    assert from_snapshot == from_artifact
    assert rpm_resume.fingerprint(from_snapshot, "release4") == rpm_resume.fingerprint(
        from_artifact, "release4"
    )
    assert len(rpm_resume.fingerprint(from_snapshot, "release4")) == 63
    assert rpm_resume.fingerprint(from_snapshot, "other") != rpm_resume.fingerprint(
        from_snapshot, "release4"
    )


def test_conflicting_snapshot_digests_fail() -> None:
    """Do not reuse a checkpoint when one filename has two input digests."""
    with pytest.raises(ValueError, match="Conflicting unsigned SHA"):
        rpm_resume.inputs_from_snapshot(
            {
                "components": [
                    {
                        "rpmsToPublish": [
                            {"rpm": "one-1-1.x86_64.rpm", "sha256": "old"},
                            {"rpm": "one-1-1.x86_64.rpm", "sha256": "new"},
                        ]
                    }
                ]
            }
        )


def test_finds_matching_successful_request() -> None:
    """Find a previous signing result only when its input and alias match."""
    response = subprocess.CompletedProcess([], 0, json.dumps({"items": [_request()]}), "")
    with patch.object(rpm_resume.subprocess_cmd, "run_cmd", return_value=response) as run:
        assert rpm_resume.find_resume_request(INPUTS, "release4") == (
            "signing-1",
            REF,
        )
    assert rpm_resume.FINGERPRINT_LABEL in run.call_args.args[0][4]


@pytest.mark.parametrize(
    "candidate",
    [
        _request(alias="other"),
        _request(inputs={"one-1-1.x86_64.rpm": "sha-one"}),
        {**_request(), "status": {"results": {}}},
        {**_request(), "status": {"results": {"signed_rpms_oci_artifact": REF}}},
        {**_request(), "status": {"results": {"signed_rpms_oci_artifact": "oci:$(bad)"}}},
    ],
)
def test_rejects_unrelated_or_incomplete_request(candidate: dict) -> None:
    """Ignore an IR with the wrong alias, input set, or no signed artifact."""
    response = subprocess.CompletedProcess([], 0, json.dumps({"items": [candidate]}), "")
    with patch.object(rpm_resume.subprocess_cmd, "run_cmd", return_value=response):
        assert rpm_resume.find_resume_request(INPUTS, "release4") is None


def test_explicit_request_must_match() -> None:
    """Fail rather than re-sign when an explicit resume request is incorrect."""
    response = subprocess.CompletedProcess([], 0, json.dumps(_request(alias="other")), "")
    with (
        patch.object(rpm_resume.subprocess_cmd, "run_cmd", return_value=response),
        pytest.raises(RuntimeError, match="exact RPM set"),
    ):
        rpm_resume.find_resume_request(INPUTS, "release4", "signing-1")


def test_validate_signed_artifact_file_set() -> None:
    """Accept an accessible artifact containing every expected signed RPM."""

    def fetch(_ref: str, archive: Path) -> None:
        with tarfile.open(archive, "w:gz") as output:
            for name in INPUTS:
                content = b"signed RPM"
                info = tarfile.TarInfo(name)
                info.size = len(content)
                output.addfile(info, io.BytesIO(content))

    with patch.object(rpm_resume.oras_utils, "oras_blob_fetch", side_effect=fetch):
        rpm_resume.validate_signed_artifact(REF, INPUTS)


def test_validate_signed_artifact_rejects_missing_rpm() -> None:
    """A partial artifact cannot be used even if the IR input set matched."""

    def fetch(_ref: str, archive: Path) -> None:
        with tarfile.open(archive, "w:gz") as output:
            info = tarfile.TarInfo("one-1-1.x86_64.rpm")
            info.size = 1
            output.addfile(info, io.BytesIO(b"x"))

    with (
        patch.object(rpm_resume.oras_utils, "oras_blob_fetch", side_effect=fetch),
        pytest.raises(RuntimeError, match="missing"),
    ):
        rpm_resume.validate_signed_artifact(REF, INPUTS)


def test_expired_artifact_fails() -> None:
    """An expired checkpoint fails before another signing request is made."""
    with (
        patch.object(
            rpm_resume.oras_utils,
            "oras_blob_fetch",
            side_effect=RuntimeError("not found"),
        ),
        pytest.raises(RuntimeError, match="not found"),
    ):
        rpm_resume.validate_signed_artifact(REF, INPUTS)


def test_reads_checkpoint_after_internal_request_is_gone() -> None:
    """Recover the signed reference from a retained OCI index after IR cleanup."""
    expected_ref = REF

    def pull(_tag: str, dest: Path) -> None:
        (dest / rpm_resume.CHECKPOINT_FILE).write_text(
            json.dumps(
                {
                    "version": 1,
                    "signing_alias": "release4",
                    "unsigned_rpms": INPUTS,
                    "signed_artifact": expected_ref,
                }
            )
        )

    with (
        patch.object(rpm_resume.oras_utils, "oras_resolve", return_value="sha256:index"),
        patch.object(rpm_resume.oras_utils, "oras_pull", side_effect=pull),
        patch.object(rpm_resume, "find_resume_request") as find_ir,
    ):
        assert (
            rpm_resume.find_resume_checkpoint("quay.io/storage", INPUTS, "release4")
            == expected_ref
        )
    find_ir.assert_not_called()


def test_checkpoint_rejects_wrong_input_set() -> None:
    """A registry tag with altered metadata cannot authorize a retry."""

    def pull(_tag: str, dest: Path) -> None:
        (dest / rpm_resume.CHECKPOINT_FILE).write_text(
            json.dumps(
                {
                    "version": 1,
                    "signing_alias": "release4",
                    "unsigned_rpms": {"one-1-1.x86_64.rpm": "different"},
                    "signed_artifact": REF,
                }
            )
        )

    with (
        patch.object(rpm_resume.oras_utils, "oras_resolve", return_value="sha256:index"),
        patch.object(rpm_resume.oras_utils, "oras_pull", side_effect=pull),
        pytest.raises(RuntimeError, match="does not match"),
    ):
        rpm_resume.read_checkpoint("quay.io/storage", INPUTS, "release4")


def test_falls_back_to_recent_internal_request() -> None:
    """A completed IR can resume a run before its OCI checkpoint is written."""
    with (
        patch.object(rpm_resume, "read_checkpoint", return_value=None),
        patch.object(
            rpm_resume,
            "find_resume_request",
            return_value=("signing-1", REF),
        ),
    ):
        assert rpm_resume.find_resume_checkpoint("quay.io/storage", INPUTS, "release4") == REF


def test_explicit_request_cleanup_uses_retained_checkpoint() -> None:
    """A legacy IR name does not block recovery after its object is deleted."""
    with (
        patch.object(
            rpm_resume,
            "find_resume_request",
            side_effect=subprocess.CalledProcessError(1, ["kubectl", "get"]),
        ),
        patch.object(rpm_resume, "read_checkpoint", return_value=REF),
    ):
        assert (
            rpm_resume.find_resume_checkpoint(
                "quay.io/storage", INPUTS, "release4", "signing-1"
            )
            == REF
        )


def test_missing_checkpoint_is_distinct_from_registry_failure() -> None:
    """Only a missing tag permits the resolver to try a recent IR."""
    with patch.object(
        rpm_resume.oras_utils,
        "oras_resolve",
        side_effect=RuntimeError("manifest unknown"),
    ):
        assert rpm_resume.read_checkpoint("quay.io/storage", INPUTS, "release4") is None
    with (
        patch.object(
            rpm_resume.oras_utils,
            "oras_resolve",
            side_effect=RuntimeError("401 unauthorized"),
        ),
        pytest.raises(RuntimeError, match="unauthorized"),
    ):
        rpm_resume.read_checkpoint("quay.io/storage", INPUTS, "release4")


def test_records_checkpoint_with_matching_ir() -> None:
    """Write a retained OCI index only for a matching successful signing IR."""
    captured: dict = {}

    def run(cmd: list[str], *, cwd: Path, check: bool) -> None:
        captured["cmd"] = cmd
        captured["record"] = json.loads((cwd / rpm_resume.CHECKPOINT_FILE).read_text())
        assert check is True

    with (
        patch.object(
            rpm_resume,
            "find_resume_request",
            return_value=("signing-1", REF),
        ),
        patch.object(rpm_resume, "read_checkpoint", return_value=None),
        patch.object(rpm_resume.oci_auth, "auth_file", return_value=nullcontext(Path("auth"))),
        patch.object(rpm_resume.subprocess_cmd, "run_cmd", side_effect=run),
    ):
        rpm_resume.record_checkpoint(
            "quay.io/storage",
            INPUTS,
            "release4",
            "signing-1",
            REF,
            "7d",
        )

    assert "--annotation=quay.expires-after=7d" in captured["cmd"]
    assert captured["record"]["unsigned_rpms"] == INPUTS


def test_record_rejects_conflicting_checkpoint() -> None:
    """Do not replace a retained artifact with another signed byte sequence."""
    with (
        patch.object(rpm_resume, "find_resume_request", return_value=("signing-1", REF)),
        patch.object(
            rpm_resume,
            "read_checkpoint",
            return_value="oci:quay.io/signed@sha256:" + "b" * 64,
        ),
        patch.object(rpm_resume.subprocess_cmd, "run_cmd") as run,
        pytest.raises(RuntimeError, match="different signed RPM checkpoint"),
    ):
        rpm_resume.record_checkpoint(
            "quay.io/storage", INPUTS, "release4", "signing-1", REF, "7d"
        )
    run.assert_not_called()
