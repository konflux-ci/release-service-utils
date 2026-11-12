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


@pytest.mark.parametrize("name", ["../package.rpm", "package.txt", "package.rpm/"])
def test_invalid_unsigned_rpm_names_fail(name: str) -> None:
    """Reject names that cannot identify a flat RPM input."""
    with pytest.raises(ValueError, match="Missing RPM name"):
        rpm_resume.inputs_from_artifact_json({"artifacts": {name: {"sha256": "sha"}}})


def test_empty_rpm_set_and_signing_alias_fail() -> None:
    """Require both an RPM input and a signing identity for a checkpoint key."""
    with pytest.raises(ValueError, match="No RPMs"):
        rpm_resume.inputs_from_snapshot({"components": []})
    with pytest.raises(ValueError, match="Signing alias"):
        rpm_resume.fingerprint(INPUTS, "")


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


@pytest.mark.parametrize("encoded", ["not-base64", base64.b64encode(b"[]").decode()])
def test_request_with_invalid_artifact_json_is_ignored(encoded: str) -> None:
    """Ignore a prior request when its recorded RPM set cannot be decoded."""
    request = _request()
    request["spec"]["params"]["artifact_json"] = encoded
    response = subprocess.CompletedProcess([], 0, json.dumps({"items": [request]}), "")
    with patch.object(rpm_resume.subprocess_cmd, "run_cmd", return_value=response):
        assert rpm_resume.find_resume_request(INPUTS, "release4") is None


def test_request_without_name_is_ignored() -> None:
    """Require an InternalRequest name to identify the signing result."""
    request = _request()
    request["metadata"].pop("name")
    response = subprocess.CompletedProcess([], 0, json.dumps({"items": [request]}), "")
    with patch.object(rpm_resume.subprocess_cmd, "run_cmd", return_value=response):
        assert rpm_resume.find_resume_request(INPUTS, "release4") is None


def test_validate_signed_artifact_file_set() -> None:
    """Accept the signed RPM archive format produced by the signing service."""

    def fetch(_ref: str, archive: Path) -> None:
        with tarfile.open(archive, "w:gz") as output:
            for name in ("digests", "digests.signed"):
                content = b"signed manifest"
                info = tarfile.TarInfo(f"./{name}")
                info.size = len(content)
                output.addfile(info, io.BytesIO(content))
            for name in INPUTS:
                for member_name in (name, f"{name}.hdr"):
                    content = b"signed RPM"
                    info = tarfile.TarInfo(f"./{member_name}")
                    info.size = len(content)
                    output.addfile(info, io.BytesIO(content))

    with patch.object(rpm_resume.oras_utils, "oras_blob_fetch", side_effect=fetch):
        rpm_resume.validate_signed_artifact(REF, INPUTS)


def test_validate_signed_artifact_ignores_directory_entry() -> None:
    """Allow tar directory entries when all required RPM files are present."""

    def fetch(_ref: str, archive: Path) -> None:
        with tarfile.open(archive, "w:gz") as output:
            directory = tarfile.TarInfo("./")
            directory.type = tarfile.DIRTYPE
            output.addfile(directory)
            for name in INPUTS:
                info = tarfile.TarInfo(name)
                info.size = 1
                output.addfile(info, io.BytesIO(b"x"))

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


def test_validate_signed_artifact_rejects_orphan_header() -> None:
    """A header sidecar cannot stand in for a signed RPM."""

    def fetch(_ref: str, archive: Path) -> None:
        with tarfile.open(archive, "w:gz") as output:
            for name in INPUTS:
                content = b"signed RPM"
                info = tarfile.TarInfo(name)
                info.size = len(content)
                output.addfile(info, io.BytesIO(content))
            header = tarfile.TarInfo("unknown-1-1.x86_64.rpm.hdr")
            header.size = 1
            output.addfile(header, io.BytesIO(b"x"))

    with (
        patch.object(rpm_resume.oras_utils, "oras_blob_fetch", side_effect=fetch),
        pytest.raises(RuntimeError, match="orphan headers"),
    ):
        rpm_resume.validate_signed_artifact(REF, INPUTS)


@pytest.mark.parametrize(
    "member_name,content",
    [
        ("digests", b""),
        ("digests.signed", b""),
        ("other.txt", b"data"),
        ("nested/one-1-1.x86_64.rpm", b"data"),
        ("one-1-1.x86_64.rpm", b""),
    ],
)
def test_validate_signed_artifact_rejects_invalid_members(
    member_name: str, content: bytes
) -> None:
    """Reject empty metadata and files outside the expected flat RPM set."""

    def fetch(_ref: str, archive: Path) -> None:
        with tarfile.open(archive, "w:gz") as output:
            info = tarfile.TarInfo(member_name)
            info.size = len(content)
            output.addfile(info, io.BytesIO(content))

    with (
        patch.object(rpm_resume.oras_utils, "oras_blob_fetch", side_effect=fetch),
        pytest.raises(RuntimeError, match="Invalid signed RPM artifact member"),
    ):
        rpm_resume.validate_signed_artifact(REF, INPUTS)


def test_validate_signed_artifact_rejects_unexpected_rpm() -> None:
    """Reject an archive with an RPM that was absent from the unsigned input."""

    def fetch(_ref: str, archive: Path) -> None:
        with tarfile.open(archive, "w:gz") as output:
            for name in (*INPUTS, "unexpected-1-1.x86_64.rpm"):
                info = tarfile.TarInfo(name)
                info.size = 1
                output.addfile(info, io.BytesIO(b"x"))

    with (
        patch.object(rpm_resume.oras_utils, "oras_blob_fetch", side_effect=fetch),
        pytest.raises(RuntimeError, match="unexpected=\\['unexpected-1-1.x86_64.rpm'\\]"),
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

    def pull(_tag: str, dest: Path, *, stream_stdout: bool) -> None:
        assert stream_stdout is False
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

    def pull(_tag: str, dest: Path, *, stream_stdout: bool) -> None:
        assert stream_stdout is False
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


@pytest.mark.parametrize("repository", ["", "empty", "quay.io/storage?bad"])
def test_checkpoint_tag_requires_repository(repository: str) -> None:
    """Reject unusable OCI storage values before attempting registry access."""
    with pytest.raises(ValueError, match="writable OCI repository"):
        rpm_resume.checkpoint_tag(repository, INPUTS, "release4")


def test_read_checkpoint_without_resolved_tag_returns_none() -> None:
    """Treat a tag with no resolved digest as an absent checkpoint."""
    with (
        patch.object(rpm_resume.oras_utils, "oras_resolve", return_value=None),
        patch.object(rpm_resume.oras_utils, "oras_pull") as pull,
    ):
        assert rpm_resume.read_checkpoint("quay.io/storage", INPUTS, "release4") is None
    pull.assert_not_called()


def test_checkpoint_rejects_invalid_signed_reference() -> None:
    """Reject a stored checkpoint whose artifact reference is not immutable."""

    def pull(_tag: str, dest: Path, *, stream_stdout: bool) -> None:
        (dest / rpm_resume.CHECKPOINT_FILE).write_text(
            json.dumps(
                {
                    "version": 1,
                    "signing_alias": "release4",
                    "unsigned_rpms": INPUTS,
                    "signed_artifact": "oci:quay.io/signed:latest",
                }
            )
        )

    with (
        patch.object(rpm_resume.oras_utils, "oras_resolve", return_value="sha256:index"),
        patch.object(rpm_resume.oras_utils, "oras_pull", side_effect=pull),
        pytest.raises(RuntimeError, match="no valid artifact reference"),
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


@pytest.mark.parametrize("request_name,expected", [("signing-1", REF), (None, None)])
def test_explicit_request_result_is_returned(
    request_name: str | None, expected: str | None
) -> None:
    """Use only the named signing request when the caller supplied one."""
    match = (request_name, REF) if request_name else None
    with (
        patch.object(rpm_resume, "find_resume_request", return_value=match) as find,
        patch.object(rpm_resume, "read_checkpoint") as read,
    ):
        assert (
            rpm_resume.find_resume_checkpoint(
                "quay.io/storage", INPUTS, "release4", "signing-1"
            )
            == expected
        )
    find.assert_called_once_with(INPUTS, "release4", "signing-1")
    read.assert_not_called()


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


def test_explicit_request_cleanup_without_checkpoint_fails() -> None:
    """Preserve the kubectl failure when neither request nor checkpoint exists."""
    with (
        patch.object(
            rpm_resume,
            "find_resume_request",
            side_effect=subprocess.CalledProcessError(1, ["kubectl", "get"]),
        ),
        patch.object(rpm_resume, "read_checkpoint", return_value=None),
        pytest.raises(subprocess.CalledProcessError),
    ):
        rpm_resume.find_resume_checkpoint("quay.io/storage", INPUTS, "release4", "signing-1")


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


def test_record_rejects_changed_request_result() -> None:
    """Do not retain an artifact that differs from the signing request result."""
    with (
        patch.object(rpm_resume, "find_resume_request", return_value=("signing-1", REF)),
        patch.object(rpm_resume, "read_checkpoint") as read,
        pytest.raises(RuntimeError, match="result changed"),
    ):
        rpm_resume.record_checkpoint(
            "quay.io/storage", INPUTS, "release4", "signing-1", "oci:other", "7d"
        )
    read.assert_not_called()


def test_record_matching_checkpoint_is_idempotent() -> None:
    """Avoid pushing another checkpoint when the stored reference already matches."""
    with (
        patch.object(rpm_resume, "find_resume_request", return_value=("signing-1", REF)),
        patch.object(rpm_resume, "read_checkpoint", return_value=REF),
        patch.object(rpm_resume.subprocess_cmd, "run_cmd") as run,
    ):
        rpm_resume.record_checkpoint(
            "quay.io/storage", INPUTS, "release4", "signing-1", REF, "7d"
        )
    run.assert_not_called()


def test_cli_fingerprint_uses_artifact_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Print the fingerprint used to label the signing InternalRequest."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "fingerprint",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
        ],
    )

    assert rpm_resume.main() == 0
    assert capsys.readouterr().out == rpm_resume.fingerprint(INPUTS, "release4") + "\n"


def test_cli_find_prints_matching_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return the prior signed reference to the signing Task."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "find",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
            "--oci-storage",
            "quay.io/storage",
        ],
    )
    with patch.object(rpm_resume, "find_resume_checkpoint", return_value=REF) as find:
        assert rpm_resume.main() == 0
    find.assert_called_once_with("quay.io/storage", INPUTS, "release4", "")
    assert capsys.readouterr().out == REF + "\n"


def test_cli_find_without_checkpoint_prints_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return an empty result when no matching signing attempt exists."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "find",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
        ],
    )
    with patch.object(rpm_resume, "find_resume_checkpoint", return_value=None):
        assert rpm_resume.main() == 0
    assert capsys.readouterr().out == ""


def test_cli_record_requires_request_and_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject an incomplete record command before writing to OCI storage."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "record",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        rpm_resume.main()


def test_cli_validate_requires_result_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a validate command that has nowhere to write its verified result."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "validate",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        rpm_resume.main()


def test_cli_record_writes_checkpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pass the successful signing request through to checkpoint storage."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "record",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
            "--oci-storage",
            "quay.io/storage",
            "--internal-request",
            "signing-1",
            "--signed-artifact",
            REF,
        ],
    )
    with patch.object(rpm_resume, "record_checkpoint") as record:
        assert rpm_resume.main() == 0
    record.assert_called_once_with(
        "quay.io/storage", INPUTS, "release4", "signing-1", REF, "7d"
    )


def test_cli_validate_backfills_and_returns_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Validate a recent signing result and retain its reference for retries."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    result = tmp_path / "signed-artifact-result"
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "validate",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
            "--oci-storage",
            "quay.io/storage",
            "--signed-artifact",
            REF,
            "--result-file",
            str(result),
        ],
    )
    with (
        patch.object(rpm_resume, "find_resume_checkpoint", return_value=REF),
        patch.object(rpm_resume, "validate_signed_artifact") as validate,
        patch.object(rpm_resume, "read_checkpoint", return_value=None),
        patch.object(rpm_resume, "find_resume_request", return_value=("signing-1", REF)),
        patch.object(rpm_resume, "record_checkpoint") as record,
    ):
        assert rpm_resume.main() == 0
    validate.assert_called_once_with(REF, INPUTS)
    record.assert_called_once_with(
        "quay.io/storage", INPUTS, "release4", "signing-1", REF, "7d"
    )
    assert result.read_text() == REF


def test_cli_validate_rejects_changed_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not write a Tekton result for an artifact that changed during retry."""
    artifact_json = tmp_path / "artifacts.json"
    artifact_json.write_text(
        json.dumps({"artifacts": {name: {"sha256": sha} for name, sha in INPUTS.items()}})
    )
    result = tmp_path / "signed-artifact-result"
    monkeypatch.setattr(
        "sys.argv",
        [
            "rpm_resume",
            "validate",
            "--artifact-json",
            str(artifact_json),
            "--signing-alias",
            "release4",
            "--oci-storage",
            "quay.io/storage",
            "--signed-artifact",
            "oci:quay.io/other@sha256:" + "b" * 64,
            "--result-file",
            str(result),
        ],
    )
    with (
        patch.object(rpm_resume, "find_resume_checkpoint", return_value=REF),
        pytest.raises(RuntimeError, match="missing or changed"),
    ):
        rpm_resume.main()
    assert not result.exists()
