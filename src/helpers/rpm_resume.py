"""Find and validate a prior signed RPM artifact for a release retry."""

from __future__ import annotations

import argparse
import base64
import binascii
import gzip
import hashlib
import json
import logging
import re
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from release_service_utils.helpers import oci_auth, oras_utils, subprocess_cmd

FINGERPRINT_LABEL = "internal-services.appstudio.openshift.io/rpm-input-fingerprint"
SIGNING_PIPELINE_PATH = "pipelines/internal/direct-rpm-signing/direct-rpm-signing.yaml"
CHECKPOINT_FILE = "signed-rpm-checkpoint.json"
logger = logging.getLogger("rpm_resume")
SIGNED_REF = re.compile(r"oci:[A-Za-z0-9._:/-]+@sha256:[a-f0-9]{64}\Z")
OCI_REPOSITORY = re.compile(r"[A-Za-z0-9._:/-]+\Z")


def _valid_signed_ref(ref: Any) -> bool:
    return isinstance(ref, str) and SIGNED_REF.fullmatch(ref) is not None


def _rpm_inputs(entries: Iterable[tuple[str, str]]) -> dict[str, str]:
    inputs: dict[str, str] = {}
    for name, digest in entries:
        if Path(name).name != name or not name.endswith(".rpm") or not digest:
            raise ValueError(f"Missing RPM name or unsigned SHA-256: {name!r}")
        if name in inputs and inputs[name] != digest:
            raise ValueError(f"Conflicting unsigned SHA-256 for {name}")
        inputs[name] = digest
    if not inputs:
        raise ValueError("No RPMs found for signing resume")
    return inputs


def inputs_from_snapshot(snapshot: dict[str, Any]) -> dict[str, str]:
    """Read the unsigned RPM name and digest set from a filtered Snapshot."""
    return _rpm_inputs(
        (str(rpm["rpm"]), str(rpm.get("sha256") or ""))
        for component in snapshot.get("components", [])
        for rpm in component.get("rpmsToPublish", [])
    )


def inputs_from_artifact_json(artifact_json: dict[str, Any]) -> dict[str, str]:
    """Read the unsigned RPM name and digest set sent to the signer."""
    artifacts = artifact_json.get("artifacts") or {}
    return _rpm_inputs(
        (str(name), str(artifact.get("sha256") or "")) for name, artifact in artifacts.items()
    )


def fingerprint(inputs: dict[str, str], signing_alias: str) -> str:
    """Return a Kubernetes label value identifying the exact signing input."""
    if not signing_alias:
        raise ValueError("Signing alias is required for RPM resume")
    canonical = json.dumps(
        {"signing_alias": signing_alias, "rpms": sorted(inputs.items())},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()[:63]


def _decode_ir_artifact_json(encoded: str) -> dict[str, Any]:
    raw = base64.b64decode(encoded, validate=True)
    if raw.startswith(b"\x1f\x8b"):
        raw = gzip.decompress(raw)
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise TypeError("InternalRequest artifact_json is not a JSON object")
    return parsed


def _matching_request(
    request: dict[str, Any], inputs: dict[str, str], signing_alias: str
) -> tuple[str, str] | None:
    spec = request.get("spec") or {}
    params = spec.get("params") or {}
    pipeline_ref = (spec.get("pipeline") or {}).get("pipelineRef") or {}
    pipeline_params = {
        item.get("name"): item.get("value")
        for item in pipeline_ref.get("params") or []
        if isinstance(item, dict)
    }
    if (
        pipeline_params.get("pathInRepo") != SIGNING_PIPELINE_PATH
        or params.get("create_oci_artifact") != "true"
        or params.get("signing_alias") != signing_alias
    ):
        return None
    try:
        recorded = inputs_from_artifact_json(
            _decode_ir_artifact_json(str(params.get("artifact_json") or ""))
        )
    except (TypeError, ValueError, OSError, binascii.Error, json.JSONDecodeError):
        return None
    if recorded != inputs:
        return None
    status = request.get("status") or {}
    conditions = status.get("conditions") or []
    if not any(
        condition.get("type") == "Succeeded" and condition.get("status") == "True"
        for condition in conditions
    ):
        return None
    ref = (status.get("results") or {}).get("signed_rpms_oci_artifact")
    name = (request.get("metadata") or {}).get("name")
    if not _valid_signed_ref(ref) or not name:
        return None
    return str(name), ref


def find_resume_request(
    inputs: dict[str, str], signing_alias: str, explicit_name: str = ""
) -> tuple[str, str] | None:
    """Find a successful InternalRequest whose inputs and alias match exactly."""
    if explicit_name:
        cmd = ["kubectl", "get", "internalrequest", explicit_name, "-o", "json"]
    else:
        cmd = [
            "kubectl",
            "get",
            "internalrequests",
            "-l",
            f"{FINGERPRINT_LABEL}={fingerprint(inputs, signing_alias)}",
            "-o",
            "json",
        ]
    data = json.loads(subprocess_cmd.run_cmd(cmd, check=True).stdout)
    requests = [data] if explicit_name else data.get("items", [])
    requests.sort(
        key=lambda item: (item.get("metadata") or {}).get("creationTimestamp") or "",
        reverse=True,
    )
    for request in requests:
        match = _matching_request(request, inputs, signing_alias)
        if match:
            return match
    if explicit_name:
        raise RuntimeError(
            f"InternalRequest {explicit_name} does not contain a successful signing "
            "of this exact RPM set with the requested alias"
        )
    return None


def checkpoint_tag(oci_storage: str, inputs: dict[str, str], signing_alias: str) -> str:
    """Return the stable OCI tag for a signed RPM checkpoint."""
    repository = oci_storage.removeprefix("oci:").rstrip("/")
    if not repository or repository == "empty" or OCI_REPOSITORY.fullmatch(repository) is None:
        raise ValueError("A writable OCI repository is required for RPM resume")
    return f"{repository}:rpm-signing-{fingerprint(inputs, signing_alias)}"


def read_checkpoint(
    oci_storage: str, inputs: dict[str, str], signing_alias: str
) -> str | None:
    """Read a retained OCI checkpoint and validate its input set and alias."""
    tag = checkpoint_tag(oci_storage, inputs, signing_alias)
    try:
        digest = oras_utils.oras_resolve(tag)
    except RuntimeError as exc:
        message = str(exc).lower()
        if "manifest unknown" in message or "not found" in message or "404" in message:
            return None
        raise
    if digest is None:
        return None
    with tempfile.TemporaryDirectory(prefix="rpm-checkpoint-") as temp_dir:
        root = Path(temp_dir)
        oras_utils.oras_pull(tag, root)
        record = json.loads((root / CHECKPOINT_FILE).read_text(encoding="utf-8"))
    if (
        record.get("version") != 1
        or record.get("signing_alias") != signing_alias
        or record.get("unsigned_rpms") != inputs
    ):
        raise RuntimeError("Signed RPM checkpoint does not match the current input set")
    ref = record.get("signed_artifact")
    if not _valid_signed_ref(ref):
        raise RuntimeError("Signed RPM checkpoint has no valid artifact reference")
    return ref


def record_checkpoint(
    oci_storage: str,
    inputs: dict[str, str],
    signing_alias: str,
    internal_request: str,
    signed_artifact: str,
    expires_after: str,
) -> None:
    """Persist an input-keyed reference to a successful signing result."""
    prior = find_resume_request(inputs, signing_alias, internal_request)
    if prior is None or prior[1] != signed_artifact:
        raise RuntimeError("Signing InternalRequest result changed before checkpointing")
    existing = read_checkpoint(oci_storage, inputs, signing_alias)
    if existing:
        if existing != signed_artifact:
            raise RuntimeError("A different signed RPM checkpoint already exists")
        return
    tag = checkpoint_tag(oci_storage, inputs, signing_alias)
    record = {
        "version": 1,
        "signing_alias": signing_alias,
        "unsigned_rpms": inputs,
        "signed_artifact": signed_artifact,
        "internal_request": internal_request,
    }
    with tempfile.TemporaryDirectory(prefix="rpm-checkpoint-") as temp_dir:
        root = Path(temp_dir)
        (root / CHECKPOINT_FILE).write_text(json.dumps(record), encoding="utf-8")
        with oci_auth.auth_file(tag) as auth_path:
            subprocess_cmd.run_cmd(
                [
                    "oras",
                    "push",
                    "--registry-config",
                    str(auth_path),
                    f"--annotation=quay.expires-after={expires_after}",
                    tag,
                    f"{CHECKPOINT_FILE}:application/json",
                ],
                cwd=root,
                check=True,
            )


def find_resume_checkpoint(
    oci_storage: str,
    inputs: dict[str, str],
    signing_alias: str,
    explicit_internal_request: str = "",
) -> str | None:
    """Resolve a verified OCI checkpoint, or a recent matching signing request."""
    if explicit_internal_request:
        try:
            request = find_resume_request(inputs, signing_alias, explicit_internal_request)
        except subprocess.CalledProcessError:
            # The request may have been cleaned up while its OCI checkpoint
            # remains valid for the configured retry window.
            stored = read_checkpoint(oci_storage, inputs, signing_alias)
            if stored:
                return stored
            raise
        return request[1] if request else None
    stored = read_checkpoint(oci_storage, inputs, signing_alias)
    if stored:
        return stored
    request = find_resume_request(inputs, signing_alias)
    return request[1] if request else None


def validate_signed_artifact(ref: str, inputs: dict[str, str]) -> None:
    """Fetch a prior signed OCI artifact and verify its exact RPM filename set."""
    with tempfile.TemporaryDirectory(prefix="rpm-resume-") as temp_dir:
        archive = Path(temp_dir) / "signed-rpms.tar.gz"
        oras_utils.oras_blob_fetch(ref.removeprefix("oci:"), archive)
        found: set[str] = set()
        with tarfile.open(archive, "r:gz") as signed:
            for member in signed:
                if member.isdir():
                    continue
                name = member.name.removeprefix("./")
                if (
                    not member.isfile()
                    or "/" in name
                    or not name.endswith(".rpm")
                    or member.size == 0
                    or name in found
                ):
                    raise RuntimeError(f"Invalid signed RPM artifact member: {member.name}")
                found.add(name)
        if found != set(inputs):
            missing = sorted(set(inputs) - found)
            unexpected = sorted(found - set(inputs))
            raise RuntimeError(
                f"Signed RPM artifact does not match signing input: "
                f"missing={missing}, unexpected={unexpected}"
            )


def main() -> int:
    """Print an input fingerprint or validate and return a resume artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("fingerprint", "find", "validate", "record"))
    parser.add_argument("--artifact-json", required=True, type=Path)
    parser.add_argument("--signing-alias", required=True)
    parser.add_argument("--oci-storage", default="")
    parser.add_argument("--internal-request", default="")
    parser.add_argument("--signed-artifact", default="")
    parser.add_argument("--expires-after", default="7d")
    parser.add_argument("--result-file", type=Path)
    args = parser.parse_args()
    inputs = inputs_from_artifact_json(json.loads(args.artifact_json.read_text()))
    if args.action == "fingerprint":
        sys.stdout.write(fingerprint(inputs, args.signing_alias) + "\n")
        return 0
    if args.action == "find":
        ref = find_resume_checkpoint(
            args.oci_storage, inputs, args.signing_alias, args.internal_request
        )
        if ref:
            sys.stdout.write(ref + "\n")
        return 0
    if args.action == "record":
        if not args.internal_request or not args.signed_artifact:
            parser.error("record requires --internal-request and --signed-artifact")
        record_checkpoint(
            args.oci_storage,
            inputs,
            args.signing_alias,
            args.internal_request,
            args.signed_artifact,
            args.expires_after,
        )
        return 0
    if args.result_file is None:
        parser.error("validate requires --result-file")
    ref = find_resume_checkpoint(
        args.oci_storage, inputs, args.signing_alias, args.internal_request
    )
    if not ref or (args.signed_artifact and ref != args.signed_artifact):
        raise RuntimeError("Signed RPM checkpoint is missing or changed")
    validate_signed_artifact(ref, inputs)
    if (
        args.oci_storage
        and read_checkpoint(args.oci_storage, inputs, args.signing_alias) is None
    ):
        request = find_resume_request(inputs, args.signing_alias, args.internal_request)
        if request and request[1] == ref:
            record_checkpoint(
                args.oci_storage,
                inputs,
                args.signing_alias,
                request[0],
                ref,
                args.expires_after,
            )
    args.result_file.write_text(ref, encoding="utf-8")
    logger.info("Reusing signed RPM artifact: %s", ref)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
