#!/usr/bin/env python3
"""Sign a SHA256SUMS checksum file via the generic-signing pipeline.

Directly signs the release checksum file by isolating it into its own Trusted
Artifact and submitting a ``generic-signing`` InternalRequest (``detachsign``).
The resulting ASCII-armored detached signature is dearmored and written as
``<checksum>.sig`` next to the checksum file.

The catalog task invokes this three times, straddling the Trusted Artifact
stepactions that build the isolated input artifact and restore the signed
output artifact:

* ``prepare`` -- discover the single ``*SHA256SUMS`` file, skip the remaining
  signing steps when its existing signature is valid, or copy it into an
  isolated directory so a create-trusted-artifact stepaction can build a
  checksum-only input Trusted Artifact.
* ``sign`` -- submit the signing request and write the output Trusted Artifact
  URI to a file for a use-trusted-artifact stepaction to restore.
* ``finalize`` -- read the restored signing results, dearmor the detached
  signature, and place ``<checksum>.sig`` next to the checksum file.

Idempotent: if a valid signature already exists next to the checksum file, the
catalog skips the signing steps; an invalid/corrupt one is removed and
re-created.
"""

from __future__ import annotations

import argparse
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from release_service_utils.helpers import file, kubectl, tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd
from release_service_utils.tasks.managed.direct_sign_generic import direct_sign_generic

SIGN_METHOD = "detachsign"
DEFAULT_CONFIG_MAP_NAME = "signing-config-map"
RESULTS_FILENAME = "results.json"


@dataclass(frozen=True)
class SigningConfig:
    """Store signing values resolved from the selected ConfigMap."""

    keyname: str
    kerberos_keytab_secret: str = ""
    kerberos_keytab: str = ""
    kerberos_principal: str = ""


def find_checksum_file(binaries_dir: Path) -> Path:
    """Return the single SHA256SUMS checksum file in *binaries_dir*.

    Matches ``*SHA256SUMS*`` files that are not themselves a ``.sig``.

    Raises:
        FileNotFoundError: If no checksum file is found.
        ValueError: If more than one checksum file is found.

    """
    candidates = [
        c
        for c in sorted(binaries_dir.iterdir())
        if c.is_file() and "SHA256SUMS" in c.name and not c.name.endswith(".sig")
    ]
    if not candidates:
        raise FileNotFoundError(f"No SHA256SUMS checksum file found in {binaries_dir}")
    if len(candidates) > 1:
        names = ", ".join(c.name for c in candidates)
        raise ValueError(f"Expected exactly one SHA256SUMS file, found: {names}")
    return candidates[0]


def signature_is_valid(sig_path: Path) -> bool:
    """Return True if *sig_path* is a non-empty, parseable GPG file."""
    if not sig_path.is_file() or sig_path.stat().st_size == 0:
        return False
    result = run_cmd(["gpg", "--list-packets", str(sig_path)], check=False)
    return result.returncode == 0


def resolve_signing_config(data_file: Path) -> SigningConfig:
    """Return signing values from the ConfigMap selected in *data_file*.

    Reads ``data.sign.configMapName`` from
    *data_file*, fetches that ConfigMap, and returns the signing key and any
    optional Kerberos overrides it provides.
    """
    data = file.load_json_dict(data_file)
    config_map_name = data.get("sign", {}).get("configMapName") or DEFAULT_CONFIG_MAP_NAME
    configmap = kubectl.get_configmap(config_map_name)
    config_data = configmap["data"]
    return SigningConfig(
        keyname=config_data["SIG_KEY_NAME"],
        kerberos_keytab_secret=config_data.get("KERBEROS_KEYTAB_SECRET", ""),
        kerberos_keytab=config_data.get("KERBEROS_KEYTAB", ""),
        kerberos_principal=config_data.get("KERBEROS_PRINCIPAL", ""),
    )


def prepare(binaries_dir: Path, isolated_dir: Path) -> tuple[bool, str]:
    """Handle the signature and return its validity plus the checksum filename."""
    checksum_file = find_checksum_file(binaries_dir)
    sig_path = checksum_file.with_name(f"{checksum_file.name}.sig")
    if signature_is_valid(sig_path):
        logger.info("Valid signature already exists at %s; skipping signing.", sig_path)
        return True, checksum_file.name
    if sig_path.exists():
        logger.info("Removing invalid signature before signing: %s", sig_path)
        sig_path.unlink()

    isolated_dir.mkdir(parents=True, exist_ok=True)
    destination = isolated_dir / checksum_file.name
    shutil.copy2(checksum_file, destination)
    logger.info("Copied %s to %s for signing.", checksum_file.name, isolated_dir)
    return False, checksum_file.name


def _signature_for_checksum(results: dict, results_dir: Path, checksum_name: str) -> Path:
    """Return the signature file path matching *checksum_name* from results.json."""
    for entry in results.get("results", []):
        if Path(entry.get("file", "")).name == checksum_name:
            signature_file = entry.get("signature_file", "")
            if not signature_file:
                raise RuntimeError(
                    f"results.json entry for {checksum_name} has no signature_file."
                )
            return results_dir / signature_file
    raise RuntimeError(f"No signature found for {checksum_name} in results.json.")


def sign(
    data_file: Path,
    input_artifact_uri: str,
    output_artifact_file: Path,
    requester: str,
    pipelinerun_uid: str,
    task_id: str,
    signing_repo: str,
    signing_revision: str,
    ta_task_git_url: str,
    ta_task_git_revision: str,
    oci_storage: str,
    oras_options: str,
    request_timeout: int,
) -> None:
    """Submit a signing request and record the output Trusted Artifact URI.

    Signature validity and cleanup are handled by ``prepare`` before this
    command is run.
    """
    signing_config = resolve_signing_config(data_file)
    request = direct_sign_generic.SigningRequest(
        source_data_artifact=input_artifact_uri,
        keyname=signing_config.keyname,
        sign_method=SIGN_METHOD,
        onbehalfof=requester,
        signing_repo=signing_repo,
        signing_revision=signing_revision,
        oci_storage=oci_storage,
        oras_options=oras_options,
        ta_task_git_url=ta_task_git_url,
        ta_task_git_revision=ta_task_git_revision,
        task_id=task_id,
        pipelinerun_uid=pipelinerun_uid,
        request_timeout=request_timeout,
        kerberos_keytab_secret=signing_config.kerberos_keytab_secret,
        kerberos_keytab=signing_config.kerberos_keytab,
        kerberos_principal=signing_config.kerberos_principal,
    )
    results = direct_sign_generic.submit(request)

    if not (output_ta_uri := results.get("sourceDataArtifact")):
        raise RuntimeError("generic-signing returned an empty sourceDataArtifact result.")

    output_artifact_file.write_text(output_ta_uri, encoding="utf-8")
    logger.info("Signing output Trusted Artifact recorded: %s", output_ta_uri)


def finalize(binaries_dir: Path, signed_output_dir: Path, checksum_name: str) -> None:
    """Dearmor the detached signature and place it next to the checksum file.

    Read ``results.json`` from *signed_output_dir* (restored by a
    use-trusted-artifact stepaction), locate the signature for *checksum_name*,
    dearmor it, and write the binary ``<checksum>.sig`` next to the original
    checksum file.
    """
    if not checksum_name or Path(checksum_name).name != checksum_name:
        raise ValueError(f"Expected a checksum filename, got: {checksum_name!r}")
    checksum_file = binaries_dir / checksum_name
    sig_path = checksum_file.with_name(f"{checksum_file.name}.sig")

    results = file.load_json_dict(signed_output_dir / RESULTS_FILENAME)
    armored_sig = _signature_for_checksum(results, signed_output_dir, checksum_file.name)

    run_cmd(
        ["gpg", "--dearmor", "--output", str(sig_path)],
        stdin=armored_sig.read_text(encoding="ascii"),
        check=True,
    )
    if not sig_path.is_file() or sig_path.stat().st_size == 0:
        raise RuntimeError(f"gpg produced an empty signature for {checksum_file.name}.")
    logger.info("Signature written to %s (%d bytes).", sig_path, sig_path.stat().st_size)


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(description="Sign a SHA256SUMS checksum file.")
    parser.add_argument(
        "command",
        choices=["prepare", "sign", "finalize"],
        help="Which step to run",
    )
    return parser


def main() -> int:
    """Read Tekton env vars and run the requested checksum-signing step."""
    args = setup_argparser().parse_args()

    data_dir = Path(tekton.require_env("DATA_DIR"))
    binaries_path = tekton.require_env("BINARIES_PATH")
    binaries_dir = data_dir / binaries_path

    if args.command == "prepare":
        signature_valid, checksum_name = prepare(
            binaries_dir=binaries_dir,
            isolated_dir=data_dir / tekton.require_env("ISOLATED_INPUT_DIR"),
        )
        Path(tekton.require_env("SIGNATURE_VALID_RESULT")).write_text(
            str(signature_valid).lower(), encoding="utf-8"
        )
        Path(tekton.require_env("CHECKSUM_FILE_RESULT")).write_text(
            checksum_name, encoding="utf-8"
        )
        return 0

    if args.command == "sign":
        input_artifact_file = Path(tekton.require_env("INPUT_ARTIFACT_FILE"))
        sign(
            data_file=data_dir / tekton.require_env("DATA_PATH"),
            input_artifact_uri=input_artifact_file.read_text(encoding="utf-8").strip(),
            output_artifact_file=Path(tekton.require_env("OUTPUT_ARTIFACT_FILE")),
            requester=tekton.require_env("REQUESTER"),
            pipelinerun_uid=tekton.require_env("PIPELINERUN_UID"),
            task_id=os.environ.get("TASK_ID", "").strip(),
            signing_repo=tekton.require_env("SIGNING_REPO"),
            signing_revision=tekton.require_env("SIGNING_REVISION"),
            ta_task_git_url=tekton.require_env("TASK_GIT_URL"),
            ta_task_git_revision=tekton.require_env("TASK_GIT_REVISION"),
            oci_storage=tekton.require_env("OCI_STORAGE"),
            oras_options=os.environ.get("ORAS_OPTIONS", "").strip(),
            request_timeout=int(os.environ.get("REQUEST_TIMEOUT", "1800")),
        )
        return 0

    finalize(
        binaries_dir=binaries_dir,
        signed_output_dir=Path(tekton.require_env("SIGNED_OUTPUT_DIR")),
        checksum_name=tekton.require_env("CHECKSUM_FILE"),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
