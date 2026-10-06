"""Sign Python wheels and sdists with SLSA provenance attestations via cosign.

Creates SLSA v1 provenance attestations for Python wheels and sdists, signs
them with ``cosign attest-blob`` using an AWS KMS key, and converts the
resulting DSSE envelopes to PEP 740 format. Chains provenance predicates
from the build pipeline are incorporated when available; otherwise a minimal
predicate is used.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from release_service_utils.helpers import (
    authentication,
    file,
    retry,
    subprocess_cmd,
    tekton,
)
from release_service_utils.helpers.logger import logger


def load_chains_predicate(provenance_dir: Path) -> dict[str, Any] | None:
    """Load the first Chains provenance predicate from *provenance_dir*.

    All wheels in a release come from the same build pipeline, so one
    predicate is sufficient.
    """
    if not provenance_dir.is_dir():
        return None

    for prov_file in sorted(provenance_dir.glob("sha256:*.json")):
        predicate = file.load_json_dict(prov_file).get("predicate")
        if isinstance(predicate, dict):
            logger.info("Loaded Chains provenance from %s", prov_file.name)
            return predicate
        break

    return None


def build_slsa_predicate(
    chains_predicate: dict[str, Any] | None,
    release_time: str,
) -> dict[str, Any]:
    """Build a SLSA v1 provenance predicate.

    When *chains_predicate* is available, maps its fields to v1 format
    (handling both v1 and v0.2 layouts).
    """
    default_build_type = "https://konflux-ci.dev/PythonWheelBuild@v1"
    default_builder_id = "https://konflux-ci.dev/calunga"

    if chains_predicate is None:
        return {
            "buildDefinition": {
                "buildType": default_build_type,
                "externalParameters": {},
                "resolvedDependencies": [],
            },
            "runDetails": {
                "builder": {"id": default_builder_id},
                "metadata": {"finishedOn": release_time},
            },
        }

    if "buildDefinition" in chains_predicate:
        build_def = chains_predicate["buildDefinition"]
    else:
        build_def = {
            "buildType": chains_predicate.get("buildType", default_build_type),
            "externalParameters": chains_predicate.get("invocation", {}).get("parameters", {}),
            "internalParameters": chains_predicate.get("invocation", {}).get(
                "environment", {}
            ),
            "resolvedDependencies": chains_predicate.get("materials", []),
        }

    if "runDetails" in chains_predicate:
        run_details = chains_predicate["runDetails"]
    else:
        metadata_src = chains_predicate.get("metadata", {})
        run_details = {
            "builder": {
                "id": chains_predicate.get("builder", {}).get("id", default_builder_id),
            },
            "metadata": {
                "invocationId": metadata_src.get("buildInvocationId"),
                "startedOn": metadata_src.get("buildStartedOn"),
                "finishedOn": metadata_src.get("buildFinishedOn", release_time),
            },
        }

    return {"buildDefinition": build_def, "runDetails": run_details}


def convert_dsse_to_pep740(dsse: dict[str, Any]) -> dict[str, Any]:
    """Convert a DSSE envelope to PEP 740 attestation format."""
    return {
        "version": 1,
        "verification_material": None,
        "envelope": {
            "statement": dsse["payload"],
            "signature": dsse["signatures"][0]["sig"],
        },
    }


def _run_cosign(
    wheel: Path,
    predicate_file: Path,
    sign_key: str,
    rekor_url: str | None,
    dsse_file: Path,
    aws_env: dict[str, str],
    max_attempts: int,
) -> None:
    """Run ``cosign attest-blob`` with retry."""
    cmd: list[str] = [
        "cosign",
        "attest-blob",
        str(wheel),
        f"--predicate={predicate_file}",
        "--type=https://slsa.dev/provenance/v1",
        "--key",
        sign_key,
        "--yes",
    ]
    if rekor_url:
        cmd += ["-y", f"--rekor-url={rekor_url}"]
    else:
        cmd.append("--tlog-upload=false")
    cmd.append(f"--output-file={dsse_file}")

    logger.info("Signing attestation for %s with cosign (AWS KMS)", wheel.name)
    try:
        retry.retry_with_exponential_backoff(
            lambda: subprocess_cmd.run_cmd(cmd, env=aws_env, check=True),
            max_attempts=max_attempts,
            retry_on=subprocess.CalledProcessError,
            base_sleep_seconds=2,
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            "cosign attest-blob failed for %s after %d attempt(s): %s",
            wheel.name,
            max_attempts,
            (e.stderr or "").strip(),
        )
        raise


def _sign_artifact(
    wheel: Path,
    wheels_dir: Path,
    chains_predicate: dict[str, Any] | None,
    release_time: str,
    sign_key: str,
    rekor_url: str | None,
    aws_env: dict[str, str],
    max_attempts: int,
) -> None:
    """Create a signed PEP 740 attestation for a single wheel or sdist."""
    logger.info("Processing: %s (SHA256: %s)", wheel.name, file.sha256(wheel))

    predicate = build_slsa_predicate(chains_predicate, release_time)
    predicate_file = file.make_tempfile_path(
        f"{wheel.name}.predicate.",
        json.dumps(predicate, separators=(",", ":")).encode("utf-8"),
    )
    dsse_file = file.make_tempfile_path(f"{wheel.name}.dsse.")

    try:
        _run_cosign(
            wheel,
            predicate_file,
            sign_key,
            rekor_url,
            dsse_file,
            aws_env,
            max_attempts,
        )

        dsse = file.load_json_dict(dsse_file)
        pep740 = convert_dsse_to_pep740(dsse)

        att_file = wheels_dir / f"{wheel.name}.attestation"
        att_file.write_text(
            json.dumps(pep740, separators=(",", ":")),
            encoding="utf-8",
        )
        logger.info("Created PEP 740 attestation: %s", att_file.name)
    finally:
        predicate_file.unlink(missing_ok=True)
        dsse_file.unlink(missing_ok=True)


def run(
    *,
    data_dir: Path,
    files_dir: str,
    secrets_dir: Path,
    max_attempts: int,
    result_path: Path,
) -> None:
    """Sign all wheels and sdists and write PEP 740 attestations."""
    wheels_dir = file.resolve_path_under_base(data_dir, files_dir)
    if not wheels_dir.is_dir():
        raise FileNotFoundError(f"Files directory does not exist: {wheels_dir}")

    aws_env = {
        "AWS_DEFAULT_REGION": authentication.read_mounted_text(
            secrets_dir, "AWS_DEFAULT_REGION"
        ),
        "AWS_ACCESS_KEY_ID": authentication.read_mounted_text(
            secrets_dir, "AWS_ACCESS_KEY_ID"
        ),
        "AWS_SECRET_ACCESS_KEY": authentication.read_mounted_text(
            secrets_dir, "AWS_SECRET_ACCESS_KEY"
        ),
    }
    sign_key = authentication.read_mounted_text(secrets_dir, "SIGN_KEY")

    rekor_url: str | None = None
    if (secrets_dir / "REKOR_URL").is_file():
        rekor_url = authentication.read_mounted_text(secrets_dir, "REKOR_URL") or None

    release_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("Release timestamp: %s", release_time)

    provenance_dir = wheels_dir / "chains-provenance"
    chains_predicate = load_chains_predicate(provenance_dir)
    if chains_predicate is None:
        logger.warning("No Chains provenance found, falling back to minimal predicate")

    att_count = 0
    for pattern in ("*.whl", "*.tar.gz"):
        for wheel in sorted(wheels_dir.glob(pattern)):
            if not wheel.is_file():
                continue
            _sign_artifact(
                wheel,
                wheels_dir,
                chains_predicate,
                release_time,
                sign_key,
                rekor_url,
                aws_env,
                max_attempts,
            )
            att_count += 1

    if provenance_dir.is_dir():
        shutil.rmtree(provenance_dir)

    logger.info("Attestation count: %d", att_count)
    result_path.write_text(str(att_count), encoding="utf-8")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--files-dir", required=True)
    parser.add_argument("--secrets-dir", default="/etc/secrets")
    parser.add_argument("--retries", type=int, default=3)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and sign Python wheels."""
    args = _parse_args(argv)
    (result_path,) = tekton.result_paths_from_env("RESULT_ATTESTATION_COUNT")
    run(
        data_dir=Path(args.data_dir),
        files_dir=args.files_dir,
        secrets_dir=Path(args.secrets_dir),
        max_attempts=args.retries + 1,
        result_path=result_path,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
