"""Entry point for the sign-and-push-to-internal-oci Tekton task.

Run each stage in sequence: extract artifacts, push unsigned content to Quay,
then sign Mac and Windows binaries using custom signing scripts that already
reside on the remote signing VMs.  Credentials are forwarded to the scripts
as environment variables over SSH.

Any exception raised by a stage is caught here: the Tekton result file
receives a short error description that names the failing phase and the
underlying cause, and the script exits with code 0 so Tekton records the
result text rather than masking it with a generic step-failure message.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from release_service_utils.helpers import (
    extract_oci_artifacts,
    push_oci_unsigned,
    sign_mac,
    sign_windows,
    tekton,
)
from release_service_utils.helpers.redact import redact_secrets

PROG = "sign-and-push-to-internal-oci.py"

logger = logging.getLogger(__name__)

T = TypeVar("T")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Return parsed arguments for the wrapper."""
    p = argparse.ArgumentParser(
        prog=PROG,
        description="Run all sign-and-push-to-internal-oci steps sequentially.",
    )
    p.add_argument(
        "--concurrent-limit",
        type=int,
        default=3,
        help="Maximum number of images to pull at once",
    )
    p.add_argument("--quay-url", required=True, help="Quay repository URL base")
    p.add_argument(
        "--pipeline-run-uid",
        required=True,
        help="Unique ID for this pipeline run",
    )
    p.add_argument(
        "--mac-signing-script",
        default=None,
        help="Path to custom Mac signing script on the remote host",
    )
    p.add_argument(
        "--windows-signing-script",
        default=None,
        help="Path to custom Windows signing script on the remote host",
    )
    p.add_argument(
        "--dest-quay-url",
        default=None,
        help="Destination Quay URL base for signed artifacts",
    )
    p.add_argument(
        "--origin",
        required=True,
        help="Origin tenant namespace that triggered the release",
    )
    return p.parse_args(argv)


def _call_phase(action: str, fn: Callable[..., T], *args: object, **kwargs: object) -> T:
    """Run one pipeline phase and attach *action* to any failure."""
    logger.info("Starting: %s", action)
    try:
        result = fn(*args, **kwargs)
    except tekton.CheckStepError as exc:
        raise tekton.CheckStepError(action, exc.cause) from exc
    except Exception as exc:
        raise tekton.CheckStepError(action, exc) from exc
    logger.info("Finished: %s", action)
    return result


def _write_failure(rpath: Path, exc: BaseException) -> None:
    """Write the Tekton result body for a failed run and log a redacted summary."""
    action = (
        exc.action
        if isinstance(exc, tekton.CheckStepError)
        else "signing and pushing to internal OCI"
    )
    logger.error("%s failed while %s: %s", PROG, action, redact_secrets(str(exc)))
    tekton.write_failure_result(rpath, PROG, exc, workflow_action=action)


def main(argv: list[str] | None = None) -> int:
    """Entry point: run every step in order and write Tekton results."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    args = parse_args(argv[1:] if argv is not None else None)

    (rpath,) = tekton.result_paths_from_env("RESULT_RESULT")

    try:
        _call_phase(
            "extracting OCI artifacts", extract_oci_artifacts.run, args.concurrent_limit
        )
        _call_phase(
            "pushing unsigned artifacts",
            push_oci_unsigned.run,
            args.quay_url,
            args.pipeline_run_uid,
        )
        _call_phase(
            "signing Mac artifacts",
            sign_mac.run_custom_signing,
            args.quay_url,
            args.pipeline_run_uid,
            signing_script=args.mac_signing_script,
            dest_quay_url=args.dest_quay_url,
            origin=args.origin,
        )
        _call_phase(
            "signing Windows artifacts",
            sign_windows.run_custom_signing,
            args.quay_url,
            args.pipeline_run_uid,
            signing_script=args.windows_signing_script,
            dest_quay_url=args.dest_quay_url,
            origin=args.origin,
        )
    except Exception as exc:
        _write_failure(rpath, exc)
        return 0

    rpath.write_text("Success", encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv))
