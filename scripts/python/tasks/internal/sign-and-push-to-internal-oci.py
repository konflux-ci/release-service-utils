"""Entry point for the sign-and-push-to-internal-oci Tekton task.

Run each stage in sequence: extract artifacts, push unsigned content to Quay,
then sign Mac and Windows binaries using custom signing scripts that already
reside on the remote signing VMs.  Credentials are forwarded to the scripts
as environment variables over SSH.

Any exception raised by a stage is caught here: the Tekton result file
receives a short error description and the script exits with code 0 so
Tekton records the result text rather than masking it with a generic
step-failure message.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tekton

import extract_oci_artifacts
import push_oci_unsigned
import sign_mac
import sign_windows

PROG = "sign-and-push-to-internal-oci.py"


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
        "--mac-signing-args",
        nargs="*",
        default=[],
        help="Extra arguments for the custom Mac signing script",
    )
    p.add_argument(
        "--windows-signing-script",
        default=None,
        help="Path to custom Windows signing script on the remote host",
    )
    p.add_argument(
        "--windows-signing-args",
        nargs="*",
        default=[],
        help="Extra arguments for the custom Windows signing script",
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


def main(argv: list[str] | None = None) -> int:
    """Entry point: run every step in order and write Tekton results."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    args = parse_args(argv[1:] if argv is not None else None)

    rpath, cmap_path = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_CHECKSUM_MAP",
    )

    checksum_map_ref = ""
    try:
        extract_oci_artifacts.run(args.concurrent_limit)
        push_oci_unsigned.run(args.quay_url, args.pipeline_run_uid)
        sign_mac.run(
            args.quay_url,
            args.pipeline_run_uid,
            signing_script=args.mac_signing_script,
            signing_args=args.mac_signing_args,
            dest_quay_url=args.dest_quay_url,
            origin=args.origin,
        )
        sign_windows.run(
            args.quay_url,
            args.pipeline_run_uid,
            signing_script=args.windows_signing_script,
            signing_args=args.windows_signing_args,
            dest_quay_url=args.dest_quay_url,
            origin=args.origin,
        )
    except Exception as exc:
        rpath.write_text(f"{PROG}: ERROR {exc}", encoding="utf-8")
        cmap_path.write_text(checksum_map_ref, encoding="utf-8")
        return 0

    rpath.write_text("Success", encoding="utf-8")
    cmap_path.write_text(checksum_map_ref, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
