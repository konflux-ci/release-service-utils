#!/usr/bin/env python3
"""Delete InternalRequest CRs associated with a specific PipelineRun."""

from __future__ import annotations

import argparse
import subprocess

from logger import logger

PROG = "cleanup_internal_requests.py"

LABEL_KEY = "internal-services.appstudio.openshift.io/pipelinerun-uid"


def cleanup(pipeline_run_uid: str) -> None:
    """Delete InternalRequest resources matching the PipelineRun UID."""
    label_selector = f"{LABEL_KEY}={pipeline_run_uid}"
    result = subprocess.run(
        [
            "kubectl",
            "delete",
            "internalrequest",
            "-l",
            label_selector,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"kubectl delete failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    logger.info("kubectl delete output: %s", result.stdout.strip())


def run(pipeline_run_uid: str) -> None:
    """Orchestrate cleanup: skip when UID is empty, otherwise delete."""
    if not pipeline_run_uid:
        logger.info("No pipelineRunUid provided, skipping cleanup")
        return

    cleanup(pipeline_run_uid)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, prog=PROG)
    parser.add_argument(
        "--pipeline-run-uid",
        default="",
        help="UID of the PipelineRun whose InternalRequests should be deleted",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run cleanup."""
    args = _parse_args(argv)
    run(args.pipeline_run_uid.strip())
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
