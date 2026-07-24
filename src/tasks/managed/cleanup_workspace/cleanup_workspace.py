#!/usr/bin/env python3
"""Clean up a workspace directory and InternalRequest CRs for a PipelineRun."""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

import cleanup_internal_requests
from logger import logger

PROG = "cleanup_workspace.py"


def cleanup_directory(workspace_path: str, subdirectory: str) -> None:
    """Remove the specified subdirectory from the workspace.

    Skip silently when the directory does not exist.
    """
    cleanup_dir = Path(workspace_path) / subdirectory
    if cleanup_dir.is_dir():
        shutil.rmtree(cleanup_dir)
        logger.info("Removed directory: %s", cleanup_dir)
    else:
        logger.info("Directory does not exist, nothing to remove: %s", cleanup_dir)


def run(
    subdirectory: str,
    delay: int,
    pipeline_run_uid: str,
    workspace_path: str,
) -> None:
    """Orchestrate workspace cleanup.

    Delete matching InternalRequest CRs, wait for the configured delay,
    then remove the target subdirectory.
    """
    cleanup_internal_requests.run(pipeline_run_uid)

    if not subdirectory:
        logger.info("The empty string is not a valid subdirectory")
        return

    logger.info("Delaying execution by %d seconds", delay)
    time.sleep(delay)

    cleanup_directory(workspace_path, subdirectory)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, prog=PROG)
    parser.add_argument(
        "--subdirectory",
        default="",
        help="The directory to remove within the workspace",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=60,
        help="Time in seconds to delay the cleanup action",
    )
    parser.add_argument(
        "--pipeline-run-uid",
        default="",
        help=("UID of the PipelineRun whose InternalRequests should be deleted"),
    )
    parser.add_argument(
        "--workspace-path",
        required=True,
        help="Path to the workspace directory",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run cleanup."""
    args = _parse_args(argv)
    run(
        args.subdirectory,
        args.delay,
        args.pipeline_run_uid,
        args.workspace_path,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
