#!/usr/bin/env python3
"""Process managed index image publishing requests.

This script processes image publishing requests by extracting component details from
an Internal Request (IR) results JSON file. For each component, it extracts the
source index, target index, and build timestamp. It then initiates parallel, internal
Tekton requests to publish the target images (optionally including a timestamped version).
The script awaits the completion of all spawned requests and reports the status.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import re
import traceback

from internal_request import (  # noqa: E402
    create_internal_request as _create_internal_request,
    get_internal_request as _get_internal_request,
    wait_for_internal_request as _wait_for_internal_request,
)
from logger import setup_logger  # noqa: E402
from decorators import async_in_executor  # noqa: E402

# Create process pool executor for parallel execution
executor = ThreadPoolExecutor()


def _create_ir_wrapper(pipeline, params, labels, task_timeout, pipeline_timeout, **kwargs):
    """Wrap create_internal_request for async executor.

    Converts params and labels from list-of-tuples format to dict format.

    Args:
        pipeline: Name of the pipeline to run.
        params: List of tuples representing parameters for the pipeline.
        labels: List of tuples representing labels for the pipeline run.
        task_timeout: Timeout for individual tasks in the pipeline.
        pipeline_timeout: Timeout for the entire pipeline run.
        **kwargs: Additional keyword arguments (ignored in this wrapper).

    """
    # Convert params from list of tuples to dict
    params_dict = dict(params)

    # Convert labels from list of tuples to dict
    labels_dict = dict(labels) if labels else None

    return _create_internal_request(
        pipeline=pipeline,
        params=params_dict,
        labels=labels_dict,
        task_timeout=task_timeout,
        pipeline_timeout=pipeline_timeout,
        finally_timeout="0h5m0s",  # Default finally timeout
    )


create_internal_request = async_in_executor(executor)(_create_ir_wrapper)
wait_for_internal_request = async_in_executor(executor)(_wait_for_internal_request)

PIPELINERUN_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"


def format_seconds(seconds: int):
    """Format seconds into a string of the form 'XXhXXmXXs'."""
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02}h{minutes:02}m{seconds:02}s"


def make_parser():
    """Create and return the argument parser for the script."""
    parser = argparse.ArgumentParser(description="Process image publishing requests.")
    parser.add_argument(
        "--publishing-credentials", type=str, default="/mnt/publishingCredentials/credential"
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=360,
        help="Timeout for each individual request in seconds.",
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Number of retries for failed requests."
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="DEBUG",
        help="Set the logging level (e.g., INFO, DEBUG, WARNING).",
    )
    parser.add_argument(
        "--task-git-url",
        type=str,
        default="https://github.com/example/repo.git",
        help="Git URL for the task repository.",
    )
    parser.add_argument(
        "--task-git-revision",
        type=str,
        default="main",
        help="Git revision (branch, tag, or commit) for the task repository.",
    )
    parser.add_argument(
        "--pipeline-run-id",
        type=str,
        default="default-run",
        help="Identifier for the current pipeline run.",
    )
    parser.add_argument(
        "--ir-results-file",
        type=str,
        default="ir_results.json",
        help="File to store internal request results.",
    )
    parser.add_argument(
        "--target-ocp-version",
        type=str,
        default="4.12",
        help="Target OpenShift Container Platform version for publishing."
    )
    return parser


def main():
    """Run main function to process image publishing requests."""
    parser = make_parser()
    args = parser.parse_args()
    # Convert string log level to logging constant
    log = setup_logger(getattr(logging, args.log_level.upper(), logging.INFO))
    pipeline_timeout_seconds = args.request_timeout + 300
    pipeline_timeout = format_seconds(pipeline_timeout_seconds)
    task_timeout = format_seconds(args.request_timeout)

    # Get components list first
    components = json.load(open(args.ir_results_file)).get("components", [])

    # Submit all extract calls asynchronously
    targets = []
    sources = []
    timestamps = []
    for component in components:
        targets.append(component.get("target_index"))
        sources.append(component.get("index_image"))
        timestamps.append(component.get("completion_time"))

    # Collect results as they complete
    ir_futures = {}
    failed = False
    for data in zip(range(0, len(components)), targets, sources, timestamps):
        i, target_index, source_index, build_timestamp = data
        log.debug(
            f"Extracted for component {i}: target_index={target_index}, "
            f"source_index={source_index}, build_timestamp={build_timestamp}"
        )

        publishing_images = [target_index]

        if not re.match(".*" + re.escape(f"{build_timestamp}") + "$", target_index):
            publishing_images.append(f"{target_index}-{build_timestamp}")

        for pi in publishing_images:
            params = [
                ("sourceIndex", source_index),
                ("targetIndex", pi),
                ("publishingCredentials", args.publishing_credentials),
                ("taskGitUrl", args.task_git_url),
                ("taskGitRevision", args.task_git_revision),
                ("retries", str(args.retries)),
                ("targetOcpVersion", str(args.target_ocp_version)),
            ]
            labels = [(PIPELINERUN_LABEL, args.pipeline_run_id)]

            log.debug(
                f"Creating internal request for component {i} "
                f"with params: {params} and labels: {labels}"
            )

            ft = create_internal_request(
                pipeline="publish-index-image-pipeline",
                params=params,
                labels=labels,
                task_timeout=task_timeout,
                pipeline_timeout=pipeline_timeout,
            )
            ir_futures[ft] = (target_index, source_index, pi)

    # Process all internal request futures
    irs = []
    for ft in as_completed(ir_futures):
        target_index, source_index, pi = ir_futures[ft]
        try:
            internal_request_id = ft.result()
            ir = _get_internal_request(internal_request_id)
            irs.append((ir, pi))
        except Exception as e:
            log.exception(f"Creating internal request for {pi} failed: {e}")
            log.exception(f"{traceback.format_tb(e.__traceback__)}")
            raise

    ir_futures = {}
    for ir, pi in irs:
        log.debug(f"Waiting for internal request {ir.metadata.name} to complete for {pi}")
        ft = wait_for_internal_request(ir.metadata.name)
        ir_futures[ft] = (ir, pi)

    for ft in as_completed(ir_futures):
        ir, pi = ir_futures[ft]
        if ir.status:
            results = ir.status.results or {}
            request_message = results.get("requestMessage", "")
        else:
            request_message = ""

        if "error" in request_message.lower():
            failed = True
            log.error(f"ERROR: Publish to {pi} failed")
            log.error(f"requestMessage: {request_message}")
        else:
            log.info(f"published successfully ({pi})")

    if failed:
        return 1
    return 0


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = main()
    finally:
        executor.shutdown(wait=True, cancel_futures=False)
    exit(exit_code)
