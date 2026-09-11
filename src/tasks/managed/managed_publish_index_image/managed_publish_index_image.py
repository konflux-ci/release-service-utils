#!/usr/bin/env python3
"""Process managed index image publishing requests.

This script processes image publishing requests by extracting component details from
an Internal Request (IR) results JSON file. For each component, it extracts the
source index, target index, and build timestamp. It then initiates parallel, internal
Tekton requests to publish the target images (optionally including a timestamped version).
The script awaits the completion of all spawned requests and reports the status.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import traceback

from release_service_utils.helpers.internal_request import (
    create as _create_ir,
    fetch_results as _fetch_results,
    wait_for_completion as _wait_for_completion,
    InternalRequestWaitError,
)
from release_service_utils.helpers.logger import setup_logger
from release_service_utils.helpers.decorators import async_in_executor

# Create process pool executor for parallel execution
executor = ThreadPoolExecutor()


def _create_ir_wrapper(pipeline, params, labels, task_timeout, pipeline_timeout, **kwargs):
    """Wrap create for async executor.

    Converts params and labels from list-of-tuples format to dict format.

    Args:
        pipeline: Name of the pipeline to run.
        params: List of tuples representing parameters for the pipeline.
        labels: List of tuples representing labels for the pipeline run.
        task_timeout: Timeout for individual tasks in the pipeline.
        pipeline_timeout: Timeout for the entire pipeline run.
        **kwargs: Additional keyword arguments (ignored in this wrapper).

    """
    params_dict = dict(params)
    labels_dict = dict(labels) if labels else None

    return _create_ir(
        pipeline=pipeline,
        params=params_dict,
        labels=labels_dict,
        sync=False,
        task_timeout=task_timeout,
        pipeline_timeout=pipeline_timeout,
        finally_timeout="0h5m0s",
    )


def _wait_for_ir_wrapper(name):
    """Wrap wait_for_completion for async executor.

    Args:
        name: Name of the InternalRequest to wait for.

    """
    _wait_for_completion(name=name)


create_internal_request = async_in_executor(executor)(_create_ir_wrapper)
wait_for_internal_request = async_in_executor(executor)(_wait_for_ir_wrapper)

PIPELINERUN_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"


def format_seconds(seconds: int) -> str:
    """Format seconds into a string of the form 'XXhXXmXXs'."""
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02}h{minutes:02}m{seconds:02}s"


def make_parser() -> argparse.ArgumentParser:
    """Create and return the argument parser for the script."""
    parser = argparse.ArgumentParser(description="Process image publishing requests.")
    parser.add_argument(
        "--data-file",
        type=str,
        required=True,
        help="Path to the merged data JSON file.",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=360,
        help="Timeout for each individual request in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries for failed requests.",
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
    return parser


def main() -> int:
    """Run main function to process image publishing requests."""
    parser = make_parser()
    args = parser.parse_args()
    log = setup_logger(getattr(logging, args.log_level.upper(), logging.INFO))
    pipeline_timeout_seconds = args.request_timeout + 300
    pipeline_timeout = format_seconds(pipeline_timeout_seconds)
    task_timeout = format_seconds(args.request_timeout)

    with open(args.data_file) as f:
        data = json.load(f)
    credentials = data["fbc"]["publishingCredentials"]

    with open(args.ir_results_file) as f:
        components = json.load(f).get("components", [])

    ir_futures = {}
    failed = False
    for i, component in enumerate(components):
        target_index = component.get("target_index")
        target_index_with_timestamp = component.get("target_index_with_timestamp")
        source_index = component.get("index_image")
        source_index_resolved = component.get("index_image_resolved")
        ocp_version = component.get("ocp_version")

        log.debug(
            f"Extracted for component {i}: target_index={target_index}, "
            f"source_index={source_index}, ocp_version={ocp_version}"
        )

        publishing_pairs = [(target_index, source_index)]
        if target_index != target_index_with_timestamp:
            publishing_pairs.append((target_index_with_timestamp, source_index_resolved))

        for target, source in publishing_pairs:
            params = [
                ("sourceIndex", source),
                ("targetIndex", target),
                ("publishingCredentials", credentials),
                ("taskGitUrl", args.task_git_url),
                ("taskGitRevision", args.task_git_revision),
                ("retries", str(args.retries)),
                ("targetOcpVersion", str(ocp_version)),
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
            ir_futures[ft] = target

    # Collect created IR names and start waiting
    wait_futures = {}
    for ft in as_completed(ir_futures):
        pi = ir_futures[ft]
        try:
            ir_name = ft.result()
            log.debug(f"Waiting for internal request {ir_name} to complete " f"for {pi}")
            wait_ft = wait_for_internal_request(ir_name)
            wait_futures[wait_ft] = (ir_name, pi)
        except Exception as e:
            log.exception(f"Creating internal request for {pi} failed: {e}")
            log.exception(f"{traceback.format_tb(e.__traceback__)}")
            raise

    # Process wait results and check outcomes
    for ft in as_completed(wait_futures):
        ir_name, pi = wait_futures[ft]
        try:
            ft.result()
            results = _fetch_results(ir_name)
        except InternalRequestWaitError:
            results = _fetch_results(ir_name)
        except Exception as e:
            log.error(f"ERROR: Waiting for {pi} failed: {e}")
            failed = True
            continue

        request_message = results.get("requestMessage", "")

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
    raise SystemExit(exit_code)
