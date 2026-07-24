#!/usr/bin/env python3
"""Add FBC contributions to index images via InternalRequests.

Create InternalRequests to add FBC (File-Based Catalog) contributions to index
images. This script batches multiple fragments into single IIB requests and can
split requests according to their OCP versions.

Fragments are batched and submitted to IIB in sets of configurable size. The
snapshot must be previously augmented by prepare-fbc-snapshot to include OCP and
target index metadata for each component.

Components are split by OCP versions and processed in series. For each OCP
version, batches are chained so that the final targetIndex produced will have
all fragments added. The index_image from one internal request is set as the
fromIndex for the next request within that OCP version.

Failed batches are retried at the end to allow timed out requests to finish.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import file as file_helpers
import iib
from logger import logger as LOGGER
from subprocess_cmd import run_cmd

PROG = "add_fbc_contribution.py"


@dataclass
class AddFBCContributionConfig:
    """Configuration for the add-fbc-contribution task."""

    snapshot_path: Path
    data_path: Path
    data_dir: Path
    results_dir_path: Path
    pipeline_run_uid: str
    task_run_uid: str
    max_batch_size: int
    must_publish_index_image: bool
    must_overwrite_from_index_image: bool
    iib_service_account_secret: str
    max_retries: int
    batch_retry_delay_seconds: int
    task_git_url: str
    task_git_revision: str


@dataclass
class OCPGroup:
    """A group of components with the same OCP version."""

    ocp_version: str
    components: list[dict[str, Any]]
    from_index: str
    target_index: str
    build_tags: list[str] = field(default_factory=list)


@dataclass
class BatchResult:
    """Result of executing a batch."""

    batch_num: int
    success: bool
    index_image: str = ""
    results: dict[str, Any] = field(default_factory=dict)
    error_message: str = ""


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Add FBC contributions to index images via InternalRequests.",
        prog=PROG,
    )
    parser.add_argument(
        "--snapshot-path",
        required=True,
        type=Path,
        help="Path to the JSON string of the mapped Snapshot spec",
    )
    parser.add_argument(
        "--data-path",
        required=True,
        type=Path,
        help="Path to the JSON string of the merged data",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        type=Path,
        help="The location where data is stored",
    )
    parser.add_argument(
        "--results-dir-path",
        required=True,
        type=Path,
        help="Path to the results directory",
    )
    parser.add_argument(
        "--pipeline-run-uid",
        required=True,
        help="The UID of the current PipelineRun",
    )
    parser.add_argument(
        "--task-run-uid",
        required=True,
        help="The UID of the current TaskRun",
    )
    parser.add_argument(
        "--max-batch-size",
        type=int,
        default=5,
        help="Maximum number of FBC fragments to process in a single batch",
    )
    parser.add_argument(
        "--must-publish-index-image",
        default="false",
        help="Whether the index image should be published",
    )
    parser.add_argument(
        "--must-overwrite-from-index-image",
        default="false",
        help="Whether to overwrite the from index image",
    )
    parser.add_argument(
        "--iib-service-account-secret",
        required=True,
        help="IIB service account secret name",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retry attempts for failed internal requests",
    )
    parser.add_argument(
        "--batch-retry-delay-seconds",
        type=int,
        default=60,
        help="Delay between batch retry attempts in seconds",
    )
    parser.add_argument(
        "--task-git-url",
        required=True,
        help="The URL to the git repo where the release-service-catalog tasks are stored",
    )
    parser.add_argument(
        "--task-git-revision",
        required=True,
        help="The revision in the taskGitUrl repo to be used",
    )
    parser.add_argument(
        "--build-timestamp-result",
        type=Path,
        help="Path to write the build timestamp result",
    )
    parser.add_argument(
        "--request-results-file-result",
        type=Path,
        help="Path to write the request results file path result",
    )
    parser.add_argument(
        "--internal-request-results-file-result",
        type=Path,
        help="Path to write the internal request results file path result",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def validate_snapshot(snapshot: dict[str, Any]) -> None:
    """Validate the snapshot has the required structure."""
    components = snapshot.get("components")
    if not isinstance(components, list):
        raise ValueError("Snapshot missing required 'components' array")
    if len(components) == 0:
        raise ValueError("No components found in snapshot")


def get_ocp_versions(snapshot: dict[str, Any]) -> list[str]:
    """Extract unique OCP versions from snapshot components."""
    ocp_versions: set[str] = set()
    for component in snapshot.get("components", []):
        ocp_version = component.get("ocpVersion", "")
        if ocp_version:
            ocp_versions.add(ocp_version)
    return sorted(ocp_versions)


def group_components_by_ocp_version(
    snapshot: dict[str, Any],
    ocp_versions: list[str],
    global_build_tags: list[str],
) -> list[OCPGroup]:
    """Group components by OCP version for isolated processing."""
    groups: list[OCPGroup] = []

    for ocp_version in ocp_versions:
        components = [
            c for c in snapshot.get("components", []) if c.get("ocpVersion") == ocp_version
        ]

        if not components:
            continue

        first_component = components[0]
        from_index = first_component.get("updatedFromIndex", "")
        target_index = first_component.get("targetIndex", "")

        build_tags = list(global_build_tags)
        if target_index:
            target_tag = target_index.rsplit(":", 1)[-1]
            if target_tag != target_index:
                build_tags.append(target_tag)

        groups.append(
            OCPGroup(
                ocp_version=ocp_version,
                components=components,
                from_index=from_index,
                target_index=target_index,
                build_tags=build_tags,
            )
        )
        LOGGER.info("OCP version %s has %d components", ocp_version, len(components))

    return groups


def get_batch_fragments(
    components: list[dict[str, Any]],
    batch_num: int,
    max_batch_size: int,
) -> list[str]:
    """Get the container image fragments for a batch."""
    start_idx = batch_num * max_batch_size
    end_idx = min((batch_num + 1) * max_batch_size, len(components))
    return [c.get("containerImage", "") for c in components[start_idx:end_idx]]


def create_internal_request(
    from_index: str,
    fragments: list[str],
    config: AddFBCContributionConfig,
    group: OCPGroup,
    data: dict[str, Any],
    pipeline_timeout: str,
    task_timeout: str,
    *,
    run_command: Any = None,
) -> str:
    """Create an InternalRequest for IIB processing and return the request name."""
    runner = run_command or run_cmd
    fbc_config = data.get("fbc", {})

    publishing_credentials = fbc_config.get(
        "publishingCredentials", "catalog-publishing-secret"
    )
    build_timeout_seconds = fbc_config.get("buildTimeoutSeconds", 3600)
    add_arches = fbc_config.get("addArches", [])
    internal_request_service_account = fbc_config.get(
        "internalRequestServiceAccount", "release-service-account"
    )
    request_timeout_seconds = fbc_config.get("requestTimeoutSeconds", 3600)

    task_label = "internal-services.appstudio.openshift.io/group-id"
    pipelinerun_label = "internal-services.appstudio.openshift.io/pipelinerun-uid"

    cmd = [
        "internal-request",
        "--pipeline",
        "update-fbc-catalog",
        "-p",
        f"fromIndex={from_index}",
        "-p",
        f"fbcFragments={json.dumps(fragments)}",
        "-p",
        f"iibServiceAccountSecret={config.iib_service_account_secret}",
        "-p",
        f"publishingCredentials={publishing_credentials}",
        "-p",
        f"buildTimeoutSeconds={build_timeout_seconds}",
        "-p",
        f"buildTags={json.dumps(group.build_tags)}",
        "-p",
        f"addArches={json.dumps(add_arches)}",
        "-p",
        f"mustPublishIndexImage={str(config.must_publish_index_image).lower()}",
        "-p",
        f"mustOverwriteFromIndexImage={str(config.must_overwrite_from_index_image).lower()}",
        "-p",
        f"taskGitUrl={config.task_git_url}",
        "-p",
        f"taskGitRevision={config.task_git_revision}",
        "--service-account",
        internal_request_service_account,
        "-l",
        f"{task_label}={config.task_run_uid}",
        "-l",
        f"{pipelinerun_label}={config.pipeline_run_uid}",
        "--pipeline-timeout",
        pipeline_timeout,
        "--task-timeout",
        task_timeout,
        "-t",
        str(request_timeout_seconds),
    ]

    result = runner(cmd, check=False)
    output = result.stdout

    for line in output.splitlines():
        if "created" in line.lower() and "'" in line:
            parts = line.split("'")
            if len(parts) >= 2:
                return parts[1]

    raise ValueError(f"Failed to extract InternalRequest name from output: {output}")


def get_internal_request_status(
    request_name: str,
    *,
    run_command: Any = None,
) -> tuple[bool, str, str]:
    """Get the status of an InternalRequest.

    Returns (succeeded, reason, message).
    """
    runner = run_command or run_cmd
    result = runner(
        [
            "kubectl",
            "get",
            "internalrequest",
            request_name,
            "-o",
            'jsonpath={.status.conditions[?(@.type=="Succeeded")]}',
        ],
        check=False,
    )

    if result.returncode != 0:
        return False, "Error", f"Failed to get status: {result.stderr}"

    try:
        condition = json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError:
        return False, "Error", f"Invalid JSON in status: {result.stdout}"

    status = condition.get("status", "")
    reason = condition.get("reason", "")
    message = condition.get("message", "")

    return status == "True", reason, message


def get_internal_request_results(
    request_name: str,
    *,
    run_command: Any = None,
) -> dict[str, Any]:
    """Get the results from an InternalRequest."""
    runner = run_command or run_cmd
    result = runner(
        [
            "kubectl",
            "get",
            "internalrequest",
            request_name,
            "-o",
            "jsonpath={.status.results}",
        ],
        check=False,
    )

    if result.returncode != 0 or not result.stdout.strip():
        return {}

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}


def calculate_timeouts(request_timeout_seconds: int) -> tuple[str, str]:
    """Calculate pipeline and task timeouts from request timeout."""
    finally_task_timeout = 300
    total_seconds = request_timeout_seconds + finally_task_timeout

    pipeline_hours = total_seconds // 3600
    pipeline_minutes = (total_seconds % 3600) // 60
    pipeline_seconds = total_seconds % 60
    pipeline_timeout = f"{pipeline_hours}h{pipeline_minutes}m{pipeline_seconds}s"

    task_hours = request_timeout_seconds // 3600
    task_minutes = (request_timeout_seconds % 3600) // 60
    task_seconds = request_timeout_seconds % 60
    task_timeout = f"{task_hours}h{task_minutes}m{task_seconds}s"

    return pipeline_timeout, task_timeout


def execute_batch(
    batch_num: int,
    from_index: str,
    group: OCPGroup,
    config: AddFBCContributionConfig,
    data: dict[str, Any],
    *,
    run_command: Any = None,
) -> BatchResult:
    """Execute a single batch and return the result."""
    LOGGER.info(
        "Executing batch %d for OCP %s: fromIndex=%s",
        batch_num + 1,
        group.ocp_version,
        from_index,
    )

    fragments = get_batch_fragments(
        group.components,
        batch_num,
        config.max_batch_size,
    )
    LOGGER.info("Batch %d fragments: %s", batch_num + 1, fragments)

    fbc_config = data.get("fbc", {})
    request_timeout_seconds = fbc_config.get("requestTimeoutSeconds", 3600)
    pipeline_timeout, task_timeout = calculate_timeouts(request_timeout_seconds)

    try:
        request_name = create_internal_request(
            from_index,
            fragments,
            config,
            group,
            data,
            pipeline_timeout,
            task_timeout,
            run_command=run_command,
        )
    except ValueError as e:
        return BatchResult(
            batch_num=batch_num,
            success=False,
            error_message=str(e),
        )

    LOGGER.info("Created InternalRequest: %s", request_name)

    succeeded, reason, message = get_internal_request_status(
        request_name,
        run_command=run_command,
    )

    if not succeeded:
        LOGGER.error(
            "Batch %d internal request failed: reason=%s, message=%s",
            batch_num + 1,
            reason,
            message,
        )
        return BatchResult(
            batch_num=batch_num,
            success=False,
            error_message=f"Reason: {reason}, Message: {message}",
        )

    results = get_internal_request_results(request_name, run_command=run_command)
    if not results:
        LOGGER.error("Batch %d succeeded but returned empty results", batch_num + 1)
        return BatchResult(
            batch_num=batch_num,
            success=False,
            error_message="Empty results from InternalRequest",
        )

    json_build_info_compressed = results.get("jsonBuildInfo", "")
    if not json_build_info_compressed:
        return BatchResult(
            batch_num=batch_num,
            success=False,
            error_message="Missing jsonBuildInfo in results",
        )

    try:
        build_info = iib.decompress_build_info(json_build_info_compressed)
    except (ValueError, gzip.BadGzipFile, json.JSONDecodeError) as e:
        return BatchResult(
            batch_num=batch_num,
            success=False,
            error_message=f"Failed to decompress build info: {e}",
        )

    index_image = build_info.get("index_image", "")

    LOGGER.info("Batch %d completed successfully", batch_num + 1)
    return BatchResult(
        batch_num=batch_num,
        success=True,
        index_image=index_image,
        results=results,
    )


def process_batch_results(
    batch_result: BatchResult,
    group: OCPGroup,
    config: AddFBCContributionConfig,
    timestamp_format: str,
    results_data: dict[str, Any],
) -> None:
    """Process batch results and add to the results data."""
    if not batch_result.success or not batch_result.results:
        return

    json_build_info_compressed = batch_result.results.get("jsonBuildInfo", "")
    if not json_build_info_compressed:
        return

    try:
        build_info = iib.decompress_build_info(json_build_info_compressed)
    except (ValueError, gzip.BadGzipFile, json.JSONDecodeError):
        return

    completion_time_raw = build_info.get("updated", "")
    try:
        if completion_time_raw:
            dt = datetime.fromisoformat(completion_time_raw.replace("Z", "+00:00"))
            completion_time = dt.strftime(timestamp_format)
        else:
            completion_time = ""
    except (ValueError, OSError):
        completion_time = completion_time_raw

    index_image_digests_str = batch_result.results.get("indexImageDigests", "")
    index_image_digests = [d for d in index_image_digests_str.split(" ") if d]

    fragments = get_batch_fragments(
        group.components,
        batch_result.batch_num,
        config.max_batch_size,
    )

    for fragment in fragments:
        component_result = {
            "fbc_fragment": fragment,
            "target_index": group.target_index,
            "ocp_version": group.ocp_version,
            "image_digests": index_image_digests,
            "index_image": build_info.get("index_image", ""),
            "index_image_resolved": build_info.get("index_image_resolved", ""),
            "completion_time": completion_time,
            "iibLog": batch_result.results.get("iibLog", ""),
        }
        results_data["components"].append(component_result)


def process_ocp_group(
    group: OCPGroup,
    config: AddFBCContributionConfig,
    data: dict[str, Any],
    timestamp_format: str,
    results_data: dict[str, Any],
    *,
    run_command: Any = None,
    sleep_fn: Any = None,
) -> bool:
    """Process all batches for an OCP group.

    Returns True if all batches succeeded, False otherwise.
    """
    sleeper = sleep_fn or time.sleep
    LOGGER.info("Processing OCP group %s", group.ocp_version)

    num_components = len(group.components)
    num_batches = (num_components + config.max_batch_size - 1) // config.max_batch_size

    LOGGER.info(
        "Creating %d batch(es) for %d components in OCP group %s",
        num_batches,
        num_components,
        group.ocp_version,
    )

    successful_batches: list[int] = []
    failed_batches: list[int] = []
    latest_iib_index_image = ""

    def get_current_from_index() -> str:
        if config.must_overwrite_from_index_image:
            return group.from_index
        if not successful_batches:
            return group.from_index
        if not latest_iib_index_image:
            raise RuntimeError(
                f"Successful batches exist but latest_iib_index_image is empty. "
                f"Successful batches: {successful_batches}"
            )
        return latest_iib_index_image

    for batch_num in range(num_batches):
        LOGGER.info(
            "Processing batch %d/%d for OCP %s",
            batch_num + 1,
            num_batches,
            group.ocp_version,
        )

        current_from_index = get_current_from_index()

        batch_result = execute_batch(
            batch_num,
            current_from_index,
            group,
            config,
            data,
            run_command=run_command,
        )

        if batch_result.success:
            successful_batches.append(batch_num)
            process_batch_results(
                batch_result,
                group,
                config,
                timestamp_format,
                results_data,
            )

            if not config.must_overwrite_from_index_image and batch_result.index_image:
                latest_iib_index_image = batch_result.index_image
                LOGGER.info("Updated fromIndex for next batch: %s", latest_iib_index_image)
        else:
            failed_batches.append(batch_num)
            LOGGER.warning(
                "Batch %d failed, will retry later for OCP %s",
                batch_num + 1,
                group.ocp_version,
            )

    for retry_attempt in range(1, config.max_retries + 1):
        if not failed_batches:
            LOGGER.info("All batches completed successfully for OCP %s", group.ocp_version)
            break

        LOGGER.info(
            "Retry attempt %d: %d batches to retry for OCP %s",
            retry_attempt,
            len(failed_batches),
            group.ocp_version,
        )

        still_failed: list[int] = []

        for batch_num in failed_batches:
            current_from_index = get_current_from_index()

            batch_result = execute_batch(
                batch_num,
                current_from_index,
                group,
                config,
                data,
                run_command=run_command,
            )

            if batch_result.success:
                LOGGER.info(
                    "Batch %d succeeded on retry attempt %d for OCP %s",
                    batch_num + 1,
                    retry_attempt,
                    group.ocp_version,
                )
                successful_batches.append(batch_num)
                process_batch_results(
                    batch_result,
                    group,
                    config,
                    timestamp_format,
                    results_data,
                )

                if not config.must_overwrite_from_index_image and batch_result.index_image:
                    latest_iib_index_image = batch_result.index_image
                    LOGGER.info("Updated fromIndex for next batch: %s", latest_iib_index_image)
            else:
                still_failed.append(batch_num)
                LOGGER.warning(
                    "Batch %d failed retry attempt %d for OCP %s",
                    batch_num + 1,
                    retry_attempt,
                    group.ocp_version,
                )

        failed_batches = still_failed

        if failed_batches and retry_attempt < config.max_retries:
            LOGGER.info(
                "Waiting %d seconds before next retry attempt...",
                config.batch_retry_delay_seconds,
            )
            sleeper(config.batch_retry_delay_seconds)

    if failed_batches:
        LOGGER.error(
            "%d batches failed after all retries for OCP %s: %s",
            len(failed_batches),
            group.ocp_version,
            [b + 1 for b in failed_batches],
        )
        return False

    LOGGER.info(
        "All %d batches completed successfully for OCP %s",
        num_batches,
        group.ocp_version,
    )
    return True


def deduplicate_results(results_data: dict[str, Any], is_staged: bool) -> dict[str, Any]:
    """Deduplicate results keeping only the last component per unique target.

    For staged releases (target_index is empty), group by ocp_version instead.
    """
    components = results_data.get("components", [])
    if not components:
        return results_data

    if is_staged:
        unique_count = len(set(c.get("ocp_version", "") for c in components))
    else:
        unique_count = len(
            set(c.get("target_index", "") for c in components if c.get("target_index"))
        )
        if unique_count == 0:
            unique_count = len(set(c.get("ocp_version", "") for c in components))

    if len(components) <= unique_count:
        LOGGER.info(
            "No deduplication needed (%d components, %d unique targets)",
            len(components),
            unique_count,
        )
        return results_data

    LOGGER.info(
        "Found %d components for %d unique targets",
        len(components),
        unique_count,
    )
    LOGGER.info("Keeping only the last (most recent) index for each OCP target")

    groups: dict[str, list[dict[str, Any]]] = {}
    for component in components:
        if is_staged or not component.get("target_index"):
            key = component.get("ocp_version", "")
        else:
            key = component.get("target_index", "")

        if key not in groups:
            groups[key] = []
        groups[key].append(component)

    deduplicated = [group[-1] for group in groups.values()]

    LOGGER.info(
        "Deduplicated from %d to %d components",
        len(components),
        len(deduplicated),
    )

    return {"components": deduplicated}


def run(
    config: AddFBCContributionConfig,
    *,
    run_command: Any = None,
    sleep_fn: Any = None,
    now_fn: Any = None,
) -> tuple[dict[str, Any], str]:
    """Execute the add-fbc-contribution workflow.

    Returns (results_data, timestamp).
    """
    snapshot = file_helpers.load_json_dict(config.snapshot_path)
    data = file_helpers.load_json_dict(config.data_path)

    validate_snapshot(snapshot)

    fbc_config = data.get("fbc", {})
    timestamp_format = fbc_config.get("timestampFormat", "%s")
    is_staged = fbc_config.get("stagedIndex", False) is True

    global_build_tags = fbc_config.get("buildTags", [])
    if not isinstance(global_build_tags, list):
        global_build_tags = []

    now_func = now_fn or (lambda: datetime.now(timezone.utc))
    now = now_func()
    timestamp = now.strftime(timestamp_format)

    ocp_versions = get_ocp_versions(snapshot)
    LOGGER.info("Found OCP versions: %s", " ".join(ocp_versions))

    LOGGER.info("Using pre-determined values from prepare-fbc-parameters:")
    LOGGER.info("  - mustPublishIndexImage: %s", config.must_publish_index_image)
    LOGGER.info("  - mustOverwriteFromIndexImage: %s", config.must_overwrite_from_index_image)
    LOGGER.info("  - iibServiceAccountSecret: %s", config.iib_service_account_secret)

    results_data: dict[str, Any] = {"components": []}

    groups = group_components_by_ocp_version(
        snapshot,
        ocp_versions,
        global_build_tags,
    )

    total_components = len(snapshot.get("components", []))
    LOGGER.info("Processing %d components", total_components)

    all_succeeded = True
    for group in groups:
        LOGGER.info("Processing OCP group: %s", group.ocp_version)
        success = process_ocp_group(
            group,
            config,
            data,
            timestamp_format,
            results_data,
            run_command=run_command,
            sleep_fn=sleep_fn,
        )
        if not success:
            all_succeeded = False
            break

    if not all_succeeded:
        raise RuntimeError("One or more OCP groups failed to process")

    results_data = deduplicate_results(results_data, is_staged)

    LOGGER.info(
        "Multi-OCP batch processing completed successfully with %d components "
        "across %d OCP versions",
        total_components,
        len(ocp_versions),
    )

    return results_data, timestamp


def main(argv: list[str] | None = None) -> int:
    """Entry point for add-fbc-contribution."""
    parser = setup_argparser()
    args = parser.parse_args(argv)

    LOGGER.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    must_publish = args.must_publish_index_image.lower() == "true"
    must_overwrite = args.must_overwrite_from_index_image.lower() == "true"

    config = AddFBCContributionConfig(
        snapshot_path=args.data_dir / args.snapshot_path,
        data_path=args.data_dir / args.data_path,
        data_dir=args.data_dir,
        results_dir_path=args.data_dir / args.results_dir_path,
        pipeline_run_uid=args.pipeline_run_uid,
        task_run_uid=args.task_run_uid,
        max_batch_size=args.max_batch_size,
        must_publish_index_image=must_publish,
        must_overwrite_from_index_image=must_overwrite,
        iib_service_account_secret=args.iib_service_account_secret,
        max_retries=args.max_retries,
        batch_retry_delay_seconds=args.batch_retry_delay_seconds,
        task_git_url=args.task_git_url,
        task_git_revision=args.task_git_revision,
    )

    results_file_rel = f"{args.results_dir_path}/internal-requests-results.json"
    results_file = config.data_dir / results_file_rel
    request_results_file = (
        config.data_dir / args.pipeline_run_uid / f"ir-{args.task_run_uid}-result.json"
    )

    if args.internal_request_results_file_result:
        args.internal_request_results_file_result.write_text(
            results_file_rel, encoding="utf-8"
        )

    if args.request_results_file_result:
        args.request_results_file_result.write_text(
            str(
                request_results_file.relative_to(config.data_dir.parent)
                if request_results_file.is_relative_to(config.data_dir.parent)
                else request_results_file
            ),
            encoding="utf-8",
        )

    try:
        results_data, timestamp = run(config)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        LOGGER.error("Task failed: %s", e)
        return 1

    if args.build_timestamp_result:
        args.build_timestamp_result.write_text(timestamp, encoding="utf-8")

    config.results_dir_path.mkdir(parents=True, exist_ok=True)
    results_file.write_text(json.dumps(results_data) + "\n", encoding="utf-8")

    LOGGER.info("Results file: %s", results_file)
    LOGGER.info("Results:")
    LOGGER.info(json.dumps(results_data, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
