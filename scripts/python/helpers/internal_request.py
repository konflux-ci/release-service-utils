"""Python implementation of internal-request bash script.

Creates and optionally waits for Kubernetes InternalRequest custom resources.
Can be used as a CLI or imported as a library.

Exit codes (CLI mode):
    0: Success
    21: InternalRequest failed or rejected
    22: InternalRequest rejected (deprecated, uses 21)
    124: Timeout waiting for completion
    1: Other errors
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from kubernetes.client.rest import ApiException

from rsmodels.internal_request_models import InternalRequest
from logger import setup_logger

logger = logging.getLogger(__name__)


# Exit codes matching bash script
EXIT_SUCCESS = 0
EXIT_FAILED = 21
EXIT_TIMEOUT = 124
EXIT_ERROR = 1


def write_result_paths(
    result_paths: Mapping[str, Path],
    *,
    pipeline_run_name: str,
    task_run_name: str,
) -> None:
    """Write pipeline and task run names to the internal-request Tekton results.

    *result_paths* must include ``internal_pr_name`` and ``internal_task_run_name``
    keys mapping to the result file paths from the task step.
    """
    result_paths["internal_pr_name"].write_text(pipeline_run_name, encoding="utf-8")
    result_paths["internal_task_run_name"].write_text(task_run_name, encoding="utf-8")


class InternalRequestError(Exception):
    """Base exception for InternalRequest errors."""

    pass


class InternalRequestFailedError(InternalRequestError):
    """Raised when InternalRequest fails or is rejected."""

    pass


class InternalRequestTimeoutError(InternalRequestError):
    """Raised when waiting for InternalRequest times out."""

    pass


class TimeoutValidationError(InternalRequestError):
    """Raised when timeout format or values are invalid."""

    pass


def _validate_timeout_format(timeout: str) -> None:
    """Validate timeout format is XhYmZs where X, Y, Z are integers.

    Args:
        timeout: Timeout string to validate

    Raises:
        TimeoutValidationError: If format is invalid

    Example:
        _validate_timeout_format("1h0m0s")  # OK
        _validate_timeout_format("90m")  # Raises error

    """
    pattern = r"^\d+h\d+m\d+s$"
    if not re.match(pattern, timeout):
        raise TimeoutValidationError(
            f"Timeout must be in XhYmZs format (e.g., 1h0m0s), got: {timeout}"
        )


def _convert_to_seconds(timeout: str) -> int:
    """Convert timeout string in XhYmZs format to total seconds.

    Args:
        timeout: Timeout string in format like "1h30m45s"

    Returns:
        Total seconds as integer

    Example:
        _convert_to_seconds("1h0m0s")  # Returns 3600
        _convert_to_seconds("0h5m30s")  # Returns 330

    """
    match = re.match(r"^(\d+)h(\d+)m(\d+)s$", timeout)
    if not match:
        raise TimeoutValidationError(f"Invalid timeout format: {timeout}")

    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3))

    return (hours * 3600) + (minutes * 60) + seconds


def _validate_timeouts(pipeline_timeout: str, task_timeout: str, finally_timeout: str) -> None:
    """Validate timeout values and relationships.

    Ensures:
    - All timeouts are in XhYmZs format
    - Pipeline timeout >= task timeout + finally timeout

    Args:
        pipeline_timeout: Total pipeline timeout
        task_timeout: Task timeout
        finally_timeout: Finally task timeout

    Raises:
        TimeoutValidationError: If validation fails

    """
    # Validate formats
    _validate_timeout_format(pipeline_timeout)
    _validate_timeout_format(task_timeout)
    _validate_timeout_format(finally_timeout)

    # Convert to seconds for comparison
    pipeline_secs = _convert_to_seconds(pipeline_timeout)
    task_secs = _convert_to_seconds(task_timeout)
    finally_secs = _convert_to_seconds(finally_timeout)

    # Validate pipeline timeout is sufficient
    all_tasks_timeout = task_secs + finally_secs
    if all_tasks_timeout > pipeline_secs:
        raise TimeoutValidationError(
            f"The sum of the task and finally timeout cannot exceed the pipeline timeout. "
            f"Pipeline timeout is {pipeline_secs}s and the sum of "
            f"the others is {all_tasks_timeout}s. "
            f"This leads to tekton validation webhook errors."
        )


def _parse_params(param_list: list[str]) -> dict[str, str]:
    """Parse parameter list from -p key=value format to dict.

    Args:
        param_list: List of "key=value" strings

    Returns:
        Dictionary mapping keys to values (all strings)

    Example:
        _parse_params(["foo=bar", "json={'key':'val'}"])
        # Returns: {"foo": "bar", "json": "{'key':'val'}"}

    """
    params = {}
    for param in param_list:
        if "=" not in param:
            raise ValueError(f"Parameter must be in key=value format: {param}")

        # Split on first = only
        key, _, value = param.partition("=")
        params[key] = value

    return params


def _parse_labels(label_list: list[str]) -> dict[str, str]:
    """Parse label list from -l key=value format to dict.

    Args:
        label_list: List of "key=value" strings

    Returns:
        Dictionary mapping label keys to values

    Example:
        _parse_labels(["app=foo", "env=prod"])
        # Returns: {"app": "foo", "env": "prod"}

    """
    labels = {}
    for label in label_list:
        if "=" not in label:
            raise ValueError(f"Label must be in key=value format: {label}")

        key, _, value = label.partition("=")
        labels[key] = value

    return labels


def _load_kube_config() -> bool:
    """Load kubernetes configuration.

    Attempts in-cluster configuration first, then falls back to kube_config.
    Returns True if in-cluster configuration was loaded, False if kube_config was loaded.
    """
    try:
        config.load_incluster_config()
        return True
    except Exception:
        config.load_kube_config()
        return False


def _get_namespace() -> str:
    """Get the active Kubernetes namespace.

    If running inside a cluster, reads the namespace from the service account file.
    Otherwise, extracts it from the active kubeconfig context.

    Returns:
        Namespace name (defaults to 'default')

    """
    if _load_kube_config():
        ns_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        if ns_path.exists():
            return ns_path.read_text().strip()
        return "default"
    else:
        try:
            _, active = config.list_kube_config_contexts()
            return active.get("context", {}).get("namespace", "default")
        except Exception:
            return "default"


def _build_internal_request_payload(
    pipeline: str,
    task_git_url: str,
    task_git_revision: str,
    params: dict[str, str],
    labels: dict[str, str] | None,
    service_account: str | None,
    pipeline_timeout: str,
    task_timeout: str,
    finally_timeout: str,
) -> dict[str, Any]:
    """Build the InternalRequest resource payload.

    Args:
        pipeline: Pipeline name
        task_git_url: Git URL for pipeline resolver
        task_git_revision: Git revision for pipeline resolver
        params: Parameters dict
        labels: Labels dict (optional)
        service_account: Service account name (optional)
        pipeline_timeout: Pipeline timeout in XhYmZs format
        task_timeout: Task timeout in XhYmZs format
        finally_timeout: Finally timeout in XhYmZs format

    Returns:
        Dictionary ready for kubernetes API

    """
    payload = {
        "apiVersion": "appstudio.redhat.com/v1alpha1",
        "kind": "InternalRequest",
        "metadata": {"generateName": f"{pipeline}-"},
        "spec": {
            "pipeline": {
                "pipelineRef": {
                    "resolver": "git",
                    "params": [
                        {"name": "url", "value": task_git_url},
                        {"name": "revision", "value": task_git_revision},
                        {
                            "name": "pathInRepo",
                            "value": f"pipelines/internal/{pipeline}/{pipeline}.yaml",
                        },
                    ],
                }
            },
            "params": params,
            "timeouts": {
                "pipeline": pipeline_timeout,
                "tasks": task_timeout,
                "finally": finally_timeout,
            },
        },
    }

    # Add optional labels
    if labels:
        payload["metadata"]["labels"] = labels

    # Add optional service account
    if service_account:
        payload["spec"]["serviceAccount"] = service_account

    return payload


def create_internal_request(
    pipeline: str,
    params: dict[str, str],
    labels: dict[str, str] | None = None,
    service_account: str | None = None,
    pipeline_timeout: str = "1h0m0s",
    task_timeout: str = "0h55m0s",
    finally_timeout: str = "0h5m0s",
) -> str:
    """Create an InternalRequest in Kubernetes.

    Args:
        pipeline: Name of the pipeline to execute
        params: Parameters dict (must include taskGitUrl and taskGitRevision)
        labels: Optional labels to add to the resource
        service_account: Optional service account name
        pipeline_timeout: Total pipeline timeout (default: 1h0m0s)
        task_timeout: Task timeout (default: 0h55m0s)
        finally_timeout: Finally task timeout (default: 0h5m0s)

    Returns:
        Name of the created InternalRequest

    Raises:
        TimeoutValidationError: If timeout validation fails
        ValueError: If required parameters are missing
        InternalRequestError: If creation fails

    Example:
        name = create_internal_request(
            pipeline="my-pipeline",
            params={
                "taskGitUrl": "https://github.com/example/repo",
                "taskGitRevision": "main",
                "key": "value",
            },
        )

    """
    # Validate required parameters
    if "taskGitUrl" not in params:
        raise ValueError("params must include 'taskGitUrl'")
    if "taskGitRevision" not in params:
        raise ValueError("params must include 'taskGitRevision'")

    task_git_url = params["taskGitUrl"]
    task_git_revision = params["taskGitRevision"]

    # Validate timeouts
    _validate_timeouts(pipeline_timeout, task_timeout, finally_timeout)

    # Get namespace
    namespace = _get_namespace()

    # Build payload
    payload = _build_internal_request_payload(
        pipeline=pipeline,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
        params=params,
        labels=labels,
        service_account=service_account,
        pipeline_timeout=pipeline_timeout,
        task_timeout=task_timeout,
        finally_timeout=finally_timeout,
    )

    # Create the resource
    try:
        cr_client = client.CustomObjectsApi()
        result = cr_client.create_namespaced_custom_object(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace=namespace,
            plural="internalrequests",
            body=payload,
        )
        ir_name = result["metadata"]["name"]
        print(f"InternalRequest '{ir_name}' created.")
        logger.info(f"Created InternalRequest '{ir_name}' in namespace '{namespace}'")
        return ir_name

    except ApiException as e:
        raise InternalRequestError(f"Failed to create InternalRequest: {e}") from e


def get_internal_request(name: str, namespace: str | None = None) -> InternalRequest:
    """Get a single InternalRequest by name.

    Args:
        name: Name of the InternalRequest to fetch
        namespace: Optional namespace to look in (defaults to current namespace)

    Returns:
        InternalRequest object with full status and spec

    Raises:
        InternalRequestError: If fetching fails or IR not found

    Example:
        ir = get_internal_request("my-pipeline-abc123")
        print(f"Status: {ir.status.conditions[0].reason}")
        print(f"Results: {ir.status.results}")

    """
    if not namespace:
        _namespace = _get_namespace()
    else:
        _load_kube_config()
        _namespace = namespace

    try:
        cr_client = client.CustomObjectsApi()
        ir_data = cr_client.get_namespaced_custom_object(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace=_namespace,
            plural="internalrequests",
            name=name,
        )
        logger.debug(f"Fetched InternalRequest '{name}' from namespace '{_namespace}'")
        return InternalRequest.from_dict(ir_data)

    except ApiException as e:
        if e.status == 404:
            raise InternalRequestError(
                f"InternalRequest '{name}' not found in namespace '{_namespace}'"
            ) from e
        raise InternalRequestError(f"Failed to get InternalRequest '{name}': {e}") from e


def wait_for_internal_request(
    name: str | None = None,
    labels: str | None = None,
    timeout: int = 600,
    write_output_files: bool = True,
) -> bool:
    """Wait for InternalRequest(s) to complete.

    Polls the Kubernetes API until all matching InternalRequests reach a
    completed state (Succeeded, Failed, or Rejected).

    Args:
        name: Name of specific InternalRequest to watch (mutually exclusive with labels)
        labels: Label selector to find IRs (mutually exclusive with name)
        timeout: Timeout in seconds (default: 600)
        write_output_files: Whether to write /tmp/<name>-output.json files (default: True)

    Returns:
        True if all IRs succeeded, False otherwise

    Raises:
        ValueError: If neither name nor labels specified, or both specified
        InternalRequestTimeoutError: If timeout is reached
        InternalRequestFailedError: If at least one IR fails or is rejected

    Example:
        # Wait for specific IR
        success = wait_for_internal_request(name="my-pipeline-abc123")

        # Wait for all IRs with label
        success = wait_for_internal_request(labels="taskrun-uid=xyz")

    """
    # Validate arguments
    if not name and not labels:
        raise ValueError("Either name or labels must be specified")
    if name and labels:
        raise ValueError("Cannot specify both name and labels")

    # Get namespace
    namespace = _get_namespace()
    logger.info(
        f"Watching InternalRequest(s) in namespace '{namespace}' "
        f"with name='{name}' labels='{labels}'"
    )

    # Calculate end time
    end_time = time.time() + timeout

    cr_client = client.CustomObjectsApi()
    success = True

    while True:
        print("Checking IR statuses...")
        logger.debug("Polling InternalRequest status")

        try:
            # Fetch IRs
            if name:
                # Get single IR and wrap in list
                ir_data = cr_client.get_namespaced_custom_object(
                    group="appstudio.redhat.com",
                    version="v1alpha1",
                    namespace=namespace,
                    plural="internalrequests",
                    name=name,
                )
                irs = [ir_data]
            else:
                # Get IRs by label selector
                ir_list = cr_client.list_namespaced_custom_object(
                    group="appstudio.redhat.com",
                    version="v1alpha1",
                    namespace=namespace,
                    plural="internalrequests",
                    label_selector=labels,
                )
                irs = ir_list.get("items", [])

            irs_length = len(irs)
            print(f"Found {irs_length} InternalRequests matching the name or label")
            print("Conditions:")

            done_count = 0
            for ir in irs:
                ir_name = ir["metadata"]["name"]
                status = ir.get("status", {})
                conditions = status.get("conditions", [])
                condition_reason = conditions[0]["reason"] if conditions else ""
                pipeline_run = status.get("pipelineRun", "")

                print(f"  {ir_name}: ", end="")

                if not condition_reason:
                    print("no condition yet")
                elif condition_reason == "Running":
                    print(f"running - pipelineRun: {pipeline_run}")
                elif condition_reason == "Succeeded":
                    print(f"succeeded - pipelineRun: {pipeline_run}")
                    if write_output_files:
                        _write_output_file(ir_name, pipeline_run)
                    done_count += 1
                else:
                    # Failed or Rejected
                    print(condition_reason)
                    if write_output_files:
                        _write_output_file(ir_name, pipeline_run)
                    done_count += 1
                    success = False

            # Check if all done
            if done_count == irs_length:
                print("All InternalRequests have been completed")
                _print_conditions(irs)

                if success:
                    print("Result: success")
                    return True
                else:
                    print("ERROR: At least one InternalRequest failed")
                    print("Result: failure")
                    raise InternalRequestFailedError(
                        "At least one InternalRequest failed or was rejected"
                    )

            # Check timeout
            if time.time() > end_time:
                print("ERROR: Timeout while waiting for the InternalRequests to complete")
                _print_conditions(irs)
                print("result: timeout")
                raise InternalRequestTimeoutError(
                    f"Timeout after {timeout}s waiting for InternalRequests"
                )

            # Sleep before next poll
            time.sleep(5)

        except ApiException as e:
            if e.status == 404:
                # IR not found yet or deleted
                logger.debug(f"InternalRequest not found: {e}")
                if time.time() > end_time:
                    raise InternalRequestTimeoutError("Timeout waiting for InternalRequest")
                time.sleep(5)
            else:
                raise InternalRequestError(f"Failed to get InternalRequest: {e}") from e


def _write_output_file(ir_name: str, pipeline_run: str) -> None:
    """Write IR result to /tmp/<name>-output.json."""
    output_file = Path(f"/tmp/{ir_name}-output.json")
    data = {"name": ir_name, "pipelineRun": pipeline_run}

    logger.debug(f"writing results to {output_file}")
    with output_file.open("w") as f:
        json.dump(data, f)


def _print_conditions(irs: list[dict[str, Any]]) -> None:
    """Print conditions for all IRs."""
    print("Conditions:")
    for ir in irs:
        ir_name = ir["metadata"]["name"]
        conditions = ir.get("status", {}).get("conditions", [])
        print(f"  {ir_name}: {json.dumps(conditions)}")


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments matching bash script interface."""
    parser = argparse.ArgumentParser(
        description="Create and optionally wait for Kubernetes InternalRequest resources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--pipeline", required=True, help="Name of the pipeline to execute")

    parser.add_argument(
        "-p",
        action="append",
        dest="params",
        default=[],
        help="Parameter in key=value format (can be specified multiple times)",
    )

    parser.add_argument(
        "-l",
        action="append",
        dest="labels",
        default=[],
        help="Label in key=value format (can be specified multiple times)",
    )

    parser.add_argument(
        "-s",
        action="store_true",
        dest="sync",
        default=False,
        help="Wait for InternalRequest to complete (default: true)",
    )

    parser.add_argument(
        "-t",
        type=int,
        dest="timeout",
        default=3600,
        help="Timeout in seconds (default: 3600)",
    )

    parser.add_argument(
        "--service-account",
        help="Service account name to use for pipeline execution",
    )

    parser.add_argument(
        "--pipeline-timeout",
        default="1h0m0s",
        help="Total pipeline timeout (default: 1h0m0s)",
    )

    parser.add_argument(
        "--task-timeout", default="0h55m0s", help="Task timeout (default: 0h55m0s)"
    )

    parser.add_argument(
        "--finally-timeout",
        default="0h5m0s",
        help="Finally task timeout (default: 0h5m0s)",
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    return parser.parse_args()


def main() -> int:
    """Run entry point for CLI."""
    args = parse_arguments()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logger(level=log_level)

    try:
        # Parse parameters and labels
        params = _parse_params(args.params)
        labels = _parse_labels(args.labels) if args.labels else None

        # Validate required parameters
        if "taskGitUrl" not in params or "taskGitRevision" not in params:
            print(
                "Error: You must pass -p taskGitUrl=foo and -p "
                "taskGitRevision=bar as parameters.",
                file=sys.stderr,
            )
            print(
                "These are used to determine the pipeline reference in the git resolver.",
                file=sys.stderr,
            )
            return EXIT_ERROR

        # Warn if pipeline timeout might exceed script timeout
        pipeline_timeout_secs = _convert_to_seconds(args.pipeline_timeout)
        if pipeline_timeout_secs > args.timeout:
            print("WARNING: The passed pipeline timeout is greater than the script timeout")
            print(
                "This means the script can fail before the pipeline times out, "
                "should it take that long"
            )

        # Create InternalRequest
        ir_name = create_internal_request(
            pipeline=args.pipeline,
            params=params,
            labels=labels,
            service_account=args.service_account,
            pipeline_timeout=args.pipeline_timeout,
            task_timeout=args.task_timeout,
            finally_timeout=args.finally_timeout,
        )

        # Wait if sync mode
        if args.sync:
            print("Sync flag set to true. Waiting for the InternalRequest to be completed.")
            wait_for_internal_request(name=ir_name, timeout=args.timeout)

        return EXIT_SUCCESS

    except TimeoutValidationError as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Timeout validation error: {e}")
        return EXIT_ERROR

    except InternalRequestTimeoutError as e:
        logger.error(f"Timeout: {e}")
        return EXIT_TIMEOUT

    except InternalRequestFailedError as e:
        logger.error(f"InternalRequest failed: {e}")
        return EXIT_FAILED

    except ConfigException as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
