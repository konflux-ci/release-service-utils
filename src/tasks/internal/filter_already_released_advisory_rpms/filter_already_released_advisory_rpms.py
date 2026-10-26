#!/usr/bin/env python3
"""Filter snapshot RPMs already published in GitLab-stored advisories.

Reads advisory repository metadata from a mounted secret, sparse-clones the
advisory Git repository, and splits snapshot RPM entries into unreleased vs
already-in-advisory lists. Filter results are pushed to OCI storage.

Reads task inputs from environment variables and writes Tekton result files
from ``RESULT_*``. After those env vars are present, the process exits with
status ``0`` even on logical failure so callers can read ``RESULT_RESULT``.
Missing env before result handling exits ``1``.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tarfile
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from git.exc import GitCommandError

from release_service_utils.helpers import advisory_data
from release_service_utils.helpers import authentication
from release_service_utils.helpers import file
from release_service_utils.helpers import oci_auth
from release_service_utils.helpers import subprocess_cmd
from release_service_utils.helpers import tekton
from release_service_utils.helpers.internal_request import internal_request_results
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.vcs import gitlab

PROG = "filter_already_released_advisory_rpms.py"
ADVISORY_SECRET_MOUNT_DEFAULT = Path("/mnt/advisory_secret")
TA_DOCKERCONFIG_DEFAULT = Path("/mnt/trusted_artifacts_dockerconfig/.dockerconfigjson")
CLONE_PARENT_DIR = Path("/tmp")
COMMAND_LOG_PATH = Path("/tmp/stderr.txt")


@dataclass(frozen=True)
class FilterOutput:
    """Values written to Tekton result files."""

    result: str
    filter_results_artifact: str
    advisory_url: str
    advisory_internal_url: str


def write_results_archive(
    unreleased: list[dict[str, Any]],
    in_advisory: list[dict[str, Any]],
) -> Path:
    """Write compact RPM JSON files and a gzip tar of them; return the directory."""
    results_dir = Path(tempfile.mkdtemp(prefix="filter-results-"))
    unreleased_path = results_dir / "unreleased_rpms.json"
    in_advisory_path = results_dir / "in_advisory_rpms.json"
    unreleased_path.write_text(
        json.dumps(unreleased, separators=(",", ":")),
        encoding="utf-8",
    )
    in_advisory_path.write_text(
        json.dumps(in_advisory, separators=(",", ":")),
        encoding="utf-8",
    )
    tar_path = results_dir / "filter-results.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(unreleased_path, arcname="unreleased_rpms.json")
        tar.add(in_advisory_path, arcname="in_advisory_rpms.json")
    return results_dir


def push_results_to_oci(
    results_dir: Path,
    oci_storage: str,
    oras_options: str,
    *,
    stderr_path: Path | None = None,
) -> str:
    """Push ``filter-results.tar.gz`` and return an ``oci:repo@digest`` reference."""
    oci_tag = f"filter-results-{int(time.time())}"
    oci_ref = f"{oci_storage}:{oci_tag}"
    logger.info("Pushing filter results to OCI storage: %s", oci_ref)
    extra = shlex.split(oras_options) if oras_options.strip() else []
    command_log = stderr_path if stderr_path is not None else COMMAND_LOG_PATH
    try:
        with oci_auth.auth_file(oci_storage, stderr_path=command_log) as auth_path:
            subprocess_cmd.run_cmd(
                [
                    "oras",
                    "push",
                    *extra,
                    "--annotation=quay.expires-after=1d",
                    "--registry-config",
                    str(auth_path),
                    oci_ref,
                    "filter-results.tar.gz:application/gzip",
                ],
                cwd=results_dir,
                check=True,
                stream_stdout=True,
                stderr_path=command_log,
            )
            descriptor = subprocess_cmd.run_cmd(
                [
                    "oras",
                    "manifest",
                    "fetch",
                    *extra,
                    oci_ref,
                    "--registry-config",
                    str(auth_path),
                    "--descriptor",
                ],
                check=True,
                stderr_path=command_log,
            ).stdout
            digest = json.loads(descriptor)["digest"]
    except subprocess.CalledProcessError as e:
        raise tekton.CheckStepError("pushing filter results to OCI", e) from e
    return f"oci:{oci_storage}@{digest}"


def _advisory_urls(git_repo: str, advisory_rel: str, repo_dir: Path) -> tuple[str, str]:
    """Return public and GitLab raw URLs for *advisory_rel* under *repo_dir*."""
    doc = advisory_data.load_advisory_yaml(repo_dir / advisory_rel)
    adv_type = advisory_data.get_advisory_spec_type(doc)
    adv_name = advisory_data.get_advisory_metadata_name(doc)
    prefix = advisory_data.advisory_url_prefix(git_repo)
    public_url = f"{prefix}/{adv_type}-{adv_name}"
    internal_url = gitlab.raw_file_url(git_repo, advisory_rel)
    return public_url, internal_url


def run_filter(
    transformed_snapshot_b64: str,
    origin: str,
    oci_storage: str,
    secret_mount: Path,
    oras_options: str,
    *,
    stderr_path: Path | None = None,
) -> FilterOutput:
    """Filter snapshot RPM entries against advisories stored in GitLab."""
    snapshot = file.decode_json_gzip_b64(transformed_snapshot_b64)
    if not isinstance(snapshot, list):
        raise ValueError("transformed snapshot must be a JSON array")
    logger.info("Transformed Snapshot JSON: %s", json.dumps(snapshot))
    command_log = stderr_path if stderr_path is not None else COMMAND_LOG_PATH

    dockerconfig = file.path_from_env_variable(
        "TA_DOCKERCONFIG_JSON_PATH",
        TA_DOCKERCONFIG_DEFAULT,
    )
    authentication.setup_docker_config(dockerconfig, optional=True)

    try:
        credentials = gitlab.read_credentials_from_mount(secret_mount)
    except OSError as e:
        raise tekton.CheckStepError("reading the mounted advisory secret", e) from e
    gitlab.export_env_for_image_helpers(credentials)
    gitlab.configure_git_oauth2_auth(credentials.access_token)

    origin_name = origin.strip()
    advisory_base_dir = f"data/advisories/{origin_name}"
    logger.info("Cloning %s...", credentials.git_repo)
    try:
        repo_dir = gitlab.clone_project_sparse(
            credentials.git_repo,
            None,
            [advisory_base_dir],
            parent_dir=CLONE_PARENT_DIR,
            stderr_path=command_log,
        )
    except (GitCommandError, subprocess.CalledProcessError) as e:
        raise tekton.CheckStepError("cloning the advisory Git repository", e) from e

    logger.info("Checking existing advisories in %s...", advisory_base_dir)
    advisory_root = repo_dir / advisory_base_dir
    subdirs = advisory_data.list_existing_advisory_subdirs(advisory_root)
    if not subdirs:
        logger.info("No existing advisories found. All RPMs are unreleased.")
        results_dir = write_results_archive(snapshot, [])
        artifact_ref = push_results_to_oci(
            results_dir,
            oci_storage,
            oras_options,
            stderr_path=command_log,
        )
        logger.info("Filter results artifact: %s", artifact_ref)
        return FilterOutput(
            result="Success",
            filter_results_artifact=artifact_ref,
            advisory_url="",
            advisory_internal_url="",
        )

    logger.info("Extracting purls from advisories...")
    purl_map = advisory_data.build_purl_to_advisory_map(repo_dir, advisory_root, subdirs)
    logger.info("Total unique purls from advisories: %s", len(purl_map))
    logger.info("Categorizing RPMs by advisory presence...")
    unreleased, in_advisory, advisory_files = advisory_data.categorize_rpm_entries(
        snapshot,
        purl_map,
    )
    logger.info("Unreleased RPMs: %s", len(unreleased))
    logger.info("In-advisory RPMs (need digest validation): %s", len(in_advisory))
    logger.info(
        "Unique advisories containing in-advisory RPMs: %s",
        len(advisory_files),
    )

    results_dir = write_results_archive(unreleased, in_advisory)
    artifact_ref = push_results_to_oci(
        results_dir,
        oci_storage,
        oras_options,
        stderr_path=command_log,
    )
    logger.info("Filter results artifact: %s", artifact_ref)

    public_url = ""
    internal_url = ""
    if not unreleased and len(advisory_files) == 1:
        single = advisory_files[0]
        logger.info("All RPMs found in the same single advisory: %s", single)
        public_url, internal_url = _advisory_urls(
            credentials.git_repo,
            single,
            repo_dir,
        )
    elif len(advisory_files) > 1:
        logger.info(
            "RPMs found in multiple advisories (%s). Not setting advisory_url.",
            len(advisory_files),
        )
        logger.info("Advisories:")
        for path in advisory_files:
            logger.info("%s", path)

    return FilterOutput(
        result="Success",
        filter_results_artifact=artifact_ref,
        advisory_url=public_url,
        advisory_internal_url=internal_url,
    )


def main() -> int:
    """Read Tekton env, write results, and always return ``0`` on a normal run."""
    (
        path_step_result,
        path_advisory_url,
        path_advisory_internal_url,
        path_internal_pr,
        path_internal_task_run,
        path_filter_artifact,
    ) = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_ADVISORY_URL",
        "RESULT_ADVISORY_INTERNAL_URL",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME",
        "RESULT_FILTER_RESULTS_ARTIFACT",
    )
    result_paths = {
        "result": path_step_result,
        "advisory_url": path_advisory_url,
        "advisory_internal_url": path_advisory_internal_url,
        "internal_pr_name": path_internal_pr,
        "internal_task_run_name": path_internal_task_run,
        "filter_results_artifact": path_filter_artifact,
    }
    program_basename = str(Path(sys.argv[0]).name) if sys.argv else PROG

    path_advisory_url.write_text("", encoding="utf-8")
    path_advisory_internal_url.write_text("", encoding="utf-8")
    path_filter_artifact.write_text("", encoding="utf-8")

    snapshot = tekton.require_env("TRANSFORMED_SNAPSHOT")
    origin = tekton.require_env("ORIGIN")
    oci_storage = tekton.require_env("OCI_STORAGE")
    pipeline_run_name = tekton.require_env("INTERNAL_REQUEST_PIPELINE_RUN_NAME")
    task_run_name = tekton.require_env("TASK_RUN_NAME")
    mount = file.path_from_env_variable(
        "ADVISORY_SECRET_MOUNT",
        ADVISORY_SECRET_MOUNT_DEFAULT,
    )
    command_log_path = COMMAND_LOG_PATH
    command_log_path.write_text("", encoding="utf-8")
    try:
        internal_request_results.write_result_paths(
            result_paths,
            pipeline_run_name=pipeline_run_name,
            task_run_name=task_run_name,
        )
        output = run_filter(
            snapshot,
            origin,
            oci_storage,
            mount,
            oras_options=os.environ.get("ORAS_OPTIONS", ""),
            stderr_path=command_log_path,
        )
        result_paths["result"].write_text(output.result, encoding="utf-8")
        result_paths["filter_results_artifact"].write_text(
            output.filter_results_artifact,
            encoding="utf-8",
        )
        result_paths["advisory_url"].write_text(output.advisory_url, encoding="utf-8")
        result_paths["advisory_internal_url"].write_text(
            output.advisory_internal_url,
            encoding="utf-8",
        )
    except Exception as e:
        tekton.write_failure_result(
            path_step_result,
            program_basename,
            e,
            command_log_path=command_log_path,
            workflow_action="filtering already released advisory rpms",
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
