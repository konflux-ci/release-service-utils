#!/usr/bin/env python3
"""Filter snapshot images already published in GitLab-stored advisories.

Reads advisory repository metadata from a mounted secret, sparse-clones the
advisory Git repository, and progressively removes arch-specific snapshot rows
that match ``spec.content.images`` entries in existing advisories.

Writes Tekton result files from ``RESULT_*`` environment variables.
The process exits with status ``0`` even on logical failure so callers can
read ``RESULT_RESULT``.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from git.exc import GitCommandError

import file
import internal_request
import subprocess_cmd
import tekton
from logger import logger
from vcs import gitlab

PROG = "filter_already_released_advisory_images.py"
USAGE = (
    f"usage: {PROG} --transformed-snapshot B64 --origin ORIGIN "
    f"--internal-request-pipeline-run-name NAME "
    f"--internal-request-task-run-name NAME\n"
    f"  --transformed-snapshot  Gzip+base64 JSON array of arch-specific images\n"
    f"  --origin                Release origin workspace (advisories subdir)\n"
)
ADVISORY_SECRET_MOUNT_DEFAULT = Path("/mnt/advisory_secret")


@dataclass(frozen=True)
class FilterOutput:
    """Values written to Tekton result files."""

    result: str
    unreleased_components_b64: str
    advisory_url: str
    advisory_internal_url: str


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse required CLI flags for Tekton parameters."""
    p = tekton.tekton_argument_parser(PROG)
    p.add_argument("-h", "--help", action="store_true")
    p.add_argument("--transformed-snapshot", metavar="B64")
    p.add_argument("--origin", metavar="ORIGIN")
    p.add_argument("--internal-request-pipeline-run-name", metavar="NAME")
    p.add_argument("--internal-request-task-run-name", metavar="NAME")
    ns = p.parse_args(argv or [])
    if ns.help or tekton.missing_blank_option_values(
        ("--transformed-snapshot", ns.transformed_snapshot),
        ("--origin", ns.origin),
        (
            "--internal-request-pipeline-run-name",
            ns.internal_request_pipeline_run_name,
        ),
        ("--internal-request-task-run-name", ns.internal_request_task_run_name),
    ):
        tekton.exit_with_usage(USAGE)
    return ns


def decode_transformed_snapshot(value: str) -> list[dict[str, Any]]:
    """Decode a gzip+base64 JSON array of arch-specific snapshot rows."""
    try:
        raw = base64.b64decode(value, validate=True)
        text = gzip.decompress(raw).decode("utf-8")
        data = json.loads(text)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        raise ValueError("invalid transformed snapshot payload") from e
    if not isinstance(data, list):
        raise ValueError("transformed snapshot must be a JSON array")
    return data


def unique_component_names(arch_images: list[dict[str, Any]]) -> list[str]:
    """Return unique ``name`` values from ``arch_images`` in first-seen order."""
    seen: set[str] = set()
    out: list[str] = []
    for row in arch_images:
        name = row.get("name")
        if not isinstance(name, str) or not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def encode_gzipped_base64_json(value: Any) -> str:
    """Return ``gzip(JSON)`` encoded as base64 without trailing newline."""
    payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.b64encode(gzip.compress(payload)).decode("ascii")


def filter_arch_images(
    arch_images: list[dict[str, Any]],
    existing_images: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Drop snapshot rows whose image triple exists in ``existing_images``."""
    kept: list[dict[str, Any]] = []
    for row in arch_images:
        ci = row.get("containerImage")
        tags = row.get("tags")
        repo = row.get("repository")
        if any(
            ex.get("containerImage") == ci
            and ex.get("tags") == tags
            and ex.get("repository") == repo
            for ex in existing_images
        ):
            continue
        kept.append(row)
    return kept


def list_advisory_subdirs(advisory_base_dir: Path) -> list[str]:
    """Return advisory subdirectories relative to ``advisory_base_dir``, newest first."""
    if not advisory_base_dir.is_dir():
        return []
    newest: dict[str, float] = {}
    for advisory_file in advisory_base_dir.rglob("advisory.yaml"):
        subdir = advisory_file.parent
        rel = subdir.relative_to(advisory_base_dir)
        if len(rel.parts) < 2:
            continue
        key = rel.as_posix()
        mtime = subdir.stat().st_mtime
        newest[key] = max(newest.get(key, 0.0), mtime)
    return [name for name, _ in sorted(newest.items(), key=lambda item: item[1], reverse=True)]


def advisory_errata_url_prefix(git_repo: str) -> str:
    """Return the public errata URL prefix for production or stage advisories."""
    if "/rhtap-release/" in git_repo:
        return "https://access.stage.redhat.com/errata"
    return "https://access.redhat.com/errata"


def build_advisory_urls(
    git_repo: str,
    advisory_file: Path,
    advisory_type: str,
    advisory_name: str,
) -> tuple[str, str]:
    """Build public and GitLab raw URLs for a matching advisory."""
    prefix = advisory_errata_url_prefix(git_repo)
    public_url = f"{prefix}/{advisory_type}-{advisory_name}"
    internal_url = gitlab.raw_file_url(git_repo, advisory_file.as_posix())
    return public_url, internal_url


def run_filter(
    transformed_snapshot_b64: str,
    origin: str,
    secret_mount: Path,
    *,
    work_dir: Path | None = None,
    clone_sparse: Callable[[], Path] | None = None,
    run_cmd: Callable[..., str] | None = None,
) -> FilterOutput:
    """Filter arch-specific snapshot rows against advisories stored in GitLab."""
    arch_images = decode_transformed_snapshot(transformed_snapshot_b64)
    logger.info("Transformed Snapshot JSON (arch-specific): %s", json.dumps(arch_images))

    try:
        credentials = gitlab.read_credentials_from_mount(secret_mount)
    except OSError as e:
        raise tekton.CheckStepError("reading the mounted advisory secret", e) from e
    gitlab.export_env_for_image_helpers(credentials)
    gitlab.configure_git_oauth2_auth(credentials.access_token)

    advisory_base_dir = f"data/advisories/{origin.strip()}"
    tmp_root = work_dir or Path("/tmp")
    try:
        if clone_sparse is not None:
            repo_dir = clone_sparse()
        else:
            repo_dir = gitlab.clone_project_sparse(
                credentials.git_repo,
                gitlab.DEFAULT_BRANCH,
                [advisory_base_dir],
                parent_dir=tmp_root,
                stderr_path=None,
            )
    except GitCommandError as e:
        raise tekton.CheckStepError("cloning the advisory Git repository", e) from e

    advisory_root = repo_dir / advisory_base_dir

    existing = list_advisory_subdirs(advisory_root)
    if not existing:
        logger.info("No existing advisories found. No components have been released yet.")
        names = unique_component_names(arch_images)
        return FilterOutput(
            result="Success",
            unreleased_components_b64=encode_gzipped_base64_json(names),
            advisory_url="",
            advisory_internal_url="",
        )

    latest_advisory_file = ""
    working = arch_images
    for subdir in existing:
        advisory_file = advisory_root / subdir / "advisory.yaml"
        existing_images = subprocess_cmd.run_yq_json(
            advisory_file,
            ".spec.content.images // []",
            run_cmd=run_cmd,
        )
        if not isinstance(existing_images, list):
            existing_images = []

        logger.info("Comparing against: %s", advisory_file)
        before = working
        working = filter_arch_images(working, existing_images)

        if len(before) > len(working) and not latest_advisory_file:
            latest_advisory_file = str(advisory_file.relative_to(repo_dir))
            logger.info(
                "Tracked latest advisory: %s (filtered %s items)",
                latest_advisory_file,
                len(before) - len(working),
            )

        if not working:
            logger.info(
                "All arch images in the snapshot have already been released in advisories. "
                "Stopping pipeline."
            )
            latest_path = repo_dir / latest_advisory_file
            adv_type = str(
                subprocess_cmd.run_yq_json(latest_path, ".spec.type", run_cmd=run_cmd)
            ).strip()
            adv_name = str(
                subprocess_cmd.run_yq_json(latest_path, ".metadata.name", run_cmd=run_cmd)
            ).strip()
            public_url, internal_url = build_advisory_urls(
                credentials.git_repo,
                Path(latest_advisory_file),
                adv_type,
                adv_name,
            )
            return FilterOutput(
                result="Success",
                unreleased_components_b64=encode_gzipped_base64_json([]),
                advisory_url=public_url,
                advisory_internal_url=internal_url,
            )

    original_count = len(arch_images)
    remaining_count = len(working)
    logger.info(
        "Filtered out %s arch-specific image(s) already in advisories",
        original_count - remaining_count,
    )
    logger.info("Remaining unpublished arch images: %s", json.dumps(working))
    names = unique_component_names(working)
    return FilterOutput(
        result="Success",
        unreleased_components_b64=encode_gzipped_base64_json(names),
        advisory_url="",
        advisory_internal_url="",
    )


def main(argv: list[str] | None = None) -> int:
    """Parse CLI args, write Tekton results, and always return ``0`` on normal runs."""
    raw_argv = sys.argv if argv is None else argv
    try:
        args = parse_args(raw_argv[1:])
    except SystemExit as e:
        code = e.code
        return code if isinstance(code, int) else 1

    (
        path_step_result,
        path_advisory_url,
        path_advisory_internal_url,
        path_internal_pr,
        path_internal_task_run,
        path_unreleased,
    ) = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_ADVISORY_URL",
        "RESULT_ADVISORY_INTERNAL_URL",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME",
        "RESULT_UNRELEASED_COMPONENTS",
    )
    result_paths = {
        "result": path_step_result,
        "advisory_url": path_advisory_url,
        "advisory_internal_url": path_advisory_internal_url,
        "internal_pr_name": path_internal_pr,
        "internal_task_run_name": path_internal_task_run,
        "unreleased_components": path_unreleased,
    }
    program_basename = str(Path(raw_argv[0]).name)

    path_advisory_url.write_text("", encoding="utf-8")
    path_advisory_internal_url.write_text("", encoding="utf-8")
    path_unreleased.write_text("", encoding="utf-8")

    mount = file.path_from_env_variable(
        "ADVISORY_SECRET_MOUNT",
        ADVISORY_SECRET_MOUNT_DEFAULT,
    )
    try:
        internal_request.write_result_paths(
            result_paths,
            pipeline_run_name=args.internal_request_pipeline_run_name,
            task_run_name=args.internal_request_task_run_name,
        )
        output = run_filter(
            args.transformed_snapshot,
            args.origin,
            mount,
        )
        result_paths["result"].write_text(output.result, encoding="utf-8")
        result_paths["unreleased_components"].write_text(
            output.unreleased_components_b64,
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
            workflow_action="filtering already released advisory images",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
