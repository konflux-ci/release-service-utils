#!/usr/bin/env python3
"""Process advisory repo changes: send Kafka messages, update Pyxis.

Clones the advisories repo to find new/updated advisory files, then sends their
data to Kafka and updates Pyxis container advisory records.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml

from release_service_utils.helpers import retry, tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.pyxis_api import (
    create_or_update_advisory,
    get_image_by_digest,
    link_image_to_advisory,
)
from release_service_utils.helpers.subprocess_cmd import run_cmd
from release_service_utils.helpers.vcs import git, gitlab
from release_service_utils.helpers.retry import retry_with_exponential_backoff

PROG = "advisory_push.py"


class AdvisoryProcessingError(Exception):
    """Aggregate error when one or more advisories failed to process."""

    def __init__(self, failures: list[str]):  # noqa: D107
        self.failures = failures
        super().__init__(
            f"{len(failures)} advisory operation(s) failed:\n" + "\n".join(failures)
        )


def send_kafka_message(
    json_path: Path,
    advisory_state: str,
    kafka_topic: str,
    username_file: str,
    password_file: str,
    bootstrap_servers_file: str,
) -> None:
    """Send Kafka message via kafka/producer.py CLI."""
    env = os.environ.copy()
    env["KAFKA_TOPIC"] = kafka_topic
    subprocess.run(
        [
            "/home/kafka/producer.py",
            "--username-file",
            username_file,
            "--password-file",
            password_file,
            "--bootstrap-servers-file",
            bootstrap_servers_file,
            "--header",
            f"advisory_state={advisory_state}",
            "--json-file",
            str(json_path),
        ],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def advisory_yaml_to_pyxis_payload(advisory_doc: dict[str, Any]) -> dict[str, Any]:
    """Transform advisory YAML document to Pyxis RedHatContainerAdvisory payload."""
    spec = advisory_doc.get("spec", {})
    metadata = advisory_doc.get("metadata", {})

    advisory_type = spec.get("type", "")
    advisory_name = metadata.get("name", "")
    advisory_id = f"{advisory_type}-{advisory_name}"

    # Collect unique CVEs from all images
    cves: list[dict[str, str]] = []
    seen_cves: set[str] = set()
    images = spec.get("content", {}).get("images", [])
    for image in images:
        if not isinstance(image, dict):
            continue
        fixed_cves = image.get("cves", {}).get("fixed", {})
        if isinstance(fixed_cves, dict):
            for cve_id in fixed_cves.keys():
                if cve_id not in seen_cves:
                    seen_cves.add(cve_id)
                    cves.append(
                        {
                            "id": cve_id,
                            "url": f"https://access.redhat.com/security/cve/{cve_id}",
                        }
                    )

    # Collect public issues
    issues: list[dict[str, str]] = []
    fixed_issues = spec.get("issues", {}).get("fixed", [])
    if isinstance(fixed_issues, list):
        for issue in fixed_issues:
            if not isinstance(issue, dict):
                continue
            if issue.get("public") is False:
                continue
            issue_id = issue.get("id")
            source = issue.get("source")
            if issue_id and source:
                issues.append({"id": issue_id, "issue_tracker": source})

    return {
        "_id": advisory_id,
        "content_type": "CONTAINER",
        "cves": cves,
        "description": spec.get("description", ""),
        "issues": issues,
        "severity": spec.get("severity", "None"),
        "ship_date": metadata.get("ship_date", ""),
        "solution": spec.get("solution", ""),
        "synopsis": spec.get("synopsis", ""),
        "topic": spec.get("topic", ""),
        "type": advisory_type,
    }


def clone_and_find_changes(
    repo_url: str,
    revision: str,
    before_revision: str,
    target_branch: str,
    access_token: str,
) -> tuple[Path, list[str], list[str]]:
    """Clone advisories repo, return (repo_dir, new_advisory_files, updated_advisory_files)."""
    logger.info(f"Cloning {repo_url} at {revision}")
    gitlab.configure_git_oauth2_auth(access_token)

    parent_dir = Path(tempfile.mkdtemp(prefix="advisory-push-"))
    repo_dir = git.clone(
        parent_dir,
        repo_url,
        stderr_path=parent_dir / "git-stderr.log",
    )

    # Configure safe directory and sparse checkout
    run_cmd(
        ["git", "config", "--global", "--add", "safe.directory", str(repo_dir)],
        check=True,
        cwd=repo_dir,
    )
    run_cmd(
        ["git", "sparse-checkout", "init", "--cone"],
        check=True,
        cwd=repo_dir,
    )
    run_cmd(
        ["git", "sparse-checkout", "set", ".gitlab"],
        check=True,
        cwd=repo_dir,
    )
    git.checkout(repo_dir, target_branch, stderr_path=parent_dir / "git-stderr.log")

    logger.info(f"Finding changed advisories between {before_revision} and {revision}")
    new_files = git.diff_files(
        repo_dir,
        before_revision,
        revision,
        "A",
        stderr_path=parent_dir / "git-stderr.log",
    )
    new_advisories = [
        f
        for f in new_files
        if f.startswith("data/advisories/") and f.endswith("/advisory.yaml")
    ]

    updated_files = git.diff_files(
        repo_dir,
        before_revision,
        revision,
        "M",
        stderr_path=parent_dir / "git-stderr.log",
    )
    updated_advisories = [
        f
        for f in updated_files
        if f.startswith("data/advisories/") and f.endswith("/advisory.yaml")
    ]

    logger.info(
        f"Found {len(new_advisories)} new, {len(updated_advisories)} updated advisories"
    )
    return repo_dir, new_advisories, updated_advisories


def send_messages(
    repo_dir: Path,
    revision: str,
    new_advisories: list[str],
    updated_advisories: list[str],
    kafka_topic: str,
    kafka_username: str,
    kafka_password: str,
    kafka_bootstrap_servers: str,
) -> None:
    """Send Kafka messages for new/updated advisories."""
    if not new_advisories and not updated_advisories:
        logger.info("No advisories to send messages for")
        return

    logger.info("Sending Kafka messages")

    with tempfile.TemporaryDirectory(prefix="advisory-msgs-") as tmpdir:
        tmp_path = Path(tmpdir)

        # Process new advisories
        for advisory_path in new_advisories:
            logger.info(f"Sending new advisory: {advisory_path}")
            yaml_content = git.show_file(repo_dir, revision, advisory_path)
            advisory_doc = yaml.safe_load(yaml_content)

            # Set updated_date to ship_date for new advisories
            ship_date = advisory_doc.get("metadata", {}).get("ship_date", "")
            advisory_doc.setdefault("metadata", {})["updated_date"] = ship_date

            json_file = tmp_path / "advisory.json"
            json_file.write_text(json.dumps(advisory_doc), encoding="utf-8")

            send_kafka_message(
                json_file,
                "new",
                kafka_topic,
                kafka_username,
                kafka_password,
                kafka_bootstrap_servers,
            )

        # Process updated advisories
        for advisory_path in updated_advisories:
            logger.info(f"Sending updated advisory: {advisory_path}")
            yaml_content = git.show_file(repo_dir, revision, advisory_path)
            advisory_doc = yaml.safe_load(yaml_content)

            # Get last commit date for this file
            updated_date = git.last_commit_date(
                repo_dir,
                advisory_path,
                revision,
                "%Y-%m-%dT%H:%M:%SZ",
            )
            advisory_doc.setdefault("metadata", {})["updated_date"] = updated_date

            # Get change log from creation to current revision
            result = run_cmd(
                [
                    "git",
                    "log",
                    "--diff-filter=A",
                    "--format=%H",
                    revision,
                    "--",
                    advisory_path,
                ],
                cwd=repo_dir,
            )
            original_commits = result.strip().split("\n")
            original_commit = original_commits[-1] if original_commits else revision

            change_log = git.file_change_log(
                repo_dir,
                advisory_path,
                original_commit,
                revision,
            )
            advisory_doc.setdefault("metadata", {})["git_log"] = change_log

            json_file = tmp_path / "advisory.json"
            json_file.write_text(json.dumps(advisory_doc), encoding="utf-8")

            send_kafka_message(
                json_file,
                "updated",
                kafka_topic,
                kafka_username,
                kafka_password,
                kafka_bootstrap_servers,
            )

    logger.info("Kafka messages sent")


def update_pyxis(
    repo_dir: Path,
    revision: str,
    modified_advisories: list[str],
    pyxis_advisory_api: str,
    pyxis_images_api: str,
    pyxis_cert: str,
    pyxis_key: str,
) -> None:
    """Update Pyxis advisory records and link images."""
    if not modified_advisories:
        logger.info("No advisories to update in Pyxis")
        return

    logger.info(f"Updating Pyxis for {len(modified_advisories)} advisories")
    cert_tuple = (pyxis_cert, pyxis_key)
    failures: list[str] = []

    for advisory_path in modified_advisories:
        try:
            logger.info(f"Processing Pyxis update for {advisory_path}")
            yaml_content = git.show_file(repo_dir, revision, advisory_path)
            advisory_doc = yaml.safe_load(yaml_content)

            # Create/update advisory record
            payload = advisory_yaml_to_pyxis_payload(advisory_doc)
            advisory_id = payload["_id"]
            logger.info(f"Creating/updating advisory {advisory_id}")

            retry_with_exponential_backoff(
                functools.partial(
                    create_or_update_advisory,
                    pyxis_advisory_api,
                    advisory_id,
                    payload,
                    cert=cert_tuple,
                ),
                max_attempts=3,
            )

            # Link images to advisory
            images = advisory_doc.get("spec", {}).get("content", {}).get("images", [])
            for image in images:
                if not isinstance(image, dict):
                    continue
                container_image = image.get("containerImage", "")
                repository = image.get("repository", "")
                if not container_image or not repository:
                    continue

                # Extract digest from container image (format: registry/repo@sha256:digest)
                if "@sha256:" not in container_image:
                    logger.warning(f"Skipping image without digest: {container_image}")
                    continue
                digest = container_image.split("@sha256:")[1]
                repository_path = (
                    repository.split("/", 1)[1] if "/" in repository else repository
                )

                logger.info(f"Linking image {digest[:12]}... to advisory {advisory_id}")

                # Retry image operations
                def _link_image() -> None:
                    image_data = get_image_by_digest(pyxis_images_api, digest, cert=cert_tuple)
                    image_id = image_data.get("_id")
                    if not image_id:
                        raise ValueError(f"No _id in Pyxis image response for digest {digest}")
                    link_image_to_advisory(
                        pyxis_images_api,
                        image_id,
                        repository_path,
                        advisory_id,
                        cert=cert_tuple,
                    )
                    time.sleep(0.1)

                retry.retry_with_exponential_backoff(
                    _link_image,
                    max_attempts=3,
                    base_sleep_seconds=2,
                )

        except Exception as exc:
            error_msg = f"{advisory_path}: {exc!s}"
            logger.error(f"Failed to process advisory: {error_msg}")
            failures.append(error_msg)

    if failures:
        raise AdvisoryProcessingError(failures)

    logger.info("Pyxis updates completed")


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Process advisory repo changes: send Kafka messages, update Pyxis.",
        prog=os.path.basename(__file__),
    )
    parser.add_argument(
        "--repo-url",
        required=True,
        help="Advisory repository URL",
    )
    parser.add_argument(
        "--revision",
        required=True,
        help="Commit SHA after the push",
    )
    parser.add_argument(
        "--before-revision",
        required=True,
        help="Commit SHA before the push",
    )
    parser.add_argument(
        "--target-branch",
        default="main",
        help="Target branch to clone (default: main)",
    )
    return parser


def run_advisory_push(
    repo_url: str,
    revision: str,
    before_revision: str,
    target_branch: str,
    gitlab_token: str,
    kafka_topic: str,
    kafka_username: str,
    kafka_password: str,
    kafka_bootstrap_servers: str,
    pyxis_advisory_api: str,
    pyxis_images_api: str,
    pyxis_cert: str,
    pyxis_key: str,
) -> None:
    """Clone repo, send messages, update Pyxis."""
    repo_dir, new_advisories, updated_advisories = clone_and_find_changes(
        repo_url,
        revision,
        before_revision,
        target_branch,
        gitlab_token,
    )

    send_messages(
        repo_dir,
        revision,
        new_advisories,
        updated_advisories,
        kafka_topic,
        kafka_username,
        kafka_password,
        kafka_bootstrap_servers,
    )

    modified_advisories = new_advisories + updated_advisories
    update_pyxis(
        repo_dir,
        revision,
        modified_advisories,
        pyxis_advisory_api,
        pyxis_images_api,
        pyxis_cert,
        pyxis_key,
    )

    logger.info("Advisory push processing completed successfully")


def main() -> int:
    """Read args/env, call run(), return exit code."""
    parser = setup_argparser()
    args = parser.parse_args()

    kafka_topic = tekton.require_env("KAFKA_TOPIC")
    kafka_username = tekton.require_env("KAFKA_USERNAME")
    kafka_password = tekton.require_env("KAFKA_PASSWORD")
    kafka_bootstrap_servers = tekton.require_env("KAFKA_BOOTSTRAP_SERVERS")

    pyxis_advisory_api = tekton.require_env("PYXIS_ADVISORY_API")
    pyxis_images_api = tekton.require_env("PYXIS_IMAGES_API")
    pyxis_cert = tekton.require_env("PYXIS_CERT")
    pyxis_key = tekton.require_env("PYXIS_KEY")

    gitlab_token_path = os.environ.get("GITLAB_OAUTH2_TOKEN_PATH", "")
    token_file = Path(gitlab_token_path or "/secrets/gitlab/provider.token")
    if token_file.exists():
        gitlab_token = token_file.read_text().strip()
    else:
        raise ValueError(f"gitlab token file not fond: {token_file}")
    if not gitlab_token:
        raise ValueError(
            "GITLAB_OAUTH2_TOKEN not set and /secrets/gitlab/provider.token not found"
        )

    run_advisory_push(
        args.repo_url,
        args.revision,
        args.before_revision,
        args.target_branch,
        gitlab_token,
        kafka_topic,
        kafka_username,
        kafka_password,
        kafka_bootstrap_servers,
        pyxis_advisory_api,
        pyxis_images_api,
        pyxis_cert,
        pyxis_key,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
