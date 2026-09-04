#!/usr/bin/env python3
"""Push snapshot images to destination registries using cosign copy and oras cp.

For each component in the snapshot, copies container images (and optionally
source containers and migration artifacts) to all configured destination
repositories with the specified tags.  Pushes are executed concurrently via
a thread pool.  Produces a JSON results file with image metadata.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
import re

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from release_service_utils.helpers import (
    authentication,
    file,
    image_ref,
    memory_throttle,
    oras_utils,
    retry,
    skopeo,
    subprocess_cmd,
)
from release_service_utils.helpers import snapshot as snapshot_helper
from release_service_utils.helpers.logger import logger

MEMORY_THRESHOLD = 80
BURST_SIZE = 5
STABILIZATION_DELAY = 2.0

MULTI_ARCH_MEDIA_TYPES = frozenset(
    {
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.index.v1+json",
    }
)


@dataclass(frozen=True)
class PushJob:
    """Parameters for a single image push operation."""

    origin_digest: str
    name: str
    container_image: str
    repository_url: str
    tag: str
    platform: str
    source_auth_file: Path
    retries: int
    copy_bundle_migrations: bool


@dataclass(frozen=True)
class MigrationJob:
    """Parameters for a migration artifact push operation."""

    source_repo: str
    migration_digest: str
    name: str
    repository_url: str
    migration_tag: str
    source_auth_file: Path
    retries: int


def select_oci_auth(reference: str) -> str:
    """Run ``select-oci-auth`` and return the raw JSON output."""
    result = subprocess_cmd.run_cmd(["select-oci-auth", reference], check=False)
    return result.stdout.strip() or "{}"


def create_source_auth_file(container_image: str) -> Path:
    """Create an auth file for the source registry with a repo-level key.

    oras has very limited support for selecting the right auth entry from
    a multi-entry credentials file, so we create a custom auth file with
    just one entry keyed by the full repository path (instead of the bare
    registry hostname) so that ``oras resolve`` and ``cosign copy`` can
    match credentials correctly.
    """
    reg = image_ref.registry(container_image)
    source_repo = image_ref.repository(container_image)
    auth_json_str = select_oci_auth(container_image)
    auth_data = json.loads(auth_json_str)

    auths = auth_data.get("auths", {})
    if reg in auths:
        auths[source_repo] = auths.pop(reg)
    auth_data["auths"] = auths

    auth_file = file.make_tempfile_path("source-auth-")
    auth_file.write_text(json.dumps(auth_data), encoding="utf-8")
    return auth_file


def create_dest_auth_file(repository_url: str) -> Path:
    """Create an auth file for the destination registry.

    For docker.io the auth entry is kept as-is (uses
    ``https://index.docker.io/v1/``).  For other registries the key is
    remapped to the full repository path so that source and destination
    entries can coexist in the combined auth file used by ``cosign copy``.
    """
    reg = image_ref.registry(repository_url)
    auth_json_str = select_oci_auth(repository_url)
    auth_data = json.loads(auth_json_str)

    if reg != "docker.io":
        auths = auth_data.get("auths", {})
        if reg in auths:
            auths[repository_url] = auths.pop(reg)
        auth_data["auths"] = auths

    auth_file = file.make_tempfile_path("dest-auth-")
    auth_file.write_text(json.dumps(auth_data), encoding="utf-8")
    return auth_file


def create_combined_docker_config(source_auth_file: Path, dest_auth_file: Path) -> Path:
    """Merge source and dest auth files into a Docker config directory.

    Returns the path to a temporary directory containing ``config.json``.
    """
    source_data = json.loads(source_auth_file.read_text(encoding="utf-8"))
    dest_data = json.loads(dest_auth_file.read_text(encoding="utf-8"))

    combined: dict[str, Any] = {}
    for d in [source_data, dest_data]:
        for key, value in d.items():
            if isinstance(value, dict) and isinstance(combined.get(key), dict):
                combined[key].update(value)
            else:
                combined[key] = value

    config_dir = Path(tempfile.mkdtemp(prefix="docker-config-"))
    config_path = config_dir / "config.json"
    config_path.write_text(json.dumps(combined), encoding="utf-8")
    return config_dir


def get_image_architectures(container_image: str) -> list[dict[str, Any]]:
    """Run ``get-image-architectures`` and parse the JSONL output."""
    result = subprocess_cmd.run_cmd(["get-image-architectures", container_image], check=True)
    entries = []
    for line in result.stdout.strip().splitlines():
        if line.strip():
            entries.append(json.loads(line))
    return entries


def oras_discover_referrers(reference: str, auth_file: Path) -> list[dict[str, Any]]:
    """Run ``oras discover`` and return the referrers list."""
    result = subprocess_cmd.run_cmd(
        [
            "oras",
            "discover",
            "--registry-config",
            str(auth_file),
            reference,
            "--format",
            "json",
        ],
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise RuntimeError(f"oras discover failed (exit {result.returncode}): {stderr}")
    data = json.loads(result.stdout)
    return data.get("referrers", [])


def cosign_copy(
    source: str, dest: str, docker_config_dir: Path, only: str | None = None
) -> None:
    """Run ``cosign copy -f`` with the specified Docker config."""
    subprocess_cmd.run_cmd(
        ["cosign", "copy", "-f"] + (["--only", only] if only else []) + [source, dest],
        env={"DOCKER_CONFIG": str(docker_config_dir)},
        check=True,
    )


def _discover_artifacts_with_retry(
    reference: str,
    source_auth_file: Path,
    retries: int,
) -> list[dict[str, Any]]:
    """Discover attached artifacts with retries; return count.

    On persistent failure falls back to 0 (caller uses cosign copy).
    """
    try:
        referrers = retry.retry_with_exponential_backoff(
            lambda: oras_discover_referrers(reference, source_auth_file),
            max_attempts=retries + 1,
            base_sleep_seconds=0,
        )
        logger.info("Found %d artifacts", len(referrers))
        return referrers
    except Exception:
        logger.warning("Max retries exceeded for oras discover. Falling back to cosign copy.")
        return []


def push_image(job: PushJob) -> dict[str, str]:
    """Push a single image to a destination repository.

    Skip the push when the destination already holds the expected digest.
    Returns a dict with ``name`` and ``url`` keys.
    """
    dest_auth_file = create_dest_auth_file(job.repository_url)
    try:
        dest_ref = f"{job.repository_url}:{job.tag}"
        destination_digest = oras_utils.oras_resolve(
            dest_ref, auth_file=dest_auth_file, check=False
        )

        if destination_digest is not None and destination_digest == job.origin_digest:
            logger.info(
                "Component push skipped (source digest exists at destination): %s (%s)",
                job.name,
                job.container_image,
            )
            return {"name": job.name, "url": dest_ref}

        logger.info("Pushing component: %s to %s:%s", job.name, job.repository_url, job.tag)

        docker_config_dir = create_combined_docker_config(job.source_auth_file, dest_auth_file)
        try:
            artifacts = []
            if job.copy_bundle_migrations:
                logger.info("Checking for attached artifacts on %s", job.container_image)
                raw_artifacts = _discover_artifacts_with_retry(
                    job.container_image, job.source_auth_file, job.retries
                )
                for item in raw_artifacts:
                    artifact_type = item.get("artifactType") or item.get("mediaType") or ""
                    if re.search(r"cosign.*signature|cosign/signature", artifact_type):
                        logger.info(
                            "Skipping attached artifact (cosign signature): %s", artifact_type
                        )
                    else:
                        artifacts.append(item)

            def do_copy() -> None:
                if job.copy_bundle_migrations and len(artifacts) > 0:
                    oras_utils.oras_cp(
                        job.container_image,
                        dest_ref,
                        from_auth=job.source_auth_file,
                        to_auth=dest_auth_file,
                        recursive=False,
                        platform=job.platform,
                    )
                    if artifacts:
                        logger.info(
                            "Copying %d attached artifacts for %s to %s",
                            len(artifacts),
                            job.container_image,
                            dest_ref,
                        )
                        for artifact in artifacts:
                            artifact_digest = artifact.get("digest")
                            if not artifact_digest:
                                logger.warning(
                                    "Skipping artifact with missing digest: %s", artifact
                                )
                                continue
                            artifact_ref = (
                                f"{job.container_image.split('@')[0]}@{artifact_digest}"
                            )
                            dest_artifact_ref = f"{dest_ref}"
                            logger.info(
                                "Copying attached artifact: %s to %s",
                                artifact_ref,
                                dest_artifact_ref,
                            )
                            oras_utils.oras_cp(
                                artifact_ref,
                                dest_artifact_ref,
                                from_auth=job.source_auth_file,
                                to_auth=dest_auth_file,
                                recursive=False,
                            )
                else:
                    skopeo.copy(
                        f"docker://{job.container_image}",
                        f"docker://{dest_ref}",
                        source_auth_file=job.source_auth_file,
                        dest_auth_file=dest_auth_file,
                        all=True,
                        check=True,
                    )
                    cosign_copy(
                        job.container_image, dest_ref, docker_config_dir, only="att,sbom"
                    )

            retry.retry_with_exponential_backoff(
                do_copy,
                max_attempts=job.retries + 1,
                base_sleep_seconds=0,
            )
        finally:
            shutil.rmtree(docker_config_dir, ignore_errors=True)

        return {"name": job.name, "url": dest_ref}
    finally:
        dest_auth_file.unlink(missing_ok=True)


def push_migration_artifact(job: MigrationJob) -> None:
    """Push a migration artifact using ``oras cp``."""
    dest_auth_file = create_dest_auth_file(job.repository_url)
    try:
        dest_ref = f"{job.repository_url}:{job.migration_tag}"
        destination_digest = oras_utils.oras_resolve(
            dest_ref, auth_file=dest_auth_file, check=False
        )

        if destination_digest is not None and destination_digest == job.migration_digest:
            logger.info(
                "Migration artifact push skipped (already exists at destination): %s (%s)",
                job.name,
                f"{job.source_repo}@{job.migration_digest}",
            )
            return

        migration_source = f"{job.source_repo}@{job.migration_digest}"
        logger.info(
            "Pushing migration artifact for component: %s to %s:%s",
            job.name,
            job.repository_url,
            job.migration_tag,
        )

        def do_copy() -> None:
            oras_utils.oras_cp(
                migration_source,
                dest_ref,
                from_auth=job.source_auth_file,
                to_auth=dest_auth_file,
            )

        retry.retry_with_exponential_backoff(
            do_copy,
            max_attempts=job.retries + 1,
            base_sleep_seconds=0,
        )
    finally:
        dest_auth_file.unlink(missing_ok=True)


def validate_snapshot(snapshot_data: dict[str, Any]) -> None:
    """Validate that all components have tags in their repositories."""
    components = snapshot_data.get("components", [])
    for component in components:
        repositories = component.get("repositories", [])
        for repo in repositories:
            if not repo.get("tags"):
                logger.error(
                    "Snapshot content:\n%s",
                    json.dumps(snapshot_data, indent=2),
                )
                raise RuntimeError(
                    "Found components in the snapshot file that do not contain tags"
                )


@dataclass(frozen=True)
class _ResolvedComponent:
    """Result of resolving a single component's metadata before job submission."""

    name: str
    container_image: str
    arches: list[str]
    oses: list[str]
    origin_digest: str
    platform: str
    source_auth_file: Path
    source_repo: str
    source_container: str
    source_container_digest: str
    source_tag: str
    migration_digest: str
    migration_tag: str
    repositories: list[dict[str, Any]]


def _resolve_component(
    component: dict[str, Any],
    *,
    default_push_src: bool,
    copy_bundle_migrations: bool,
) -> _ResolvedComponent:
    """Resolve image metadata, auth, and source container for a single component."""
    container_image = component["containerImage"]
    name = component["name"]

    source_auth_file = create_source_auth_file(container_image)

    arch_entries = get_image_architectures(container_image)
    arches = [e["platform"]["architecture"] for e in arch_entries]
    oses = [e["platform"]["os"] for e in arch_entries]

    inspect_result = skopeo.inspect(container_image, raw=True)
    if inspect_result.returncode != 0:
        stderr = inspect_result.stderr.strip()
        raise RuntimeError(f"skopeo inspect failed for {container_image}: {stderr}")

    media_type_data = json.loads(inspect_result.stdout) if inspect_result.stdout else {}
    media_type = media_type_data.get("mediaType", "")

    platform = ""
    if media_type in MULTI_ARCH_MEDIA_TYPES:
        os_name = oses[0] if oses else ""
        arch = arches[0] if arches else ""
        platform = f"{os_name}/{arch}"

    origin_digest = oras_utils.oras_resolve(
        container_image, auth_file=source_auth_file, check=False
    )
    if origin_digest is None:
        raise RuntimeError(f"Failed to resolve digest for {container_image}")

    source_repo = image_ref.repository(container_image)
    source_container = ""
    source_container_digest = ""
    source_tag = ""

    should_push_source = snapshot_helper.component_push_source_container(
        component, default_push_src
    )
    if should_push_source:
        source_tag = origin_digest.replace(":", "-") + ".src"
        source_container = f"{source_repo}:{source_tag}"
        source_container_digest = oras_utils.oras_resolve(
            source_container, auth_file=source_auth_file, check=False
        )
        if source_container_digest is None:
            raise RuntimeError(f"Source container {source_container} not found")

    migration_digest = ""
    migration_tag = ""
    if copy_bundle_migrations:
        ann_list = component.get("metadata", {}).get("annotations") or []
        ann_map = {a["name"]: a.get("value", "") for a in ann_list if "name" in a}
        migration_digest = ann_map.get("dev.konflux-ci.task.migration.digest", "")
        migration_tag = ann_map.get("dev.konflux-ci.task.migration.tag", "")
        if migration_digest and migration_tag:
            logger.info(
                "Found migration annotations for %s: digest=%s, tag=%s",
                name,
                migration_digest,
                migration_tag,
            )

    return _ResolvedComponent(
        name=name,
        container_image=container_image,
        arches=arches,
        oses=oses,
        origin_digest=origin_digest,
        platform=platform,
        source_auth_file=source_auth_file,
        source_repo=source_repo,
        source_container=source_container,
        source_container_digest=source_container_digest,
        source_tag=source_tag,
        migration_digest=migration_digest,
        migration_tag=migration_tag,
        repositories=component.get("repositories", []),
    )


def _build_component_jobs(
    resolved: _ResolvedComponent,
    *,
    retries: int,
    copy_bundle_migrations: bool,
) -> list[tuple[Any, PushJob | MigrationJob]]:
    """Build all push/migration jobs for a resolved component."""
    jobs: list[tuple[Any, PushJob | MigrationJob]] = []

    for repo in resolved.repositories:
        tags = repo.get("tags", [])
        repository_url = repo["url"]

        if resolved.source_container_digest:
            jobs.append(
                (
                    push_image,
                    PushJob(
                        origin_digest=resolved.source_container_digest,
                        name=resolved.name,
                        container_image=resolved.source_container,
                        repository_url=repository_url,
                        tag=resolved.source_tag,
                        platform="",
                        source_auth_file=resolved.source_auth_file,
                        retries=retries,
                        copy_bundle_migrations=False,
                    ),
                )
            )

        for tag in tags:
            jobs.append(
                (
                    push_image,
                    PushJob(
                        origin_digest=resolved.origin_digest,
                        name=resolved.name,
                        container_image=resolved.container_image,
                        repository_url=repository_url,
                        tag=tag,
                        platform=resolved.platform,
                        source_auth_file=resolved.source_auth_file,
                        retries=retries,
                        copy_bundle_migrations=copy_bundle_migrations,
                    ),
                )
            )

            if resolved.source_container_digest:
                jobs.append(
                    (
                        push_image,
                        PushJob(
                            origin_digest=resolved.source_container_digest,
                            name=resolved.name,
                            container_image=resolved.source_container,
                            repository_url=repository_url,
                            tag=f"{tag}-source",
                            platform="",
                            source_auth_file=resolved.source_auth_file,
                            retries=retries,
                            copy_bundle_migrations=False,
                        ),
                    )
                )

        if copy_bundle_migrations and resolved.migration_digest and resolved.migration_tag:
            jobs.append(
                (
                    push_migration_artifact,
                    MigrationJob(
                        source_repo=resolved.source_repo,
                        migration_digest=resolved.migration_digest,
                        name=resolved.name,
                        repository_url=repository_url,
                        migration_tag=resolved.migration_tag,
                        source_auth_file=resolved.source_auth_file,
                        retries=retries,
                    ),
                )
            )

    return jobs


def run(
    snapshot_path: Path,
    data_path: Path,
    results_dir: Path,
    concurrent_limit: int,
    retries: int,
    copy_bundle_migrations: bool,
) -> None:
    """Push snapshot images to destination registries."""
    if not snapshot_path.is_file():
        raise RuntimeError("No valid snapshot file was provided.")
    if not data_path.is_file():
        raise RuntimeError("No data JSON was provided.")

    snapshot_data = file.load_json_dict(snapshot_path)
    data = file.load_json_dict(data_path)

    validate_snapshot(snapshot_data)

    default_push_src = snapshot_helper.default_push_source_container(data)
    results_json: dict[str, Any] = {"images": []}
    components = snapshot_data.get("components", [])
    component_group = snapshot_data.get("componentGroup", "")

    logger.info('Beginning "push-snapshot" for "%s"', component_group)
    memory_throttle.log_memory_throttle_status(MEMORY_THRESHOLD)

    source_auth_files: list[Path] = []
    futures: list[Any] = []
    jobs_spawned = 0

    def _submit_throttled(fn: Any, job: Any) -> None:
        nonlocal jobs_spawned
        memory_throttle.wait_for_memory(MEMORY_THRESHOLD)
        futures.append(executor.submit(fn, job))
        jobs_spawned += 1
        if jobs_spawned % BURST_SIZE == 0:
            time.sleep(STABILIZATION_DELAY)

    try:
        with ThreadPoolExecutor(max_workers=concurrent_limit) as executor:
            for component in components:
                resolved = _resolve_component(
                    component,
                    default_push_src=default_push_src,
                    copy_bundle_migrations=copy_bundle_migrations,
                )
                source_auth_files.append(resolved.source_auth_file)

                results_json["images"].append(
                    {
                        "arches": resolved.arches,
                        "oses": resolved.oses,
                        "name": resolved.name,
                        "shasum": resolved.origin_digest,
                        "urls": [],
                    }
                )

                for fn, job in _build_component_jobs(
                    resolved, retries=retries, copy_bundle_migrations=copy_bundle_migrations
                ):
                    _submit_throttled(fn, job)

            push_results: list[dict[str, str]] = []
            failures: list[str] = []

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if isinstance(result, dict):
                        push_results.append(result)
                except Exception as exc:
                    failures.append(str(exc))
                    logger.error("Push failed: %s", exc)

        if failures:
            raise RuntimeError(
                "One or more jobs failed. Please check the logs above for details."
            )
    finally:
        for auth_file in source_auth_files:
            auth_file.unlink(missing_ok=True)

    images_by_name = {img["name"]: img for img in results_json["images"]}
    for push_result in push_results:
        images_by_name[push_result["name"]]["urls"].append(push_result["url"])

    results_dir.mkdir(parents=True, exist_ok=True)
    results_file = results_dir / "push-snapshot-results.json"
    results_file.write_text(json.dumps(results_json), encoding="utf-8")

    logger.info('Completed "push-snapshot" for "%s"', component_group)


def main() -> None:
    """Read environment variables and call ``run()``."""
    snapshot_file = os.environ.get("SNAPSHOT_FILE", "").strip()
    data_file = os.environ.get("DATA_FILE", "").strip()
    results_dir_str = os.environ.get("RESULTS_DIR", "").strip()

    if not snapshot_file:
        raise ValueError("SNAPSHOT_FILE must be set")
    if not data_file:
        raise ValueError("DATA_FILE must be set")
    if not results_dir_str:
        raise ValueError("RESULTS_DIR must be set")

    concurrent_limit = int(os.environ.get("CONCURRENT_LIMIT", "20"))
    retries = int(os.environ.get("RETRIES", "3"))
    copy_bundle_migrations = (
        os.environ.get("COPY_BUNDLE_MIGRATIONS", "false").lower() == "true"
    )

    authentication.setup_ca_cert()

    run(
        Path(snapshot_file),
        Path(data_file),
        Path(results_dir_str),
        concurrent_limit,
        retries,
        copy_bundle_migrations,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
