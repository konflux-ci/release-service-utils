#!/usr/bin/env python3
"""Push container image metadata to Pyxis for all images in a mapped snapshot.

For each component in the snapshot, fetch the OCI manifest, optionally
decompress gzip layers to record uncompressed sizes, create or update
the Pyxis ContainerImage entry, and optionally clean up stale tags.

Components sharing the same digest are processed serially within their
group while independent digest groups run in parallel, preventing Pyxis
race conditions.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import file as file_helpers
import image_architectures
import memory_throttle
import oras_utils
import pyxis_api
import skopeo
from cleanup_tags import cleanup_tags_with_retry
from create_container_image import (
    ContainerImageArgs,
    MANIFEST_LIST_TYPES,
    create_or_update,
    proxymap,
)
from file import load_json_dict
from image_ref import split_image_ref
from logger import logger

_DECOMPRESS_CHUNK_SIZE = 65536


@dataclass(frozen=True)
class RunConfig:
    """Immutable configuration that stays constant for the entire run."""

    pyxis_url: str
    pyxis_graphql_url: str
    certified: str
    is_latest: str
    rh_push: str
    append_tags: str
    include_layers: bool
    process_helm_charts: bool
    data_dir: Path
    snapshot_dir: Path


@dataclass(frozen=True)
class ComponentContext:
    """Per-component state threaded through repository/architecture processing."""

    index: int
    digest: str
    auth_path: Path
    dockerfile_path: Path | None
    metadata_path: Path | None


_GZIP_MEDIA_RE = re.compile(r"\.gzip$|\+gzip$")


def _write_auth_file(reference: str, auth_path: Path) -> None:
    """Run ``select-oci-auth`` and write the result to *auth_path*."""
    result = subprocess.run(
        ["select-oci-auth", reference],
        capture_output=True,
        text=True,
        check=False,
    )
    auth_path.write_text(result.stdout, encoding="utf-8")


def _try_pull_dockerfile(source_repo: str, digest: str) -> Path | None:
    """Attempt to pull a Dockerfile artifact and return its path, or ``None``."""
    dockerfile_dir = Path(tempfile.mkdtemp())
    pull_spec = f"{source_repo}:{digest.replace(':', '-')}.dockerfile"
    try:
        oras_utils.oras_pull(pull_spec, dockerfile_dir)
    except subprocess.CalledProcessError:
        logger.info(
            "Unable to get Dockerfile for the image. "
            "Maybe it's not enabled in the build pipeline?"
        )
        return None
    dockerfile_path = dockerfile_dir / "Dockerfile"
    if not dockerfile_path.is_file():
        raise RuntimeError("Dockerfile pull succeeded, but the Dockerfile was not saved.")
    return dockerfile_path


def _decompress_gzip_layer(
    blob_digest: str,
    repository: str,
    auth_path: Path,
    component_index: int,
) -> dict[str, Any]:
    """Download, decompress, and measure a single gzip layer.

    Returns a dict with ``digest`` (sha256 of decompressed content) and ``size``
    (byte count of the decompressed layer).
    """
    blob_pullspec = f"{repository}@{blob_digest}"
    gz_path = file_helpers.make_tempfile_path(
        f"oras-blob-fetch-{component_index}-",
    )
    try:
        oras_utils.oras_blob_fetch(blob_pullspec, gz_path, auth_path)
        with gzip.open(gz_path, "rb") as gz:
            h = hashlib.sha256()
            size = 0
            while True:
                chunk = gz.read(_DECOMPRESS_CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
                size += len(chunk)
        return {"digest": f"sha256:{h.hexdigest()}", "size": size}
    finally:
        gz_path.unlink(missing_ok=True)


def _build_cci_args(
    *,
    config: RunConfig,
    component: ComponentContext,
    tags: str,
    oras_manifest_fetch: str,
    name: str,
    media_type: str,
    architecture_digest: str,
    architecture: str,
) -> ContainerImageArgs:
    """Build a ``ContainerImageArgs`` for create_container_image functions."""
    return ContainerImageArgs(
        pyxis_url=config.pyxis_url,
        certified=config.certified,
        tags=tags,
        is_latest=config.is_latest,
        oras_manifest_fetch=oras_manifest_fetch,
        name=name,
        media_type=media_type,
        digest=component.digest,
        architecture_digest=architecture_digest,
        architecture=architecture,
        rh_push=config.rh_push,
        append_tags=config.append_tags,
        dockerfile=str(component.dockerfile_path) if component.dockerfile_path else "",
        metadata=str(component.metadata_path) if component.metadata_path else "",
    )


def _process_repository(
    repo_obj: dict[str, Any],
    *,
    component: ComponentContext,
    config: RunConfig,
) -> list[dict[str, Any]] | None:
    """Process a single mapped repository for a component.

    Inspects the image at the repository, iterates over its architectures,
    creates/updates Pyxis images, and optionally cleans up stale tags.

    Returns a list of per-architecture result dicts, or ``None`` if the
    component should be skipped entirely (e.g. Helm chart detected).
    """
    repo_url = repo_obj.get("url", "")
    repo_url = repo_url.rsplit(":", 1)[0] if ":" in repo_url else repo_url
    pullspec = f"{repo_url}@{component.digest}"
    tags_list = repo_obj.get("tags", [])
    tags_str = " ".join(tags_list)

    raw_result = skopeo.inspect(pullspec, raw=True, no_tags=True)
    if raw_result.returncode != 0:
        raise RuntimeError(
            f"skopeo inspect --raw failed for {pullspec}: " f"{raw_result.stderr.strip()}"
        )
    media_type = json.loads(raw_result.stdout).get("mediaType", "")

    _write_auth_file(repo_url, component.auth_path)
    arch_details = image_architectures.get_image_architectures(pullspec)

    config_mt = (arch_details[0].get("configMediaType") or "") if arch_details else ""
    if (
        config_mt == image_architectures.HELM_CONFIG_MEDIA_TYPE
        and not config.process_helm_charts
    ):
        logger.info(
            "Detected Helm chart artifact for component %d, " "skipping Pyxis image creation",
            component.index,
        )
        return None

    results: list[dict[str, Any]] = []
    for arch_index, arch_detail in enumerate(arch_details):
        result = _process_architecture(
            arch_index=arch_index,
            arch_detail=arch_detail,
            pullspec=pullspec,
            repo_url=repo_url,
            media_type=media_type,
            tags_str=tags_str,
            component=component,
            config=config,
        )
        results.append(result)

    return results


def _process_architecture(
    *,
    arch_index: int,
    arch_detail: dict[str, Any],
    pullspec: str,
    repo_url: str,
    media_type: str,
    tags_str: str,
    component: ComponentContext,
    config: RunConfig,
) -> dict[str, Any]:
    """Create or update the Pyxis image for a single architecture.

    Fetches the OCI manifest, processes layers, creates/updates the Pyxis
    ContainerImage, and optionally cleans up stale tags.

    Returns a result dict with arch, imageId, digest, arch_digest, and os.
    """
    os_name = arch_detail.get("platform", {}).get("os", "")
    arch = arch_detail.get("platform", {}).get("architecture", "")
    arch_digest = arch_detail.get("digest", "")

    platform = f"{os_name}/{arch}" if media_type in MANIFEST_LIST_TYPES else None

    manifest_file = (
        config.data_dir
        / config.snapshot_dir
        / f"oras-manifest-fetch-{component.index}-{arch_index}.json"
    )
    manifest_raw = oras_utils.oras_manifest_fetch(
        pullspec,
        component.auth_path,
        platform=platform,
    )
    manifest_data = json.loads(manifest_raw)

    if not config.include_layers:
        logger.info(".pyxis.includeLayers is not true in data file, so delete the layers")
        manifest_data["layers"] = []

    for layer in list(manifest_data.get("layers", [])):
        blob_type = layer.get("mediaType", "")
        blob_digest = layer.get("digest", "")
        if _GZIP_MEDIA_RE.search(blob_type):
            uncompressed = _decompress_gzip_layer(
                blob_digest,
                repo_url,
                component.auth_path,
                component.index,
            )
            manifest_data.setdefault("uncompressed_layers", []).append(uncompressed)

    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps(manifest_data), encoding="utf-8")

    cci_args = _build_cci_args(
        config=config,
        component=component,
        tags=tags_str,
        oras_manifest_fetch=str(manifest_file),
        name=repo_url,
        media_type=media_type,
        architecture_digest=arch_digest,
        architecture=arch,
    )

    image_id = create_or_update(cci_args)
    logger.info("The image id is: %s", image_id)

    if config.rh_push == "true":
        cleanup_tags_with_retry(config.pyxis_graphql_url, image_id, proxymap(repo_url))

    return {
        "arch": arch,
        "imageId": image_id,
        "digest": component.digest,
        "arch_digest": arch_digest,
        "os": os_name,
    }


def process_component(
    component_index: int,
    snapshot: dict[str, Any],
    *,
    config: RunConfig,
) -> dict[str, Any] | None:
    """Process a single snapshot component: create Pyxis images for all repos/arches.

    Returns the component result dict, or ``None`` if the component was
    skipped (e.g. Helm chart).
    """
    component = snapshot["components"][component_index]
    container_image = component["containerImage"]
    source_repo, digest = split_image_ref(container_image)

    metadata = component.get("metadata") or {}
    metadata_path = file_helpers.make_tempfile_path(
        "metadata-", json.dumps(metadata).encode("utf-8")
    )

    auth_path = file_helpers.make_tempfile_path(f"auth-{component_index}-")
    try:
        _write_auth_file(source_repo, auth_path)
        dockerfile_path = _try_pull_dockerfile(source_repo, digest)

        ctx = ComponentContext(
            index=component_index,
            digest=digest,
            auth_path=auth_path,
            dockerfile_path=dockerfile_path,
            metadata_path=metadata_path if metadata else None,
        )

        component_json: dict[str, Any] = {
            "containerImage": container_image,
            "componentIndex": component_index,
            "pyxisImages": [],
        }

        # Bug fix: the original bash script re-initialized COMPONENT_JSON inside
        # the repository loop, so only the last repository's images were kept.
        # Here we correctly accumulate pyxisImages across all repositories.
        repositories = component.get("repositories") or []
        for repo_obj in repositories:
            repo_images = _process_repository(
                repo_obj,
                component=ctx,
                config=config,
            )
            if repo_images is None:
                return None
            component_json["pyxisImages"].extend(repo_images)

        return component_json
    finally:
        auth_path.unlink(missing_ok=True)
        metadata_path.unlink(missing_ok=True)


def run(
    server: str,
    snapshot_file: Path,
    data_file: Path,
    certified: str,
    is_latest: str,
    rh_push: str,
    process_helm_charts: bool,
    concurrent_limit: int,
    pyxis_data_path_result: Path,
    data_dir: Path,
    snapshot_path_relative: str,
) -> None:
    """Orchestrate Pyxis image creation for all snapshot components."""
    pyxis_base = pyxis_api.PYXIS_BASE_URL_BY_SERVER.get(server)
    if pyxis_base is None:
        raise ValueError(pyxis_api.INVALID_SERVER_MESSAGE)
    pyxis_url = f"{pyxis_base}/"
    pyxis_graphql_url = pyxis_api.pyxis_graphql_url_for_server(server)

    snapshot_dir = Path(snapshot_path_relative).parent
    pyxis_data_path = snapshot_dir / "pyxis.json"
    pyxis_data_path_result.parent.mkdir(parents=True, exist_ok=True)
    pyxis_data_path_result.write_text(str(pyxis_data_path), encoding="utf-8")

    snapshot = load_json_dict(snapshot_file)
    data = load_json_dict(data_file)

    pyxis_data = data.get("pyxis", {})
    raw_include = pyxis_data.get("includeLayers", False)
    include_layers = raw_include is True or str(raw_include).lower() == "true"
    append_tags = str(pyxis_data.get("appendTags", False)).lower()

    config = RunConfig(
        pyxis_url=pyxis_url,
        pyxis_graphql_url=pyxis_graphql_url,
        certified=certified,
        is_latest=is_latest,
        rh_push=rh_push,
        append_tags=append_tags,
        include_layers=include_layers,
        process_helm_charts=process_helm_charts,
        data_dir=data_dir,
        snapshot_dir=snapshot_dir,
    )

    components = snapshot.get("components", [])

    digest_groups: dict[str, list[int]] = defaultdict(list)
    for i, comp in enumerate(components):
        ci = comp.get("containerImage", "")
        _, d = split_image_ref(ci)
        digest_groups[d].append(i)

    logger.info("Processing %d digest groups in parallel...", len(digest_groups))

    results: dict[int, dict[str, Any] | None] = {}
    errors: list[str] = []

    def _process_digest_group(
        group_digest: str, indices: list[int]
    ) -> list[tuple[int, dict[str, Any] | None]]:
        group_results = []
        for idx in indices:
            logger.info("Processing component %d for digest %s", idx, group_digest)
            result = process_component(
                idx,
                snapshot,
                config=config,
            )
            group_results.append((idx, result))
        return group_results

    BURST_SIZE = 5
    STABILIZATION_DELAY = 2

    memory_throttle.log_memory_throttle_status(80)

    with ThreadPoolExecutor(max_workers=concurrent_limit) as executor:
        futures: dict[Any, str] = {}
        submitted = 0
        for d, indices in digest_groups.items():
            memory_throttle.wait_for_memory(80)
            futures[executor.submit(_process_digest_group, d, indices)] = d
            submitted += 1
            if submitted % BURST_SIZE == 0:
                time.sleep(STABILIZATION_DELAY)

        for future in as_completed(futures):
            digest_key = futures[future]
            try:
                for idx, result in future.result():
                    results[idx] = result
            except Exception as exc:
                errors.append(f"Digest group {digest_key} failed: {exc}")
                logger.error("Digest group %s failed: %s", digest_key, exc, exc_info=True)

    if errors:
        raise RuntimeError(
            "One or more component processing jobs failed:\n" + "\n".join(errors)
        )

    json_output: dict[str, Any] = {
        "components": [results[i] for i in sorted(results) if results[i] is not None],
    }

    output_path = data_dir / pyxis_data_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(json_output, indent=2), encoding="utf-8")
    logger.info("Pyxis data written to %s", output_path)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--snapshot-file", required=True)
    parser.add_argument("--data-file", required=True)
    parser.add_argument("--pyxis-secret-path", default="/etc/secrets")
    parser.add_argument("--certified", default="false")
    parser.add_argument("--is-latest", default="false")
    parser.add_argument("--rh-push", default="false")
    parser.add_argument("--process-helm-charts", default="false")
    parser.add_argument("--concurrent-limit", type=int, default=16)
    parser.add_argument("--pyxis-data-path-result", required=True)
    parser.add_argument("--data-dir", default="/var/workdir/release")
    parser.add_argument("--snapshot-path-relative", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run Pyxis image creation."""
    args = _parse_args(argv)
    pyxis_secret_path = Path(args.pyxis_secret_path)
    os.environ["PYXIS_CERT_PATH"] = str(pyxis_secret_path / "cert")
    os.environ["PYXIS_KEY_PATH"] = str(pyxis_secret_path / "key")
    run(
        server=args.server,
        snapshot_file=Path(args.snapshot_file),
        data_file=Path(args.data_file),
        certified=args.certified,
        is_latest=args.is_latest,
        rh_push=args.rh_push,
        process_helm_charts=args.process_helm_charts == "true",
        concurrent_limit=args.concurrent_limit,
        pyxis_data_path_result=Path(args.pyxis_data_path_result),
        data_dir=Path(args.data_dir),
        snapshot_path_relative=args.snapshot_path_relative,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
