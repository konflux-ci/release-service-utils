#!/usr/bin/env python3
"""Prepare container signing batches for Konflux release snapshot components."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyxis
from release_service_utils.helpers.kubectl import get_configmap
from release_service_utils.helpers.logger import logger as LOGGER
from release_service_utils.helpers.oras_utils import oras_resolve
from release_service_utils.helpers.subprocess_cmd import run_cmd

PYXIS_INSTANCE_MAP = {
    "production": "https://graphql-pyxis.api.redhat.com/graphql/",
    "production-internal": "https://graphql.pyxis.engineering.redhat.com/graphql/",
    "stage": "https://graphql-pyxis.preprod.api.redhat.com/graphql/",
    "stage-internal": "https://graphql.pyxis.stage.engineering.redhat.com/graphql/",
}

SINGLE_MANIFEST_MEDIA_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}


@dataclass(frozen=True)
class PyxisSignature:
    """An existing signature record returned from Pyxis."""

    reference: str
    sig_key_id: str


@dataclass
class SigningItem:
    """A single image reference, digest, and signing key that needs to be signed."""

    reference: str  # e.g. "registry.redhat.io/myproduct/myrepo:v1.0"
    digest: str  # e.g. "sha256:abc123"
    repository: str  # stripped repo used for Pyxis lookup, e.g. "myproduct/myrepo"
    key: str  # signing key ID, e.g. "redhate2etesting"


_TASK_LABEL = "internal-services.appstudio.openshift.io/group-id"
_PIPELINERUN_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"
_INTENTION_LABEL = "internal-services.appstudio.openshift.io/intention"


@dataclass(frozen=True)
class SubmitConfig:
    """Configuration for submitting signing batches via the internal-request CLI."""

    pipeline: str
    pipeline_image: str
    requester: str
    pyxis_ssl_cert_secret_name: str
    pyxis_graphql_url: str
    kerberos_keytab_secret: str
    kerberos_keytab: str
    kerberos_principal: str
    signing_repo: str
    signing_revision: str
    service_account: str
    request_timeout: str
    pipeline_timeout: str
    task_timeout: str
    task_id: str
    pipelinerun_uid: str
    concurrent_limit: int
    intention: str


def validate_file(arg: str) -> Path:
    """Return a Path for *arg* if the file exists, otherwise raise FileNotFoundError.

    Intended for use as an argparse ``type`` validator.

    Args:
        arg: File path string provided on the command line.

    Returns:
        Resolved Path object pointing to the existing file.

    Raises:
        FileNotFoundError: If the path does not point to an existing file.

    """
    if (file := Path(arg)).is_file():
        return file
    raise FileNotFoundError(arg)


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured argument parser with all signing preparation arguments.

    """
    parser = argparse.ArgumentParser(description="Prepare container signing configuration.")
    parser.add_argument(
        "--pyxis-server",
        required=True,
        choices=PYXIS_INSTANCE_MAP.keys(),
        help="Pyxis server instance to use",
    )
    parser.add_argument(
        "--snapshot", required=True, type=validate_file, help="Konflux release snapshot path"
    )
    parser.add_argument(
        "--data-file", required=True, type=validate_file, help="Konflux data file path"
    )
    parser.add_argument(
        "--sign-registry-access-file",
        required=True,
        type=validate_file,
        help="File listing repositories that require registry-access signing (one per line)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Directory where batch files are written (one per batch); omit to skip",
    )
    parser.add_argument(
        "--batch-max-size",
        type=int,
        default=14 * 1024,
        help="Maximum size in bytes of each base64-encoded batch (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    submit = parser.add_argument_group(
        "request submission (active when --submit-requests is set)"
    )
    submit.add_argument(
        "--submit-requests",
        action="store_true",
        help="Submit signing batches via internal-request after collecting them",
    )
    submit.add_argument(
        "--pipeline",
        default="container-signing",
        help="Internal pipeline name for signing (default: %(default)s)",
    )
    submit.add_argument(
        "--pipeline-image",
        required=True,
        help="Container image override for the signing pipeline",
    )
    submit.add_argument(
        "--requester",
        required=True,
        help="Name of the user requesting signing, for auditing",
    )
    submit.add_argument(
        "--request-timeout",
        default="1800",
        help="InternalRequest timeout in seconds (default: %(default)s)",
    )
    submit.add_argument(
        "--pipeline-timeout",
        default="0h30m0s",
        help="Pipeline timeout (default: %(default)s)",
    )
    submit.add_argument(
        "--task-timeout",
        default="0h25m0s",
        help="Task timeout (default: %(default)s)",
    )
    submit.add_argument(
        "--service-account",
        default="signing-pipeline-sa",
        help="Service account for the signing pipeline (default: %(default)s)",
    )
    submit.add_argument(
        "--task-id",
        default="",
        help="Task run UID used as a label on internal requests",
    )
    submit.add_argument(
        "--pipelinerun-uid",
        default="",
        help="Pipeline run UID used as a label on internal requests",
    )
    submit.add_argument(
        "--signing-repo",
        default="https://gitlab.cee.redhat.com/signing/signing.git",
        help="Git repository URL for signing tasks (default: %(default)s)",
    )
    submit.add_argument(
        "--signing-revision",
        default="main",
        help="Git revision in the signing repository (default: %(default)s)",
    )
    submit.add_argument(
        "--concurrent-limit",
        type=int,
        default=8,
        help="Maximum number of parallel signing requests (default: %(default)s)",
    )

    return parser


def get_signing_keys(configmap: dict[str, Any]) -> list[str]:
    """Extract the list of signing key IDs from a signing ConfigMap.

    Prefers the comma-separated ``SIG_KEY_NAMES`` field when present; falls
    back to the single-value ``SIG_KEY_NAME`` field otherwise.

    Args:
        configmap: Parsed ConfigMap dictionary as returned by get_configmap.

    Returns:
        List of signing key ID strings.

    Raises:
        KeyError: If neither SIG_KEY_NAMES nor SIG_KEY_NAME is present in data.

    """
    data = configmap["data"]
    if "SIG_KEY_NAMES" in data:
        return [k for k in re.split(r"[,\s]+", data["SIG_KEY_NAMES"].strip()) if k]
    return [data["SIG_KEY_NAME"]]


def get_all_image_digests(image_reference: str) -> list[str]:
    """Return all manifest digests for an image.

    Always includes the top-level digest. For multi-arch index images, also
    includes the digest of every nested manifest (e.g. per-arch manifests).

    Args:
        image_reference: Fully qualified image reference including digest,
            e.g. ``registry.redhat.io/repo/image@sha256:abc123``.

    Returns:
        List of digest strings starting with the top-level digest.

    """
    top_level_digest = image_reference.split("@", 1)[1]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as auth_file:
        select_auth = run_cmd(["select-oci-auth", image_reference])
        auth_file.write(select_auth.stdout)
        auth_file.flush()

        result = run_cmd(
            [
                "skopeo",
                "inspect",
                "--retry-times",
                "3",
                "--raw",
                "--authfile",
                auth_file.name,
                f"docker://{image_reference}",
            ]
        )
    raw_manifest = json.loads(result.stdout)

    digests = [top_level_digest]

    media_type = raw_manifest.get("mediaType", "")
    if media_type not in SINGLE_MANIFEST_MEDIA_TYPES:
        manifests = raw_manifest.get("manifests")
        if manifests:
            digests.extend(m["digest"] for m in manifests)
        else:
            LOGGER.info("Single-manifest artifact (e.g. Helm chart), no nested digests to add")

    return digests


def get_source_container_digest(
    component: dict[str, Any], default_push_source_container: bool
) -> str | None:
    """Resolve the source container digest for a component.

    Uses ``select-oci-auth`` and ``oras resolve`` to look up the digest of the
    ``.src`` source container image. Returns ``None`` when source container
    signing is disabled for this component.

    Args:
        component: A single component entry from the Konflux release snapshot.
        default_push_source_container: Fallback value when the component does
            not specify its own ``pushSourceContainer`` flag.

    Returns:
        The source container digest string, or None if not applicable.

    """
    if not component.get("pushSourceContainer", default_push_source_container):
        return None

    reference_container_image = component["containerImage"]
    source_repo = reference_container_image.split("@sha256:", 1)[0]
    sha = reference_container_image.split("@sha256:", 1)[1]
    source_reference = f"{source_repo}:sha256-{sha}.src"
    return oras_resolve(source_reference)


SIGNATURES_GRAPHQL_QUERY = """
query ($repository: String!, $manifest_digest: String!, $page: Int!, $page_size: Int!) {
    find_signatures(
        page: $page
        page_size: $page_size
        sort_by: [{field: "last_update_date", order: DESC}]
        filter: {and: [{manifest_digest: {eq: $manifest_digest}},
            {repository: {eq: $repository}}]}
    ) {
        error { detail status }
        data { reference sig_key_id }
    }
}
"""


def find_signatures_for_repository(
    graphql_api: str, repository: str, manifest_digest: str, page_size: int = 50
) -> set[PyxisSignature]:
    """Query Pyxis for all existing signatures for a given repository and digest.

    Paginates automatically until all results have been retrieved.

    Args:
        graphql_api: The Pyxis GraphQL endpoint URL.
        repository: Repository path without registry prefix, e.g.
            ``myproduct/myrepo``.
        manifest_digest: The image manifest digest to look up, e.g.
            ``sha256:abc123``.
        page_size: Number of records to request per page.

    Returns:
        Set of PyxisSignature objects already recorded in Pyxis.

    """
    signatures: set[PyxisSignature] = set()
    page = 0
    while True:
        variables = {
            "repository": repository,
            "manifest_digest": manifest_digest,
            "page": page,
            "page_size": page_size,
        }
        data = pyxis.graphql_query(
            graphql_api, {"query": SIGNATURES_GRAPHQL_QUERY, "variables": variables}
        )
        page_data = data["find_signatures"]["data"]
        signatures.update(
            PyxisSignature(reference=s["reference"], sig_key_id=s["sig_key_id"])
            for s in page_data
        )
        if len(page_data) < page_size:
            break
        page += 1
    LOGGER.info(
        "Found %d existing signatures for %s @ %s",
        len(signatures),
        repository,
        manifest_digest,
    )
    return signatures


def find_existing_signatures(
    pyxis_url: str,
    lookups: set[tuple[str, str]],
    max_workers: int = 10,
) -> dict[tuple[str, str], set[PyxisSignature]]:
    """Look up existing signatures in Pyxis for a set of (digest, repository) pairs.

    Issues concurrent requests using a thread pool to minimise latency.

    Args:
        pyxis_url: The Pyxis GraphQL endpoint URL.
        lookups: Exact set of ``(digest, repository)`` pairs to query.
        max_workers: Maximum number of concurrent Pyxis requests.

    Returns:
        Mapping from ``(digest, repository)`` tuples to the set of
        PyxisSignature objects already recorded in Pyxis.

    """
    results: dict[tuple[str, str], set[PyxisSignature]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(find_signatures_for_repository, pyxis_url, repo, digest): (
                digest,
                repo,
            )
            for digest, repo in lookups
        }
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()

    return results


def write_batches(batches: list[str], output_dir: Path) -> None:
    """Write each batch to a numbered file in output_dir, creating it if needed.

    Files are named ``batch_0000.txt``, ``batch_0001.txt``, and so on.

    Args:
        batches: List of base64-encoded batch strings to write.
        output_dir: Directory where batch files will be created.

    """
    output_dir.mkdir(parents=True, exist_ok=True)
    for i, batch in enumerate(batches):
        (output_dir / f"batch_{i:04d}.txt").write_text(batch)


def _encode_batch(items: list[SigningItem]) -> str:
    data = [{"reference": i.reference, "digest": i.digest, "key": i.key} for i in items]
    return base64.b64encode(json.dumps(data).encode()).decode()


def batch_signing_items(
    items: list[SigningItem], max_batch_bytes: int = 14 * 1024
) -> list[str]:
    """Split signing items into base64-encoded JSON batches, each under max_batch_bytes.

    Uses a greedy packing strategy: items are added to the current batch until
    adding the next item would exceed the byte limit, at which point the current
    batch is flushed and a new one is started.

    Args:
        items: Ordered list of SigningItem objects to batch.
        max_batch_bytes: Maximum encoded size in bytes for each batch.

    Returns:
        List of base64-encoded JSON strings, one per batch.

    """
    batches: list[str] = []
    current: list[SigningItem] = []

    for item in items:
        candidate = current + [item]
        encoded = _encode_batch(candidate)
        if current and len(encoded) >= max_batch_bytes:
            batches.append(_encode_batch(current))
            current = [item]
        else:
            current = candidate

    if current:
        batches.append(_encode_batch(current))

    return batches


def collect_signing_items(
    component: dict[str, Any],
    sign_registry_access_repos: set[str],
    digests: list[str],
    source_container_digest: str | None,
    signing_keys: list[str],
) -> list[SigningItem]:
    """Enumerate all (reference, digest, key) triples that are candidates for signing.

    For each repository defined on the component, signing items are produced
    for every combination of tag, digest, registry reference, and signing key.
    The ``registry-access-repo`` variant is only included when the repository
    path appears in ``sign_registry_access_repos``. Source container items are
    appended with a ``-source`` tag suffix when a source digest is provided.

    Args:
        component: A single component entry from the Konflux release snapshot.
        sign_registry_access_repos: Set of repository paths for which
            registry-access signing is required.
        digests: All manifest digests for the component's container image.
        source_container_digest: Digest of the source container image, or None.
        signing_keys: List of signing key IDs to produce items for.

    Returns:
        List of SigningItem objects covering all signing candidates.

    """
    items: list[SigningItem] = []

    for repo in component.get("repositories", []):
        rh_registry_repo = repo["rh-registry-repo"]
        registry_access_repo = repo.get("registry-access-repo")
        tags: list[str] = repo.get("tags", [])
        repository = rh_registry_repo.split("/", 1)[1]

        registry_references = [rh_registry_repo]
        if registry_access_repo and repository in sign_registry_access_repos:
            registry_references.append(registry_access_repo)

        for digest in digests:
            for tag in tags:
                for ref in registry_references:
                    for key in signing_keys:
                        items.append(SigningItem(f"{ref}:{tag}", digest, repository, key))

        if source_container_digest:
            for tag in tags:
                for ref in registry_references:
                    for key in signing_keys:
                        items.append(
                            SigningItem(
                                f"{ref}:{tag}-source", source_container_digest, repository, key
                            )
                        )

    return items


def filter_already_signed(
    items: list[SigningItem],
    pyxis_url: str,
    max_workers: int = 10,
) -> list[SigningItem]:
    """Return only items that do not yet have a matching signature in Pyxis.

    Derives the set of repositories and digests directly from the items, so it
    is independent of how the items were produced.

    Args:
        items: All signing candidates to check.
        pyxis_url: The Pyxis GraphQL endpoint URL.
        max_workers: Maximum number of concurrent Pyxis lookup requests.

    Returns:
        Subset of items for which no matching ``(reference, key)`` signature
        exists in Pyxis.

    """
    lookups = {(item.digest, item.repository) for item in items}
    existing_signatures = find_existing_signatures(pyxis_url, lookups, max_workers)

    to_sign: list[SigningItem] = []
    for item in items:
        existing = existing_signatures.get((item.digest, item.repository), set())
        if any(
            sig.reference == item.reference and sig.sig_key_id == item.key for sig in existing
        ):
            LOGGER.info(
                "Signature already exists for %s @ %s with key %s, skipping",
                item.reference,
                item.digest,
                item.key,
            )
        else:
            to_sign.append(item)

    return to_sign


def process_component(
    component: dict[str, Any],
    data_file: dict[str, Any],
    sign_registry_access_repos: set[str],
    signing_keys: list[str],
) -> list[SigningItem]:
    """Collect all signing candidates for a single snapshot component.

    Resolves all manifest digests (including nested manifests for multi-arch
    images) and optionally the source container digest, then enumerates every
    ``(reference, digest, key)`` triple that is a candidate for signing.

    Args:
        component: A single component entry from the Konflux release snapshot.
        data_file: Parsed Konflux data file, used for the default
            ``pushSourceContainer`` flag.
        sign_registry_access_repos: Set of repository paths for which
            registry-access signing is required.
        signing_keys: List of signing key IDs to produce candidates for.

    Returns:
        List of all SigningItem candidates for this component.

    """
    LOGGER.info("Processing component: %s", component.get("name"))
    default_push_source_container = (
        data_file.get("mapping", {}).get("defaults", {}).get("pushSourceContainer", True)
    )

    digests = get_all_image_digests(component["containerImage"])
    source_container_digest = get_source_container_digest(
        component, default_push_source_container
    )

    items = collect_signing_items(
        component, sign_registry_access_repos, digests, source_container_digest, signing_keys
    )

    LOGGER.info(
        "Found %d signing candidates for component %s",
        len(items),
        component.get("name"),
    )

    return items


def get_submit_config(
    configmap: dict[str, Any], args: argparse.Namespace, data_file: dict[str, Any]
) -> SubmitConfig:
    """Build a SubmitConfig from a signing ConfigMap, CLI arguments, and data file.

    Args:
        configmap: Parsed ConfigMap dictionary as returned by get_configmap.
        args: Parsed CLI arguments containing submission-related flags.
        data_file: Parsed Konflux data file; provides the ``intention`` value.

    Returns:
        SubmitConfig populated from the configmap, CLI args, and data file.

    Raises:
        KeyError: If a required key is missing from the configmap data.

    """
    data = configmap["data"]
    return SubmitConfig(
        pipeline=args.pipeline,
        pipeline_image=args.pipeline_image,
        requester=args.requester,
        pyxis_ssl_cert_secret_name=data["PYXIS_SSL_CERT_SECRET_NAME"],
        pyxis_graphql_url=data["PYXIS_GRAPHQL_URL"],
        kerberos_keytab_secret=data["KERBEROS_KEYTAB_SECRET"],
        kerberos_keytab=data["KERBEROS_KEYTAB"],
        kerberos_principal=data["KERBEROS_PRINCIPAL"],
        signing_repo=args.signing_repo,
        signing_revision=args.signing_revision,
        service_account=args.service_account,
        request_timeout=args.request_timeout,
        pipeline_timeout=args.pipeline_timeout,
        task_timeout=args.task_timeout,
        task_id=args.task_id,
        pipelinerun_uid=args.pipelinerun_uid,
        concurrent_limit=args.concurrent_limit,
        intention=data_file.get("intention", "unknown"),
    )


def submit_batch(batch_file: Path, config: SubmitConfig) -> None:
    """Submit a single signing batch via the internal-request CLI.

    Reads the base64-encoded batch content from *batch_file* and passes it
    as the ``signing_requests`` parameter.

    Args:
        batch_file: Path to a file containing the base64-encoded batch string.
        config: Submission configuration containing all CLI parameters.

    """
    batch_content = batch_file.read_text()
    cmd = [
        "internal-request",
        "--pipeline",
        config.pipeline,
        "-p",
        f"pipeline_image={config.pipeline_image}",
        "-p",
        f"signing_requests={batch_content}",
        "-p",
        f"requester={config.requester}",
        "-p",
        f"pyxis_ssl_cert_secret_name={config.pyxis_ssl_cert_secret_name}",
        "-p",
        f"pyxis_graphql_url={config.pyxis_graphql_url}",
        "-p",
        f"kerberos_keytab_secret={config.kerberos_keytab_secret}",
        "-p",
        f"kerberos_keytab={config.kerberos_keytab}",
        "-p",
        f"kerberos_principal={config.kerberos_principal}",
        "-p",
        f"taskGitUrl={config.signing_repo}",
        "-p",
        f"taskGitRevision={config.signing_revision}",
        "-l",
        f"{_TASK_LABEL}={config.task_id}",
        "-l",
        f"{_PIPELINERUN_LABEL}={config.pipelinerun_uid}",
        "-l",
        f"{_INTENTION_LABEL}={config.intention}",
        "-l",
        "internal-services.appstudio.openshift.io/rate-limited=true",
        "-l",
        "internal-services.appstudio.openshift.io/rate-limiting-group=signing-server",
        "-t",
        config.request_timeout,
        "--pipeline-timeout",
        config.pipeline_timeout,
        "--task-timeout",
        config.task_timeout,
        "--service-account",
        config.service_account,
        "-s",
        "true",
    ]

    LOGGER.debug("Submitting batch with command: %s", " ".join(cmd))
    result = run_cmd(
        cmd,
        check=False,
    )
    if result.returncode != 0:
        LOGGER.error("Failed to submit batch '%s': %s", batch_file, result.stderr.strip())
        raise RuntimeError(f"Failed to submit batch '{batch_file}': {result.stderr.strip()}")
    LOGGER.info("Submitted batch '%s' successfully", batch_file)


def submit_batches(batch_dir: Path, config: SubmitConfig) -> None:
    """Submit all batch files in *batch_dir* concurrently via internal-request.

    Each file in *batch_dir* is submitted as a separate signing request.
    Runs up to ``config.concurrent_limit`` requests in parallel. If any
    batch submission fails, the exception is re-raised after all in-flight
    requests have settled.

    Args:
        batch_dir: Directory containing batch files written by write_batches.
        config: Submission configuration including concurrency limit.

    """
    batch_files = sorted(batch_dir.iterdir())
    LOGGER.info(
        "Submitting %d batch file(s) from '%s' with concurrent limit %d",
        len(batch_files),
        batch_dir,
        config.concurrent_limit,
    )
    failures: list[Exception] = []
    with ThreadPoolExecutor(max_workers=config.concurrent_limit) as pool:
        futures = [pool.submit(submit_batch, bf, config) for bf in batch_files]
        for future in as_completed(futures):
            exc = future.exception()
            if exc is not None:
                failures.append(exc)

    succeeded = len(batch_files) - len(failures)
    LOGGER.info("Batch submission summary: %d succeeded, %d failed", succeeded, len(failures))
    if failures:
        for i, failure in enumerate(failures, 1):
            LOGGER.error("Batch submission failure %d/%d: %s", i, len(failures), failure)
        raise RuntimeError(f"{len(failures)} batch(es) failed during submission")


def main() -> int:
    """Entry point: parse arguments, collect signing items, and write batch files."""
    parser = setup_argparser()

    try:
        args = parser.parse_args()
        LOGGER.setLevel(logging.DEBUG if args.verbose else logging.INFO)
        pyxis_url = PYXIS_INSTANCE_MAP[args.pyxis_server]
        LOGGER.info("Using Pyxis instance URL: %s", pyxis_url)
        sign_registry_access_repos = set(
            args.sign_registry_access_file.read_text().splitlines()
        )

        data_file = json.loads(args.data_file.read_text())
        snapshot = json.loads(args.snapshot.read_text())

        config_map_name = data_file.get("sign", {}).get("configMapName", "signing-config-map")
        configmap = get_configmap(config_map_name)
        signing_keys = get_signing_keys(configmap)
        LOGGER.info("Signing keys: %s", signing_keys)

        all_candidates: list[SigningItem] = []
        for component in snapshot.get("components", []):
            candidates = process_component(
                component, data_file, sign_registry_access_repos, signing_keys
            )
            all_candidates.extend(candidates)

        LOGGER.info("Total signing candidates across all components: %d", len(all_candidates))
        all_to_sign = filter_already_signed(all_candidates, pyxis_url)
        LOGGER.info("Total items to sign after excluding already signed: %d", len(all_to_sign))

        batches = batch_signing_items(all_to_sign, max_batch_bytes=args.batch_max_size)
        batch_dir = args.output if args.output else Path(tempfile.mkdtemp())
        write_batches(batches, batch_dir)
        LOGGER.info("Wrote %d batch(es) to '%s'", len(batches), batch_dir)

        if args.submit_requests:
            submit_config = get_submit_config(configmap, args, data_file)
            submit_batches(batch_dir, submit_config)
    except Exception as e:
        LOGGER.error("Fatal error during signing preparation: %s", e, exc_info=True)
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
