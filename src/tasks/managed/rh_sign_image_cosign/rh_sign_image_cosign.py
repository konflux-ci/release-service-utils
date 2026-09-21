#!/usr/bin/env python3
"""Sign container images in a Konflux release snapshot using cosign."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from itertools import zip_longest
from pathlib import Path
from typing import Any

from release_service_utils.helpers import memory_throttle
from release_service_utils.helpers import skopeo
from release_service_utils.helpers.file import path_from_env_variable
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd

PROG = "rh_sign_image_cosign.py"

MANIFEST_LIST_MEDIA_TYPES = frozenset(
    {
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.index.v1+json",
    }
)

BURST_SIZE = 5
STABILIZATION_DELAY = 2


@dataclass
class SignItem:
    """A container image that needs to be signed by cosign.

    ``identity`` is the public reference used as the cosign identity
    (e.g. ``registry.redhat.io/myrepo:v1.0``).  ``source`` is the
    internal image location used for authentication and the cosign
    reference argument (e.g. ``quay.io/internal/myrepo``).  ``digest``
    is the manifest digest (e.g. ``sha256:abc123``).
    """

    identity: str
    source: str
    digest: str


@dataclass
class SigningSecrets:
    """Cosign secrets read from the mounted secret volume."""

    sign_key: str
    public_key: str
    rekor_public_key: str
    rekor_url: str | None
    aws_default_region: str
    aws_access_key_id: str
    aws_secret_access_key: str


def load_signing_secrets(secrets_dir: Path) -> SigningSecrets:
    """Read cosign and AWS signing secrets from *secrets_dir*.

    Args:
        secrets_dir: Directory containing mounted secret files.

    Returns:
        Populated SigningSecrets dataclass.

    """
    rekor_url: str | None = None
    rekor_url_path = secrets_dir / "REKOR_URL"
    if rekor_url_path.is_file():
        content = rekor_url_path.read_text(encoding="utf-8").strip()
        if content:
            rekor_url = content

    return SigningSecrets(
        sign_key=(secrets_dir / "SIGN_KEY").read_text(encoding="utf-8").strip(),
        public_key=(secrets_dir / "PUBLIC_KEY").read_text(encoding="utf-8").strip(),
        rekor_public_key=(secrets_dir / "REKOR_PUBLIC_KEY")
        .read_text(encoding="utf-8")
        .strip(),
        rekor_url=rekor_url,
        aws_default_region=(secrets_dir / "AWS_DEFAULT_REGION")
        .read_text(encoding="utf-8")
        .strip(),
        aws_access_key_id=(secrets_dir / "AWS_ACCESS_KEY_ID")
        .read_text(encoding="utf-8")
        .strip(),
        aws_secret_access_key=(secrets_dir / "AWS_SECRET_ACCESS_KEY")
        .read_text(encoding="utf-8")
        .strip(),
    )


def get_manifest_digests(component: dict[str, Any]) -> tuple[bool, list[str]]:
    """Return all manifest digests for a component's container image.

    First tries the ``imageDigests`` field on the component (populated by IIB
    for multi-arch manifest lists). Falls back to ``skopeo inspect --raw`` when
    ``imageDigests`` is absent.  Always includes the top-level digest.

    Args:
        component: A single component entry from the Konflux release snapshot.

    Returns:
        A ``(is_manifest_list, digests)`` tuple where ``is_manifest_list``
        indicates whether the image is a manifest list index and ``digests``
        is the ordered list of manifest digests to sign.

    """
    container_image = component["containerImage"]
    top_level_digest = container_image.split("@", 1)[1]

    image_digests: list[str] = component.get("imageDigests") or []
    if image_digests:
        logger.info(
            "Using imageDigests from snapshot for %s: %d nested digests",
            component.get("name"),
            len(image_digests),
        )
        return True, [top_level_digest, *image_digests]

    result = skopeo.inspect(container_image, raw=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"skopeo inspect failed for {container_image}: {result.stderr.strip()}"
        )
    raw_manifest = json.loads(result.stdout)
    media_type = raw_manifest.get("mediaType", "")

    if media_type in MANIFEST_LIST_MEDIA_TYPES:
        nested = [m["digest"] for m in raw_manifest.get("manifests", [])]
        return True, [top_level_digest, *nested]

    return False, [top_level_digest]


def collect_component_sign_items(
    component: dict[str, Any],
    sign_registry_access_repos: set[str],
    sign_external_registries: bool,
) -> list[SignItem]:
    """Enumerate all SignItems for a single snapshot component.

    For each repository defined on the component, signing items are
    produced for every combination of digest, tag, and registry reference.
    Multi-arch manifest lists generate one item per nested manifest in
    addition to one item for the top-level digest.

    Args:
        component: A single component entry from the Konflux release snapshot.
        sign_registry_access_repos: Set of ``rh-registry-repo`` path suffixes
            (without the leading registry host) that require
            ``registry.access.redhat.com`` signing.
        sign_external_registries: When True, sign images whose repositories
            have no ``rh-registry-repo`` field using the ``url`` field directly.

    Returns:
        List of SignItem objects covering all signing candidates for the
        component.

    """
    component_name = component.get("name", "<unknown>")
    logger.info("Processing component: %s", component_name)

    is_list, digests = get_manifest_digests(component)
    if is_list:
        logger.info(
            "Component %s is a manifest list with %d digests", component_name, len(digests)
        )

    items: list[SignItem] = []

    for repo in component.get("repositories", []):
        internal_ref = repo["url"]
        rh_registry_repo: str = repo.get("rh-registry-repo") or ""
        registry_access_repo: str = repo.get("registry-access-repo") or ""
        tags: list[str] = repo.get("tags") or []

        if not tags:
            logger.info("No tags found for repository %s, skipping signing", internal_ref)
            continue

        registry_refs: list[str] = []
        if rh_registry_repo:
            repository = rh_registry_repo.split("/", 1)[1]
            registry_refs = [rh_registry_repo]
            if registry_access_repo and repository in sign_registry_access_repos:
                registry_refs.append(registry_access_repo)
        elif sign_external_registries:
            logger.info("Signing external registry image: %s", internal_ref)
            registry_refs = [internal_ref]
        else:
            logger.info("No rh-registry-repo found for %s, skipping signing", internal_ref)
            continue

        top_level_digest = digests[0]

        for registry_ref in registry_refs:
            if is_list:
                for nested_digest in digests[1:]:
                    for tag in tags:
                        items.append(
                            SignItem(
                                identity=f"{registry_ref}:{tag}",
                                source=internal_ref,
                                digest=nested_digest,
                            )
                        )
            for tag in tags:
                items.append(
                    SignItem(
                        identity=f"{registry_ref}:{tag}",
                        source=internal_ref,
                        digest=top_level_digest,
                    )
                )

    logger.info("Found %d signing candidates for component %s", len(items), component_name)
    return items


def _cosign_rekor_args(
    secrets: SigningSecrets, rekor_key_path: Path | None, verify: bool = False
) -> list[str]:
    """Build rekor-related cosign arguments based on available secrets.

    Args:
        secrets: Loaded signing secrets.
        rekor_key_path: Path to the temporary file containing the rekor
            public key, or None when rekor is unavailable.
        verify: When True, build args for ``cosign verify`` rather than sign.

    Returns:
        List of argument strings to append to a cosign command.

    """
    if secrets.rekor_url and rekor_key_path:
        if verify:
            return [f"--rekor-url={secrets.rekor_url}"]
        return ["-y", f"--rekor-url={secrets.rekor_url}"]
    if verify:
        return ["--insecure-ignore-tlog=true"]
    return ["--tlog-upload=false"]


def run_cosign_with_retry(
    args: list[str],
    *,
    retries: int,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a cosign command with Fibonacci backoff retries on failure.

    Attempts the command up to ``retries + 1`` times. On each failure the
    wait is the current Fibonacci number in the sequence (3, 5, 8, 13, ...
    seconds starting from the second value of the pair 2, 3).

    Args:
        args: Full cosign command including the ``cosign`` binary name.
        retries: Maximum number of retries (not counting the initial attempt).
        env: Optional environment variable overrides merged on top of the
            current process environment.

    Returns:
        The CompletedProcess result from the final successful attempt.

    Raises:
        subprocess.CalledProcessError: When the command fails on every attempt.

    """
    backoff1, backoff2 = 2, 3

    for attempt in range(retries + 1):
        try:
            merged_env = {**os.environ, **(env or {})}
            return subprocess.run(
                args,
                env=merged_env,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            if attempt >= retries:
                logger.error("Max retries exceeded for cosign command")
                raise
            logger.warning(
                "cosign attempt %d/%d failed, sleeping %ds: %s",
                attempt + 1,
                retries + 1,
                backoff2,
                exc.stderr.strip()[:200],
            )
            time.sleep(backoff2)
            backoff1, backoff2 = backoff2, backoff1 + backoff2

    raise AssertionError("unreachable: loop above always returns or re-raises")


def check_existing_cosign_signature(
    identity: str,
    source: str,
    digest: str,
    secrets: SigningSecrets,
    *,
    public_key_path: Path,
    rekor_key_path: Path | None,
    retries: int,
    aws_env: dict[str, str],
) -> bool:
    """Return True if a cosign signature for the given identity and digest exists.

    Runs ``cosign verify`` against ``source@digest`` and inspects the JSON
    output for a record matching both ``identity`` and ``digest``.

    Args:
        identity: Public image reference with tag (e.g.
            ``registry.redhat.io/myrepo:v1.0``).
        source: Internal image location used as the cosign reference target
            (e.g. ``quay.io/internal/myrepo``).
        digest: Manifest digest to check (e.g. ``sha256:abc123``).
        secrets: Loaded signing secrets.
        public_key_path: Path to the temporary public key file.
        rekor_key_path: Path to the temporary rekor public key file, or None.
        retries: Number of cosign retries.
        aws_env: AWS credential environment variables for cosign.

    Returns:
        True when a matching signature is found, False otherwise.

    Raises:
        subprocess.CalledProcessError: When ``cosign verify`` fails on every
            retry attempt. A verification failure must not be treated as "no
            signature found", since that would cause the task to add a
            duplicate signature instead of failing loudly.
        ValueError: When ``cosign verify`` exits successfully but prints
            output that is not valid JSON.

    """
    verify_env = dict(aws_env)
    if rekor_key_path:
        verify_env["SIGSTORE_REKOR_PUBLIC_KEY"] = str(rekor_key_path)

    verify_args = (
        ["cosign", "verify"]
        + _cosign_rekor_args(secrets, rekor_key_path, verify=True)
        + ["--key", str(public_key_path), f"{source}@{digest}"]
    )

    result = run_cosign_with_retry(verify_args, retries=retries, env=verify_env)
    verify_output = result.stdout.strip() or "[]"

    try:
        sigs = json.loads(verify_output)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"cosign verify output for {source}@{digest} was not valid JSON: "
            f"{verify_output[:200]}"
        ) from exc

    found = sum(
        1
        for sig in sigs
        if digest in sig.get("critical", {}).get("image", {}).get("docker-manifest-digest", "")
        and sig.get("critical", {}).get("identity", {}).get("docker-reference") == identity
    )
    if found:
        logger.info(
            "Skip signing %s (%s): %d existing signature(s) found", identity, digest, found
        )
    return bool(found)


def sign_item(
    item: SignItem,
    secrets: SigningSecrets,
    *,
    public_key_path: Path,
    rekor_key_path: Path | None,
    retries: int,
    aws_env: dict[str, str],
) -> None:
    """Sign a single container image reference with cosign.

    Authenticates using ``select-oci-auth`` for the source registry, checks
    for an existing signature, and calls ``cosign sign`` when one is absent.

    Args:
        item: The signing target (identity, source, digest).
        secrets: Loaded signing secrets.
        public_key_path: Path to the temporary public key file.
        rekor_key_path: Path to the temporary rekor public key file, or None.
        retries: Number of cosign retries for each command.
        aws_env: AWS credential environment variables for cosign.

    """
    with tempfile.TemporaryDirectory() as docker_config_dir:
        auth_result = run_cmd(["select-oci-auth", item.source])
        (Path(docker_config_dir) / "config.json").write_text(
            auth_result.stdout, encoding="utf-8"
        )

        # DOCKER_CONFIG must be set for both verify and sign: cosign has no way
        # to select the right auth entry for `item.source`, so a single-entry
        # auth file scoped to it is required for every cosign call against it.
        cosign_env = {**aws_env, "DOCKER_CONFIG": docker_config_dir}

        already_signed = check_existing_cosign_signature(
            item.identity,
            item.source,
            item.digest,
            secrets,
            public_key_path=public_key_path,
            rekor_key_path=rekor_key_path,
            retries=retries,
            aws_env=cosign_env,
        )
        if already_signed:
            return

        sign_env = dict(cosign_env)
        if rekor_key_path:
            sign_env["SIGSTORE_REKOR_PUBLIC_KEY"] = str(rekor_key_path)

        sign_args = (
            ["cosign", "-t", "3m0s", "sign"]
            + _cosign_rekor_args(secrets, rekor_key_path, verify=False)
            + [
                "--key",
                secrets.sign_key,
                "--sign-container-identity",
                item.identity,
                f"{item.source}@{item.digest}",
            ]
        )

        logger.info("Signing %s (%s)", item.identity, item.digest)
        run_cosign_with_retry(sign_args, retries=retries, env=sign_env)
        logger.info("Signed %s (%s) successfully", item.identity, item.digest)


def sign_all(
    items: list[SignItem],
    secrets: SigningSecrets,
    *,
    public_key_path: Path,
    rekor_key_path: Path | None,
    retries: int,
    concurrent_limit: int,
    aws_env: dict[str, str],
) -> None:
    """Sign all items concurrently, grouping by source+digest to avoid races.

    Items are partitioned into groups keyed by ``(source, digest)`` so that
    two sign operations for the same manifest never run at the same time (which
    can cause signature conflicts).  Within each round, one item from each
    group is dispatched to the thread pool; the pool is drained before the
    next round begins, matching the bash task's group-separator ``---`` logic.

    Memory-based throttling is applied before each submission and a small
    stabilization delay is inserted every ``BURST_SIZE`` submissions.

    Args:
        items: All signing candidates collected from the snapshot.
        secrets: Loaded signing secrets.
        public_key_path: Path to the temporary public key file.
        rekor_key_path: Path to the temporary rekor public key file, or None.
        retries: Number of cosign retries for each individual sign call.
        concurrent_limit: Maximum number of parallel cosign sign jobs.
        aws_env: AWS credential environment variables for cosign.

    """
    digest_groups: dict[str, list[SignItem]] = {}
    for item in items:
        group_key = f"{item.source}@{item.digest}"
        digest_groups.setdefault(group_key, []).append(item)

    logger.info(
        "Signing %d item(s) across %d digest group(s) with concurrent limit %d",
        len(items),
        len(digest_groups),
        concurrent_limit,
    )

    failures: list[Exception] = []
    spawn_count = 0

    with ThreadPoolExecutor(max_workers=concurrent_limit) as executor:
        for batch in zip_longest(*digest_groups.values()):
            batch_futures: list[Future[None]] = []

            for item in filter(None, batch):
                memory_throttle.wait_for_memory(80)

                future: Future[None] = executor.submit(
                    sign_item,
                    item,
                    secrets,
                    public_key_path=public_key_path,
                    rekor_key_path=rekor_key_path,
                    retries=retries,
                    aws_env=aws_env,
                )
                batch_futures.append(future)
                spawn_count += 1

                if spawn_count % BURST_SIZE == 0:
                    time.sleep(STABILIZATION_DELAY)

            logger.info(
                "Waiting for %d item(s) in this group round to complete ...",
                len(batch_futures),
            )
            for future in batch_futures:
                exc = future.exception()
                if exc is not None:
                    failures.append(exc)

    succeeded = len(items) - len(failures)
    logger.info("Signing summary: %d succeeded, %d failed", succeeded, len(failures))

    if failures:
        for i, failure in enumerate(failures, 1):
            logger.error("Signing failure %d/%d: %s", i, len(failures), failure)
        raise RuntimeError(f"{len(failures)} signing job(s) failed")


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured argument parser with all cosign signing arguments.

    """
    parser = argparse.ArgumentParser(
        prog=PROG, description="Sign container images in a snapshot using cosign."
    )
    parser.add_argument(
        "--snapshot",
        required=True,
        type=Path,
        help="Path to the JSON snapshot file containing component image references",
    )
    parser.add_argument(
        "--sign-registry-access-file",
        type=str,
        default="",
        help=(
            "Path to a text file listing repositories (one per line) that require "
            "registry.access.redhat.com signing.  Pass an empty string or omit to skip."
        ),
    )
    parser.add_argument(
        "--sign-external-registries",
        default="false",
        help=(
            "Set to 'true' to sign images with no rh-registry-repo using the repository "
            "url field directly (for external registries like quay.io)"
        ),
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of cosign retry attempts on failure (default: %(default)s)",
    )
    parser.add_argument(
        "--concurrent-limit",
        type=int,
        default=90,
        help="Maximum number of parallel cosign signing jobs (default: %(default)s)",
    )
    return parser


def main() -> int:
    """Parse arguments, load secrets, and sign all snapshot components."""
    parser = setup_argparser()
    args = parser.parse_args()

    secrets_dir = path_from_env_variable("SECRETS_DIR", "/etc/secrets")
    secrets = load_signing_secrets(secrets_dir)

    aws_env = {
        "AWS_DEFAULT_REGION": secrets.aws_default_region,
        "AWS_ACCESS_KEY_ID": secrets.aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": secrets.aws_secret_access_key,
    }

    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))

    sign_external_registries = args.sign_external_registries.lower() == "true"

    sign_registry_access_repos: set[str] = set()
    if args.sign_registry_access_file:
        access_file = Path(args.sign_registry_access_file)
        if access_file.is_file():
            sign_registry_access_repos = {
                line.strip()
                for line in access_file.read_text(encoding="utf-8").splitlines()
                if line.strip()
            }
        elif not access_file.is_dir():
            # A real path was given but the file is absent — surface this as an error.
            # When signRegistryAccessPath is empty the combined path is the data directory
            # itself (is_dir() is True), so that case is silently skipped.
            raise FileNotFoundError(f"signRegistryAccessPath file not found: {access_file}")

    memory_throttle.log_memory_throttle_status(80)

    all_items: list[SignItem] = []
    for component in snapshot.get("components", []):
        component_items = collect_component_sign_items(
            component,
            sign_registry_access_repos,
            sign_external_registries,
        )
        all_items.extend(component_items)

    logger.info("Total signing candidates across all components: %d", len(all_items))

    with tempfile.NamedTemporaryFile(mode="w", suffix=".pub", delete=False) as public_key_file:
        public_key_path = Path(public_key_file.name)
        public_key_file.write(secrets.public_key)

    rekor_key_path: Path | None = None
    if secrets.rekor_url and secrets.rekor_public_key:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".pub", delete=False
        ) as rekor_key_file:
            rekor_key_path = Path(rekor_key_file.name)
            rekor_key_file.write(secrets.rekor_public_key)

    try:
        sign_all(
            all_items,
            secrets,
            public_key_path=public_key_path,
            rekor_key_path=rekor_key_path,
            retries=args.retries,
            concurrent_limit=args.concurrent_limit,
            aws_env=aws_env,
        )
    finally:
        public_key_path.unlink(missing_ok=True)
        if rekor_key_path:
            rekor_key_path.unlink(missing_ok=True)

    logger.info("All signing jobs completed successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
