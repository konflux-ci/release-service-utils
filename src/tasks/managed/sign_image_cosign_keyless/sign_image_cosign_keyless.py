#!/usr/bin/env python3
"""Sign container images in a Konflux release snapshot using keyless cosign."""

from __future__ import annotations

import argparse
import base64
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

from release_service_utils.helpers import authentication, file, memory_throttle, skopeo
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd

PROG = "sign_image_cosign_keyless.py"

DEFAULT_OIDC_TOKEN_PATH = "/var/run/secrets/tokens/oidc-token"
DEFAULT_CA_CERT_PATH = "/mnt/trusted-ca/ca-bundle.crt"

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
    """A container image that needs to be signed by keyless cosign.

    ``identity`` is the public reference used as the cosign identity
    (e.g. ``quay.io/pending/myrepo:v1.0``).  ``source`` is the internal
    image location used for authentication and the cosign reference
    argument (e.g. ``quay.io/pending/myrepo``).  ``digest`` is the
    manifest digest (e.g. ``sha256:abc123``).
    """

    identity: str
    source: str
    digest: str


@dataclass(frozen=True)
class KeylessConfig:
    """Keyless signing endpoints and identity used for cosign verify/sign."""

    oidc_issuer: str
    fulcio_url: str
    rekor_url: str
    tuf_url: str
    oidc_token_path: Path
    certificate_identity: str


def certificate_identity_from_oidc_token(token_path: Path) -> str:
    """Return the Fulcio certificate identity encoded in a Kubernetes OIDC token.

    JWT payloads use base64url encoding (no padding, ``-_`` instead of ``+/``).
    Decode the payload and build
    ``https://kubernetes.io/namespaces/<ns>/serviceaccounts/<name>``.

    Args:
        token_path: Path to the projected service-account token file.

    Returns:
        Certificate identity URL used as ``--certificate-identity``.

    Raises:
        FileNotFoundError: When *token_path* does not exist.
        ValueError: When the token is not a JWT or lacks Kubernetes SA claims.

    """
    token = token_path.read_text(encoding="utf-8").strip()
    parts = token.split(".")
    if len(parts) < 2:
        raise ValueError(f"OIDC token at {token_path} is not a JWT")

    payload_b64 = parts[1].translate(str.maketrans("-_", "+/"))
    remainder = len(payload_b64) % 4
    if remainder:
        payload_b64 += "=" * (4 - remainder)

    try:
        payload = json.loads(base64.b64decode(payload_b64))
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"OIDC token payload at {token_path} is not valid JSON") from exc

    kube = payload.get("kubernetes.io")
    if not isinstance(kube, dict):
        raise ValueError(f"OIDC token at {token_path} is missing kubernetes.io claims")

    namespace = kube.get("namespace")
    serviceaccount = kube.get("serviceaccount")
    sa_name = serviceaccount.get("name") if isinstance(serviceaccount, dict) else None
    if not namespace or not sa_name:
        raise ValueError(
            f"OIDC token at {token_path} is missing serviceaccount name or namespace"
        )

    return f"https://kubernetes.io/namespaces/{namespace}/serviceaccounts/{sa_name}"


def initialize_tuf(tuf_url: str) -> None:
    """Initialize the cosign TUF root from *tuf_url*.

    Run ``cosign initialize --mirror=... --root=.../root.json``.
    A failure aborts the task immediately (no retries).

    Args:
        tuf_url: TUF repository URL passed as ``--mirror`` and as the root prefix.

    """
    logger.info("Initializing cosign TUF root from %s", tuf_url)
    run_cmd(
        [
            "cosign",
            "initialize",
            f"--mirror={tuf_url}",
            f"--root={tuf_url}/root.json",
        ]
    )


def get_manifest_digests(component: dict[str, Any]) -> tuple[bool, list[str]]:
    """Return all manifest digests for a component's container image.

    First tries the ``imageDigests`` field on the component (populated by IIB
    for multi-arch manifest lists). Falls back to ``skopeo inspect --raw`` when
    ``imageDigests`` is absent. Always includes the top-level digest.

    Args:
        component: A single component entry from the Konflux release snapshot.

    Returns:
        A ``(is_manifest_list, digests)`` tuple where ``is_manifest_list``
        indicates whether the image is a manifest list index and ``digests``
        is the ordered list of manifest digests to sign.

    Raises:
        RuntimeError: When ``skopeo inspect`` fails.

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


def collect_component_sign_items(component: dict[str, Any]) -> list[SignItem]:
    """Enumerate all SignItems for a single snapshot component.

    For each repository on the component, signing items are produced for
    every combination of digest and tag. Multi-arch manifest lists generate
    one item per nested manifest in addition to one item for the top-level
    digest. The repository ``url`` is both the cosign source and the
    identity prefix.

    Args:
        component: A single component entry from the Konflux release snapshot.

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
    top_level_digest = digests[0]

    for repo in component.get("repositories", []):
        internal_ref = repo["url"]
        tags: list[str] = repo.get("tags") or []

        if not tags:
            logger.info("No tags found for repository %s, skipping signing", internal_ref)
            continue

        if is_list:
            for nested_digest in digests[1:]:
                for tag in tags:
                    items.append(
                        SignItem(
                            identity=f"{internal_ref}:{tag}",
                            source=internal_ref,
                            digest=nested_digest,
                        )
                    )
        for tag in tags:
            items.append(
                SignItem(
                    identity=f"{internal_ref}:{tag}",
                    source=internal_ref,
                    digest=top_level_digest,
                )
            )

    logger.info("Found %d signing candidates for component %s", len(items), component_name)
    return items


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
            stderr_tail = (exc.stderr or "").strip()[:500]
            if attempt >= retries:
                logger.error(
                    "Max retries exceeded for cosign command: %s",
                    stderr_tail,
                )
                raise
            logger.warning(
                "cosign attempt %d/%d failed, sleeping %ds: %s",
                attempt + 1,
                retries + 1,
                backoff2,
                stderr_tail,
            )
            time.sleep(backoff2)
            backoff1, backoff2 = backoff2, backoff1 + backoff2

    raise AssertionError("unreachable: loop above always returns or re-raises")


def check_existing_cosign_signature(
    identity: str,
    source: str,
    digest: str,
    config: KeylessConfig,
    *,
    retries: int,
    env: dict[str, str],
) -> bool:
    """Return True if a keyless cosign signature for identity and digest exists.

    Runs ``cosign verify`` against ``source@digest`` and inspects the JSON
    output for a record matching both ``identity`` and ``digest``.

    Args:
        identity: Public image reference with tag.
        source: Internal image location used as the cosign reference target.
        digest: Manifest digest to check.
        config: Keyless endpoints and certificate identity.
        retries: Number of cosign retries.
        env: Environment for the cosign process (must include DOCKER_CONFIG).

    Returns:
        True when a matching signature is found, False otherwise.

    Raises:
        subprocess.CalledProcessError: When ``cosign verify`` fails on every
            retry attempt. A verification failure must not be treated as "no
            signature found", since that would cause the task to add a
            duplicate signature instead of failing loudly.
        ValueError: When ``cosign verify`` exits successfully but prints
            output that is not a JSON array.

    """
    verify_args = [
        "cosign",
        "verify",
        f"--rekor-url={config.rekor_url}",
        f"--certificate-identity={config.certificate_identity}",
        f"--certificate-oidc-issuer={config.oidc_issuer}",
        f"{source}@{digest}",
    ]

    result = run_cosign_with_retry(verify_args, retries=retries, env=env)
    verify_output = result.stdout.strip() or "[]"

    try:
        sigs = json.loads(verify_output)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"cosign verify output for {source}@{digest} was not valid JSON: "
            f"{verify_output[:200]}"
        ) from exc

    if not isinstance(sigs, list):
        raise ValueError(
            f"cosign verify output for {source}@{digest} was not valid JSON: "
            f"{verify_output[:200]}"
        )

    found = sum(
        1
        for sig in sigs
        if digest in sig.get("critical", {}).get("image", {}).get("docker-manifest-digest", "")
        and sig.get("critical", {}).get("identity", {}).get("docker-reference") == identity
    )
    logger.info("FOUND SIGNATURES for %s %s: %d", identity, digest, found)
    if found:
        logger.info(
            "Skip signing %s (%s): %d existing signature(s) found", identity, digest, found
        )
    return bool(found)


def sign_item(
    item: SignItem,
    config: KeylessConfig,
    *,
    retries: int,
) -> None:
    """Sign a single container image reference with keyless cosign.

    Authenticates using ``select-oci-auth`` for the source registry, checks
    for an existing signature, and calls ``cosign sign`` when one is absent.

    Args:
        item: The signing target (identity, source, digest).
        config: Keyless endpoints, OIDC token path, and certificate identity.
        retries: Number of cosign retries for each command.

    """
    with tempfile.TemporaryDirectory() as docker_config_dir:
        auth_result = run_cmd(["select-oci-auth", item.source])
        (Path(docker_config_dir) / "config.json").write_text(
            auth_result.stdout, encoding="utf-8"
        )

        # DOCKER_CONFIG must be set for both verify and sign: cosign has no way
        # to select the right auth entry for `item.source`, so a single-entry
        # auth file scoped to it is required for every cosign call against it.
        # SIGSTORE_ID_TOKEN is exported once in main() and inherited here.
        cosign_env = {"DOCKER_CONFIG": docker_config_dir}

        already_signed = check_existing_cosign_signature(
            item.identity,
            item.source,
            item.digest,
            config,
            retries=retries,
            env=cosign_env,
        )
        if already_signed:
            return

        sign_args = [
            "cosign",
            "-t",
            "3m0s",
            "sign",
            "-y",
            f"--rekor-url={config.rekor_url}",
            "--identity-token",
            str(config.oidc_token_path),
            "--fulcio-url",
            config.fulcio_url,
            "--sign-container-identity",
            item.identity,
            f"{item.source}@{item.digest}",
        ]

        logger.info("Signing %s (%s)", item.identity, item.digest)
        run_cosign_with_retry(sign_args, retries=retries, env=cosign_env)
        logger.info("Signed %s (%s) successfully", item.identity, item.digest)


def sign_all(
    items: list[SignItem],
    config: KeylessConfig,
    *,
    retries: int,
    concurrent_limit: int,
) -> None:
    """Sign all items concurrently, grouping by source+digest to avoid races.

    Items are partitioned into groups keyed by ``(source, digest)`` so that
    two sign operations for the same manifest never run at the same time
    (which can cause signature conflicts). Within each round, one item from
    each group is dispatched to the thread pool; the pool is drained before
    the next round begins.

    Memory-based throttling is applied before each submission and a small
    stabilization delay is inserted every ``BURST_SIZE`` submissions.

    Args:
        items: All signing candidates collected from the snapshot.
        config: Keyless endpoints and certificate identity.
        retries: Number of cosign retries for each individual sign call.
        concurrent_limit: Maximum number of parallel cosign sign jobs.

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

    failures: list[BaseException] = []
    spawn_count = 0

    with ThreadPoolExecutor(max_workers=max(1, concurrent_limit)) as executor:
        for batch in zip_longest(*digest_groups.values()):
            batch_futures: list[Future[None]] = []

            for item in filter(None, batch):
                memory_throttle.wait_for_memory(80)

                future: Future[None] = executor.submit(
                    sign_item,
                    item,
                    config,
                    retries=retries,
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
            if isinstance(failure, subprocess.CalledProcessError):
                stderr_tail = (failure.stderr or "").strip()[:500]
                logger.error(
                    "Signing failure %d/%d: %s; stderr: %s",
                    i,
                    len(failures),
                    failure,
                    stderr_tail,
                    exc_info=failure,
                )
            else:
                logger.error(
                    "Signing failure %d/%d: %s",
                    i,
                    len(failures),
                    failure,
                    exc_info=failure,
                )
        raise RuntimeError(f"{len(failures)} signing job(s) failed")


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Sign container images in a snapshot using keyless cosign.",
    )
    parser.add_argument(
        "--snapshot",
        required=True,
        type=Path,
        help="Path to the JSON snapshot file containing component image references",
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
    parser.add_argument(
        "--oidc-issuer",
        required=True,
        help="OIDC issuer used as --certificate-oidc-issuer during verify",
    )
    parser.add_argument(
        "--fulcio-url",
        required=True,
        help="Fulcio URL for keyless signing",
    )
    parser.add_argument(
        "--rekor-url",
        required=True,
        help="Rekor URL for keyless signing and verification",
    )
    parser.add_argument(
        "--tuf-url",
        required=True,
        help="TUF repository URL used by cosign initialize",
    )
    parser.add_argument(
        "--oidc-token-path",
        type=Path,
        default=Path(DEFAULT_OIDC_TOKEN_PATH),
        help="Path to the projected Sigstore OIDC token (default: %(default)s)",
    )
    parser.add_argument(
        "--ca-cert-path",
        default=DEFAULT_CA_CERT_PATH,
        help="CA bundle path exported as SSL_CERT_FILE when the file exists",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and sign all snapshot components with keyless cosign."""
    parser = setup_argparser()
    args = parser.parse_args(argv)

    os.environ["CA_CERT_PATH"] = args.ca_cert_path
    authentication.setup_ca_cert()

    os.environ["SIGSTORE_ID_TOKEN"] = str(args.oidc_token_path)

    certificate_identity = certificate_identity_from_oidc_token(args.oidc_token_path)
    logger.info("Using certificate identity %s", certificate_identity)

    config = KeylessConfig(
        oidc_issuer=args.oidc_issuer,
        fulcio_url=args.fulcio_url,
        rekor_url=args.rekor_url,
        tuf_url=args.tuf_url,
        oidc_token_path=args.oidc_token_path,
        certificate_identity=certificate_identity,
    )

    initialize_tuf(args.tuf_url)

    snapshot = file.load_json_dict(args.snapshot)

    memory_throttle.log_memory_throttle_status(80)

    all_items: list[SignItem] = []
    for component in snapshot.get("components", []):
        all_items.extend(collect_component_sign_items(component))

    logger.info("Total signing candidates across all components: %d", len(all_items))

    sign_all(
        all_items,
        config,
        retries=args.retries,
        concurrent_limit=args.concurrent_limit,
    )

    logger.info("All signing jobs completed successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
