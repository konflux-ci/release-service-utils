#!/usr/bin/env python3
"""Sign FBC index images via the simple-signing-pipeline.

Reads FBC results, translates quay.io references to public registry.redhat.io
references, queries Pyxis for existing signatures, filters already-signed
items, batches them by string length, and submits signing requests via the
``internal-request`` or ``internal-pipelinerun`` CLI.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import file
import tekton
from direct_sign_index_image import translate_reference
from kubectl import get_configmap
from logger import logger
from memory_throttle import log_memory_throttle_status, wait_for_memory
from retry import retry_with_exponential_backoff
from rh_direct_sign_image import (
    PYXIS_INSTANCE_MAP,
    PyxisSignature,
    find_signatures_for_repository,
    get_signing_keys,
)
from subprocess_cmd import run_cmd

import pyxis

_TASK_LABEL = "internal-services.appstudio.openshift.io/group-id"
_PIPELINERUN_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"
_INTENTION_LABEL = "internal-services.appstudio.openshift.io/intention"


@dataclass
class FbcSigningItem:
    """A (reference, digest, repository) triple to sign."""

    reference: str
    digest: str
    repository: str


@dataclass(frozen=True)
class SignIndexSubmitConfig:
    """Configuration for submitting signing requests via simple-signing-pipeline."""

    request_type: str
    pipeline: str
    requester: str
    config_map_name: str
    umb_listen_topic: str
    umb_publish_topic: str
    signing_pyxis_url: str
    signing_umb_url: str
    signing_umb_client: str
    signing_pyxis_ssl_secret: str
    signing_umb_ssl_secret: str
    signer_type: str
    signing_key_names: str
    task_git_url: str
    task_git_revision: str
    task_id: str
    pipelinerun_uid: str
    intention: str
    request_timeout: str
    pipeline_timeout: str = "0h30m0s"
    task_timeout: str = "0h25m0s"
    concurrent_limit: int = 16
    extra_args: list[str] = field(default_factory=list)


def resolve_umb_topics(configmap_data: dict[str, str], signer_type: str) -> tuple[str, str]:
    """Resolve UMB listen and publish topics based on the signer type.

    When ``signer_type`` is ``batch``, prefer ``UMB_BATCH_LISTEN_TOPIC`` and
    ``UMB_BATCH_PUBLISH_TOPIC`` keys, falling back to the non-batch variants.
    For any other signer type, use ``UMB_LISTEN_TOPIC`` and ``UMB_PUBLISH_TOPIC``.
    """
    if signer_type == "batch":
        listen = configmap_data.get(
            "UMB_BATCH_LISTEN_TOPIC", configmap_data["UMB_LISTEN_TOPIC"]
        )
        publish = configmap_data.get(
            "UMB_BATCH_PUBLISH_TOPIC", configmap_data["UMB_PUBLISH_TOPIC"]
        )
    else:
        listen = configmap_data["UMB_LISTEN_TOPIC"]
        publish = configmap_data["UMB_PUBLISH_TOPIC"]
    return listen, publish


def collect_fbc_items(fbc_results: dict[str, Any]) -> list[FbcSigningItem]:
    """Build signing items from FBC results.

    For each component, translates the ``target_index`` to a public
    registry.redhat.io reference, extracts the repository path from
    ``rh-registry-repo``, and creates an FbcSigningItem for every digest.
    """
    items: list[FbcSigningItem] = []

    for component in fbc_results.get("components", []):
        target_index = component["target_index"]
        reference = translate_reference(target_index)
        logger.info("Translated %s -> %s", target_index, reference)

        rh_registry_repo = component.get("rh-registry-repo", "")
        repository = (
            rh_registry_repo.split("/", 1)[1] if "/" in rh_registry_repo else rh_registry_repo
        )

        for digest in component.get("image_digests", []):
            items.append(FbcSigningItem(reference, digest, repository))

    return items


def find_existing_signatures_with_retry(
    pyxis_url: str,
    lookups: set[tuple[str, str]],
    max_workers: int = 10,
    max_attempts: int = 3,
) -> dict[tuple[str, str], set[PyxisSignature]]:
    """Look up existing signatures with per-lookup retry.

    Wraps each ``find_signatures_for_repository`` call with exponential-backoff
    retry, running lookups concurrently via a thread pool.
    """
    results: dict[tuple[str, str], set[PyxisSignature]] = {}

    def _lookup_with_retry(
        repo: str, digest: str
    ) -> tuple[tuple[str, str], set[PyxisSignature]]:
        wait_for_memory()
        sigs = retry_with_exponential_backoff(
            lambda r=repo, d=digest: find_signatures_for_repository(pyxis_url, r, d),
            max_attempts=max_attempts,
            base_sleep_seconds=2,
        )
        return (digest, repo), sigs

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_lookup_with_retry, repo, digest) for digest, repo in lookups]
        for future in as_completed(futures):
            key, sigs = future.result()
            results[key] = sigs

    return results


def filter_already_signed_items(
    items: list[FbcSigningItem],
    signing_keys: list[str],
    pyxis_url: str,
    max_workers: int = 10,
    max_attempts: int = 3,
    fail_on_error: bool = True,
) -> list[FbcSigningItem]:
    """Filter out items for which ALL signing keys already have signatures.

    An item is skipped only when every key in ``signing_keys`` has a matching
    ``(reference, key)`` signature in Pyxis. If any key is missing, the item
    is kept for signing.
    """
    lookups = {(item.digest, item.repository) for item in items}

    try:
        existing = find_existing_signatures_with_retry(
            pyxis_url, lookups, max_workers, max_attempts
        )
    except Exception:
        if fail_on_error:
            raise
        logger.warning(
            "Pyxis lookup failed; failOnSignatureLookupError=false."
            " Submitting all %d items without filtering.",
            len(items),
        )
        return items

    to_sign: list[FbcSigningItem] = []
    for item in items:
        sigs = existing.get((item.digest, item.repository), set())
        all_signed = all(
            any(sig.reference == item.reference and sig.sig_key_id == key for sig in sigs)
            for key in signing_keys
        )
        if all_signed:
            logger.info(
                "Signature already exists for reference=%s digest=%s",
                item.reference,
                item.digest,
            )
        else:
            to_sign.append(item)

    return to_sign


def batch_items(
    items: list[FbcSigningItem], batch_limit: int = 4096
) -> list[list[FbcSigningItem]]:
    """Batch items so that space-joined references and digests stay within batch_limit.

    A new batch is started whenever adding the next item would cause either
    the space-joined references or digests string to exceed ``batch_limit``
    characters.
    """
    if not items:
        return []

    batches: list[list[FbcSigningItem]] = []
    current_batch: list[FbcSigningItem] = []
    refs_len = 0
    digs_len = 0

    for item in items:
        separator = 1 if current_batch else 0
        new_refs_len = refs_len + separator + len(item.reference)
        new_digs_len = digs_len + separator + len(item.digest)

        if current_batch and (new_refs_len > batch_limit or new_digs_len > batch_limit):
            batches.append(current_batch)
            current_batch = [item]
            refs_len = len(item.reference)
            digs_len = len(item.digest)
        else:
            current_batch.append(item)
            refs_len = new_refs_len
            digs_len = new_digs_len

    if current_batch:
        batches.append(current_batch)

    return batches


def submit_batch(
    items: list[FbcSigningItem],
    config: SignIndexSubmitConfig,
) -> None:
    """Submit a single signing batch via the internal-request/pipelinerun CLI."""
    wait_for_memory()
    references = " ".join(item.reference for item in items)
    digests = " ".join(item.digest for item in items)
    repositories = " ".join(item.repository for item in items)

    params = {
        "references": references,
        "manifest_digests": digests,
        "repositories": repositories,
        "config_map_name": config.config_map_name,
        "requester": config.requester,
        "umb_listen_topic": config.umb_listen_topic,
        "umb_publish_topic": config.umb_publish_topic,
        "pyxis_url": config.signing_pyxis_url,
        "umb_url": config.signing_umb_url,
        "umb_client_name": config.signing_umb_client,
        "pyxis_ssl_cert_secret_name": config.signing_pyxis_ssl_secret,
        "umb_ssl_cert_secret_name": config.signing_umb_ssl_secret,
        "signer_type": config.signer_type,
        "signing_key_names": config.signing_key_names,
        "taskGitUrl": config.task_git_url,
        "taskGitRevision": config.task_git_revision,
    }
    labels = {
        _TASK_LABEL: config.task_id,
        _PIPELINERUN_LABEL: config.pipelinerun_uid,
        _INTENTION_LABEL: config.intention,
    }

    cmd: list[str] = [config.request_type, "--pipeline", config.pipeline]
    for key, val in params.items():
        cmd.extend(["-p", f"{key}={val}"])
    for key, val in labels.items():
        cmd.extend(["-l", f"{key}={val}"])
    cmd.extend(
        [
            "-t",
            config.request_timeout,
            "--pipeline-timeout",
            config.pipeline_timeout,
            "--task-timeout",
            config.task_timeout,
            *config.extra_args,
            "-s",
            "true",
        ]
    )

    logger.debug("Submitting batch: %s", " ".join(cmd))
    start = time.monotonic()
    result = run_cmd(cmd, check=False)
    duration = time.monotonic() - start

    if result.returncode != 0:
        logger.error("Batch submission failed: %s", result.stderr.strip())
        raise RuntimeError(f"Batch submission failed: {result.stderr.strip()}")
    logger.info("Batch completed in %.1fs", duration)


def submit_all_batches(
    batches: list[list[FbcSigningItem]],
    config: SignIndexSubmitConfig,
) -> None:
    """Submit all signing batches concurrently."""
    logger.info(
        "Submitting %d batch(es) with concurrent limit %d",
        len(batches),
        config.concurrent_limit,
    )
    failures: list[Exception] = []

    with ThreadPoolExecutor(max_workers=config.concurrent_limit) as pool:
        futures = [pool.submit(submit_batch, batch, config) for batch in batches]
        for future in as_completed(futures):
            exc = future.exception()
            if exc is not None:
                failures.append(exc)

    succeeded = len(batches) - len(failures)
    logger.info(
        "Batch submission summary: %d succeeded, %d failed",
        succeeded,
        len(failures),
    )
    if failures:
        for i, failure in enumerate(failures, 1):
            logger.error("Failure %d/%d: %s", i, len(failures), failure)
        raise RuntimeError(f"{len(failures)} batch(es) failed during submission")


def _build_submit_config(
    *,
    data_file: dict[str, Any],
    cm_data: dict[str, str],
    signing_keys: list[str],
    signer_type: str,
    requester: str,
    config_map_name: str,
    request_timeout: str,
    task_git_url: str,
    task_git_revision: str,
    task_run_uid: str,
    pipeline_run_uid: str,
    concurrent_limit: int,
    rpa_file_path: Path,
) -> SignIndexSubmitConfig:
    """Build signing submission configuration from data file and configmap.

    Reads request type and pipeline name from the data file. For
    ``internal-pipelinerun`` requests, loads the RPA file to determine the
    service account.
    """
    umb_listen, umb_publish = resolve_umb_topics(cm_data, signer_type)

    request_type = data_file.get("requestType", "internal-request")
    extra_args: list[str] = []
    if request_type == "internal-pipelinerun":
        rpa = file.load_json_dict(rpa_file_path)
        sa_name = (
            rpa.get("spec", {})
            .get("pipeline", {})
            .get("serviceAccountName", "release-service-account")
        )
        extra_args = ["--service-account", sa_name]
    else:
        request_type = "internal-request"

    pipeline = data_file.get("sign", {}).get("request", "simple-signing-pipeline")

    return SignIndexSubmitConfig(
        request_type=request_type,
        pipeline=pipeline,
        requester=requester,
        config_map_name=config_map_name,
        umb_listen_topic=umb_listen,
        umb_publish_topic=umb_publish,
        signing_pyxis_url=cm_data["PYXIS_URL"],
        signing_umb_url=cm_data["UMB_URL"],
        signing_umb_client=cm_data["UMB_CLIENT_NAME"],
        signing_pyxis_ssl_secret=cm_data["PYXIS_SSL_CERT_SECRET_NAME"],
        signing_umb_ssl_secret=cm_data["UMB_SSL_CERT_SECRET_NAME"],
        signer_type=signer_type,
        signing_key_names="\n".join(signing_keys),
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
        task_id=task_run_uid,
        pipelinerun_uid=pipeline_run_uid,
        intention=data_file.get("intention", "unknown"),
        request_timeout=request_timeout,
        concurrent_limit=concurrent_limit,
        extra_args=extra_args,
    )


def run(
    *,
    pyxis_url: str,
    data_file_path: Path,
    fbc_results_path: Path,
    rpa_file_path: Path,
    requester: str,
    pipeline_run_uid: str,
    task_run_uid: str,
    task_git_url: str,
    task_git_revision: str,
    request_timeout: str,
    concurrent_limit: int,
    batch_limit: int,
    fail_on_error: bool,
    max_attempts: int,
) -> None:
    """Sign FBC index images, filtering already-signed items and batching requests."""
    pyxis.session = pyxis._get_session(retry_allowed_methods=None)
    log_memory_throttle_status()

    data_file = file.load_json_dict(data_file_path)
    fbc_results = file.load_json_dict(fbc_results_path)

    config_map_name = (
        data_file.get("sign", {}).get("configMapName")
        or data_file.get("fbc", {}).get("configMapName")
        or "signing-config-map"
    )
    configmap = get_configmap(config_map_name)
    cm_data = configmap["data"]
    signing_keys = get_signing_keys(configmap)
    if not signing_keys:
        raise ValueError("No signing keys found in configmap (SIG_KEY_NAMES is empty)")
    logger.info("Signing keys: %s", signing_keys)

    signer_type = cm_data.get("SIGNER_TYPE", "single")

    submit_config = _build_submit_config(
        data_file=data_file,
        cm_data=cm_data,
        signing_keys=signing_keys,
        signer_type=signer_type,
        requester=requester,
        config_map_name=config_map_name,
        request_timeout=request_timeout,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
        task_run_uid=task_run_uid,
        pipeline_run_uid=pipeline_run_uid,
        concurrent_limit=concurrent_limit,
        rpa_file_path=rpa_file_path,
    )

    all_items = collect_fbc_items(fbc_results)
    logger.info("Total signing candidates: %d", len(all_items))

    if not all_items:
        logger.info("No signing candidates found")
        return

    to_sign = filter_already_signed_items(
        all_items,
        signing_keys,
        pyxis_url,
        max_workers=concurrent_limit,
        max_attempts=max_attempts,
        fail_on_error=fail_on_error,
    )
    logger.info("Items to sign after filtering: %d", len(to_sign))

    if not to_sign:
        logger.info("All items already signed, nothing to submit")
        return

    batches = batch_items(to_sign, batch_limit)
    logger.info("Created %d batch(es)", len(batches))

    submit_all_batches(batches, submit_config)


def main() -> None:
    """Read Tekton environment variables and sign FBC index images."""
    pyxis_server = tekton.require_env("PYXIS_SERVER")
    if pyxis_server not in PYXIS_INSTANCE_MAP:
        raise ValueError(
            f"Invalid PYXIS_SERVER '{pyxis_server}'. "
            f"Allowed: {', '.join(PYXIS_INSTANCE_MAP)}"
        )
    pyxis_url = PYXIS_INSTANCE_MAP[pyxis_server]

    data_file_path = Path(tekton.require_env("DATA_FILE"))
    fbc_results_path = Path(tekton.require_env("FBC_RESULTS_FILE"))
    rpa_file_path = Path(tekton.require_env("RPA_FILE"))
    requester = tekton.require_env("REQUESTER")
    pipeline_run_uid = tekton.require_env("PIPELINE_RUN_UID")
    task_run_uid = tekton.require_env("TASK_RUN_UID")
    task_git_url = tekton.require_env("TASK_GIT_URL")
    task_git_revision = tekton.require_env("TASK_GIT_REVISION")
    pyxis_cert_path = Path(tekton.require_env("PYXIS_CERT_PATH"))
    pyxis_key_path = Path(tekton.require_env("PYXIS_KEY_PATH"))

    for cred_path in (pyxis_cert_path, pyxis_key_path):
        if not cred_path.is_file() or cred_path.stat().st_size == 0:
            raise FileNotFoundError(f"Missing or empty credential file: {cred_path}")

    request_timeout = os.environ.get("REQUEST_TIMEOUT", "1800")
    concurrent_limit = int(os.environ.get("CONCURRENT_LIMIT", "16"))
    batch_limit = int(os.environ.get("BATCH_LIMIT", "4096"))
    fail_on_error = os.environ.get("FAIL_ON_SIGNATURE_LOOKUP_ERROR", "true").lower() != "false"
    raw_attempts = os.environ.get("SIGNATURE_LOOKUP_MAX_ATTEMPTS", "3")
    max_attempts = (
        int(raw_attempts) if raw_attempts.isdigit() and int(raw_attempts) >= 1 else 3
    )

    logger.info("Using Pyxis instance: %s", pyxis_url)

    run(
        pyxis_url=pyxis_url,
        data_file_path=data_file_path,
        fbc_results_path=fbc_results_path,
        rpa_file_path=rpa_file_path,
        requester=requester,
        pipeline_run_uid=pipeline_run_uid,
        task_run_uid=task_run_uid,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
        request_timeout=request_timeout,
        concurrent_limit=concurrent_limit,
        batch_limit=batch_limit,
        fail_on_error=fail_on_error,
        max_attempts=max_attempts,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
