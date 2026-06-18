#!/usr/bin/env python3
"""Compute advisory severity from release-notes images and OSIDB flaw data.

* Reads OSIDB credentials from `/mnt/osidb-service-account/` (or
  `OSIDB_SERVICE_ACCOUNT_MOUNT`): `name`, `base64_keytab`, `osidb_url`.
* Decodes `IMAGES_ENCODED` (base64+gzip JSON array of release-note images).
* Queries OSIDB for each fixed CVE, then returns the highest impact as a Tekton
  result (title-cased, e.g. `Critical`).
* Writes `RESULT_RESULT`, `RESULT_SEVERITY`, and internal-request result paths.
* After a valid invocation with those env vars, always exits with status `0`;
  success or failure is in the result files.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import authentication
import file
import internal_request
import osidb
import requests
import tekton
from find_matching_purl import find_matching_purl
from logger import logger

_FLAW_INCLUDE_FIELDS = "cve_id,impact,affects.purl,affects.impact"
_BATCH_SIZE = 30
_MAX_PARALLEL_BATCHES = 8
_SEVERITY_LEVELS = ("CRITICAL", "IMPORTANT", "MODERATE", "LOW")
_NO_SEVERITY_MSG = "Unable to find severity on any cve listed in the releaseNotes"
_MAX_RELEASE_NOTES_DECOMPRESSED_BYTES = 32 * 1024 * 1024  # 32MB


def decode_release_notes_images(encoded: str) -> list[dict[str, Any]]:
    """Decode base64+gzip *encoded* JSON to a list of release-note image dicts."""
    b64_decoded = base64.standard_b64decode(encoded.strip())
    gzip_decoded = file.decompress_gzip_bounded(
        b64_decoded,
        max_bytes=_MAX_RELEASE_NOTES_DECOMPRESSED_BYTES,
    )
    data = json.loads(gzip_decoded.decode("utf-8"))
    if not isinstance(data, list):
        msg = "releaseNotesImages must be a JSON array"
        raise ValueError(msg)
    return data


def unique_fixed_cves(images: Sequence[dict[str, Any]]) -> list[str]:
    """Return unique CVE ids from each image's `cves.fixed` map (stable order)."""
    seen: set[str] = set()
    ordered: list[str] = []
    for image in images:
        if not isinstance(image, dict):
            continue
        cves = image.get("cves")
        if not isinstance(cves, dict):
            continue
        fixed = cves.get("fixed")
        if not isinstance(fixed, dict):
            continue
        for cve_id in fixed:
            if cve_id not in seen:
                seen.add(cve_id)
                ordered.append(str(cve_id))
    return ordered


def higher_severity(current: str, new: str) -> str:
    """Return the higher of two OSIDB impact strings (CRITICAL down to LOW)."""
    cur = (current or "").upper()
    nxt = (new or "").upper()
    for level in _SEVERITY_LEVELS:
        if cur == level or nxt == level:
            return level
    return cur


def purl_impact_entries(flaw: dict[str, Any]) -> list[dict[str, Any]]:
    """Build `{purl, impact}` rows from a flaw's `affects` list."""
    affects = flaw.get("affects")
    if not isinstance(affects, list):
        return []
    rows: list[dict[str, Any]] = []
    for affect in affects:
        if not isinstance(affect, dict):
            continue
        purl = affect.get("purl")
        if not purl or not isinstance(purl, str):
            continue
        impact = affect.get("impact")
        rows.append({"purl": purl, "impact": impact if impact is not None else ""})
    return rows


def resolve_impact_for_repository(
    flaw: dict[str, Any],
    repository: str,
    *,
    find_purl_fn: Callable[[list[dict[str, Any]], str], str | None] = find_matching_purl,
) -> str:
    """Return the impact for *repository* on *flaw*.

    Uses the flaw-level `impact` unless a matching affected-component purl
    supplies a non-empty component impact.
    """
    general = flaw.get("impact")
    impact = str(general).upper() if general is not None else ""
    rows = purl_impact_entries(flaw)
    if rows:
        component_impact = find_purl_fn(rows, repository)
        if component_impact:
            impact = str(component_impact).upper()
    return impact


def fetch_flaw_record(osidb_url: str, token: str, cve_id: str) -> dict[str, Any]:
    """GET one flaw from OSIDB v2 and return the first `results` row."""
    body = osidb.fetch_flaw_response(
        osidb_url,
        token,
        cve_id,
        include_fields=_FLAW_INCLUDE_FIELDS,
    )
    if not body.strip():
        msg = f"empty OSIDB response for {cve_id}"
        raise ValueError(msg)
    data: dict[str, Any] = json.loads(body)
    results = data.get("results")
    if not isinstance(results, list) or not results:
        msg = f"no OSIDB flaw row for {cve_id}"
        raise ValueError(msg)
    first = results[0]
    if first is None or not isinstance(first, dict):
        msg = f"invalid OSIDB flaw row for {cve_id}"
        raise ValueError(msg)
    return first


def fetch_flaw_with_token_retry(
    osidb_url: str,
    token: str,
    cve_id: str,
    *,
    get_token: Callable[[str], str],
    fetch_flaw: Callable[[str, str, str], dict[str, Any]] = fetch_flaw_record,
) -> tuple[dict[str, Any], str]:
    """Fetch one flaw; refresh the bearer token and retry once on failure."""
    refresh_reason: str | None = None
    try:
        return fetch_flaw(osidb_url, token, cve_id), token
    except requests.HTTPError as err:
        # http_client.get_text() raises HTTPError for non-2xx; only auth failures
        # should trigger a token refresh (not 5xx or other client errors).
        response = err.response
        status = response.status_code if response is not None else None
        if status not in (401, 403):
            raise
        refresh_reason = f"HTTP {status}"
    except OSError:
        raise
    except (ValueError, json.JSONDecodeError) as err:
        # Empty body, bad JSON, or missing results may mean an expired token too.
        refresh_reason = str(err)

    # Both recoverable paths share one refresh-and-retry (no duplicate logic).
    logger.warning(f"OSIDB query for {cve_id} failed ({refresh_reason}), refreshing token")
    fresh_token = get_token(osidb_url)
    return fetch_flaw(osidb_url, fresh_token, cve_id), fresh_token


def _process_cve_batch(
    batch_id: int,
    cve_ids: Sequence[str],
    osidb_url: str,
    cache: dict[str, dict[str, Any]],
    cache_lock: threading.Lock,
    *,
    get_token: Callable[[str], str],
    fetch_flaw: Callable[[str, str, str], dict[str, Any]],
) -> None:
    """Fetch flaws for *cve_ids* into *cache* (one token per batch)."""
    logger.info(f"Batch {batch_id}: getting token")
    token = get_token(osidb_url)
    logger.info(f"Batch {batch_id}: processing {len(cve_ids)} CVE(s)")
    for cve_id in cve_ids:
        with cache_lock:
            if cve_id in cache:
                continue
        logger.info(f"Batch {batch_id}: processing CVE {cve_id}")
        record, token = fetch_flaw_with_token_retry(
            osidb_url,
            token,
            cve_id,
            get_token=get_token,
            fetch_flaw=fetch_flaw,
        )
        with cache_lock:
            cache[cve_id] = record
    logger.info(f"Batch {batch_id}: completed")


def fetch_flaws_parallel(
    osidb_url: str,
    cve_ids: Sequence[str],
    *,
    get_token: Callable[[str], str],
    fetch_flaw: Callable[[str, str, str], dict[str, Any]] = fetch_flaw_record,
    batch_size: int = _BATCH_SIZE,
    max_workers: int = _MAX_PARALLEL_BATCHES,
) -> dict[str, dict[str, Any]]:
    """Fetch flaw JSON for each CVE in parallel batches."""
    if not cve_ids:
        return {}
    batches: list[list[str]] = []
    for start in range(0, len(cve_ids), batch_size):
        batches.append(list(cve_ids[start : start + batch_size]))
    logger.info(f"Processing {len(cve_ids)} unique CVE(s) in {len(batches)} batch(es)")
    cache: dict[str, dict[str, Any]] = {}
    cache_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                _process_cve_batch,
                batch_idx,
                batch,
                osidb_url,
                cache,
                cache_lock,
                get_token=get_token,
                fetch_flaw=fetch_flaw,
            )
            for batch_idx, batch in enumerate(batches)
        ]
        for future in as_completed(futures):
            future.result()
    logger.info("All CVE data retrieved")
    return cache


def highest_severity_for_images(
    images: Sequence[dict[str, Any]],
    flaw_cache: dict[str, dict[str, Any]],
    *,
    find_purl_fn: Callable[[list[dict[str, Any]], str], str | None] = find_matching_purl,
) -> str:
    """Return the highest impact across all fixed CVEs on all *images*."""
    top_severity = ""
    for image in images:
        if not isinstance(image, dict):
            continue
        repository = image.get("repository", "")
        repo_str = repository if isinstance(repository, str) else str(repository)
        cves = image.get("cves")
        if not isinstance(cves, dict):
            continue
        fixed = cves.get("fixed")
        if not isinstance(fixed, dict):
            continue
        for cve_id in fixed:
            if cve_id not in flaw_cache:
                msg = f"CVE {cve_id} not found in cache"
                raise ValueError(msg)
            logger.info(f"Checking CVE {cve_id} for component with repository {repo_str}")
            impact = resolve_impact_for_repository(
                flaw_cache[cve_id],
                repo_str,
                find_purl_fn=find_purl_fn,
            )
            top_severity = higher_severity(top_severity, impact)
    return top_severity


def run_get_advisory_severity(
    *,
    images_encoded: str,
    mount: Path,
    result_paths: dict[str, Path],
    pipeline_run_name: str,
    task_run_name: str,
    get_token: Callable[[str], str] = osidb.get_access_token,
    fetch_flaw: Callable[[str, str, str], dict[str, Any]] = fetch_flaw_record,
    find_purl_fn: Callable[..., str | None] = find_matching_purl,
    krb5_template: Path = Path("/etc/krb5.conf"),
) -> None:
    """Query OSIDB and write the highest advisory severity to *result_paths*."""
    internal_request.write_result_paths(
        result_paths,
        pipeline_run_name=pipeline_run_name,
        task_run_name=task_run_name,
    )

    keytab_path: Path | None = None
    ccache_path: Path | None = None
    krb5_config_path: Path | None = None
    try:
        images = decode_release_notes_images(images_encoded)
        cve_ids = unique_fixed_cves(images)

        try:
            principal, keytab_bytes, text = authentication.load_service_account(
                mount,
                ("osidb_url",),
                principal_file="name",
                keytab_b64_file="base64_keytab",
            )
        except (OSError, ValueError) as exc:
            raise tekton.CheckStepError(
                "reading the mounted OSIDB service account", exc
            ) from exc
        osidb_url = text["osidb_url"]

        keytab_path = file.make_tempfile_path("keytab-", keytab_bytes)
        ccache_path = file.make_tempfile_path("ccache-")
        try:
            krb5_source = krb5_template.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            raise tekton.CheckStepError("reading the Kerberos configuration", exc) from exc
        krb5_config_path = file.make_tempfile_path(
            "krb5-",
            authentication.patch_krb5_config(krb5_source).encode("utf-8"),
        )
        kenv = {
            "KRB5CCNAME": str(ccache_path),
            "KRB5_CONFIG": str(krb5_config_path),
        }
        try:
            authentication.kinit_with_retry(principal, keytab_path, kenv, max_attempts=5)
        except subprocess.CalledProcessError as exc:
            raise tekton.CheckStepError("logging in with Kerberos (kinit)", exc) from exc
        os.environ.update(kenv)

        flaw_cache = fetch_flaws_parallel(
            osidb_url,
            cve_ids,
            get_token=get_token,
            fetch_flaw=fetch_flaw,
        )
        highest = highest_severity_for_images(
            images,
            flaw_cache,
            find_purl_fn=find_purl_fn,
        )
        if not highest:
            raise tekton.CheckStepError(
                "determining advisory severity from release notes",
                ValueError(_NO_SEVERITY_MSG),
            )
        result_paths["severity"].write_text(highest.lower().capitalize(), encoding="utf-8")
        result_paths["result"].write_text("Success", encoding="utf-8")
    finally:
        for temp_path in (keytab_path, ccache_path, krb5_config_path):
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)


def main() -> int:
    """CLI entry: write Tekton results and always return 0 on a normal run."""
    (
        path_result,
        path_severity,
        path_internal_pr,
        path_internal_task_run,
    ) = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_SEVERITY",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME",
    )
    result_paths = {
        "result": path_result,
        "severity": path_severity,
        "internal_pr_name": path_internal_pr,
        "internal_task_run_name": path_internal_task_run,
    }
    program_basename = str(Path(sys.argv[0]).name)

    path_severity.write_text("", encoding="utf-8")

    images_encoded = tekton.require_env("IMAGES_ENCODED")
    mount = file.path_from_env_variable(
        "OSIDB_SERVICE_ACCOUNT_MOUNT",
        "/mnt/osidb-service-account",
    )
    pipeline_run_name = tekton.require_env("PARAM_INTERNAL_REQUEST_PIPELINE_RUN_NAME")
    task_run_name = tekton.require_env("PARAM_TASK_RUN_NAME")

    try:
        run_get_advisory_severity(
            images_encoded=images_encoded,
            mount=mount,
            result_paths=result_paths,
            pipeline_run_name=pipeline_run_name,
            task_run_name=task_run_name,
        )
    except Exception as exc:
        tekton.write_failure_result(
            path_result,
            program_basename,
            exc,
            command_log_path=None,
            workflow_action="computing advisory severity",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
