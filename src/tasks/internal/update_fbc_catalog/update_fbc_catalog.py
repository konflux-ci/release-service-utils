#!/usr/bin/env python3
"""Submit and monitor IIB FBC catalog update builds.

Authenticate with Kerberos, check for reusable previous builds, submit a
new FBC operation to IIB if needed, poll for completion, validate the
resulting index image, and retrieve manifest digests.
"""

from __future__ import annotations

import argparse
import binascii
import json
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import authentication
import file
import http_client
import iib
import requests
import skopeo
import tekton
from logger import logger
from requests_kerberos import OPTIONAL, HTTPKerberosAuth

PROG = "update_fbc_catalog.py"
POLL_INTERVAL_SECONDS = 30
LOG_INTERVAL_SECONDS = 300


@dataclass
class FBCCatalogInput:
    """Parsed CLI inputs for the update-fbc-catalog flow."""

    fbc_fragments: list[str]
    from_index: str
    build_tags: list[str]
    add_arches: list[str]
    must_overwrite: bool
    must_publish: bool
    build_timeout_seconds: int


@dataclass
class RunResult:
    """Outcome of a full update-fbc-catalog run."""

    build_info: iib.IIBBuild
    state: str
    state_reason: str
    index_image_digests: str
    iib_log_url: str
    exit_code: int


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse CLI arguments for the update FBC catalog task."""
    parser = argparse.ArgumentParser(prog=PROG)
    parser.add_argument(
        "--fbc-fragments",
        required=True,
        help="FBC fragments as JSON array",
    )
    parser.add_argument(
        "--from-index",
        required=True,
        help="Index image the FBC fragment will be added to",
    )
    parser.add_argument(
        "--build-tags",
        default="[]",
        help="Additional tags for the internal index image copy (JSON array)",
    )
    parser.add_argument(
        "--add-arches",
        default="[]",
        help="Architectures to build the index image for (JSON array)",
    )
    parser.add_argument(
        "--must-publish-index-image",
        default="false",
        help="Whether the index image should be published",
    )
    parser.add_argument(
        "--must-overwrite-from-index-image",
        default="false",
        help="Whether to overwrite the from index image",
    )
    parser.add_argument(
        "--build-timeout-seconds",
        type=int,
        default=3600,
        help="Timeout in seconds for the IIB build",
    )
    return parser.parse_args(argv)


def parse_fbc_fragments(raw: str) -> list[str]:
    """Parse *raw* as a sorted JSON array of non-empty strings."""
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("fbcFragments must be a JSON array")
    if len(data) == 0:
        raise ValueError("fbcFragments array is empty")
    for item in data:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("fbcFragments items must be non-empty strings")
    return sorted(data)


def inspect_image_created(image_ref: str) -> str | None:
    """Return the creation date from ``skopeo inspect --config``.

    Return ``None`` on any failure.
    """
    result = skopeo.inspect(image_ref, config=True)
    if result.returncode != 0:
        return None
    try:
        created = json.loads(result.stdout).get("created")
        if not created or created == "null":
            return None
        return str(created)
    except (json.JSONDecodeError, OSError):
        return None


def is_build_newer_than_index(
    build: iib.IIBBuild,
    from_index: str,
    iib_url: str,
    user: str,
) -> bool:
    """Check whether a completed build's index image is newer than from_index.

    Compare creation dates via skopeo.  Fall back to IIB build history
    when skopeo cannot determine the answer.  Return ``True`` if the
    build is newer.
    """
    index_image_resolved = build.get("index_image_resolved")
    if not index_image_resolved:
        logger.warning("No index_image_resolved in build, skipping reuse")
        return False

    new_catalog_created = inspect_image_created(index_image_resolved)
    new_catalog_ts: int | None = None
    if new_catalog_created:
        try:
            new_catalog_ts = iib.parse_date_to_epoch(new_catalog_created)
        except (ValueError, OSError) as e:
            logger.warning("Could not parse index_image_resolved date: %s", e)

    # Try direct tag inspection of from_index
    from_index_created = inspect_image_created(from_index)

    # Fallback: try from_index_resolved stored in the build
    if not from_index_created:
        resolved = build.get("from_index_resolved")
        if resolved:
            from_index_created = inspect_image_created(resolved)

    if from_index_created and new_catalog_ts is not None:
        try:
            upstream_ts = iib.parse_date_to_epoch(from_index_created)
            if new_catalog_ts < upstream_ts:
                logger.warning("Completed build is older than from_index, skipping reuse")
                return False
            return True
        except (ValueError, OSError) as e:
            logger.warning("Could not parse from_index date: %s", e)

    return _is_build_newer_via_iib(
        build,
        from_index,
        iib_url,
        user,
    )


def _is_build_newer_via_iib(
    build: iib.IIBBuild,
    from_index: str,
    iib_url: str,
    user: str,
) -> bool:
    """Fall-back check using IIB build history.

    If a newer build exists for the same ``from_index``, the candidate is
    stale (another product may have updated the catalog concurrently).
    """
    completed_from_index = build.get("from_index")
    build_updated = build.get("updated")

    if completed_from_index != from_index or not build_updated:
        logger.error(
            "Could not verify from_index via skopeo, and cannot "
            "validate via IIB (missing from_index or updated "
            "timestamp). Skipping reuse for safety."
        )
        return False

    try:
        all_data = iib.query_builds(
            iib_url,
            user=user,
            from_index=from_index,
            state="complete",
        )
    except (
        requests.RequestException,
        OSError,
        json.JSONDecodeError,
    ) as e:
        logger.error(
            "Could not verify from_index via skopeo, and IIB query "
            "failed: %s. Skipping reuse for safety.",
            e,
        )
        return False

    items = all_data.get("items", [])
    if not items:
        logger.error(
            "Could not verify from_index via skopeo, and IIB query "
            "returned empty. Skipping reuse for safety."
        )
        return False

    valid = [
        b
        for b in items
        if b.get("distribution_scope") in ("prod", "stage", None) and b.get("updated")
    ]
    if not valid:
        logger.error("No valid builds in IIB response, skipping reuse")
        return False

    valid.sort(key=lambda b: b.get("updated", ""), reverse=True)
    last_updated = valid[0].get("updated", "")

    if last_updated and last_updated != build_updated:
        build_ts = 0
        last_ts = 0
        try:
            build_ts = iib.parse_date_to_epoch(build_updated)
            last_ts = iib.parse_date_to_epoch(last_updated)
        except (ValueError, OSError) as e:
            logger.warning("Could not parse build timestamps: %s", e)

        if last_ts > build_ts:
            logger.error(
                "A newer build exists for this from_index "
                "(last: %s, candidate: %s). Skipping reuse to "
                "avoid race condition.",
                last_updated,
                build_updated,
            )
            return False

    logger.warning(
        "Could not verify from_index via skopeo, but IIB confirms "
        "no newer builds exist. Proceeding with reuse."
    )
    return True


def check_previous_build(
    iib_url: str,
    user: str,
    from_index: str,
    fbc_fragments: list[str],
    build_tags: list[str],
) -> iib.IIBBuild | None:
    """Check for a reusable completed or in-progress IIB build.

    Search for completed builds first; if a fresh one is found, return it.
    Otherwise search for in-progress builds (optionally filtered by
    *build_tags* for PLR collision prevention).
    """
    sorted_frags = sorted(fbc_fragments)

    try:
        completed = iib.query_builds(
            iib_url,
            user=user,
            from_index=from_index,
            state="complete",
        )
    except (
        requests.RequestException,
        OSError,
        json.JSONDecodeError,
    ) as e:
        logger.warning("Failed to query IIB for completed builds: %s", e)
        return None

    # Find builds whose fbc_fragments match the requested fragments
    # (compared sorted so order is irrelevant) and whose
    # distribution_scope is prod, stage, or unset.  Builds with null
    # fbc_fragments or other scopes (e.g. "dev") are excluded.
    matching = [
        b
        for b in completed.get("items", [])
        if b.get("fbc_fragments") is not None
        and sorted(b["fbc_fragments"]) == sorted_frags
        and b.get("distribution_scope") in ("prod", "stage", None)
    ]
    if matching:
        matching.sort(key=lambda b: b.get("updated", ""))
        candidate = matching[-1]
        if is_build_newer_than_index(
            candidate,
            from_index,
            iib_url,
            user,
        ):
            return candidate

    try:
        in_progress = iib.query_builds(
            iib_url,
            user=user,
            from_index=from_index,
            state="in_progress",
        )
    except (
        requests.RequestException,
        OSError,
        json.JSONDecodeError,
    ) as e:
        logger.warning("Failed to query IIB for in-progress builds: %s", e)
        return None

    candidates = [
        b
        for b in in_progress.get("items", [])
        if b.get("fbc_fragments") is not None
        and sorted(b["fbc_fragments"]) == sorted_frags
        and b.get("distribution_scope") in ("prod", "stage", None)
    ]
    if build_tags:
        candidates = [
            b
            for b in candidates
            if b.get("build_tags") and any(t in b["build_tags"] for t in build_tags)
        ]

    if candidates:
        candidates.sort(key=lambda b: b.get("updated", ""))
        return candidates[-1]

    return None


def poll_build_status(
    iib_url: str,
    build_id: int,
    timeout_seconds: int,
    iib_log_path: Path | None = None,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> iib.IIBBuild:
    """Poll IIB build status until a terminal state or timeout.

    Poll every 30 s, log a status line every 5 min.  Raise
    ``TimeoutError`` when *timeout_seconds* elapses before the build
    reaches a terminal state.
    """
    logger.info("Monitoring IIB build %d", build_id)
    logger.info("IIB service URL: %s", iib_url)
    start = clock_fn()
    last_log = start
    poll_count = 0

    while True:
        elapsed = clock_fn() - start
        if elapsed >= timeout_seconds:
            raise TimeoutError(
                f"Timeout after {timeout_seconds}s waiting for build {build_id}"
            )

        now = clock_fn()

        try:
            build_info = iib.get_build(iib_url, build_id)
        except (
            requests.RequestException,
            OSError,
            json.JSONDecodeError,
        ) as e:
            logger.warning("Failed to fetch build info: %s", e)
            poll_count += 1
            sleep_fn(POLL_INTERVAL_SECONDS)
            continue

        if build_info.get("error") is not None:
            logger.warning(
                "IIB service returned error: %s",
                build_info.get("error"),
            )
            sleep_fn(POLL_INTERVAL_SECONDS)
            continue

        state = build_info.get("state", "")
        poll_count += 1

        if now - last_log >= LOG_INTERVAL_SECONDS:
            logger.info(
                "Build status check #%d (elapsed: %.0fs): "
                "state=%s, reason=%s, created=%s, updated=%s",
                poll_count,
                elapsed,
                state,
                build_info.get("state_reason", "none"),
                build_info.get("created", ""),
                build_info.get("updated", ""),
            )
            last_log = now

        build_info.pop("state_history", None)  # type: ignore[misc]

        log_url = iib.extract_log_url(build_info)
        if iib_log_path and log_url:
            try:
                iib_log_path.write_text(f"IIB log url is: {log_url}", encoding="utf-8")
            except OSError as e:
                logger.warning("Failed to write IIB log URL: %s", e)

        if state in ("complete", "failed"):
            return build_info

        sleep_fn(POLL_INTERVAL_SECONDS)


def validate_index_image(
    build_info: iib.IIBBuild,
    must_overwrite: bool,
    must_publish: bool,
) -> None:
    """Validate the index image based on the release strategy.

    Raise ``ValueError`` on invalid strategy combinations or when the
    expected image field is missing.
    """
    if must_overwrite and must_publish:
        index_image = build_info.get("index_image", "")
        expected = build_info.get("from_index", "")
        if index_image != expected:
            raise ValueError(
                f"Index image mismatch: expected {expected}, got "
                f"{index_image}. The from index was not properly "
                "overwritten."
            )
    elif must_overwrite and not must_publish:
        raise ValueError(
            "Invalid combination: mustOverwriteFromIndexImage=true "
            "and mustPublishIndexImage=false. This could be caused "
            "by multiple pipelines releasing to production running "
            "in parallel."
        )


def get_manifest_digests(image_ref: str) -> str:
    """Return space-separated v2 manifest digests from a multi-arch image.

    Raise ``RuntimeError`` if the image is not a manifest list.
    """
    result = skopeo.inspect(image_ref, raw=True)
    if result.returncode != 0:
        raise RuntimeError(f"skopeo inspect --raw failed for {image_ref}: {result.stderr}")
    raw = json.loads(result.stdout)
    media = "application/vnd.docker.distribution.manifest.v2+json"
    digests = [m["digest"] for m in raw.get("manifests", []) if m.get("mediaType") == media]
    if not digests:
        raise RuntimeError("Index image produced is not multi-arch with a manifest list")
    return " ".join(digests)


def run(
    input: FBCCatalogInput,
    service_account_mount: Path,
    iib_config_mount: Path,
    overwrite_creds_mount: Path,
    publishing_creds_mount: Path,
    build_state_path: Path | None = None,
    iib_log_path: Path | None = None,
) -> RunResult:
    """Execute the full update-fbc-catalog flow.

    Authenticate, check for previous builds, submit if needed, poll for
    completion, validate, and retrieve manifest digests.
    """
    try:
        iib_url = authentication.read_mounted_text(iib_config_mount, "url")
        krb5_source = (iib_config_mount / "krb5.conf").read_text(
            encoding="utf-8", errors="replace"
        )

        overwrite_username = authentication.read_mounted_text(
            overwrite_creds_mount, "username"
        )
        overwrite_token = authentication.read_mounted_text(overwrite_creds_mount, "token")
    except OSError as e:
        raise tekton.CheckStepError("reading mounted secrets", e) from e

    pub_cred_path = publishing_creds_mount / "targetIndexCredential"
    publishing_credential = ""
    if pub_cred_path.is_file() and pub_cred_path.stat().st_size > 0:
        publishing_credential = pub_cred_path.read_text(encoding="utf-8").strip()

    try:
        principal, keytab_bytes = authentication.load_keytab_from_mount(
            service_account_mount,
            principal_file="principal",
            keytab_b64_file="keytab",
        )
    except (OSError, ValueError, binascii.Error) as e:
        raise tekton.CheckStepError("reading the mounted IIB service account", e) from e

    try:
        with authentication.kerberos_login(
            principal,
            keytab_bytes,
            krb5_source,
        ):
            return _authenticated_run(
                input,
                iib_url,
                principal,
                publishing_credential,
                overwrite_username,
                overwrite_token,
                build_state_path=build_state_path,
                iib_log_path=iib_log_path,
            )
    except subprocess.CalledProcessError as e:
        raise tekton.CheckStepError("logging in with Kerberos (kinit)", e) from e


def _authenticated_run(
    input: FBCCatalogInput,
    iib_url: str,
    principal: str,
    publishing_credential: str,
    overwrite_username: str,
    overwrite_token: str,
    build_state_path: Path | None = None,
    iib_log_path: Path | None = None,
) -> RunResult:
    """Run the IIB flow after Kerberos auth is established."""
    authentication.create_container_auth_config(input.from_index, publishing_credential)

    logger.info("Processing fragments: %s", json.dumps(input.fbc_fragments))
    logger.info(
        "Publishing decisions: mustOverwrite=%s, mustPublish=%s",
        input.must_overwrite,
        input.must_publish,
    )

    previous = check_previous_build(
        iib_url,
        principal,
        input.from_index,
        input.fbc_fragments,
        input.build_tags,
    )

    if previous:
        logger.info("=== A previous build for this fragment was found ===")
        build_info = previous
    else:
        payload: iib.FBCOperationPayload = {
            "fbc_fragments": input.fbc_fragments,
            "from_index": input.from_index,
        }
        if input.must_overwrite:
            payload["overwrite_from_index"] = True
            payload["overwrite_from_index_token"] = f"{overwrite_username}:{overwrite_token}"
        if input.build_tags:
            payload["build_tags"] = input.build_tags
        if input.add_arches:
            payload["add_arches"] = input.add_arches

        if build_state_path:
            build_state_path.write_text(
                json.dumps(
                    {
                        "state": "in_progress",
                        "state_reason": "Calling IIB endpoint",
                    }
                ),
                encoding="utf-8",
            )

        auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        try:
            build_info = iib.submit_fbc_operation(
                iib_url,
                payload,
                auth=auth,
            )
        except (requests.RequestException, ValueError) as e:
            raise tekton.CheckStepError("submitting IIB build", e) from e

    return _poll_and_collect(
        iib_url,
        build_info,
        input.build_timeout_seconds,
        input.must_overwrite,
        input.must_publish,
        iib_log_path=iib_log_path,
    )


def _poll_and_collect(
    iib_url: str,
    build_info: iib.IIBBuild,
    timeout_seconds: int,
    must_overwrite: bool,
    must_publish: bool,
    iib_log_path: Path | None = None,
) -> RunResult:
    """Poll for completion, validate, and collect manifest digests."""
    build_id = build_info.get("id")
    if not build_id:
        raise ValueError("Build response missing 'id' field")

    build_info.pop("state_history", None)  # type: ignore[misc]

    state = build_info.get("state", "")
    if state not in ("complete", "failed"):
        try:
            build_info = poll_build_status(
                iib_url,
                build_id,
                timeout_seconds,
                iib_log_path=iib_log_path,
            )
        except TimeoutError:
            return RunResult(
                build_info=build_info,
                state="failed",
                state_reason="Build timeout",
                index_image_digests="",
                iib_log_url=iib.extract_log_url(build_info),
                exit_code=124,
            )
        state = build_info.get("state", "")
    else:
        try:
            build_info = iib.get_build(iib_url, build_id)
            build_info.pop("state_history", None)  # type: ignore[misc]
        except (requests.RequestException, OSError, json.JSONDecodeError) as e:
            logger.warning("Failed to fetch full build details: %s", e)

    log_url = iib.extract_log_url(build_info)
    state_reason = build_info.get("state_reason", "")

    if state != "complete":
        return RunResult(
            build_info=build_info,
            state=state,
            state_reason=state_reason or "Build failed with exit code 1",
            index_image_digests="",
            iib_log_url=log_url,
            exit_code=1,
        )

    try:
        validate_index_image(build_info, must_overwrite, must_publish)
    except ValueError as e:
        return RunResult(
            build_info=build_info,
            state="failed",
            state_reason=str(e),
            index_image_digests="",
            iib_log_url=log_url,
            exit_code=1,
        )

    internal_copy = build_info.get("internal_index_image_copy", "")
    if not internal_copy:
        return RunResult(
            build_info=build_info,
            state="failed",
            state_reason="Missing internal_index_image_copy",
            index_image_digests="",
            iib_log_url=log_url,
            exit_code=1,
        )

    try:
        digests = get_manifest_digests(internal_copy)
    except (RuntimeError, json.JSONDecodeError) as e:
        return RunResult(
            build_info=build_info,
            state="failed",
            state_reason=f"Failed to get manifest digests: {e}",
            index_image_digests="",
            iib_log_url=log_url,
            exit_code=1,
        )

    return RunResult(
        build_info=build_info,
        state=state,
        state_reason=state_reason,
        index_image_digests=digests,
        iib_log_url=log_url,
        exit_code=0,
    )


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, run the full flow, and write Tekton result files."""
    args = parse_args(argv)

    (
        json_build_info_path,
        build_state_path,
        index_image_digests_path,
        iib_log_path,
        exit_code_path,
    ) = tekton.result_paths_from_env(
        "RESULT_JSON_BUILD_INFO",
        "RESULT_BUILD_STATE",
        "RESULT_INDEX_IMAGE_DIGESTS",
        "RESULT_IIB_LOG",
        "RESULT_EXIT_CODE",
    )

    if not args.from_index.strip():
        _write_failure(
            build_state_path,
            exit_code_path,
            "from-index is required",
            json_build_info_path,
            index_image_digests_path,
            iib_log_path,
        )
        raise SystemExit(f"{PROG}: from-index is required")

    try:
        fbc_fragments = parse_fbc_fragments(args.fbc_fragments)
    except (json.JSONDecodeError, ValueError) as e:
        _write_failure(
            build_state_path,
            exit_code_path,
            str(e),
            json_build_info_path,
            index_image_digests_path,
            iib_log_path,
        )
        raise SystemExit(f"{PROG}: {e}") from e

    try:
        build_tags: list[str] = json.loads(args.build_tags)
        add_arches: list[str] = json.loads(args.add_arches)
    except json.JSONDecodeError as e:
        _write_failure(
            build_state_path,
            exit_code_path,
            str(e),
            json_build_info_path,
            index_image_digests_path,
            iib_log_path,
        )
        raise SystemExit(f"{PROG}: Invalid JSON: {e}") from e

    input = FBCCatalogInput(
        fbc_fragments=fbc_fragments,
        from_index=args.from_index,
        build_tags=build_tags,
        add_arches=add_arches,
        must_overwrite=(args.must_overwrite_from_index_image.lower() == "true"),
        must_publish=args.must_publish_index_image.lower() == "true",
        build_timeout_seconds=args.build_timeout_seconds,
    )

    sa_mount = file.path_from_env_variable(
        "IIB_SERVICE_ACCOUNT_MOUNT", "/mnt/service-account-secret"
    )
    iib_cfg_mount = file.path_from_env_variable(
        "IIB_SERVICES_CONFIG_MOUNT", "/mnt/iib-services-config"
    )
    overwrite_mount = file.path_from_env_variable(
        "IIB_OVERWRITE_FROMIMAGE_CREDENTIALS_MOUNT",
        "/mnt/iib-overwrite-fromimage-credentials",
    )
    pub_mount = file.path_from_env_variable(
        "PUBLISHING_CREDENTIALS_MOUNT", "/mnt/publishing-credentials"
    )

    try:
        result = run(
            input,
            sa_mount,
            iib_cfg_mount,
            overwrite_mount,
            pub_mount,
            build_state_path=build_state_path,
            iib_log_path=iib_log_path,
        )
    except tekton.CheckStepError as e:
        _write_failure(
            build_state_path,
            exit_code_path,
            str(e),
            json_build_info_path,
            index_image_digests_path,
            iib_log_path,
        )
        raise SystemExit(f"{PROG}: {e}") from e

    json_build_info_path.write_text(
        iib.compress_build_info(result.build_info), encoding="utf-8"
    )
    build_state_path.write_text(
        json.dumps(
            {
                "state": result.state,
                "state_reason": result.state_reason,
            }
        ),
        encoding="utf-8",
    )
    index_image_digests_path.write_text(result.index_image_digests, encoding="utf-8")
    iib_log_path.write_text(
        f"IIB log url is: {result.iib_log_url}" if result.iib_log_url else "",
        encoding="utf-8",
    )
    exit_code_path.write_text(str(result.exit_code), encoding="utf-8")

    if result.iib_log_url:
        try:
            log_content = http_client.get_text(result.iib_log_url)
            logger.info("IIB build log:\n%s", log_content)
        except (requests.RequestException, OSError):
            logger.warning("Could not fetch IIB log from %s", result.iib_log_url)

    return result.exit_code


def _write_failure(
    build_state_path: Path,
    exit_code_path: Path,
    reason: str,
    json_build_info_path: Path,
    index_image_digests_path: Path,
    iib_log_path: Path,
) -> None:
    """Write failure state to Tekton result files.

    All declared Tekton result files must exist by the time the Task
    finishes or the TaskRun fails. Results that have no data on early
    failure paths are written empty.
    """
    build_state_path.write_text(
        json.dumps({"state": "failed", "state_reason": reason}),
        encoding="utf-8",
    )
    exit_code_path.write_text("1", encoding="utf-8")
    json_build_info_path.write_text("", encoding="utf-8")
    index_image_digests_path.write_text("", encoding="utf-8")
    iib_log_path.write_text("", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
