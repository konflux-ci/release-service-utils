#!/usr/bin/env python3
"""Check FBC opt-in status in Pyxis for container images."""

from __future__ import annotations

import base64
import binascii
import json
import os
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests
from requests.auth import AuthBase
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

import authentication
import file
import http_client
import image_ref
import tekton
from logger import logger

PROG = "check_fbc_opt_in.py"


def parse_container_images(value: str) -> list[str]:
    """Parse `value` as a JSON array of non-empty image strings."""
    # Input is a JSON array string; keep validation strict so malformed input fails fast.
    data = json.loads(value)
    if not isinstance(data, list):
        raise ValueError("container images must be a JSON array")
    out: list[str] = []
    for item in data:
        if not isinstance(item, str):
            raise ValueError("container images must be strings")
        stripped = item.strip()
        if not stripped:
            raise ValueError("container images must be non-empty")
        out.append(stripped)
    return out


def get_fbc_opt_in(
    pyxis_url: str,
    pull_spec: str,
    auth: AuthBase | None,
    *,
    warn_on_query_failure: bool = True,
) -> bool:
    """
    Return `True` only when the Pyxis JSON body sets `fbc_opt_in` to JSON boolean
    true.

    Query failures, non-JSON responses, and missing fields are treated as
    opt-out (`False`).
    """
    try:
        # Build the Pyxis endpoint for this image pull spec.
        body = http_client.get_text(
            image_ref.pyxis_url_for_pull_spec(pyxis_url, pull_spec),
            auth=auth,
        )
        data: dict[str, Any] = json.loads(body)
    except (
        OSError,
        ValueError,
        TypeError,
        json.JSONDecodeError,
        requests.RequestException,
    ):
        # Fail closed: treat query/parsing errors as not opted in.
        if warn_on_query_failure:
            logger.warning(
                f"Failed to query Pyxis for {pull_spec}, assuming opt-out",
            )
        return False
    return data.get("fbc_opt_in") is True


def run_check(
    container_images: list[str],
    pyxis_url: str,
    service_account_mount: Path,
    iib_services_config_mount: Path,
    *,
    kinit: Callable[..., None] = authentication.kinit_with_retry,
    get_opt_in: Callable[[str, str, AuthBase | None], bool] = get_fbc_opt_in,
) -> list[dict[str, Any]]:
    """
    Authenticate with Kerberos then query Pyxis FBC opt-in for each image.

    Returns one result object per input image:
    `{"containerImage": "...", "fbcOptIn": bool}`.
    """
    logger.info("Setting up Kerberos authentication for Pyxis...")
    # Read mounted principal/keytab from the service-account secret volume.
    try:
        principal = authentication.read_mounted_text(service_account_mount, "principal")
        keytab_b64 = authentication.read_mounted_text(service_account_mount, "keytab")
        keytab_bytes = base64.b64decode(keytab_b64.encode("ascii"))
    except (OSError, ValueError, binascii.Error) as e:
        raise tekton.CheckStepError("reading the mounted IIB service account", e) from e

    # Read krb5.conf from the iib-services-config mount.
    try:
        krb5_source = (iib_services_config_mount / "krb5.conf").read_text(
            encoding="utf-8", errors="replace"
        )
    except OSError as e:
        raise tekton.CheckStepError("reading the Kerberos configuration", e) from e

    # Create ephemeral files for keytab and kerberos configuration.
    keytab_path = file.make_tempfile_path("keytab-", keytab_bytes)
    krb5_path = file.make_tempfile_path("krb5-", krb5_source.encode("utf-8"))
    # Use a private credentials cache file per run instead of default user cache path.
    ccache_fd, ccache_name = tempfile.mkstemp()
    os.close(ccache_fd)
    ccache_path = Path(ccache_name)

    try:
        # Kerberos env for config path, ccache path, and optional trace output.
        kenv = {
            "KRB5_CONFIG": str(krb5_path),
            "KRB5CCNAME": str(ccache_path),
            "KRB5_TRACE": "/dev/stderr",
        }
        try:
            # Retry kinit with exponential backoff from the shared retry helper.
            logger.info("Logging in with Kerberos (kinit)...")
            kinit(principal, keytab_path, kenv, max_attempts=5)
        except subprocess.CalledProcessError as e:
            raise tekton.CheckStepError("logging in with Kerberos (kinit)", e) from e
        # requests-kerberos reads Kerberos env from this process, not the kinit child.
        os.environ.update(kenv)

        # Reuse one Kerberos auth object while querying each image's Pyxis record.
        auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        logger.info("Checking FBC opt-in status for provided container images...")
        opt_in_rows: list[dict[str, Any]] = []
        for image in container_images:
            logger.info("Checking opt-in status for: %s with pyxis url: %s", image, pyxis_url)
            opted_in = get_opt_in(pyxis_url, image, auth)
            logger.info("Container %s opt-in status: %s", image, str(opted_in).lower())
            opt_in_rows.append(
                {
                    "containerImage": image,
                    "fbcOptIn": opted_in,
                }
            )
        return opt_in_rows
    finally:
        # Always clean up secret-bearing temp files.
        for p in (keytab_path, krb5_path, ccache_path):
            p.unlink(missing_ok=True)


def main() -> int:
    """Write `RESULT_OPT_IN_RESULTS` and return exit code `0` on success."""
    # `RESULT_OPT_IN_RESULTS` is the path of the JSON file this step writes.
    rpath = tekton.result_paths("RESULT_OPT_IN_RESULTS")[0]
    raw_images = os.environ.get("CONTAINER_IMAGES", "")

    try:
        pyxis_url = os.environ.get("PYXIS_URL", "").strip()
        if not pyxis_url:
            raise ValueError("PYXIS_URL must be set")
        # Mount paths are overridable for tests but fixed in task runtime.
        service_account_mount = file.path_from_env_variable(
            "IIB_SERVICE_ACCOUNT_MOUNT", "/mnt/service-account-secret"
        )
        iib_services_config_mount = file.path_from_env_variable(
            "IIB_SERVICES_CONFIG_MOUNT", "/mnt/iib-services-config"
        )

        images = parse_container_images(raw_images)
        results = run_check(
            images,
            pyxis_url,
            service_account_mount,
            iib_services_config_mount,
        )
    except (ValueError, tekton.CheckStepError) as e:
        # Surface validation and step failures as a clear one-line SystemExit message.
        raise SystemExit(f"{PROG}: {e}") from e

    logger.info("FBC opt-in check completed")
    logger.info("Results:")
    logger.info(json.dumps(results, indent=2))

    # Downstream tasks consume a JSON array from this result file.
    rpath.write_text(json.dumps(results), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
