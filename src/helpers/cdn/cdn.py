"""Provide per-environment config and shared CDN delivery helpers.

Covers the systems involved in pushing released artifacts to customers:
Exodus GW, Pulp, UDCache, and Content Gateway.
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
from pathlib import Path

log = logging.getLogger("cdn")

# The Exodus GW secret is the same for production and stage — only the env
# (live vs pre) and the Pulp URL differ. The stage CGW uses the 'qa' host:
# developers.qa.redhat.com.
_CDN_ENV_SECRETS: dict[str, dict[str, str]] = {
    "production": {
        "exodusGwSecret": "exodus-prod-secret",
        "exodusGwEnv": "live",
        "pulpSecret": "rhsm-pulp-prod-secret",
        "udcacheSecret": "udcache-prod-secret",
        "cgwHostname": "https://developers.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-prod-secret",
    },
    "stage": {
        "exodusGwSecret": "exodus-prod-secret",
        "exodusGwEnv": "pre",
        "pulpSecret": "rhsm-pulp-stage-secret",
        "udcacheSecret": "udcache-stage-secret",
        "cgwHostname": "https://developers.qa.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-stage-secret",
    },
    "qa": {
        "exodusGwSecret": "exodus-stage-secret",
        "exodusGwEnv": "live",
        "pulpSecret": "rhsm-pulp-qa-secret",
        "udcacheSecret": "udcache-qa-secret",
        "cgwHostname": "https://developers.qa.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-stage-secret",
    },
}


def cdn_env_secrets(env: str) -> dict[str, str]:
    """Return Exodus/Pulp/UDCache/Content-Gateway secret and hostname config for *env*.

    Raise ``ValueError`` when *env* is not one of production, stage, or qa.
    """
    config = _CDN_ENV_SECRETS.get(env)
    if config is None:
        msg = f"cdn.env in the data file must be one of [production, stage, qa], got {env!r}"
        raise ValueError(msg)
    return dict(config)


def create_exodus_conf(
    conf_path: Path,
    gw_cert: Path,
    gw_key: Path,
    gw_url: str,
    gw_env: str,
) -> None:
    """Write an exodus-rsync configuration file to *conf_path*."""
    conf_path.write_text(
        f"gwcert:   {gw_cert}\n"
        f"gwkey:    {gw_key}\n"
        f"gwurl:    {gw_url}\n"
        f"gwenv:    {gw_env}\n"
        "\n"
        "logger: file:/proc/1/fd/1\n"
        "loglevel: info\n"
        "\n"
        "environments:\n"
        "  - prefix: exodus\n"
    )


def push_files_to_cdn(
    content_dir: Path,
    exodus_conf_path: Path,
    exclude: set[str] | None = None,
) -> None:
    """Push every file under *content_dir* to CDN via exodus-rsync.

    Each file is addressed by its SHA-256 checksum on the content-addressable
    CDN path ``exodus:/content/origin/files/sha256/{sha[:2]}/{sha}/{name}``.
    Files whose names appear in *exclude* are skipped.
    """
    prefix = "exodus:/content/origin/files/sha256"
    exclude = exclude or set()

    for f in sorted(content_dir.rglob("*")):
        if not f.is_file():
            continue
        if f.name in exclude:
            log.info("  Skipping %s (excluded)", f.name)
            continue
        h = hashlib.sha256()
        with open(f, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        checksum = h.hexdigest()
        destination = f"{prefix}/{checksum[:2]}/{checksum}/{f.name}"
        subprocess.check_call(
            ["rsync", "--exodus-conf", str(exodus_conf_path), str(f), destination]
        )
