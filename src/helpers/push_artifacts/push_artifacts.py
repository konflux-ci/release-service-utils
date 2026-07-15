#!/usr/bin/env python3
"""Push release artifacts to the Customer Portal (Pulp), CDN (exodus-rsync), and/or CGW.

Routing logic per component:
* If component has ``staged`` data → push to Customer Portal via ``pulp_push_wrapper``.
* If component has only ``contentGateway`` data (no ``staged``) → push to CDN via
  ``rsync`` with exodus-rsync.
* If component has ``contentGateway`` data (with or without ``staged``) → publish to
  Content Gateway via ``publish_to_cgw_wrapper``.

Also validates that the Exodus Gateway, Pulp, and UDCache TLS certificates are not
close to expiry before proceeding.

Writes the list of published filenames to the path specified by ``RESULT_PUBLISHED_FILES``.

CLI arguments:
  ``--exodus-gw-env``
  ``--cgw-hostname``
  ``--cert-expiration-warn-days``

Output env var:
  ``RESULT_PUBLISHED_FILES``  – file path to write the published-files list to

Secret mounts:
  ``EXODUS_GW_SECRET_MOUNT``  (default: ``/mnt/exodusGwSecret``)
  ``PULP_SECRET_MOUNT``       (default: ``/mnt/pulpSecret``)
  ``UDCACHE_SECRET_MOUNT``    (default: ``/mnt/udcacheSecret``)
  ``CGW_SECRET_MOUNT``        (default: ``/mnt/cgwSecret``)

Other env vars:
  ``SNAPSHOT_JSON``      – JSON string of the Snapshot spec
  ``REQUESTS_CA_BUNDLE`` – set by task to use system CA bundle
  ``CONTENT_DIR``        – override base directory (default: ``/shared/artifacts``)
  ``SHARED_DIR``         – override shared volume root (default: ``/shared``)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import yaml  # type: ignore

from release_service_utils.helpers import disk_image_utils
import publish_to_cgw_wrapper
import pulp_push_wrapper

PROG = "push_artifacts.py"

EXODUS_GW_SECRET_MOUNT = Path(os.environ.get("EXODUS_GW_SECRET_MOUNT", "/mnt/exodusGwSecret"))
PULP_SECRET_MOUNT = Path(os.environ.get("PULP_SECRET_MOUNT", "/mnt/pulpSecret"))
UDCACHE_SECRET_MOUNT = Path(os.environ.get("UDCACHE_SECRET_MOUNT", "/mnt/udcacheSecret"))
CGW_SECRET_MOUNT = Path(os.environ.get("CGW_SECRET_MOUNT", "/mnt/cgwSecret"))
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))
SHARED_DIR = Path(os.environ.get("SHARED_DIR", "/shared"))

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument(
        "--exodus-gw-env", required=True, help="Exodus Gateway environment [live|pre]"
    )
    p.add_argument("--cgw-hostname", required=True, help="Content Gateway hostname")
    p.add_argument(
        "--cert-expiration-warn-days",
        type=int,
        default=7,
        help="Days before cert expiry to warn",
    )
    return p.parse_args(argv)


def _check_cert_expiration(cert_path: str, warn_days: int) -> None:
    """Run check_cert_expiration utility; raise on failure."""
    result = subprocess.run(
        ["check_cert_expiration", cert_path, str(warn_days)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Certificate validation failed for {cert_path}: {result.stdout}{result.stderr}"
        )


def _write_cert_files(
    exodus_cert: str,
    exodus_key: str,
    pulp_cert: str,
    pulp_key: str,
    udc_cert: str,
    udc_key: str,
) -> tuple[Path, Path, Path, Path, Path, Path]:
    """Write certificate and key material to /tmp files; return the six resulting paths."""

    def _write(name: str, content: str) -> Path:
        """Write content to /tmp/<name> with restricted permissions and return the path."""
        p = Path(f"/tmp/{name}")
        p.write_text(content.strip() + "\n")
        p.chmod(0o600)
        return p

    return (
        _write("exodus.crt", exodus_cert),
        _write("exodus.key", exodus_key),
        _write("pulp.crt", pulp_cert),
        _write("pulp.key", pulp_key),
        _write("udc.crt", udc_cert),
        _write("udc.key", udc_key),
    )


def _create_exodus_conf(
    conf_path: Path,
    gw_cert: Path,
    gw_key: Path,
    gw_url: str,
    gw_env: str,
) -> None:
    """Write an exodus-rsync configuration file to conf_path."""
    logger.info("Creating Exodus configuration file.....")
    conf_content = (
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
    conf_path.write_text(conf_content)


def _push_component_to_pulp(
    component_name: str,
    snapshot: dict,
    pulp_url: str,
    pulp_cert: Path,
    pulp_key: Path,
    udc_url: str,
) -> None:
    """Stage component files and invoke pulp_push_wrapper to push to the Customer Portal."""
    component = next(
        (c for c in snapshot.get("components", []) if c.get("name") == component_name), {}
    )
    staged = component.get("staged") or {}
    version = staged.get("version", "")
    staged_destination = staged.get("destination", "")

    if not version or not staged_destination:
        raise RuntimeError(
            f"Pulp push requires both staged.version and staged.destination "
            f"for component {component_name}"
        )

    logger.info("Pushing component %s to customer portal with pulp", component_name)

    component_dir = CONTENT_DIR / component_name / "ready_for_distribution"
    staging_dir = Path(tempfile.mkdtemp())
    try:
        dest_files_dir = staging_dir / staged_destination / "FILES"
        dest_files_dir.mkdir(parents=True)

        staged_files = (component.get("staged") or {}).get("files") or []
        if not staged_files:
            logger.warning(
                "No staged.files specified in RPA for Customer Portal push for %s",
                component_name,
            )
            return

        for sf in staged_files:
            source_path = sf.get("source", "")
            dest_filename = sf.get("filename", "")
            if not source_path:
                continue

            source_filename = Path(source_path).name
            # Handle windows .tar.gz/.tar → .zip conversion
            if "windows" in source_filename:
                for old_ext, new_ext in [(".tar.gz", ".zip"), (".tar", ".zip")]:
                    if source_filename.endswith(old_ext):
                        candidate = source_filename[: -len(old_ext)] + new_ext
                        if (component_dir / candidate).exists():
                            source_filename = candidate
                            if dest_filename.endswith(old_ext):
                                dest_filename = dest_filename[: -len(old_ext)] + new_ext
                        break

            if not dest_filename:
                dest_filename = source_filename

            src = component_dir / source_filename
            if src.is_file():
                shutil.copy2(str(src), str(dest_files_dir / dest_filename))
                logger.info(
                    "  Including file for Customer Portal: %s -> %s",
                    source_filename,
                    dest_filename,
                )
            else:
                logger.warning("  File not found: %s", source_filename)

        staged_json: dict = {"header": {"version": "0.2"}, "payload": {"files": []}}
        for f in sorted(staging_dir.rglob("*")):
            if f.is_file():
                staged_json["payload"]["files"].append(
                    {
                        "filename": f.name,
                        "relative_path": str(f.relative_to(staging_dir)),
                        "version": version,
                    }
                )

        staged_yaml_path = staging_dir / "staged.yaml"
        staged_yaml_path.write_text(yaml.dump(staged_json, indent=4, default_flow_style=False))

        pulp_push_wrapper.main(
            [
                "--source",
                str(staging_dir),
                "--pulp-url",
                pulp_url,
                "--pulp-cert",
                str(pulp_cert),
                "--pulp-key",
                str(pulp_key),
                "--udcache-url",
                udc_url,
            ]
        )
    finally:
        shutil.rmtree(str(staging_dir), ignore_errors=True)


def _push_component_to_cdn(
    component_name: str, exodus_conf_path: Path, exclude: set[str] | None = None
) -> None:
    """Push files in a component's ready_for_distribution dir to CDN via exodus-rsync.

    Files whose names appear in ``exclude`` are skipped. This is used when a component
    publishes to both Pulp and CGW: staged files are already pushed to CDN by Pulp, so
    only CGW files and the shared checksum files need to be exodus-rsync'd.
    """
    component_dir = CONTENT_DIR / component_name / "ready_for_distribution"
    exclude = exclude or set()
    logger.info("Pushing component %s to CDN with exodus-rsync", component_name)
    prefix = "exodus:/content/origin/files/sha256"

    for file in sorted(component_dir.rglob("*")):
        if not file.is_file():
            continue
        if file.name in exclude:
            logger.info("  Skipping %s (owned by Pulp on CDN)", file.name)
            continue
        h = hashlib.sha256()
        with open(file, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        checksum = h.hexdigest()
        destination_path = f"{prefix}/{checksum[:2]}/{checksum}/{file.name}"
        subprocess.check_call(
            ["rsync", "--exodus-conf", str(exodus_conf_path), str(file), destination_path]
        )


def run(exodus_gw_env: str, cgw_hostname: str, cert_expiration_warn_days: int) -> None:
    """Validate certificates and push artifacts to Pulp/CDN/CGW for each component."""
    logger.info("=== Checking certificate expiration ===")
    logger.info("Checking Exodus Gateway certificate")
    _check_cert_expiration(str(EXODUS_GW_SECRET_MOUNT / "cert"), cert_expiration_warn_days)
    logger.info("Checking Pulp certificate")
    _check_cert_expiration(
        str(PULP_SECRET_MOUNT / "konflux-release-rhsm-pulp.crt"), cert_expiration_warn_days
    )
    logger.info("Checking UDCache certificate")
    _check_cert_expiration(str(UDCACHE_SECRET_MOUNT / "cert"), cert_expiration_warn_days)
    logger.info("=== All certificates are valid ===")

    shared_snapshot = SHARED_DIR / "snapshot.json"
    if shared_snapshot.exists():
        snapshot = json.loads(shared_snapshot.read_text())
    else:
        snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    exodus_cert = (EXODUS_GW_SECRET_MOUNT / "cert").read_text()
    exodus_key = (EXODUS_GW_SECRET_MOUNT / "key").read_text()
    exodus_url = (EXODUS_GW_SECRET_MOUNT / "url").read_text().strip()
    pulp_url = (PULP_SECRET_MOUNT / "pulp_url").read_text().strip()
    pulp_cert_raw = (PULP_SECRET_MOUNT / "konflux-release-rhsm-pulp.crt").read_text()
    pulp_key_raw = (PULP_SECRET_MOUNT / "konflux-release-rhsm-pulp.key").read_text()
    udc_url = (UDCACHE_SECRET_MOUNT / "url").read_text().strip()
    udc_cert_raw = (UDCACHE_SECRET_MOUNT / "cert").read_text()
    udc_key_raw = (UDCACHE_SECRET_MOUNT / "key").read_text()
    cgw_username = (CGW_SECRET_MOUNT / "username").read_text().strip()
    cgw_password = (CGW_SECRET_MOUNT / "token").read_text().strip()

    os.environ["CGW_USERNAME"] = cgw_username
    os.environ["CGW_PASSWORD"] = cgw_password

    exodus_gw_cert, exodus_gw_key, pulp_cert, pulp_key, udc_cert, udc_key = _write_cert_files(
        exodus_cert, exodus_key, pulp_cert_raw, pulp_key_raw, udc_cert_raw, udc_key_raw
    )
    exodus_conf_path = Path("/tmp/exodus-rsync.conf")

    os.environ["EXODUS_GW_CERT"] = str(exodus_gw_cert)
    os.environ["EXODUS_GW_KEY"] = str(exodus_gw_key)
    os.environ["PULP_CERT_FILE"] = str(pulp_cert)
    os.environ["PULP_KEY_FILE"] = str(pulp_key)
    os.environ["UDCACHE_CERT"] = str(udc_cert)
    os.environ["UDCACHE_KEY"] = str(udc_key)
    os.environ["EXODUS_GW_ENV"] = exodus_gw_env
    os.environ["EXODUS_GW_URL"] = exodus_url
    os.environ["EXODUS_PULP_HOOK_ENABLED"] = "True"
    os.environ["EXODUS_GW_TIMEOUT"] = "7200"

    result_published_path = Path(os.environ["RESULT_PUBLISHED_FILES"])
    published_files = []
    for ready_dir in sorted(CONTENT_DIR.glob("*/ready_for_distribution")):
        for f in sorted(ready_dir.iterdir()):
            if f.is_file():
                published_files.append(f.name)

    result_published_path.write_text("\n".join(published_files), encoding="utf-8")

    for component in snapshot.get("components", []):
        name = component.get("name", "")
        has_staged = bool(component.get("staged"))
        has_cgw = bool(component.get("contentGateway"))

        logger.info("Processing component: %s (staged=%s, cgw=%s)", name, has_staged, has_cgw)

        if has_staged:
            _push_component_to_pulp(name, snapshot, pulp_url, pulp_cert, pulp_key, udc_url)

        if has_cgw:
            _create_exodus_conf(
                exodus_conf_path, exodus_gw_cert, exodus_gw_key, exodus_url, exodus_gw_env
            )
            # When a component has both staged and CGW content, exclude staged filenames
            # from the CDN push — Pulp owns those files on CDN. CGW-only files (files[])
            # and the shared checksum files must still be exodus-rsync'd so the CGW
            # downloadURLs resolve on the CDN origin.
            staged_filenames: set[str] = set()
            if has_staged:
                for sf in (component.get("staged") or {}).get("files") or []:
                    src = sf.get("source", "")
                    if src:
                        staged_filenames.add(Path(src).name)
            _push_component_to_cdn(name, exodus_conf_path, exclude=staged_filenames)

        if has_cgw:
            component_dir = CONTENT_DIR / name / "ready_for_distribution"
            cg = component.get("contentGateway") or {}
            cg["contentDir"] = str(component_dir)
            component["contentGateway"] = cg
            # Disk-image components that target both CDN and CGW describe their
            # deliverables in staged.files[] (consumed by the CDN/Customer Portal
            # flow) but also need those files listed in files[] for CGW registration.
            # If files[] is already populated the team provided it directly (e.g. a
            # CGW-only release), so we leave it untouched.
            # NOTE: this intentionally mutates the component dict in-place. It is safe
            # because the Pulp push and CDN exclusion logic for this component have already
            # completed above, and the only remaining consumer is publish_to_cgw_wrapper
            # called below via json.dumps(snapshot).
            is_disk_image = disk_image_utils.is_disk_image_component(component)
            if is_disk_image and not component.get("files"):
                component["files"] = (component.get("staged") or {}).get("files", [])

    cgw_push = any(bool(c.get("contentGateway")) for c in snapshot.get("components", []))
    if cgw_push:
        logger.info("Publishing all components to CGW...")
        if "developers.qa.redhat.com" in cgw_hostname:
            os.environ["HTTP_PROXY"] = "http://squid.corp.redhat.com:3128"
            os.environ["HTTPS_PROXY"] = "http://squid.corp.redhat.com:3128"
            logger.info("Using squid proxy for preprod CGW access")

        publish_to_cgw_wrapper.main(
            [
                "--cgw_host",
                cgw_hostname,
                "--data_json",
                json.dumps(snapshot),
            ]
        )


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and push artifacts to all configured destinations; return exit code."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv[1:] if argv is not None else None)
    try:
        run(args.exodus_gw_env, args.cgw_hostname, args.cert_expiration_warn_days)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
