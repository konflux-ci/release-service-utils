"""Pull signed artifacts and publish to CDN and Content Gateway.

Receives a snapshot and signed Trusted Artifact URIs from the managed
gpg-sign-and-push-to-cdn task via InternalRequest.  Re-extracts files from
each component's container image, merges them with the detached signatures
pulled from the signed TAs, pushes everything to CDN via exodus-rsync, and
registers the files in Content Gateway.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import requests

import publish_to_cgw_wrapper
from release_service_utils.helpers import disk_image_utils, skopeo, tekton
from release_service_utils.helpers.cdn.cdn import create_exodus_conf, push_files_to_cdn
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.oras_utils import (
    FLAT_ARTIFACT_CONFIG_MEDIA_TYPE,
    copy_all_flat_artifact_files,
    copy_all_layered_image_files,
    oras_pull,
)
from release_service_utils.helpers.redact import redact_secrets

PROG = "gpg_sign_and_push_to_cdn.py"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Build and return the CLI argument parser."""
    p = argparse.ArgumentParser(
        prog=PROG,
        description="Pull signed artifacts and publish to CDN/CGW.",
    )
    p.add_argument(
        "--snapshot-json",
        required=True,
        help="Full snapshot JSON string",
    )
    p.add_argument(
        "--signed-artifacts-json",
        required=True,
        help="JSON mapping: component name to list of signed TA URIs",
    )
    p.add_argument(
        "--exodus-gw-env",
        required=True,
        choices=["live", "pre"],
        help="Exodus Gateway environment",
    )
    p.add_argument(
        "--cgw-hostname",
        required=True,
        help="Content Gateway API hostname",
    )
    return p.parse_args(argv)


def pull_and_extract(container_image: str, work_dir: Path) -> Path:
    """Pull an OCI artifact and extract its file contents."""
    image_dir = work_dir / "image"
    image_dir.mkdir(parents=True, exist_ok=True)
    skopeo.copy(container_image, image_dir, authenticated=True, check=True)

    manifest = json.loads((image_dir / "manifest.json").read_text())
    config_media_type = manifest.get("config", {}).get("mediaType", "")

    extract_dir = work_dir / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    if config_media_type == FLAT_ARTIFACT_CONFIG_MEDIA_TYPE:
        copy_all_flat_artifact_files(manifest, image_dir, extract_dir)
    else:
        copy_all_layered_image_files(manifest, image_dir, extract_dir)

    return extract_dir


def build_content_dir(
    extract_dir: Path,
    signed_ta_uris: list[str],
    content_dir: Path,
) -> list[str]:
    """Merge original files and signing output into a content directory.

    Return the list of signature filenames that were copied (e.g.
    ``["binary.tar.gz.sig"]``).
    """
    shutil.copytree(extract_dir, content_dir, dirs_exist_ok=True)

    sig_names: list[str] = []
    for uri in signed_ta_uris:
        pull_spec = uri.removeprefix("oci://")
        with tempfile.TemporaryDirectory() as pull_tmp:
            pull_dir = Path(pull_tmp)
            oras_pull(pull_spec, pull_dir)

            results_file = pull_dir / "results.json"
            if not results_file.is_file():
                raise RuntimeError(f"No results.json in signed TA {uri}")

            results = json.loads(results_file.read_text())
            for entry in results.get("results", []):
                original = Path(entry.get("file", "")).name
                sig_path_str = entry.get("signature_file", "")
                if not original or not sig_path_str:
                    raise RuntimeError(
                        f"Incomplete signing result in {uri}: "
                        f"file={entry.get('file')!r}, "
                        f"signature_file={sig_path_str!r}"
                    )
                sig_path = pull_dir / sig_path_str
                if not sig_path.is_file():
                    raise RuntimeError(f"Signature file {sig_path_str} not found in TA {uri}")
                dest_name = f"{original}.sig"
                shutil.copy2(sig_path, content_dir / dest_name)
                sig_names.append(dest_name)

    return sig_names


def publish_to_cgw(
    components: list[dict[str, Any]],
    content_dirs: dict[str, Path],
    sig_names_by_component: dict[str, list[str]],
    cgw_hostname: str,
) -> None:
    """Register files in Content Gateway for components with contentGateway data."""
    mount = Path(os.environ.get("CGW_SECRET_MOUNT", "/mnt/cgwSecret"))
    username = (mount / "username").read_text().strip()
    token = (mount / "token").read_text().strip()

    if "developers.qa.redhat.com" in cgw_hostname:
        os.environ["HTTP_PROXY"] = "http://squid.corp.redhat.com:3128"
        os.environ["HTTPS_PROXY"] = "http://squid.corp.redhat.com:3128"
        logger.info("Using squid proxy for preprod CGW access")

    session = requests.Session()
    session.auth = (username, token)

    for i, component in enumerate(components):
        name = component.get("name", "unknown")
        if "contentGateway" not in component:
            logger.info("Skipping CGW for '%s' (no contentGateway data)", name)
            continue
        if name not in content_dirs:
            logger.warning("No content directory for '%s', skipping CGW", name)
            continue

        if disk_image_utils.is_disk_image_component(component) and not component.get("files"):
            component["files"] = (component.get("staged") or {}).get("files", [])

        for sig_name in sig_names_by_component.get(name, []):
            component.setdefault("files", []).append({"source": sig_name})

        component["contentGateway"]["contentDir"] = str(content_dirs[name])
        logger.info("Publishing '%s' to CGW", name)
        publish_to_cgw_wrapper.process_component(
            host=cgw_hostname,
            session=session,
            component=component,
            component_index=i,
        )


def publish_all_components(
    components: list[dict[str, Any]],
    signed_artifacts: dict[str, list[str]],
    exodus_gw_env: str,
    cgw_hostname: str,
) -> None:
    """Extract files, build content dirs, push to CDN, and register in CGW."""
    try:
        mount = Path(os.environ.get("EXODUS_GW_SECRET_MOUNT", "/mnt/exodusGwSecret"))
        gw_url = (mount / "url").read_text().strip()

        cert_path = Path("/tmp/exodus.crt")
        key_path = Path("/tmp/exodus.key")
        cert_path.write_text((mount / "cert").read_text().strip() + "\n")
        cert_path.chmod(0o600)
        key_path.write_text((mount / "key").read_text().strip() + "\n")
        key_path.chmod(0o600)

        exodus_conf_path = Path("/tmp/exodus-rsync.conf")
        create_exodus_conf(exodus_conf_path, cert_path, key_path, gw_url, exodus_gw_env)
    except Exception as exc:
        raise tekton.CheckStepError("reading Exodus GW credentials", exc) from exc

    with tempfile.TemporaryDirectory() as work_root:
        work_root_path = Path(work_root)

        content_dirs: dict[str, Path] = {}
        sig_names_by_component: dict[str, list[str]] = {}
        for component in components:
            name = component.get("name", "unknown")
            container_image = component.get("containerImage", "")
            if not container_image:
                logger.warning("No containerImage for '%s', skipping", name)
                continue

            work_dir = work_root_path / name
            work_dir.mkdir(parents=True, exist_ok=True)

            try:
                extract_dir = pull_and_extract(container_image, work_dir)
            except Exception as exc:
                raise tekton.CheckStepError(f"extracting files from '{name}'", exc) from exc

            try:
                content_dir = work_dir / "content"
                sig_names = build_content_dir(
                    extract_dir,
                    signed_artifacts.get(name, []),
                    content_dir,
                )
            except Exception as exc:
                raise tekton.CheckStepError(
                    f"building content directory for '{name}'", exc
                ) from exc
            content_dirs[name] = content_dir
            sig_names_by_component[name] = sig_names

            try:
                logger.info("Pushing '%s' to CDN", name)
                push_files_to_cdn(content_dir, exodus_conf_path)
            except Exception as exc:
                raise tekton.CheckStepError(f"pushing '{name}' to CDN", exc) from exc

        try:
            publish_to_cgw(components, content_dirs, sig_names_by_component, cgw_hostname)
        except Exception as exc:
            raise tekton.CheckStepError("publishing to Content Gateway", exc) from exc


def main(argv: list[str] | None = None) -> int:
    """Parse inputs, publish to CDN and CGW, and write Tekton result."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    args = parse_args(argv[1:] if argv is not None else None)

    (rpath,) = tekton.result_paths_from_env("RESULT_RESULT")

    try:
        snapshot = json.loads(args.snapshot_json)
        signed_artifacts = json.loads(args.signed_artifacts_json)

        components = snapshot.get("components", [])
        logger.info("Found %d component(s) in snapshot", len(components))

        if not components:
            logger.info("No components to publish")
            rpath.write_text("Success", encoding="utf-8")
            return 0

        publish_all_components(
            components,
            signed_artifacts,
            args.exodus_gw_env,
            args.cgw_hostname,
        )
    except (Exception, SystemExit) as exc:
        if isinstance(exc, SystemExit) and exc.code in (0, None):
            raise
        logger.error("%s failed: %s", PROG, redact_secrets(str(exc)))
        tekton.write_failure_result(
            rpath,
            PROG,
            exc,
            workflow_action="publishing to CDN and Content Gateway",
        )
        return 0

    rpath.write_text("Success", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
