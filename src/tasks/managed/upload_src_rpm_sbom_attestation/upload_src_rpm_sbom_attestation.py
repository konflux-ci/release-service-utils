#!/usr/bin/env python3
"""Download image SBOMs and attestations and upload them to a Pulp file repo."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from release_service_utils.helpers import cosign, file, image_ref, pulp_client, tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.pulp_client import PulpDigestStatus

METADATA_REPO_NAME = "metadata"
DEFAULT_PULP_SECRET = Path("/etc/secrets/cli.toml")
SBOM_DIR = Path("/var/workdir/sboms")


def _source_rpm_entries(component: dict[str, Any]) -> list[dict[str, Any]]:
    """Return ``rpmsToPublish`` entries whose RPM path ends with ``.src.rpm``."""
    entries = component.get("rpmsToPublish") or []
    if not isinstance(entries, list):
        return []
    return [
        entry
        for entry in entries
        if isinstance(entry, dict) and str(entry.get("rpm") or "").endswith(".src.rpm")
    ]


def _write_snapshot(path: Path, snapshot: dict[str, Any]) -> None:
    """Write *snapshot* as pretty-printed JSON."""
    path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")


def _upload_artifact(
    *,
    config_file: Path,
    domain: str,
    relative_path: str,
    local_file: Path,
    label: str,
    component_name: str,
) -> None:
    """Upload *local_file* unless Pulp already has a matching checksum."""
    status = pulp_client.check_file_content(config_file, domain, relative_path, local_file)
    if status is PulpDigestStatus.MATCH:
        logger.info(
            "%s already exists in Pulp, skipping upload: %s",
            label,
            relative_path,
        )
        return
    if status is PulpDigestStatus.NOT_FOUND:
        logger.info("Uploading %s to Pulp: %s", label, relative_path)
        pulp_client.upload_file_content(
            config_file,
            domain,
            local_file,
            relative_path,
            METADATA_REPO_NAME,
        )
        logger.info("%s uploaded successfully for %s", label, component_name)
        return
    raise RuntimeError(f"{label} checksum mismatch for {component_name}")


def _process_component(
    component: dict[str, Any],
    *,
    config_file: Path,
    domain: str,
    sbom_dir: Path,
) -> bool:
    """Download and upload SBOM/attestation for *component*.

    Return True when the component had source RPMs and was updated.
    """
    component_name = str(component.get("name") or "")
    source_rpms = _source_rpm_entries(component)
    if not source_rpms:
        logger.info("Skipping component %s: no source RPMs", component_name)
        return False

    container_image = component.get("containerImage")
    if not container_image:
        raise RuntimeError(f"No containerImage for component {component_name}")
    image = str(container_image)

    _, digest = image_ref.split_image_ref(image)
    if not digest.startswith("sha256:"):
        raise RuntimeError(f"Could not extract sha256 digest from {image}")

    digest_filename = digest.replace(":", "-")
    sbom_filename = f"{digest_filename}.sbom"
    att_filename = f"{digest_filename}.att"
    sbom_relative_path = f"sboms/{component_name}/{sbom_filename}"
    att_relative_path = f"attestations/{component_name}/{att_filename}"
    sbom_local_file = sbom_dir / sbom_filename
    att_local_file = sbom_dir / att_filename

    with cosign.docker_config(image) as config_dir:
        cosign.download("sbom", image, sbom_local_file, docker_config_dir=config_dir)
        cosign.download("attestation", image, att_local_file, docker_config_dir=config_dir)

    _upload_artifact(
        config_file=config_file,
        domain=domain,
        relative_path=sbom_relative_path,
        local_file=sbom_local_file,
        label="SBOM",
        component_name=component_name,
    )
    _upload_artifact(
        config_file=config_file,
        domain=domain,
        relative_path=att_relative_path,
        local_file=att_local_file,
        label="Attestation",
        component_name=component_name,
    )

    sbom_snapshot_path = f"{METADATA_REPO_NAME}/{sbom_relative_path}"
    att_snapshot_path = f"{METADATA_REPO_NAME}/{att_relative_path}"
    for entry in source_rpms:
        entry["sbomPath"] = sbom_snapshot_path
        entry["attestationPath"] = att_snapshot_path
    logger.info("Updated snapshot with sbomPath for %s", component_name)
    logger.info("Updated snapshot with attestationPath for %s", component_name)
    return True


def run(
    *,
    data_dir: Path,
    snapshot_path: str,
    pulp_domain: str,
    pulp_config_file: Path,
) -> None:
    """Download SBOM/attestation artifacts and upload them to Pulp.

    Updates ``*.src.rpm`` entries in the snapshot with ``sbomPath`` and
    ``attestationPath``.
    """
    secret_bytes = pulp_config_file.read_bytes()
    if not secret_bytes.strip():
        raise RuntimeError(f"Missing cli.toml in secret {pulp_config_file}")

    writable_config = file.make_tempfile_path("pulp-cli-", secret_bytes)
    try:
        pulp_client.ensure_file_repo_exists(writable_config, pulp_domain, METADATA_REPO_NAME)
        snapshot_file = file.resolve_path_under_base(data_dir, snapshot_path)
        snapshot = file.load_json_dict(snapshot_file)
        components = snapshot.get("components") or []
        if not isinstance(components, list):
            raise TypeError("snapshot field 'components' must be an array")

        logger.info(
            "Processing %s components for SBOM/attestation upload...",
            len(components),
        )
        sbom_dir = SBOM_DIR
        sbom_dir.mkdir(parents=True, exist_ok=True)

        updated = False
        for component in components:
            if not isinstance(component, dict):
                raise TypeError("Each snapshot component must be an object")
            if _process_component(
                component,
                config_file=writable_config,
                domain=pulp_domain,
                sbom_dir=sbom_dir,
            ):
                updated = True

        if updated:
            _write_snapshot(snapshot_file, snapshot)

        logger.info("SBOM/attestation upload complete.")
    finally:
        writable_config.unlink(missing_ok=True)


def main() -> int:
    """Read Tekton env vars and upload source RPM SBOMs and attestations."""
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        snapshot_path=tekton.require_env("PARAM_SNAPSHOT_PATH"),
        pulp_domain=tekton.require_env("PULP_DOMAIN"),
        pulp_config_file=file.path_from_env_variable("PULP_CONFIG_FILE", DEFAULT_PULP_SECRET),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
