"""Determine the architecture(s) of a container image reference."""

from __future__ import annotations

import json
from typing import Any

import oras_utils
import skopeo

_SINGLE_ARCH_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }
)

HELM_CONFIG_MEDIA_TYPE = "application/vnd.cncf.helm.config.v1+json"


def get_image_architectures(image: str) -> list[dict[str, Any]]:
    """Return architecture details for each platform in *image*.

    Each entry is a dict with ``platform`` (containing ``architecture`` and
    ``os``), ``digest``, and optionally ``multiarch`` and ``configMediaType``
    keys, matching the output of the ``get-image-architectures`` bash script.
    """
    raw_result = skopeo.inspect(image, raw=True, no_tags=True)
    if raw_result.returncode != 0:
        raise RuntimeError(
            f"skopeo inspect --raw failed for {image}: {raw_result.stderr.strip()}"
        )
    raw_data = json.loads(raw_result.stdout)

    artifact_type = raw_data.get("artifactType")
    config_media_type = (raw_data.get("config") or {}).get("mediaType", "")

    if artifact_type is not None or config_media_type == HELM_CONFIG_MEDIA_TYPE:
        return _handle_oci_artifact(image, raw_data, config_media_type)

    media_type = raw_data.get("mediaType", "")

    if media_type in _SINGLE_ARCH_MEDIA_TYPES:
        return _handle_single_arch(image, media_type)

    return _handle_multi_arch(raw_data)


def _handle_oci_artifact(
    image: str,
    raw_data: dict[str, Any],
    config_media_type: str,
) -> list[dict[str, Any]]:
    """Handle OCI artifacts (non-image manifests, Helm charts, etc.)."""
    digest = _digest_from_ref(image)
    if not digest.startswith("sha256:"):
        digest = oras_utils.oras_resolve(image)

    return [
        {
            "platform": {"architecture": "amd64", "os": "linux"},
            "digest": digest,
            "multiarch": False,
            "configMediaType": config_media_type,
        }
    ]


def _handle_single_arch(
    image: str,
    media_type: str,
) -> list[dict[str, Any]]:
    """Handle single-arch OCI or Docker v2 manifests."""
    inspect_result = skopeo.inspect(image, no_tags=True)
    if inspect_result.returncode != 0:
        raise RuntimeError(
            f"skopeo inspect failed for {image}: {inspect_result.stderr.strip()}"
        )
    data = json.loads(inspect_result.stdout)

    architecture = data.get("Architecture") or ""
    os_name = data.get("Os") or ""
    digest = data.get("Digest", "")

    if media_type == "application/vnd.docker.distribution.manifest.v2+json":
        architecture = architecture or "amd64"
        os_name = os_name or "linux"

    return [
        {
            "platform": {"architecture": architecture, "os": os_name},
            "digest": digest,
            "multiarch": False,
        }
    ]


def _handle_multi_arch(raw_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Handle multi-arch manifest lists / OCI image indexes."""
    manifests = raw_data.get("manifests", [])
    results: list[dict[str, Any]] = []
    for manifest in manifests:
        entry = dict(manifest)
        entry["multiarch"] = True
        results.append(entry)
    return results


def _digest_from_ref(image: str) -> str:
    """Extract the digest portion from an image reference like ``repo@sha256:abc``."""
    if "@" in image:
        return image.split("@", 1)[1]
    return ""
