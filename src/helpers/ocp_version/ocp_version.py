"""Inspect FBC fragment images to read their target OCP version.

Use ``skopeo inspect`` to read the ``org.opencontainers.image.base.name``
annotation and return the OCP version tag. Multi-arch images (OCI index or
Docker manifest-list) are resolved to a single platform's manifest first via
``get-image-architectures``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from release_service_utils.helpers import skopeo
from release_service_utils.helpers.subprocess_cmd import run_cmd_text

logger = logging.getLogger("ocp_version")

MULTI_ARCH_MEDIA_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}


def base_name_tag(manifest: dict[str, Any]) -> str:
    """Return the tag portion of a manifest's base-image annotation.

    The ``org.opencontainers.image.base.name`` annotation has the form
    ``registry/path:vX.Y``; only the text after the last colon is kept.
    """
    annotations = manifest.get("annotations") or {}
    base_name = annotations.get("org.opencontainers.image.base.name") or ""
    return base_name.rsplit(":", 1)[-1] if base_name else ""


def resolve_ocp_version(fbc_fragment: str) -> str:
    """Return the OCP version tag for *fbc_fragment*, resolving multi-arch images."""
    manifest = json.loads(skopeo.inspect(fbc_fragment, raw=True, check=True).stdout)

    if manifest.get("mediaType") in MULTI_ARCH_MEDIA_TYPES:
        logger.info("Multiplatform image detected, extracting manifest")
        arch_output = run_cmd_text(["get-image-architectures", fbc_fragment])
        platforms = [json.loads(line) for line in arch_output.splitlines() if line.strip()]
        manifest_image_sha = platforms[0]["digest"]
        fbc_fragment = f"{fbc_fragment.rsplit('@', 1)[0]}@{manifest_image_sha}"
        manifest = json.loads(skopeo.inspect(fbc_fragment, raw=True, check=True).stdout)

    return base_name_tag(manifest)
