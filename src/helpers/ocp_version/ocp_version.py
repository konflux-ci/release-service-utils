"""Inspect FBC fragment images to read their target OCP version.

``read_openshift_version_label`` reads the ``com.redhat.fbc.openshift.version``
image label. ``resolve_ocp_version`` reads the
``org.opencontainers.image.base.name`` annotation tag, resolving multi-arch
images (OCI index or Docker manifest-list) to a single platform's manifest
first via ``get-image-architectures``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from release_service_utils.helpers import skopeo
from release_service_utils.helpers.subprocess_cmd import run_cmd_text

logger = logging.getLogger("ocp_version")

FBC_OPENSHIFT_VERSION_LABEL = "com.redhat.fbc.openshift.version"

MULTI_ARCH_MEDIA_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}


def _label_value_for_error(raw: Any) -> str:
    """Return a display string for an invalid openshift.version label value."""
    if raw is None:
        return "null"
    if isinstance(raw, str):
        return raw
    return json.dumps(raw)


def _invalid_label_error(raw: Any, image_ref: str) -> ValueError:
    """Build the error raised when the openshift.version label is malformed."""
    displayed = _label_value_for_error(raw)
    return ValueError(
        f'"{FBC_OPENSHIFT_VERSION_LABEL}" label has invalid value '
        f"'{displayed}' for {image_ref}, "
        'array with ocp_versions e.g. ["v4.21"] is expected.'
    )


def _parse_version_label(raw: Any, image_ref: str) -> list[str]:
    """Parse a present openshift.version label into a non-empty version list."""
    value = raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise _invalid_label_error(raw, image_ref) from exc
    if not isinstance(value, list) or not value:
        raise _invalid_label_error(raw, image_ref)
    return [str(item) for item in value]


def read_openshift_version_label(image_ref: str) -> list[str] | None:
    """Return versions from the FBC openshift.version label, or None if absent.

    Inspect the image (non-raw, ``--no-tags``) and read
    ``com.redhat.fbc.openshift.version``. A missing label is not an error; a
    present but empty or non-array value raises ``ValueError``.
    """
    payload = json.loads(skopeo.inspect(image_ref, no_tags=True, check=True).stdout)
    labels = payload.get("Labels")
    if not isinstance(labels, dict) or FBC_OPENSHIFT_VERSION_LABEL not in labels:
        return None
    return _parse_version_label(labels[FBC_OPENSHIFT_VERSION_LABEL], image_ref)


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
