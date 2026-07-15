"""Shared helpers for identifying disk-image files and components."""

from __future__ import annotations

# Unambiguous disk-image file suffixes (simple and compound). Files matching
# these are handled as raw binary blobs rather than tar archives, even when the
# component does not carry contentType: disk-image.
# NOTE: .tar.gz is intentionally excluded — it is ambiguous between binary
# archives and disk images (e.g. GCP images packaged as tarballs). Use
# contentType: disk-image on the component to handle those cases.
DISK_IMAGE_SUFFIXES: frozenset[str] = frozenset(
    {".qcow2", ".iso", ".iso.gz", ".raw.gz", ".vhd.gz"}
)


def is_disk_image_file(filename: str) -> bool:
    """Return True if *filename* has an unambiguous disk-image file suffix."""
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in DISK_IMAGE_SUFFIXES)


def is_disk_image_component(component: dict) -> bool:
    """Return True if *component* is declared as a disk-image release.

    A component is a disk-image if contentType: disk-image appears at the
    top-level component field OR nested under contentGateway.
    """
    return (
        component.get("contentType") == "disk-image"
        or (component.get("contentGateway") or {}).get("contentType") == "disk-image"
    )
