"""Populate releaseNotes artifact PURLs from a checksum map OCI artifact."""

from __future__ import annotations

from .release_notes_purl import TA_DOCKERCONFIG_DEFAULT, update_artifact_purls

__all__ = ["TA_DOCKERCONFIG_DEFAULT", "update_artifact_purls"]
