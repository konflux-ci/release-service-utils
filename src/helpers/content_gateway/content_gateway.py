"""Content Gateway and CDN helpers for artifact filenames and download URLs."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

_CGW_PRODUCTS_PROD = "https://developers.redhat.com/products"
_CDN_DOWNLOADS_PROD = "https://access.redhat.com/downloads"
_CGW_PRODUCTS_PREPROD = "https://developers.qa.redhat.com/products"
_CDN_DOWNLOADS_PREPROD = "https://access.stage.redhat.com/downloads"


def component_content_type(component: dict[str, Any]) -> str:
    """Return contentGateway.contentType, else top-level contentType, else empty.

    A missing, null, or empty nested type falls back to the top-level
    field so an explicit empty ``contentGateway.contentType`` cannot
    hide a usable top-level type.
    """
    content_gateway_cfg = component.get("contentGateway")
    if isinstance(content_gateway_cfg, dict) and "contentType" in content_gateway_cfg:
        content_type = content_gateway_cfg["contentType"]
        if content_type is not None and str(content_type):
            return str(content_type)
    content_type = component.get("contentType")
    return str(content_type) if content_type else ""


def cdn_env(data: dict[str, Any]) -> str:
    """Return the CDN environment from *data*, defaulting to production."""
    return str(data.get("cdn", {}).get("env", "production"))


def cdn_base_urls(data: dict[str, Any]) -> tuple[str, str]:
    """Return CGW and CDN download base URLs for the data.json CDN environment."""
    return cdn_base_urls_for_env(cdn_env(data))


def cdn_base_urls_for_env(cdn_env_value: str) -> tuple[str, str]:
    """Return CGW and CDN download base URLs for *cdn_env_value*."""
    if cdn_env_value in {"stage", "qa"}:
        return (_CGW_PRODUCTS_PREPROD, _CDN_DOWNLOADS_PREPROD)
    return (_CGW_PRODUCTS_PROD, _CDN_DOWNLOADS_PROD)


def component_file_entries(component: dict[str, Any]) -> list[dict[str, Any]]:
    """Return file rows from ``files[]``, falling back to ``staged.files[]`` when empty."""
    files = component.get("files")
    if isinstance(files, list) and files:
        return [row for row in files if isinstance(row, dict)]
    staged = component.get("staged")
    if not isinstance(staged, dict):
        return []
    staged_files = staged.get("files")
    if not isinstance(staged_files, list):
        return []
    return [row for row in staged_files if isinstance(row, dict)]


def disk_image_file_entries(component: dict[str, Any]) -> list[dict[str, Any]]:
    """Return disk-image file rows from ``staged.files[]``, falling back to ``files[]``.

    Unlike ``component_file_entries`` (files-first, used for binary/generic
    content), disk-image components can populate both arrays on the same
    component: ``staged.files[]`` for the Customer Portal (Pulp), with an
    explicit published ``filename`` (e.g. after tag-template substitution),
    and top-level ``files[]`` for the Content Gateway / Developer Portal.
    When both are present, ``staged.files[]`` carries the authoritative
    published filename and must take priority; ``files[]`` is only used as a
    fallback for CGW-only releases that have no ``staged`` block at all.
    """
    staged = component.get("staged")
    if isinstance(staged, dict):
        staged_files = staged.get("files")
        if isinstance(staged_files, list) and staged_files:
            return [row for row in staged_files if isinstance(row, dict)]
    files = component.get("files")
    if isinstance(files, list):
        return [row for row in files if isinstance(row, dict)]
    return []


def resolved_filename(entry: dict[str, Any]) -> str:
    """Return a file entry's published filename.

    ``staged.files[]`` entries declare an explicit ``filename`` (which can
    differ from ``source``, e.g. after tag-template substitution). Top-level
    ``files[]`` entries have no ``filename`` field at all -- the published
    name is always the basename of ``source``.

    Only entries with no ``filename`` key fall back to deriving a name from
    ``source``. An explicit but invalid ``filename`` (missing, empty, or the
    literal ``"null"`` string) is rejected instead of silently substituting
    the source basename, so malformed staged data still fails loudly rather
    than publishing under the wrong name. Returns ``""`` when no usable name
    can be resolved.
    """
    if "filename" in entry:
        filename = entry.get("filename")
        if isinstance(filename, str) and filename and filename != "null":
            return filename
        return ""
    source = entry.get("source")
    if isinstance(source, str) and source:
        return Path(source).name
    return ""


def filenames_for_binary_or_generic(
    component: dict[str, Any],
    *,
    architecture: str,
    operating_system: str,
) -> list[str]:
    """Return every ``source`` for binary/generic rows matching arch and operating_system.

    Multiple files can legitimately share the same (architecture, operating_system)
    pair (e.g. two differently-named binaries built for the same target), so all
    matches are returned instead of only the first.
    """
    sources: list[str] = []
    for file_row in component_file_entries(component):
        if file_row.get("arch") == architecture and file_row.get("os") == operating_system:
            source = file_row.get("source")
            if isinstance(source, str):
                sources.append(source)
    return sources


def windows_zip_filename(filename: str) -> str:
    """Replace ``.tar.gz`` or ``.tar`` extension with ``.zip`` for Windows archives."""
    if filename.endswith(".tar.gz"):
        return filename[: -len(".tar.gz")] + ".zip"
    if filename.endswith(".tar"):
        return filename[: -len(".tar")] + ".zip"
    return filename


def validated_short_url_segment(segment: str) -> str:
    """Return *segment* percent-encoded as one short URL path segment.

    Reject an absolute segment or any ``..`` path part. Callers that insert a
    Pulp repository into a Content Gateway or Developer Portal URL use this
    so both flows encode the same way.
    """
    if segment.startswith("/") or ".." in segment.split("/"):
        raise ValueError(
            f"short URL segment must be a relative path without '..': {segment!r}"
        )
    return quote(segment, safe="")


def segments_for_shared_names(
    entries: Sequence[Mapping[str, Any]],
    key_fn: Callable[[Mapping[str, Any]], str],
    default_destination: str,
) -> list[str]:
    """Return each entry's short URL segment, aligned with *entries*.

    *key_fn* selects the filename that decides whether a name is shared.
    Each entry's ``destination`` is the repository that publishes it. A name
    published by one repository gets ``""``. When several repositories
    publish it, *default_destination* gets ``""`` if it is one of them, and
    every other repository gets its own destination.
    """
    keys = [key_fn(entry) for entry in entries]
    destinations = [str(entry["destination"]) for entry in entries]
    owners: dict[str, set[str]] = {}
    for key, destination in zip(keys, destinations):
        owners.setdefault(key, set()).add(destination)

    segments: list[str] = []
    for key, destination in zip(keys, destinations):
        shared = owners[key]
        plain = default_destination if default_destination in shared else None
        if len(shared) >= 2 and destination != plain:
            segments.append(destination)
        else:
            segments.append("")
    return segments


def windows_archive_basename(filename: str, operating_system: str) -> str:
    """Return basename with Windows archive extensions normalized to ``.zip``."""
    basename = Path(filename).name
    if operating_system == "windows":
        return windows_zip_filename(basename)
    return basename
