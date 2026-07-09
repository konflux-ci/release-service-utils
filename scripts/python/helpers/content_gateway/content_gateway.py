"""Content Gateway and CDN helpers for artifact filenames and download URLs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_CGW_PRODUCTS_PROD = "https://developers.redhat.com/products"
_CDN_DOWNLOADS_PROD = "https://access.redhat.com/downloads"
_CGW_PRODUCTS_PREPROD = "https://developers.qa.redhat.com/products"
_CDN_DOWNLOADS_PREPROD = "https://access.stage.redhat.com/downloads"


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


def filename_for_disk_image(component: dict[str, Any], architecture: str) -> str:
    """Return ``staged.files[].filename`` containing *architecture* for disk-image rows."""
    staged = component.get("staged")
    if not isinstance(staged, dict):
        return ""
    files = staged.get("files")
    if not isinstance(files, list):
        return ""
    for file_row in files:
        if not isinstance(file_row, dict):
            continue
        filename = file_row.get("filename")
        if isinstance(filename, str) and architecture in filename:
            return filename
    return ""


def filename_for_binary_or_generic(
    component: dict[str, Any],
    *,
    architecture: str,
    operating_system: str,
) -> str:
    """Return ``source`` for binary/generic rows matching arch and operating_system."""
    for file_row in component_file_entries(component):
        if file_row.get("arch") == architecture and file_row.get("os") == operating_system:
            source = file_row.get("source")
            if isinstance(source, str):
                return source
    return ""


def windows_zip_filename(filename: str) -> str:
    """Replace ``.tar.gz`` or ``.tar`` extension with ``.zip`` for Windows archives."""
    if filename.endswith(".tar.gz"):
        return filename[: -len(".tar.gz")] + ".zip"
    if filename.endswith(".tar"):
        return filename[: -len(".tar")] + ".zip"
    return filename


def windows_archive_basename(filename: str, operating_system: str) -> str:
    """Return basename with Windows archive extensions normalized to ``.zip``."""
    basename = Path(filename).name
    if operating_system == "windows":
        return windows_zip_filename(basename)
    return basename
