"""Content Gateway and CDN helpers for artifact delivery."""

from __future__ import annotations

from .content_gateway import (
    cdn_base_urls,
    cdn_base_urls_for_env,
    cdn_env,
    component_file_entries,
    filename_for_binary_or_generic,
    filename_for_disk_image,
    windows_archive_basename,
    windows_zip_filename,
)

__all__ = [
    "cdn_base_urls",
    "cdn_base_urls_for_env",
    "cdn_env",
    "component_file_entries",
    "filename_for_binary_or_generic",
    "filename_for_disk_image",
    "windows_archive_basename",
    "windows_zip_filename",
]
