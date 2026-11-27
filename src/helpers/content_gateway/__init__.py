"""Content Gateway and CDN helpers for artifact delivery."""

from __future__ import annotations

from .content_gateway import (
    cdn_base_urls,
    cdn_base_urls_for_env,
    cdn_env,
    component_content_type,
    component_file_entries,
    disk_image_file_entries,
    filenames_for_binary_or_generic,
    resolved_filename,
    segments_for_shared_names,
    validated_short_url_segment,
    windows_archive_basename,
    windows_zip_filename,
)

__all__ = [
    "cdn_base_urls",
    "cdn_base_urls_for_env",
    "cdn_env",
    "component_content_type",
    "component_file_entries",
    "disk_image_file_entries",
    "filenames_for_binary_or_generic",
    "resolved_filename",
    "segments_for_shared_names",
    "validated_short_url_segment",
    "windows_archive_basename",
    "windows_zip_filename",
]
