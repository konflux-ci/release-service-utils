"""Shared helpers for identifying disk-image files and components."""

from .disk_image_utils import (  # noqa: F401
    DISK_IMAGE_SUFFIXES,
    DISK_IMAGE_DEFAULT_OS,
    architecture_from_filename,
    is_disk_image_component,
    is_disk_image_file,
)
