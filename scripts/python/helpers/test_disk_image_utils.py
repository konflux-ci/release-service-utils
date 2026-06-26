"""Tests for disk_image_utils."""

from __future__ import annotations

import pytest

import disk_image_utils

# ---------------------------------------------------------------------------
# is_disk_image_file
# ---------------------------------------------------------------------------


def test_is_disk_image_file_qcow2() -> None:
    """A .qcow2 file is recognised as a disk image."""
    assert disk_image_utils.is_disk_image_file("disk.qcow2") is True


def test_is_disk_image_file_iso() -> None:
    """A .iso file is recognised as a disk image."""
    assert disk_image_utils.is_disk_image_file("install.iso") is True


def test_is_disk_image_file_iso_gz() -> None:
    """A .iso.gz file is recognised as a disk image."""
    assert disk_image_utils.is_disk_image_file("install.iso.gz") is True


def test_is_disk_image_file_raw_gz() -> None:
    """A .raw.gz file is recognised as a disk image."""
    assert disk_image_utils.is_disk_image_file("disk.raw.gz") is True


def test_is_disk_image_file_vhd_gz() -> None:
    """A .vhd.gz file is recognised as a disk image."""
    assert disk_image_utils.is_disk_image_file("disk.vhd.gz") is True


def test_is_disk_image_file_case_insensitive() -> None:
    """Extension matching is case-insensitive."""
    assert disk_image_utils.is_disk_image_file("disk.QCOW2") is True
    assert disk_image_utils.is_disk_image_file("install.ISO") is True


def test_is_disk_image_file_tar_gz_is_false() -> None:
    """A .tar.gz file is not a disk image (ambiguous with binary archives)."""
    assert disk_image_utils.is_disk_image_file("binary.tar.gz") is False


def test_is_disk_image_file_zip_is_false() -> None:
    """A .zip file is not a disk image."""
    assert disk_image_utils.is_disk_image_file("binary.zip") is False


def test_is_disk_image_file_generic_binary_is_false() -> None:
    """A generic binary with no extension is not a disk image."""
    assert disk_image_utils.is_disk_image_file("my-tool-linux-amd64") is False


def test_is_disk_image_file_with_path() -> None:
    """A full path ending in a disk-image suffix is recognised correctly."""
    assert disk_image_utils.is_disk_image_file("/releases/product-x86_64.iso") is True


# ---------------------------------------------------------------------------
# is_disk_image_component
# ---------------------------------------------------------------------------


def test_is_disk_image_component_top_level_content_type() -> None:
    """A component with top-level contentType: disk-image is identified correctly."""
    component = {"contentType": "disk-image", "name": "my-image"}
    assert disk_image_utils.is_disk_image_component(component) is True


def test_is_disk_image_component_content_gateway_content_type() -> None:
    """A component with contentGateway.contentType: disk-image is identified correctly."""
    component = {"contentGateway": {"contentType": "disk-image"}, "name": "my-image"}
    assert disk_image_utils.is_disk_image_component(component) is True


def test_is_disk_image_component_binary_is_false() -> None:
    """A binary component is not a disk image."""
    component = {"contentType": "binary", "name": "my-binary"}
    assert disk_image_utils.is_disk_image_component(component) is False


def test_is_disk_image_component_image_is_false() -> None:
    """A container-image component is not a disk image."""
    component = {"contentType": "image", "name": "my-image"}
    assert disk_image_utils.is_disk_image_component(component) is False


def test_is_disk_image_component_no_content_type_is_false() -> None:
    """A component with no contentType field is not a disk image."""
    component = {"name": "my-component"}
    assert disk_image_utils.is_disk_image_component(component) is False


def test_is_disk_image_component_empty_content_gateway_is_false() -> None:
    """A component with an empty contentGateway dict is not a disk image."""
    component = {"contentGateway": {}, "name": "my-component"}
    assert disk_image_utils.is_disk_image_component(component) is False


def test_is_disk_image_component_none_content_gateway_is_false() -> None:
    """A component with contentGateway: null is not a disk image."""
    component = {"contentGateway": None, "name": "my-component"}
    assert disk_image_utils.is_disk_image_component(component) is False


@pytest.mark.parametrize("content_type", ["image", "binary", "generic", "rpm", ""])
def test_is_disk_image_component_non_disk_image_content_types(content_type: str) -> None:
    """Non-disk-image contentType values return False."""
    component = {"contentType": content_type}
    assert disk_image_utils.is_disk_image_component(component) is False
