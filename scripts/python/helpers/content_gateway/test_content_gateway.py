"""Tests for content_gateway helpers."""

from __future__ import annotations

from content_gateway import content_gateway


def test_cdn_base_urls_production() -> None:
    """Production CDN env uses public CGW and CDN download hosts."""
    data = {"cdn": {"env": "production"}}
    assert content_gateway.cdn_base_urls(data) == (
        "https://developers.redhat.com/products",
        "https://access.redhat.com/downloads",
    )


def test_cdn_base_urls_stage() -> None:
    """Stage CDN env uses preprod CGW and CDN download hosts."""
    data = {"cdn": {"env": "stage"}}
    assert content_gateway.cdn_base_urls(data) == (
        "https://developers.qa.redhat.com/products",
        "https://access.stage.redhat.com/downloads",
    )


def test_cdn_base_urls_defaults_to_production() -> None:
    """Missing cdn.env defaults to production URLs."""
    assert content_gateway.cdn_base_urls({}) == (
        "https://developers.redhat.com/products",
        "https://access.redhat.com/downloads",
    )


def test_filename_for_disk_image_matches_architecture_in_filename() -> None:
    """Disk-image filenames are matched by architecture substring."""
    component = {
        "staged": {
            "files": [
                {"filename": "product-1.0-x86_64.iso.gz"},
                {"filename": "product-1.0-aarch64.iso.gz"},
            ],
        },
    }
    assert content_gateway.filename_for_disk_image(component, "x86_64") == (
        "product-1.0-x86_64.iso.gz"
    )


def test_filename_for_binary_or_generic_prefers_files_array() -> None:
    """Binary/generic lookup prefers top-level files[] over staged.files[]."""
    component = {
        "files": [{"arch": "amd64", "os": "linux", "source": "app-linux.tgz"}],
        "staged": {
            "files": [{"arch": "amd64", "os": "linux", "source": "staged-linux.tgz"}],
        },
    }
    assert (
        content_gateway.filename_for_binary_or_generic(
            component,
            architecture="amd64",
            operating_system="linux",
        )
        == "app-linux.tgz"
    )


def test_filename_for_binary_or_generic_falls_back_to_staged_files() -> None:
    """Binary/generic lookup uses staged.files[] when files[] is empty."""
    component = {
        "staged": {
            "files": [{"arch": "amd64", "os": "linux", "source": "staged-linux.tgz"}],
        },
    }
    assert (
        content_gateway.filename_for_binary_or_generic(
            component,
            architecture="amd64",
            operating_system="linux",
        )
        == "staged-linux.tgz"
    )


def test_windows_zip_filename_tar_gz() -> None:
    """A .tar.gz filename is converted to the equivalent .zip name."""
    assert content_gateway.windows_zip_filename("binary-amd64.tar.gz") == "binary-amd64.zip"


def test_windows_zip_filename_tar() -> None:
    """A .tar filename is converted to the equivalent .zip name."""
    assert content_gateway.windows_zip_filename("binary-amd64.tar") == "binary-amd64.zip"


def test_windows_zip_filename_already_zip() -> None:
    """Zip filenames are returned unchanged."""
    assert content_gateway.windows_zip_filename("binary-amd64.zip") == "binary-amd64.zip"


def test_windows_archive_basename_converts_windows_tar_gz() -> None:
    """Windows archive basenames normalize tar.gz to zip for checksum lookup."""
    out = content_gateway.windows_archive_basename("app.tar.gz", "windows")
    assert out == "app.zip"


def test_windows_archive_basename_skips_non_windows() -> None:
    """Non-Windows operating systems keep the original basename."""
    assert content_gateway.windows_archive_basename("app.tar.gz", "linux") == "app.tar.gz"


def test_component_file_entries_empty_when_staged_not_dict() -> None:
    """Return no rows when staged is not a mapping."""
    assert content_gateway.component_file_entries({"staged": "invalid"}) == []


def test_component_file_entries_empty_when_staged_files_not_list() -> None:
    """Return no rows when staged.files is not a list."""
    assert content_gateway.component_file_entries({"staged": {"files": "invalid"}}) == []


def test_filename_for_disk_image_empty_when_staged_not_dict() -> None:
    """Return empty when staged is not a mapping."""
    assert content_gateway.filename_for_disk_image({"staged": []}, "x86_64") == ""


def test_filename_for_disk_image_empty_when_files_not_list() -> None:
    """Return empty when staged.files is not a list."""
    component = {"staged": {"files": "invalid"}}
    assert content_gateway.filename_for_disk_image(component, "x86_64") == ""


def test_filename_for_disk_image_skips_non_dict_rows() -> None:
    """Skip invalid file rows and match the first valid filename."""
    component = {
        "staged": {
            "files": [
                "invalid-row",
                {"filename": "product-1.0-x86_64.iso.gz"},
            ],
        },
    }
    assert content_gateway.filename_for_disk_image(component, "x86_64") == (
        "product-1.0-x86_64.iso.gz"
    )


def test_filename_for_binary_or_generic_empty_when_no_match() -> None:
    """Return empty when no file row matches arch and operating system."""
    component = {
        "files": [{"arch": "amd64", "os": "linux", "source": "app-linux.tgz"}],
    }
    assert (
        content_gateway.filename_for_binary_or_generic(
            component,
            architecture="aarch64",
            operating_system="linux",
        )
        == ""
    )
