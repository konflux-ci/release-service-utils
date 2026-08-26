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


def test_filenames_for_binary_or_generic_returns_all_matches() -> None:
    """All files sharing the same (arch, os) are returned, not just the first."""
    component = {
        "files": [
            {"arch": "amd64", "os": "linux", "source": "cli-a.tgz"},
            {"arch": "amd64", "os": "linux", "source": "cli-b.tgz"},
            {"arch": "arm64", "os": "linux", "source": "cli-c.tgz"},
        ],
    }
    assert content_gateway.filenames_for_binary_or_generic(
        component,
        architecture="amd64",
        operating_system="linux",
    ) == ["cli-a.tgz", "cli-b.tgz"]


def test_filenames_for_binary_or_generic_empty_when_no_match() -> None:
    """Return an empty list when no file row matches arch and operating system."""
    component = {
        "files": [{"arch": "amd64", "os": "linux", "source": "app-linux.tgz"}],
    }
    assert (
        content_gateway.filenames_for_binary_or_generic(
            component,
            architecture="aarch64",
            operating_system="linux",
        )
        == []
    )


def test_filename_for_binary_or_generic_uses_first_of_multiple_matches() -> None:
    """The singular helper still returns just the first match, for compatibility."""
    component = {
        "files": [
            {"arch": "amd64", "os": "linux", "source": "cli-a.tgz"},
            {"arch": "amd64", "os": "linux", "source": "cli-b.tgz"},
        ],
    }
    assert (
        content_gateway.filename_for_binary_or_generic(
            component,
            architecture="amd64",
            operating_system="linux",
        )
        == "cli-a.tgz"
    )
