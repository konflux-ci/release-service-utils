"""Tests for content_gateway helpers."""

from __future__ import annotations

from release_service_utils.helpers.content_gateway import content_gateway


def test_component_content_type_prefers_content_gateway() -> None:
    """Prefer contentGateway.contentType over the top-level contentType field."""
    assert (
        content_gateway.component_content_type(
            {
                "contentType": "image",
                "contentGateway": {"contentType": "disk-image"},
            }
        )
        == "disk-image"
    )


def test_component_content_type_falls_back_to_top_level() -> None:
    """Use top-level contentType when contentGateway has none."""
    assert (
        content_gateway.component_content_type({"contentGateway": {}, "contentType": "binary"})
        == "binary"
    )


def test_component_content_type_empty_nested_falls_back() -> None:
    """Fall back to top-level contentType when the nested type is empty."""
    assert (
        content_gateway.component_content_type(
            {
                "contentType": "binary",
                "contentGateway": {"contentType": ""},
            }
        )
        == "binary"
    )


def test_component_content_type_null_nested_falls_back() -> None:
    """Fall back to top-level contentType when the nested type is null."""
    assert (
        content_gateway.component_content_type(
            {
                "contentType": "binary",
                "contentGateway": {"contentType": None},
            }
        )
        == "binary"
    )


def test_component_content_type_empty_when_unset() -> None:
    """Return empty when neither contentGateway nor top-level type is set."""
    assert content_gateway.component_content_type({}) == ""
    assert content_gateway.component_content_type({"contentGateway": {}}) == ""
    assert content_gateway.component_content_type({"contentGateway": "bad"}) == ""


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


def test_filenames_for_binary_or_generic_prefers_files_array() -> None:
    """Binary/generic lookup prefers top-level files[] over staged.files[]."""
    component = {
        "files": [{"arch": "amd64", "os": "linux", "source": "app-linux.tgz"}],
        "staged": {
            "files": [{"arch": "amd64", "os": "linux", "source": "staged-linux.tgz"}],
        },
    }
    assert content_gateway.filenames_for_binary_or_generic(
        component,
        architecture="amd64",
        operating_system="linux",
    ) == ["app-linux.tgz"]


def test_filenames_for_binary_or_generic_falls_back_to_staged_files() -> None:
    """Binary/generic lookup uses staged.files[] when files[] is empty."""
    component = {
        "staged": {
            "files": [{"arch": "amd64", "os": "linux", "source": "staged-linux.tgz"}],
        },
    }
    assert content_gateway.filenames_for_binary_or_generic(
        component,
        architecture="amd64",
        operating_system="linux",
    ) == ["staged-linux.tgz"]


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


def test_disk_image_file_entries_prefers_staged_files() -> None:
    """Disk-image lookup prefers staged.files[] over top-level files[]."""
    component = {
        "files": [{"source": "raw-x86_64.iso"}],
        "staged": {
            "files": [{"source": "raw-x86_64.iso", "filename": "published-x86_64.iso"}]
        },
    }
    assert content_gateway.disk_image_file_entries(component) == [
        {"source": "raw-x86_64.iso", "filename": "published-x86_64.iso"},
    ]


def test_disk_image_file_entries_falls_back_to_top_level_files() -> None:
    """Disk-image lookup uses files[] when there is no staged block (CGW-only)."""
    component = {"files": [{"source": "raw-x86_64.iso"}]}
    assert content_gateway.disk_image_file_entries(component) == [
        {"source": "raw-x86_64.iso"},
    ]


def test_disk_image_file_entries_falls_back_when_staged_files_empty() -> None:
    """Disk-image lookup uses files[] when staged.files[] is present but empty."""
    component = {"files": [{"source": "raw-x86_64.iso"}], "staged": {"files": []}}
    assert content_gateway.disk_image_file_entries(component) == [
        {"source": "raw-x86_64.iso"},
    ]


def test_disk_image_file_entries_empty_when_neither_present() -> None:
    """Return no rows when neither staged.files[] nor files[] is usable."""
    assert content_gateway.disk_image_file_entries({}) == []


def test_resolved_filename_uses_explicit_filename() -> None:
    """A valid explicit filename is returned as-is."""
    assert content_gateway.resolved_filename(
        {"filename": "app-1.0.iso", "source": "x.iso"}
    ) == ("app-1.0.iso")


def test_resolved_filename_derives_from_source_when_no_filename_key() -> None:
    """An entry with no filename key derives the name from source's basename."""
    assert content_gateway.resolved_filename({"source": "images/app-1.0.iso"}) == "app-1.0.iso"


def test_resolved_filename_rejects_literal_null_without_falling_back_to_source() -> None:
    """A literal "null" filename is rejected, not replaced by the source basename.

    Regression test: staged.files[] entries with a "null" filename must fail
    instead of silently publishing under a different (source-derived) name.
    """
    assert content_gateway.resolved_filename({"filename": "null", "source": "x.iso"}) == ""


def test_resolved_filename_rejects_empty_filename_without_falling_back_to_source() -> None:
    """An explicit empty-string filename is rejected, not replaced by source."""
    assert content_gateway.resolved_filename({"filename": "", "source": "x.iso"}) == ""


def test_resolved_filename_empty_when_no_filename_or_source() -> None:
    """Return "" when neither filename nor source is usable."""
    assert content_gateway.resolved_filename({}) == ""
