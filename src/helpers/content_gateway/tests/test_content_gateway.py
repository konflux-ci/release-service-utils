"""Tests for content_gateway helpers."""

from __future__ import annotations

from release_service_utils.helpers.content_gateway import content_gateway


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


def test_delivered_archive_basename_windows_tar_gz_to_zip() -> None:
    """A Windows source that already names a tar.gz archive is rewritten to .zip."""
    assert content_gateway.delivered_archive_basename("app.tar.gz", "windows") == "app.zip"


def test_delivered_archive_basename_linux_archive_unchanged() -> None:
    """A Linux source that already names an archive keeps its name."""
    assert content_gateway.delivered_archive_basename("app.tar.gz", "linux") == "app.tar.gz"


def test_delivered_archive_basename_linux_raw_binary_gets_tar_gz() -> None:
    """A raw Linux binary with no archive suffix is delivered as .tar.gz."""
    out = content_gateway.delivered_archive_basename("roxctl-linux-amd64", "linux")
    assert out == "roxctl-linux-amd64.tar.gz"


def test_delivered_archive_basename_darwin_raw_binary_gets_tar_gz() -> None:
    """A raw macOS binary with no archive suffix is delivered as .tar.gz."""
    out = content_gateway.delivered_archive_basename("roxctl-darwin-arm64", "darwin")
    assert out == "roxctl-darwin-arm64.tar.gz"


def test_delivered_archive_basename_windows_exe_gets_zip() -> None:
    """A raw Windows .exe is delivered as a .zip archive (with .exe stripped)."""
    out = content_gateway.delivered_archive_basename("roxctl-windows-amd64.exe", "windows")
    assert out == "roxctl-windows-amd64.zip"


def test_delivered_archive_basename_windows_raw_no_ext_gets_zip() -> None:
    """A raw Windows binary with no extension is delivered as .zip."""
    out = content_gateway.delivered_archive_basename("roxctl-windows-amd64", "windows")
    assert out == "roxctl-windows-amd64.zip"


def test_delivered_archive_basename_disk_image_unchanged() -> None:
    """Disk images are delivered as-is and never gain an archive suffix."""
    assert content_gateway.delivered_archive_basename("image.qcow2", "linux") == "image.qcow2"


def test_delivered_archive_basename_is_idempotent() -> None:
    """Re-applying the mapping to a normalized name is a no-op."""
    linux = content_gateway.delivered_archive_basename("roxctl-linux-amd64", "linux")
    assert content_gateway.delivered_archive_basename(linux, "linux") == linux
    win = content_gateway.delivered_archive_basename("roxctl-windows-amd64.exe", "windows")
    assert content_gateway.delivered_archive_basename(win, "windows") == win


def test_delivered_archive_basename_strips_directory() -> None:
    """A full source path is reduced to the delivered basename."""
    out = content_gateway.delivered_archive_basename("/releases/roxctl-linux-amd64", "linux")
    assert out == "roxctl-linux-amd64.tar.gz"


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
