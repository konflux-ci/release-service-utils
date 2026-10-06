"""Tests for ``rpm_utils`` helper."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.helpers import rpm_utils

_RUN_CMD = "release_service_utils.helpers.rpm_utils.rpm_utils.subprocess_cmd.run_cmd"


def _rpm_result(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    """Build a CompletedProcess for a mocked ``rpm -qp`` call."""
    return subprocess.CompletedProcess(
        args=["rpm"],
        returncode=returncode,
        stdout=stdout,
        stderr="",
    )


class TestParseCommaList:
    """Test comma-delimited list parsing."""

    def test_strips_and_drops_empty(self) -> None:
        """Strip whitespace and skip empty items."""
        assert rpm_utils.parse_comma_list("x86_64, aarch64, ,s390x,") == [
            "x86_64",
            "aarch64",
            "s390x",
        ]

    def test_empty_string(self) -> None:
        """An empty string yields an empty list."""
        assert rpm_utils.parse_comma_list("") == []


class TestShouldExcludeFile:
    """Test file exclusion by pattern."""

    def test_matches_pattern(self) -> None:
        """File containing an exclude pattern is excluded."""
        assert rpm_utils.should_exclude_file(
            "hello-debuginfo-1.0.rpm", ["-debuginfo-", "-debugsource-"]
        )

    def test_no_match(self) -> None:
        """Normal file is not excluded."""
        assert not rpm_utils.should_exclude_file(
            "hello-1.0.rpm", ["-debuginfo-", "-debugsource-"]
        )

    def test_empty_patterns(self) -> None:
        """Empty pattern list excludes nothing."""
        assert not rpm_utils.should_exclude_file("hello-1.0.rpm", [])

    def test_whitespace_pattern(self) -> None:
        """Whitespace-only patterns are skipped."""
        assert not rpm_utils.should_exclude_file("hello-1.0.rpm", ["", " "])


class TestListRpmFiles:
    """Test directory listing of RPM files."""

    def test_filters_and_sorts(self, tmp_path: Path) -> None:
        """Keep regular RPMs, skip dirs/non-RPMs/excludes, sort by name."""
        (tmp_path / "logs").mkdir()
        (tmp_path / "readme.txt").write_text("x", encoding="utf-8")
        (tmp_path / "z-last-1.0-1.x86_64.rpm").write_bytes(b"")
        (tmp_path / "a-first-1.0-1.x86_64.rpm").write_bytes(b"")
        (tmp_path / "hello-debuginfo-1.0-1.x86_64.rpm").write_bytes(b"")
        paths = rpm_utils.list_rpm_files(tmp_path, ["-debuginfo-"])
        assert [path.name for path in paths] == [
            "a-first-1.0-1.x86_64.rpm",
            "z-last-1.0-1.x86_64.rpm",
        ]

    def test_empty_directory(self, tmp_path: Path) -> None:
        """An empty directory yields no RPMs."""
        assert rpm_utils.list_rpm_files(tmp_path, []) == []


class TestParseNevra:
    """Test NEVRA parsing from RPM headers and filenames."""

    def test_header_success(self, tmp_path: Path) -> None:
        """Parse rpm -qp output into RpmNevra."""
        path = tmp_path / "hello.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello|0|2.12|1.fc44|x86_64\n")):
            result = rpm_utils.parse_nevra(path)
        assert result == rpm_utils.RpmNevra(
            name="hello",
            epoch="0",
            version="2.12",
            release="1.fc44",
            arch="x86_64",
        )

    def test_none_epoch(self, tmp_path: Path) -> None:
        """(none) epoch normalizes to '0'."""
        path = tmp_path / "hello.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello|(none)|2.12|1.fc44|x86_64\n")):
            result = rpm_utils.parse_nevra(path)
        assert result.epoch == "0"

    def test_empty_epoch(self, tmp_path: Path) -> None:
        """Blank epoch normalizes to '0'."""
        path = tmp_path / "hello.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello||2.12|1.fc44|x86_64\n")):
            result = rpm_utils.parse_nevra(path)
        assert result.epoch == "0"

    def test_incomplete_header_falls_back_to_filename(self, tmp_path: Path) -> None:
        """Incomplete header fields fall back to filename parsing."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello|0||6.fc44|x86_64\n")):
            result = rpm_utils.parse_nevra(path)
        assert result == rpm_utils.RpmNevra(
            name="hello",
            epoch="0",
            version="2.12.1",
            release="6.fc44",
            arch="x86_64",
        )

    def test_no_filename_fallback_raises_on_incomplete_header(self, tmp_path: Path) -> None:
        """Incomplete headers raise when filename fallback is disabled."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello|0||6.fc44|x86_64\n")):
            with pytest.raises(ValueError, match="Failed to parse NEVRA from header"):
                rpm_utils.parse_nevra(path, fallback_to_filename=False)

    def test_filename_fallback(self, tmp_path: Path) -> None:
        """Parse NEVRA from filename when rpm -qp fails."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            result = rpm_utils.parse_nevra(path)
        assert result == rpm_utils.RpmNevra(
            name="hello",
            epoch="0",
            version="2.12.1",
            release="6.fc44",
            arch="x86_64",
        )

    def test_filename_epoch(self, tmp_path: Path) -> None:
        """Parse epoch embedded in the filename version field."""
        path = tmp_path / "hello-1:2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            result = rpm_utils.parse_nevra(path)
        assert result.epoch == "1"
        assert result.version == "2.12.1"

    def test_src_rpm_forces_src_arch(self, tmp_path: Path) -> None:
        """Header-reported non-src arch is forced to src for *.src.rpm."""
        path = tmp_path / "hello-2.12.1-6.fc44.src.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello|0|2.12.1|6.fc44|noarch\n")):
            result = rpm_utils.parse_nevra(path)
        assert result.arch == "src"

    def test_src_header_already_src(self, tmp_path: Path) -> None:
        """Header that already reports arch=src is left unchanged."""
        path = tmp_path / "hello-2.12.1-6.fc44.src.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello|0|2.12.1|6.fc44|src\n")):
            result = rpm_utils.parse_nevra(path)
        assert result.arch == "src"

    def test_src_rpm_filename_fallback(self, tmp_path: Path) -> None:
        """Filename fallback for *.src.rpm reports arch=src."""
        path = tmp_path / "hello-2.12.1-6.fc44.src.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            result = rpm_utils.parse_nevra(path)
        assert result.arch == "src"
        assert result.name == "hello"

    def test_os_error_falls_back_to_filename(self, tmp_path: Path) -> None:
        """OSError from rpm -qp falls back to filename parsing."""
        path = tmp_path / "hello-2.12.1-6.fc44.noarch.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, side_effect=OSError("not found")):
            result = rpm_utils.parse_nevra(path)
        assert result.arch == "noarch"
        assert result.name == "hello"

    def test_malformed_header_falls_back(self, tmp_path: Path) -> None:
        """Malformed rpm -qp output falls back to filename parsing."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("bad-output\n")):
            result = rpm_utils.parse_nevra(path)
        assert result.name == "hello"

    def test_filename_failure_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when header and filename both fail."""
        path = tmp_path / "bad.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA"):
                rpm_utils.parse_nevra(path)

    def test_filename_missing_version_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when the filename version is empty after the epoch colon."""
        path = tmp_path / "hello-1:-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA version"):
                rpm_utils.parse_nevra(path)

    def test_filename_not_rpm_raises(self, tmp_path: Path) -> None:
        """Raise ValueError for a non-rpm filename."""
        path = tmp_path / "hello.txt"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA"):
                rpm_utils.parse_nevra(path)

    def test_filename_missing_release_dash_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when the NVRA has no release dash."""
        path = tmp_path / "hello.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA"):
                rpm_utils.parse_nevra(path)

    def test_filename_missing_name_dash_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when name and version cannot be split."""
        path = tmp_path / "hello-1.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA"):
                rpm_utils.parse_nevra(path)

    def test_filename_empty_release_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when the filename release is empty."""
        path = tmp_path / "hello-1-.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA"):
                rpm_utils.parse_nevra(path)

    def test_filename_empty_arch_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when the filename arch is empty."""
        path = tmp_path / "hello-1.0-1..rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA"):
                rpm_utils.parse_nevra(path)

    def test_subprocess_error_falls_back(self, tmp_path: Path) -> None:
        """SubprocessError from rpm -qp falls back to filename parsing."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(
            _RUN_CMD,
            side_effect=subprocess.SubprocessError("failed"),
        ):
            result = rpm_utils.parse_nevra(path)
        assert result.name == "hello"

    def test_no_filename_fallback_raises_on_header_failure(self, tmp_path: Path) -> None:
        """Do not parse the filename when fallback_to_filename is False."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("", returncode=1)):
            with pytest.raises(ValueError, match="Failed to parse NEVRA from header"):
                rpm_utils.parse_nevra(path, fallback_to_filename=False)

    def test_no_filename_fallback_raises_on_os_error(self, tmp_path: Path) -> None:
        """OSError from rpm -qp raises when filename fallback is disabled."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, side_effect=OSError("not found")):
            with pytest.raises(ValueError, match="Failed to parse NEVRA from header"):
                rpm_utils.parse_nevra(path, fallback_to_filename=False)

    def test_no_filename_fallback_still_uses_header(self, tmp_path: Path) -> None:
        """A readable header is used even when filename fallback is disabled."""
        path = tmp_path / "hello-2.12.1-6.fc44.x86_64.rpm"
        path.write_bytes(b"")
        with patch(_RUN_CMD, return_value=_rpm_result("hello|0|2.12.1|6.fc44|x86_64\n")):
            result = rpm_utils.parse_nevra(path, fallback_to_filename=False)
        assert result == rpm_utils.RpmNevra(
            name="hello",
            epoch="0",
            version="2.12.1",
            release="6.fc44",
            arch="x86_64",
        )
