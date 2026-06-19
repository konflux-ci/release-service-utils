"""Tests for base64_encode_checksum."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from base64_encode_checksum import encode_checksums, main


def test_single_checksum_file(tmp_path: Path) -> None:
    """Encode a single SHA256SUMS file."""
    content = b"abc123  somefile.tar.gz\n"
    (tmp_path / "SHA256SUMS").write_bytes(content)

    result = encode_checksums(tmp_path)

    assert result == base64.b64encode(content).decode("ascii")


def test_multiple_checksum_files(tmp_path: Path) -> None:
    """Concatenate multiple SHA256SUMS files before encoding."""
    content_a = b"aaa  file_a\n"
    content_b = b"bbb  file_b\n"
    (tmp_path / "A-SHA256SUMS").write_bytes(content_a)
    (tmp_path / "B-SHA256SUMS").write_bytes(content_b)

    result = encode_checksums(tmp_path)

    assert result == base64.b64encode(content_a + content_b).decode("ascii")


def test_no_matching_files(tmp_path: Path) -> None:
    """Raise FileNotFoundError when no *SHA256SUMS files exist."""
    (tmp_path / "unrelated.txt").write_text("hello")

    with pytest.raises(FileNotFoundError, match="No \\*SHA256SUMS files"):
        encode_checksums(tmp_path)


def test_missing_directory(tmp_path: Path) -> None:
    """Raise FileNotFoundError when the binaries directory is absent."""
    missing = tmp_path / "nonexistent"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        encode_checksums(missing)


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write the base64 blob to the result file and return 0."""
    content = b"deadbeef  binary.tar.gz\n"
    binaries = tmp_path / "binaries"
    binaries.mkdir()
    (binaries / "SHA256SUMS").write_bytes(content)

    result_file = tmp_path / "blob_result"
    monkeypatch.setenv("RESULT_BLOB", str(result_file))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("BINARIES_DIR", "binaries")

    rc = main()

    assert rc == 0
    assert result_file.read_text() == base64.b64encode(content).decode("ascii")


def test_main_failure_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise FileNotFoundError when the directory is missing."""
    result_file = tmp_path / "blob_result"
    monkeypatch.setenv("RESULT_BLOB", str(result_file))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("BINARIES_DIR", "nonexistent")

    with pytest.raises(FileNotFoundError):
        main()

    assert not result_file.exists()


def test_main_missing_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when RESULT_BLOB is not set."""
    monkeypatch.delenv("RESULT_BLOB", raising=False)

    with pytest.raises(SystemExit):
        main()
