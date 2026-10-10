"""Tests for ``oot_kmods`` helpers."""

from __future__ import annotations

import tarfile
from pathlib import Path

import pytest

from release_service_utils.helpers import oot_kmods


def _write_envfile(path: Path, content: str) -> Path:
    """Write an envfile at *path* and return it."""
    path.write_text(content, encoding="utf-8")
    return path


def _make_signed_archive(
    data_dir: Path,
    files: dict[str, str],
    dest_name: str = oot_kmods.SIGNED_KMODS_DIR,
    archive_name: str = oot_kmods.SIGNED_KMODS_ARCHIVE,
) -> Path:
    """Create a kmod tarball under *data_dir* with the given files."""
    staging = data_dir / "staging" / dest_name
    for rel, content in files.items():
        dest = staging / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")
    archive = data_dir / archive_name
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(staging, arcname=dest_name)
    return archive


def test_extract_signed_kmods_archive_missing(tmp_path: Path) -> None:
    """Missing archive is a no-op."""
    dest = tmp_path / oot_kmods.SIGNED_KMODS_DIR
    oot_kmods.extract_signed_kmods_archive(tmp_path, dest)
    assert not dest.exists()


def test_extract_signed_kmods_archive_extracts_ko_files(tmp_path: Path) -> None:
    """Archive members are extracted into the destination directory."""
    _make_signed_archive(
        tmp_path,
        {
            "x86_64/mod1.ko": "one",
            "x86_64/nested/mod2.ko": "two",
            "x86_64/envfile": "ARCH=x86_64\n",
        },
    )

    dest = tmp_path / oot_kmods.SIGNED_KMODS_DIR
    oot_kmods.extract_signed_kmods_archive(tmp_path, dest)

    assert (dest / "x86_64/mod1.ko").is_file()
    assert (dest / "x86_64/nested/mod2.ko").is_file()


def test_extract_signed_kmods_archive_no_ko_files(tmp_path: Path) -> None:
    """Extraction succeeds even when the archive contains no ``.ko`` files."""
    _make_signed_archive(tmp_path, {"x86_64/envfile": "ARCH=x86_64\n"})

    dest = tmp_path / oot_kmods.SIGNED_KMODS_DIR
    oot_kmods.extract_signed_kmods_archive(tmp_path, dest)

    assert (dest / "x86_64/envfile").is_file()
    assert not list(dest.rglob("*.ko"))


def test_extract_signed_kmods_archive_uses_fixed_name(tmp_path: Path) -> None:
    """A non-default dest still extracts signed-kmods.tar.gz."""
    dest_name = "custom-kmods"
    _make_signed_archive(
        tmp_path,
        {"x86_64/mod1.ko": "one"},
        dest_name=dest_name,
    )

    dest = tmp_path / dest_name
    oot_kmods.extract_signed_kmods_archive(tmp_path, dest)

    assert (dest / "x86_64/mod1.ko").is_file()
    assert (tmp_path / oot_kmods.SIGNED_KMODS_ARCHIVE).is_file()


def test_extract_signed_kmods_archive_nested_path(tmp_path: Path) -> None:
    """Archive in data_dir reconstructs a nested signedKmodsPath."""
    dest_name = "nested/signed-kmods"
    _make_signed_archive(
        tmp_path,
        {"x86_64/mod1.ko": "one"},
        dest_name=dest_name,
    )

    dest = tmp_path / dest_name
    oot_kmods.extract_signed_kmods_archive(tmp_path, dest)

    assert (dest / "x86_64/mod1.ko").is_file()


def test_extract_signed_kmods_archive_ignores_archive_beside_nested_dest(
    tmp_path: Path,
) -> None:
    """An archive beside a nested dest is ignored; only data_dir is searched."""
    dest = tmp_path / "nested" / oot_kmods.SIGNED_KMODS_DIR
    _make_signed_archive(
        dest.parent,
        {"x86_64/mod1.ko": "one"},
    )

    oot_kmods.extract_signed_kmods_archive(tmp_path, dest)

    assert not dest.exists()


def test_extract_signed_kmods_archive_ignores_dest_named_tarball(
    tmp_path: Path,
) -> None:
    """A tarball named after dest is ignored; only signed-kmods.tar.gz is used."""
    _make_signed_archive(
        tmp_path,
        {"x86_64/mod1.ko": "one"},
        dest_name="custom-kmods",
        archive_name="custom-kmods.tar.gz",
    )

    oot_kmods.extract_signed_kmods_archive(tmp_path, tmp_path / "custom-kmods")

    assert not (tmp_path / "custom-kmods").exists()
    assert not (tmp_path / oot_kmods.SIGNED_KMODS_DIR).exists()


def test_load_kmod_envfile_parses_keys(tmp_path: Path) -> None:
    """Quoted dotenv values are returned without quotes."""
    envfile = _write_envfile(
        tmp_path / "envfile",
        'DRIVER_VENDOR="acme"\nDRIVER_VERSION=1.0\nKERNEL_VERSION=6.5.0\nARCH=x86_64\n',
    )
    env = oot_kmods.load_kmod_envfile(envfile)
    assert env["DRIVER_VENDOR"] == "acme"
    assert env["DRIVER_VERSION"] == "1.0"
    assert env["KERNEL_VERSION"] == "6.5.0"
    assert env["ARCH"] == "x86_64"


def test_load_kmod_envfile_missing_file(tmp_path: Path) -> None:
    """Missing envfile raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="envfile not found"):
        oot_kmods.load_kmod_envfile(tmp_path / "envfile")


def test_load_kmod_envfile_missing_keys(tmp_path: Path) -> None:
    """Envfile without required keys raises ValueError."""
    envfile = _write_envfile(tmp_path / "envfile", "ARCH=x86_64\n")
    with pytest.raises(ValueError, match="missing required keys"):
        oot_kmods.load_kmod_envfile(envfile)


def test_load_kmod_envfile_empty_required_key(tmp_path: Path) -> None:
    """Blank required values are treated as missing."""
    envfile = _write_envfile(
        tmp_path / "envfile",
        "DRIVER_VENDOR=acme\nDRIVER_VERSION=\nKERNEL_VERSION=6.5.0\n",
    )
    with pytest.raises(ValueError, match="DRIVER_VERSION"):
        oot_kmods.load_kmod_envfile(envfile)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("6.5.0-s3.x86_64", "6.5.0-s3"),
        ("6.5.0-s3.amd64", "6.5.0-s3"),
        ("6.5.0-s3.aarch64+64k", "6.5.0-s3"),
        ("6.5.0-s3.arm64", "6.5.0-s3"),
        ("6.5.0-s3.ppc64le", "6.5.0-s3"),
        ("6.5.0-s3.s390x", "6.5.0-s3"),
        ("6.5.0-s3", "6.5.0-s3"),
    ],
)
def test_clean_kernel_version(raw: str, expected: str) -> None:
    """Architecture suffixes are stripped from kernel versions."""
    assert oot_kmods.clean_kernel_version(raw) == expected


@pytest.mark.parametrize(
    ("env", "fallback", "expected"),
    [
        ({"ARCH": "x86_64"}, "amd64", "x86_64"),
        ({"ARCH": "MULTI_PLATFORM"}, "amd64", "amd64"),
        ({"ARCH": ""}, "ppc64le", "ppc64le"),
        ({"ARCH": "  "}, "s390x", "s390x"),
        ({}, "arm64", "arm64"),
        ({"VERSION": "1.0"}, "s390x", "s390x"),
    ],
)
def test_arch_from_env(env: dict[str, str], fallback: str, expected: str) -> None:
    """ARCH from a parsed env dict is used unless empty or MULTI_PLATFORM."""
    assert oot_kmods.arch_from_env(env, fallback) == expected


def test_resolve_arch_name_from_envfile(tmp_path: Path) -> None:
    """ARCH from envfile overrides the directory name."""
    _write_envfile(tmp_path / "envfile", "ARCH=x86_64\n")
    assert oot_kmods.resolve_arch_name(tmp_path, "amd64") == "x86_64"


def test_resolve_arch_name_multi_platform(tmp_path: Path) -> None:
    """ARCH=MULTI_PLATFORM falls back to the directory name."""
    _write_envfile(tmp_path / "envfile", "ARCH=MULTI_PLATFORM\n")
    assert oot_kmods.resolve_arch_name(tmp_path, "amd64") == "amd64"


def test_resolve_arch_name_no_envfile(tmp_path: Path) -> None:
    """Missing envfile falls back to the directory name."""
    assert oot_kmods.resolve_arch_name(tmp_path, "arm64") == "arm64"


def test_resolve_arch_name_empty_arch(tmp_path: Path) -> None:
    """Empty ARCH= falls back to the directory name."""
    _write_envfile(tmp_path / "envfile", "ARCH=\nOTHER=val\n")
    assert oot_kmods.resolve_arch_name(tmp_path, "ppc64le") == "ppc64le"


def test_resolve_arch_name_no_arch_line(tmp_path: Path) -> None:
    """Envfile without ARCH falls back to the directory name."""
    _write_envfile(tmp_path / "envfile", "VERSION=1.0\nVENDOR=test\n")
    assert oot_kmods.resolve_arch_name(tmp_path, "s390x") == "s390x"


def test_resolve_arch_name_double_quoted(tmp_path: Path) -> None:
    """Double-quoted ARCH values have quotes stripped."""
    _write_envfile(tmp_path / "envfile", 'ARCH="x86_64"\n')
    assert oot_kmods.resolve_arch_name(tmp_path, "amd64") == "x86_64"


def test_resolve_arch_name_single_quoted(tmp_path: Path) -> None:
    """Single-quoted ARCH values have quotes stripped."""
    _write_envfile(tmp_path / "envfile", "ARCH='aarch64'\n")
    assert oot_kmods.resolve_arch_name(tmp_path, "arm64") == "aarch64"


def test_destination_prefix() -> None:
    """Prefix always ends with a trailing slash."""
    assert (
        oot_kmods.destination_prefix("acme", "1.0", "6.5.0", "x86_64")
        == "acme/1.0/6.5.0/x86_64/"
    )
    assert (
        oot_kmods.destination_prefix("acme", "1.0", "6.5.0", "multi-arch-summary")
        == "acme/1.0/6.5.0/multi-arch-summary/"
    )


def test_arch_directories(tmp_path: Path) -> None:
    """Immediate subdirectories are returned in sorted order."""
    (tmp_path / "x86_64").mkdir()
    (tmp_path / "aarch64").mkdir()
    (tmp_path / "note.txt").write_text("skip", encoding="utf-8")
    names = [path.name for path in oot_kmods.arch_directories(tmp_path)]
    assert names == ["aarch64", "x86_64"]


def test_arch_directories_empty(tmp_path: Path) -> None:
    """No architecture directories raises RuntimeError."""
    (tmp_path / "note.txt").write_text("skip", encoding="utf-8")
    with pytest.raises(RuntimeError, match="No architecture directories"):
        oot_kmods.arch_directories(tmp_path)
