"""Tests for extract_sboms_from_wheels."""

from __future__ import annotations

import hashlib
import json
import runpy
import zipfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

import pytest

from release_service_utils.helpers import tekton
from release_service_utils.tasks.managed.extract_sboms_from_wheels.extract_sboms_from_wheels import (  # noqa: E501
    _FILENAME_MAX_BYTES,
    _READABLE_SUFFIX_BYTES,
    _sbom_output_name,
    _wheel_identity,
    extract_sboms_from_wheel,
    main,
    run,
)

TASK = (
    "release_service_utils.tasks.managed"
    ".extract_sboms_from_wheels.extract_sboms_from_wheels"
)

_DEFAULT_WHEEL = "test_package-1.0.0-py3-none-any.whl"
_DEFAULT_MEMBER = "test_package-1.0.0.dist-info/sboms/sbom.spdx.json"
_SBOM = json.dumps(
    {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "test-sbom",
    }
)


def _write_wheel(
    path: Path,
    members: dict[str, str] | None = None,
    *,
    directory_entries: tuple[str, ...] = (),
) -> None:
    """Write a zip archive at *path* with the given members."""
    with zipfile.ZipFile(path, "w") as zf:
        for name in directory_entries:
            zf.writestr(name, b"")
        for name, content in (members or {}).items():
            zf.writestr(name, content)


def _wheel_with_sbom(
    wheels_dir: Path,
    name: str = _DEFAULT_WHEEL,
    sbom_member: str = _DEFAULT_MEMBER,
    content: str = _SBOM,
) -> Path:
    """Create a wheel containing one SBOM file and return its path."""
    wheels_dir.mkdir(parents=True, exist_ok=True)
    wheel = wheels_dir / name
    _write_wheel(wheel, {sbom_member: content})
    return wheel


def _output_name(
    wheel: Path,
    member: str,
    wheels_dir: Path | None = None,
) -> str:
    """Return the expected extracted SBOM filename for *wheel* and *member*."""
    identity = _wheel_identity(wheel, wheels_dir) if wheels_dir is not None else wheel.name
    return _sbom_output_name(identity, member)


def test_extract_sboms_from_wheel_one_sbom(tmp_path: Path) -> None:
    """Extract a single SBOM from a wheel."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = _wheel_with_sbom(tmp_path / "files")

    count = extract_sboms_from_wheel(wheel, sboms_dir)

    assert count == 1
    out = sboms_dir / _output_name(wheel, "test_package-1.0.0.dist-info/sboms/sbom.spdx.json")
    assert out.read_text(encoding="utf-8") == _SBOM


def test_extract_sboms_from_wheel_multiple_sboms(tmp_path: Path) -> None:
    """Extract every SBOM file from a wheel that contains more than one."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
    _write_wheel(
        wheel,
        {
            "pkg-1.0.dist-info/sboms/sbom.spdx.json": _SBOM,
            "pkg-1.0.dist-info/sboms/sbom.cdx.json": '{"bomFormat":"CycloneDX"}',
        },
    )

    count = extract_sboms_from_wheel(wheel, sboms_dir)

    assert count == 2
    assert (
        sboms_dir / _output_name(wheel, "pkg-1.0.dist-info/sboms/sbom.spdx.json")
    ).is_file()
    assert (sboms_dir / _output_name(wheel, "pkg-1.0.dist-info/sboms/sbom.cdx.json")).is_file()


def test_extract_sboms_from_wheel_skips_directory_entries(tmp_path: Path) -> None:
    """Ignore zip directory entries under ``.dist-info/sboms/``."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
    _write_wheel(
        wheel,
        members={"pkg-1.0.dist-info/sboms/sbom.spdx.json": _SBOM},
        directory_entries=("pkg-1.0.dist-info/sboms/",),
    )

    count = extract_sboms_from_wheel(wheel, sboms_dir)

    assert count == 1
    assert list(sboms_dir.iterdir()) == [
        sboms_dir / _output_name(wheel, "pkg-1.0.dist-info/sboms/sbom.spdx.json")
    ]


def test_extract_sboms_from_wheel_rejects_malformed_archive(tmp_path: Path) -> None:
    """Wrap a non-zip wheel as CheckStepError."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
    wheel.write_text("not-a-zip", encoding="utf-8")

    with pytest.raises(tekton.CheckStepError) as exc_info:
        extract_sboms_from_wheel(wheel, sboms_dir)

    assert exc_info.value.action == "extracting SBOMs from wheels"
    assert isinstance(exc_info.value.cause, zipfile.BadZipFile)
    assert list(sboms_dir.iterdir()) == []


def test_extract_sboms_from_wheel_wraps_encrypted_member(
    tmp_path: Path,
) -> None:
    """Wrap a ZIP member that cannot be opened as CheckStepError."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = _wheel_with_sbom(tmp_path / "files")
    real_getinfo = zipfile.ZipFile.getinfo

    def encrypted(self: zipfile.ZipFile, name: str) -> zipfile.ZipInfo:
        info = real_getinfo(self, name)
        info.flag_bits |= 0x1
        return info

    with (
        patch.object(zipfile.ZipFile, "getinfo", encrypted),
        pytest.raises(tekton.CheckStepError, match="encrypted") as exc_info,
    ):
        extract_sboms_from_wheel(wheel, sboms_dir)

    assert exc_info.value.action == "extracting SBOMs from wheels"
    assert isinstance(exc_info.value.cause, RuntimeError)
    assert exc_info.value.__cause__ is exc_info.value.cause
    assert list(sboms_dir.iterdir()) == []


def test_extract_sboms_from_wheel_rejects_oversized_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a ZIP member whose declared uncompressed size exceeds the limit."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = _wheel_with_sbom(tmp_path / "files")
    monkeypatch.setattr(f"{TASK}._MAX_SBOM_UNCOMPRESSED_BYTES", 10)

    with pytest.raises(tekton.CheckStepError, match="uncompressed bytes") as exc_info:
        extract_sboms_from_wheel(wheel, sboms_dir)

    assert exc_info.value.action == "extracting SBOMs from wheels"
    assert isinstance(exc_info.value.cause, ValueError)
    assert list(sboms_dir.iterdir()) == []


def test_extract_sboms_from_wheel_rejects_streamed_oversize(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a member whose stream exceeds the limit despite a small ZipInfo size."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = _wheel_with_sbom(tmp_path / "files")
    monkeypatch.setattr(f"{TASK}._MAX_SBOM_UNCOMPRESSED_BYTES", 10)
    real_getinfo = zipfile.ZipFile.getinfo
    calls = {"n": 0}

    def lie(self: zipfile.ZipFile, name: str) -> object:
        info = real_getinfo(self, name)
        calls["n"] += 1
        if calls["n"] == 1:
            return type("ZipInfo", (), {"file_size": 1})()
        return info

    with (
        patch.object(zipfile.ZipFile, "getinfo", lie),
        pytest.raises(tekton.CheckStepError, match="read data exceeds") as exc_info,
    ):
        extract_sboms_from_wheel(wheel, sboms_dir)

    assert exc_info.value.action == "extracting SBOMs from wheels"
    assert isinstance(exc_info.value.cause, ValueError)
    assert list(sboms_dir.iterdir()) == []


def test_extract_sboms_from_wheel_no_sboms(tmp_path: Path) -> None:
    """Return 0 when the wheel has no ``.dist-info/sboms/`` files."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = tmp_path / "empty-1.0-py3-none-any.whl"
    _write_wheel(wheel, {"empty-1.0.dist-info/METADATA": "Name: empty"})

    assert extract_sboms_from_wheel(wheel, sboms_dir) == 0
    assert list(sboms_dir.iterdir()) == []


def test_extract_sboms_from_wheel_ignores_unrelated_paths(tmp_path: Path) -> None:
    """Do not treat files outside ``.dist-info/sboms/`` as SBOMs."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
    _write_wheel(
        wheel,
        {
            "pkg-1.0.dist-info/sboms": "not-a-dir-file",
            "other/sboms/sbom.spdx.json": _SBOM,
        },
    )

    assert extract_sboms_from_wheel(wheel, sboms_dir) == 0


def test_extract_sboms_from_wheel_nested_sbom(tmp_path: Path) -> None:
    """Extract an SBOM nested under ``.dist-info/sboms/``."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
    _write_wheel(
        wheel,
        {"pkg-1.0.dist-info/sboms/nested/sbom.spdx.json": _SBOM},
    )

    count = extract_sboms_from_wheel(wheel, sboms_dir)

    assert count == 1
    assert (
        sboms_dir / _output_name(wheel, "pkg-1.0.dist-info/sboms/nested/sbom.spdx.json")
    ).read_text(encoding="utf-8") == _SBOM


def test_sbom_output_name_stays_within_filename_limit() -> None:
    """Keep hashed output names under the filesystem component-length limit."""
    member = "y" * 400 + "/sbom.spdx.json"
    first = _sbom_output_name("x" * 400, member)
    second = _sbom_output_name("z" * 400, member)
    assert first != second
    assert first.endswith(".json")
    assert second.endswith(".json")
    assert len(first.encode()) <= _FILENAME_MAX_BYTES
    assert len(second.encode()) <= _FILENAME_MAX_BYTES


def test_sbom_output_name_keeps_json_suffix_for_long_basename() -> None:
    """Keep ``.json`` when the member basename exceeds the readable suffix."""
    member = ("n" * (_READABLE_SUFFIX_BYTES + 20)) + ".spdx.json"
    assert len(Path(member).name.encode()) > _READABLE_SUFFIX_BYTES
    name = _sbom_output_name("pkg.whl", member)
    assert name.endswith(".json")
    assert len(name.encode()) <= _FILENAME_MAX_BYTES


def test_wheel_identity_distinguishes_hyphen_and_nested_paths(tmp_path: Path) -> None:
    """Keep distinct identities for paths that collide after slash-to-hyphen."""
    wheels_dir = tmp_path / "files"
    hyphen = wheels_dir / "foo-bar.whl"
    nested = wheels_dir / "foo" / "bar.whl"
    hyphen.parent.mkdir(parents=True)
    nested.parent.mkdir(parents=True)
    hyphen.touch()
    nested.touch()

    assert _wheel_identity(hyphen, wheels_dir) != _wheel_identity(nested, wheels_dir)


def test_run_discovers_nested_wheels(tmp_path: Path) -> None:
    """Discover a wheel stored under a subdirectory of the files directory."""
    files_dir = tmp_path / "files"
    _wheel_with_sbom(files_dir / "oci" / "python")

    found = run(tmp_path, "files")

    assert found == 1
    sboms = tmp_path / "sboms"
    wheel = files_dir / "oci" / "python" / "test_package-1.0.0-py3-none-any.whl"
    assert (
        sboms
        / _output_name(
            wheel,
            "test_package-1.0.0.dist-info/sboms/sbom.spdx.json",
            files_dir,
        )
    ).is_file()


def test_run_keeps_distinct_sboms_from_same_named_nested_wheels(tmp_path: Path) -> None:
    """Retain both SBOMs when same-named wheels live in different directories."""
    files_dir = tmp_path / "files"
    linux_sbom = json.dumps({"spdxVersion": "SPDX-2.3", "name": "linux-sbom"})
    darwin_sbom = json.dumps({"spdxVersion": "SPDX-2.3", "name": "darwin-sbom"})
    _wheel_with_sbom(files_dir / "linux", content=linux_sbom)
    _wheel_with_sbom(files_dir / "darwin", content=darwin_sbom)

    found = run(tmp_path, "files")

    sboms = tmp_path / "sboms"
    wheel_name = "test_package-1.0.0-py3-none-any.whl"
    member = "test_package-1.0.0.dist-info/sboms/sbom.spdx.json"
    linux_out = sboms / _output_name(files_dir / "linux" / wheel_name, member, files_dir)
    darwin_out = sboms / _output_name(files_dir / "darwin" / wheel_name, member, files_dir)
    assert found == 2
    assert found == len(list(sboms.iterdir()))
    assert linux_out.read_text(encoding="utf-8") == linux_sbom
    assert darwin_out.read_text(encoding="utf-8") == darwin_sbom


def test_run_retains_sboms_when_slash_to_hyphen_prefixes_collide(
    tmp_path: Path,
) -> None:
    """Keep both SBOMs when slash-to-hyphen prefixes would have collided."""
    files_dir = tmp_path / "files"
    hyphen_sbom = json.dumps({"spdxVersion": "SPDX-2.3", "name": "hyphen-root"})
    nested_sbom = json.dumps({"spdxVersion": "SPDX-2.3", "name": "nested-path"})
    hyphen_wheel = _wheel_with_sbom(files_dir, name="foo-bar.whl", content=hyphen_sbom)
    nested_wheel = _wheel_with_sbom(files_dir / "foo", name="bar.whl", content=nested_sbom)

    found = run(tmp_path, "files")

    sboms = tmp_path / "sboms"
    member = "test_package-1.0.0.dist-info/sboms/sbom.spdx.json"
    hyphen_out = sboms / _output_name(hyphen_wheel, member, files_dir)
    nested_out = sboms / _output_name(nested_wheel, member, files_dir)
    assert found == 2
    assert found == len(list(sboms.iterdir()))
    assert hyphen_out.read_text(encoding="utf-8") == hyphen_sbom
    assert nested_out.read_text(encoding="utf-8") == nested_sbom
    assert hyphen_out.name != nested_out.name


def test_run_extracts_deeply_nested_wheel_with_bounded_names(tmp_path: Path) -> None:
    """Extract a deep wheel whose encoded relative path exceeds 255 bytes."""
    files_dir = tmp_path / "files"
    segment = "nested-directory-name"
    deep = files_dir
    for _ in range(20):
        deep = deep / segment
    first = json.dumps({"spdxVersion": "SPDX-2.3", "name": "deep-one"})
    second = json.dumps({"spdxVersion": "SPDX-2.3", "name": "deep-two"})
    first_wheel = _wheel_with_sbom(deep / "one", content=first)
    second_wheel = _wheel_with_sbom(deep / "two", content=second)
    encoded = quote(first_wheel.relative_to(files_dir).as_posix(), safe="")
    assert len(encoded.encode()) > _FILENAME_MAX_BYTES

    found = run(tmp_path, "files")

    sboms = tmp_path / "sboms"
    first_name = _output_name(first_wheel, _DEFAULT_MEMBER, files_dir)
    second_name = _output_name(second_wheel, _DEFAULT_MEMBER, files_dir)
    names = {path.name for path in sboms.iterdir()}
    assert found == 2
    assert names == {first_name, second_name}
    assert first_name != second_name
    assert all(len(name.encode()) <= _FILENAME_MAX_BYTES for name in names)
    assert (sboms / first_name).read_text(encoding="utf-8") == first
    assert (sboms / second_name).read_text(encoding="utf-8") == second


def test_extract_sboms_from_wheel_rejects_conflicting_output(tmp_path: Path) -> None:
    """Refuse to overwrite a retained SBOM with different content."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    first = _wheel_with_sbom(tmp_path / "first")
    second = _wheel_with_sbom(
        tmp_path / "second",
        content=json.dumps({"spdxVersion": "SPDX-2.3", "name": "other"}),
    )
    written: dict[Path, tuple[str, int]] = {}

    assert extract_sboms_from_wheel(first, sboms_dir, written=written) == 1
    with pytest.raises(tekton.CheckStepError, match="Conflicting SBOM output path"):
        extract_sboms_from_wheel(second, sboms_dir, written=written)
    digest, size = next(iter(written.values()))
    assert size == len(_SBOM.encode())
    assert digest == hashlib.sha256(_SBOM.encode()).hexdigest()


def test_extract_sboms_from_wheel_skips_identical_retained(tmp_path: Path) -> None:
    """Skip a later wheel whose SBOM hashes to the same retained output."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    first = _wheel_with_sbom(tmp_path / "first")
    second = _wheel_with_sbom(tmp_path / "second")
    written: dict[Path, tuple[str, int]] = {}

    assert extract_sboms_from_wheel(first, sboms_dir, written=written) == 1
    assert extract_sboms_from_wheel(second, sboms_dir, written=written) == 0
    assert len(list(sboms_dir.iterdir())) == 1


def test_extract_sboms_from_wheel_skips_identical_existing_file(tmp_path: Path) -> None:
    """Hash an on-disk SBOM when the retained map does not yet include it."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    first = _wheel_with_sbom(tmp_path / "first")
    second = _wheel_with_sbom(tmp_path / "second")

    assert extract_sboms_from_wheel(first, sboms_dir) == 1
    assert extract_sboms_from_wheel(second, sboms_dir) == 0
    assert len(list(sboms_dir.iterdir())) == 1


def test_run_extracts_from_matching_wheels(tmp_path: Path) -> None:
    """run() extracts SBOMs and skips wheels that have none."""
    files_dir = tmp_path / "files"
    _wheel_with_sbom(files_dir)
    _write_wheel(
        files_dir / "nosbom-1.0-py3-none-any.whl",
        {"nosbom-1.0.dist-info/METADATA": "Name: nosbom"},
    )

    found = run(tmp_path, "files")

    assert found == 1
    sboms = tmp_path / "sboms"
    assert (
        sboms / _output_name(files_dir / _DEFAULT_WHEEL, _DEFAULT_MEMBER, files_dir)
    ).is_file()


def test_run_rejects_symlink_wheel_outside_files_dir(tmp_path: Path) -> None:
    """Reject a nested wheel symlink whose target is outside the files directory."""
    outside = _wheel_with_sbom(tmp_path / "outside")
    files_dir = tmp_path / "files"
    nested = files_dir / "nested"
    nested.mkdir(parents=True)
    (nested / "evil.whl").symlink_to(outside)

    with pytest.raises(tekton.CheckStepError, match="source path must stay under"):
        run(tmp_path, "files")

    assert not (tmp_path / "sboms").exists()


def test_run_ignores_wheel_behind_directory_symlink(tmp_path: Path) -> None:
    """Do not discover a wheel that is reachable only through a directory symlink."""
    outside_dir = tmp_path / "outside"
    _wheel_with_sbom(outside_dir)
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    (files_dir / "linked").symlink_to(outside_dir)

    with pytest.raises(tekton.CheckStepError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")

    assert not (tmp_path / "sboms").exists()


def test_run_skips_non_file_whl_paths(tmp_path: Path) -> None:
    """run() ignores directories whose names end in ``.whl``."""
    files_dir = tmp_path / "files"
    _wheel_with_sbom(files_dir)
    (files_dir / "not-a-file.whl").mkdir()

    found = run(tmp_path, "files")

    assert found == 1


def test_run_no_wheels_raises(tmp_path: Path) -> None:
    """run() fails when the files directory has no wheels."""
    (tmp_path / "files").mkdir()

    with pytest.raises(tekton.CheckStepError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")


def test_run_rejects_malformed_wheel(tmp_path: Path) -> None:
    """Wrap a malformed wheel discovered by run() as CheckStepError."""
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    (files_dir / "broken-1.0-py3-none-any.whl").write_text("not-a-zip", encoding="utf-8")

    with pytest.raises(tekton.CheckStepError) as exc_info:
        run(tmp_path, "files")

    assert exc_info.value.action == "extracting SBOMs from wheels"
    assert isinstance(exc_info.value.cause, zipfile.BadZipFile)
    assert not (tmp_path / "sboms").exists()


def test_run_no_sboms_raises(tmp_path: Path) -> None:
    """run() fails when wheels exist but none contain SBOMs."""
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    _write_wheel(
        files_dir / "empty-1.0-py3-none-any.whl",
        {"empty-1.0.dist-info/METADATA": "Name: empty"},
    )

    with pytest.raises(tekton.CheckStepError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")


def test_run_creates_sboms_dir(tmp_path: Path) -> None:
    """run() creates the sboms directory when it does not exist."""
    _wheel_with_sbom(tmp_path / "files")

    run(tmp_path, "files")

    assert (tmp_path / "sboms").is_dir()


def test_run_rerun_regenerates_existing_outputs(tmp_path: Path) -> None:
    """Count regenerated SBOMs when run() is invoked again unchanged."""
    _wheel_with_sbom(tmp_path / "files")
    first = run(tmp_path, "files")
    leftover = tmp_path / "sboms" / "stale.spdx.json"
    leftover.write_text("old", encoding="utf-8")

    second = run(tmp_path, "files")

    assert first == 1
    assert second == 1
    assert not leftover.exists()
    assert (
        tmp_path
        / "sboms"
        / _output_name(
            tmp_path / "files" / _DEFAULT_WHEEL, _DEFAULT_MEMBER, tmp_path / "files"
        )
    ).is_file()


def test_run_failed_rerun_keeps_previous_output(tmp_path: Path) -> None:
    """Keep the last successful sboms directory when a later run finds none."""
    files_dir = tmp_path / "files"
    _wheel_with_sbom(files_dir)
    run(tmp_path, "files")
    previous = (
        tmp_path
        / "sboms"
        / _output_name(files_dir / _DEFAULT_WHEEL, _DEFAULT_MEMBER, files_dir)
    )
    previous_text = previous.read_text(encoding="utf-8")

    for wheel in files_dir.glob("*.whl"):
        wheel.unlink()
    _write_wheel(
        files_dir / "empty-1.0-py3-none-any.whl",
        {"empty-1.0.dist-info/METADATA": "Name: empty"},
    )

    with pytest.raises(tekton.CheckStepError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")

    assert previous.read_text(encoding="utf-8") == previous_text
    assert list(tmp_path.glob(".sboms-*")) == []


@pytest.mark.parametrize("error", [ValueError("not a directory"), OSError("replace failed")])
def test_run_wraps_replace_directory_errors(tmp_path: Path, error: Exception) -> None:
    """Wrap directory publication failures as CheckStepError."""
    files_dir = tmp_path / "files"
    _wheel_with_sbom(files_dir)
    sboms = tmp_path / "sboms"
    sboms.mkdir()
    previous = sboms / "keep.json"
    previous.write_text("previous", encoding="utf-8")

    with (
        patch(f"{TASK}.file.replace_directory", side_effect=error),
        pytest.raises(tekton.CheckStepError, match="extracting SBOMs from wheels") as exc_info,
    ):
        run(tmp_path, "files")

    assert exc_info.value.cause is error
    assert exc_info.value.__cause__ is error
    assert previous.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.glob(".sboms-*")) == []


def test_run_collision_keeps_previous_output(tmp_path: Path) -> None:
    """Keep the last successful sboms directory when a later run collides."""
    files_dir = tmp_path / "files"
    _wheel_with_sbom(files_dir)
    run(tmp_path, "files")
    previous = (
        tmp_path
        / "sboms"
        / _output_name(files_dir / _DEFAULT_WHEEL, _DEFAULT_MEMBER, files_dir)
    )
    previous_text = previous.read_text(encoding="utf-8")

    for wheel in files_dir.glob("*.whl"):
        wheel.unlink()
    _wheel_with_sbom(files_dir / "one", content=_SBOM)
    _wheel_with_sbom(
        files_dir / "two",
        content=json.dumps({"spdxVersion": "SPDX-2.3", "name": "other"}),
    )

    with (
        patch(f"{TASK}._sbom_output_name", return_value="same-sbom.spdx.json"),
        pytest.raises(tekton.CheckStepError, match="Conflicting SBOM output path"),
    ):
        run(tmp_path, "files")

    assert previous.read_text(encoding="utf-8") == previous_text
    assert list(tmp_path.glob(".sboms-*")) == []


def test_run_missing_files_dir_raises(tmp_path: Path) -> None:
    """run() fails when the wheels directory does not exist."""
    with pytest.raises(tekton.CheckStepError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")


def test_run_rejects_path_traversal(tmp_path: Path) -> None:
    """run() rejects a files_dir that escapes data_dir."""
    with pytest.raises(tekton.CheckStepError, match="path must stay under"):
        run(tmp_path, "../outside")


def test_run_rejects_wheels_dir_equal_to_sboms_dir(tmp_path: Path) -> None:
    """Do not delete wheel inputs when files_dir is the sboms output directory."""
    wheel = _wheel_with_sbom(tmp_path / "sboms")

    with pytest.raises(tekton.CheckStepError, match="sboms output directory"):
        run(tmp_path, "sboms")

    assert wheel.is_file()


def test_run_rejects_wheels_dir_under_sboms_dir(tmp_path: Path) -> None:
    """Do not delete wheel inputs when files_dir sits under the sboms directory."""
    wheel = _wheel_with_sbom(tmp_path / "sboms" / "wheels")

    with pytest.raises(tekton.CheckStepError, match="sboms output directory"):
        run(tmp_path, "sboms/wheels")

    assert wheel.is_file()


def test_run_rejects_sboms_symlink_to_in_tree_directory(tmp_path: Path) -> None:
    """Fail when data_dir/sboms is a symlink without changing either path."""
    files_dir = tmp_path / "files"
    _wheel_with_sbom(files_dir)
    keep = tmp_path / "keep"
    keep.mkdir()
    marker = keep / "important.txt"
    marker.write_text("do-not-delete", encoding="utf-8")
    sboms = tmp_path / "sboms"
    sboms.symlink_to("keep")

    with pytest.raises(tekton.CheckStepError, match="sboms output path must be a directory"):
        run(tmp_path, "files")

    assert sboms.is_symlink()
    assert sboms.readlink() == Path("keep")
    assert marker.read_text(encoding="utf-8") == "do-not-delete"
    assert list(keep.iterdir()) == [marker]
    assert list(tmp_path.glob(".sboms-*")) == []


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads env and calls run()."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_FILES_DIR", "files")

    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0

    mock_run.assert_called_once_with(data_dir=tmp_path, files_dir="files")


def test_main_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when a required env var is missing."""
    monkeypatch.delenv("PARAM_DATA_DIR", raising=False)
    monkeypatch.delenv("PARAM_FILES_DIR", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_missing_files_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when PARAM_FILES_DIR is unset."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("PARAM_FILES_DIR", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_dunder_main_invokes_main() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.managed.extract_sboms_from_wheels"
    with patch(f"{TASK}.main", return_value=0) as mock_main:
        with pytest.raises(SystemExit) as exc:
            runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()
