"""Tests for extract_sboms_from_wheels."""

from __future__ import annotations

import json
import runpy
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.tasks.managed.extract_sboms_from_wheels.extract_sboms_from_wheels import (  # noqa: E501
    extract_sboms_from_wheel,
    main,
    run,
)

TASK = (
    "release_service_utils.tasks.managed"
    ".extract_sboms_from_wheels.extract_sboms_from_wheels"
)

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
    name: str = "test_package-1.0.0-py3-none-any.whl",
    sbom_member: str = "test_package-1.0.0.dist-info/sboms/sbom.spdx.json",
    content: str = _SBOM,
) -> Path:
    """Create a wheel containing one SBOM file and return its path."""
    wheels_dir.mkdir(parents=True, exist_ok=True)
    wheel = wheels_dir / name
    _write_wheel(wheel, {sbom_member: content})
    return wheel


def test_extract_sboms_from_wheel_one_sbom(tmp_path: Path) -> None:
    """Extract a single SBOM from a wheel."""
    sboms_dir = tmp_path / "sboms"
    sboms_dir.mkdir()
    wheel = _wheel_with_sbom(tmp_path / "files")

    count = extract_sboms_from_wheel(wheel, sboms_dir)

    assert count == 1
    out = sboms_dir / "test_package-1.0.0-py3-none-any-sbom.spdx.json"
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
    assert (sboms_dir / "pkg-1.0-py3-none-any-sbom.spdx.json").is_file()
    assert (sboms_dir / "pkg-1.0-py3-none-any-sbom.cdx.json").is_file()


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
    assert list(sboms_dir.iterdir()) == [sboms_dir / "pkg-1.0-py3-none-any-sbom.spdx.json"]


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
    assert (sboms_dir / "pkg-1.0-py3-none-any-sbom.spdx.json").read_text(
        encoding="utf-8"
    ) == _SBOM


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
    assert (sboms / "test_package-1.0.0-py3-none-any-sbom.spdx.json").is_file()


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

    with pytest.raises(RuntimeError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")


def test_run_no_sboms_raises(tmp_path: Path) -> None:
    """run() fails when wheels exist but none contain SBOMs."""
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    _write_wheel(
        files_dir / "empty-1.0-py3-none-any.whl",
        {"empty-1.0.dist-info/METADATA": "Name: empty"},
    )

    with pytest.raises(RuntimeError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")


def test_run_creates_sboms_dir(tmp_path: Path) -> None:
    """run() creates the sboms directory when it does not exist."""
    _wheel_with_sbom(tmp_path / "files")

    run(tmp_path, "files")

    assert (tmp_path / "sboms").is_dir()


def test_run_missing_files_dir_raises(tmp_path: Path) -> None:
    """run() fails when the wheels directory does not exist."""
    with pytest.raises(RuntimeError, match="No SBOMs found in any wheel"):
        run(tmp_path, "files")


def test_run_rejects_path_traversal(tmp_path: Path) -> None:
    """run() rejects a files_dir that escapes data_dir."""
    with pytest.raises(ValueError, match="path must stay under"):
        run(tmp_path, "../outside")


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
