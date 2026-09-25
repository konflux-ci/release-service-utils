"""Test extract_py_artifacts."""

from __future__ import annotations

import base64
import json
import os
import runpy
import subprocess
import urllib.parse
import zipfile
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.helpers import tekton
from release_service_utils.tasks.managed.extract_py_artifacts import (
    extract_py_artifacts as epa,
)
from release_service_utils.tasks.managed.extract_sboms_from_wheels import (
    run as extract_sboms_from_wheels,
)
from release_service_utils.tasks.managed.extract_sboms_from_wheels.extract_sboms_from_wheels import (  # noqa: E501
    _sbom_output_name,
)

TASK = "release_service_utils.tasks.managed.extract_py_artifacts.extract_py_artifacts"


def _sha256(prefix: str) -> str:
    """Return a 64-hex ``sha256:`` digest that starts with *prefix*."""
    return "sha256:" + prefix.ljust(64, "0")


_SHA256_1 = _sha256("1")
_SHA256_2 = _sha256("2")
_SHA256_ABC = _sha256("abc")
_SHA256_ABC123 = _sha256("abc123")

_SBOM = {
    "spdxVersion": "SPDX-2.3",
    "packages": [
        {
            "name": "test-package",
            "externalRefs": [
                {
                    "referenceCategory": "PACKAGE-MANAGER",
                    "referenceType": "purl",
                    "referenceLocator": "pkg:pypi/test_package@1.0.0",
                }
            ],
        }
    ],
}

_STATEMENT = {
    "_type": "https://in-toto.io/Statement/v0.1",
    "predicateType": "https://slsa.dev/provenance/v1",
    "predicate": {"buildDefinition": {"buildType": "https://tekton.dev/chains/v2/slsa"}},
}


def _write_json(path: Path, payload: dict) -> None:
    """Write *payload* as JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_wheel(
    path: Path,
    members: dict[str, str] | None = None,
) -> Path:
    """Write a zip wheel at *path* and return it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in (members or {}).items():
            archive.writestr(name, content)
    return path


def _wheel_with_sbom(
    directory: Path,
    filename: str = "test_package-1.0.0-py3-none-any.whl",
    sbom: dict | None = None,
    sbom_member: str = "test_package-1.0.0.dist-info/sboms/redhat.spdx.json",
) -> Path:
    """Create a wheel containing a Red Hat SPDX SBOM."""
    return _write_wheel(
        directory / filename,
        {sbom_member: json.dumps(sbom or _SBOM)},
    )


def _statement_for(image: str, **updates: object) -> dict:
    """Return a SLSA statement whose subject identifies *image*."""
    repository, digest = epa._image_identity(image)
    statement: dict = {
        **_STATEMENT,
        "subject": [
            {
                "name": repository,
                "digest": {"sha256": digest.removeprefix("sha256:")},
            }
        ],
    }
    statement.update(updates)
    return statement


def _envelope(statement: dict | None = None, *, image: str | None = None) -> str:
    """Return a one-line cosign DSSE envelope for *statement*."""
    if statement is None:
        statement = _statement_for(image) if image is not None else _STATEMENT
    payload = base64.b64encode(json.dumps(statement).encode()).decode()
    return json.dumps({"payloadType": "application/vnd.in-toto+json", "payload": payload})


def _provenance_with_statement(tmp_path: Path, image: str, statement: dict) -> None:
    """Fetch Chains provenance for *image* using a fixed decoded *statement*."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(statement))
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa.fetch_chains_provenance([image], tmp_path)


def _completed(
    cmd: Sequence[str | Path],
    stdout: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    """Return a CompletedProcess for mocked subprocess calls."""
    return subprocess.CompletedProcess(
        [str(part) for part in cmd],
        returncode,
        stdout=stdout,
        stderr="",
    )


def test_container_images_from_snapshot() -> None:
    """Collect non-empty containerImage values and skip other rows."""
    images = epa.container_images_from_snapshot(
        {
            "components": [
                {"name": "a", "containerImage": f"quay.io/a@{_SHA256_1}"},
                "ignored",
                {"name": "b", "containerImage": "  "},
                {"name": "c", "containerImage": f"quay.io/c@{_SHA256_2}"},
            ]
        }
    )
    assert images == [f"quay.io/a@{_SHA256_1}", f"quay.io/c@{_SHA256_2}"]


def test_container_images_from_snapshot_keeps_shared_digest_repos() -> None:
    """Keep two repositories that share one digest as distinct images."""
    images = epa.container_images_from_snapshot(
        {
            "components": [
                {"containerImage": f"quay.io/org/one@{_SHA256_ABC123}"},
                {"containerImage": f"quay.io/org/two@{_SHA256_ABC123}"},
            ]
        }
    )
    assert images == [
        f"quay.io/org/one@{_SHA256_ABC123}",
        f"quay.io/org/two@{_SHA256_ABC123}",
    ]


def test_container_images_from_snapshot_dedupes_same_repository_digest() -> None:
    """Record one image when the same repository and digest appear twice."""
    images = epa.container_images_from_snapshot(
        {
            "components": [
                {"containerImage": f"quay.io/org/repo:1.0@{_SHA256_ABC123}"},
                {"containerImage": f"quay.io/org/repo@{_SHA256_ABC123}"},
            ]
        }
    )
    assert images == [f"quay.io/org/repo:1.0@{_SHA256_ABC123}"]


def test_container_images_from_snapshot_missing_components() -> None:
    """Reject a snapshot without a components array."""
    with pytest.raises(tekton.CheckStepError, match="snapshot has no components"):
        epa.container_images_from_snapshot({})


def test_container_images_from_snapshot_no_images() -> None:
    """Reject a snapshot whose components have no images."""
    with pytest.raises(tekton.CheckStepError, match="snapshot has no containerImage values"):
        epa.container_images_from_snapshot({"components": [{"name": "a"}]})


@pytest.mark.parametrize(
    "image",
    [
        "quay.io/org/repo:latest",
        "quay.io/org/repo@latest",
        "quay.io/org/repo@sha256:",
        "quay.io/org/repo@sha256:abc",
        "quay.io/org/repo@sha256:" + "a" * 63,
        "quay.io/org/repo@sha256:" + "a" * 65,
        "@sha256:abc",
    ],
)
def test_container_images_from_snapshot_rejects_malformed_digest(image: str) -> None:
    """Reject tag-like suffixes, empty digest data, and an empty repository."""
    with pytest.raises(tekton.CheckStepError, match="digest-qualified"):
        epa.container_images_from_snapshot({"components": [{"containerImage": image}]})


def test_container_images_from_snapshot_accepts_tag_and_digest() -> None:
    """Keep a tag when the reference is still digest-qualified."""
    image = f"quay.io/org/repo:1.0@{_SHA256_ABC}"
    assert epa.container_images_from_snapshot({"components": [{"containerImage": image}]}) == [
        image
    ]


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("test_package-1.0.0-py3-none-any.whl", ("test_package", "1.0.0", ["any"])),
        ("pkg-1.0.0-1-py3-none-any.whl", ("pkg", "1.0.0", ["any"])),
        (
            "pkg-1.0.0-py3-none-manylinux2014_x86_64.whl",
            ("pkg", "1.0.0", ["manylinux2014_x86_64"]),
        ),
        ("My_Pkg-1.0.0-py3-none-any.whl", ("My_Pkg", "1.0.0", ["any"])),
        (
            "pkg-1.0.0-py3-none-manylinux2014_x86_64.manylinux_2_17_x86_64.whl",
            (
                "pkg",
                "1.0.0",
                ["manylinux2014_x86_64", "manylinux_2_17_x86_64"],
            ),
        ),
        (
            "pkg-1.0.0-py3-none-manylinux2014_x86_64.manylinux2014_aarch64.whl",
            (
                "pkg",
                "1.0.0",
                ["manylinux2014_aarch64", "manylinux2014_x86_64"],
            ),
        ),
    ],
)
def test_parse_wheel_filename(filename: str, expected: tuple[str, str, list[str]]) -> None:
    """Parse valid PEP 427 wheel filenames via packaging.utils."""
    assert epa.parse_wheel_filename(filename) == expected


@pytest.mark.parametrize(
    "filename",
    ["not-a-wheel", "a-b-c-d.whl", "a-b-c-d-e-f-g.whl"],
)
def test_parse_wheel_filename_invalid(filename: str) -> None:
    """Reject unparseable wheel filenames."""
    with pytest.raises(tekton.CheckStepError, match="Cannot parse wheel filename"):
        epa.parse_wheel_filename(filename)


@pytest.mark.parametrize(
    ("platform", "expected"),
    [
        ("any", [("noarch", "any")]),
        ("none", [("noarch", "any")]),
        ("manylinux2014_x86_64", [("x86_64", "linux")]),
        ("musllinux_1_1_aarch64", [("aarch64", "linux")]),
        ("linux_x86_64", [("x86_64", "linux")]),
        ("manylinux_2_17_ppc64le", [("ppc64le", "linux")]),
        ("macosx_10_9_x86_64", [("x86_64", "darwin")]),
        ("macosx_11_0_arm64", [("arm64", "darwin")]),
        (
            "macosx_11_0_universal2",
            [("arm64", "darwin"), ("x86_64", "darwin")],
        ),
        (
            "universal2",
            [("arm64", "darwin"), ("x86_64", "darwin")],
        ),
        ("win_amd64", [("amd64", "windows")]),
        ("win32", [("win32", "windows")]),
        ("freebsd_14_x86_64", [("x86_64", "freebsd")]),
        ("freebsd_12_0_release_amd64", [("amd64", "freebsd")]),
    ],
)
def test_parse_wheel_platform(platform: str, expected: list[tuple[str, str]]) -> None:
    """Map platform tags to architecture and OS targets."""
    assert epa.parse_wheel_platform(platform) == expected


def test_parse_wheel_platform_unsupported() -> None:
    """Reject platform-specific tags whose operating system is unknown."""
    with pytest.raises(
        tekton.CheckStepError, match="Unsupported wheel platform tag"
    ) as exc_info:
        epa.parse_wheel_platform("other_s390x")
    assert exc_info.value.action == "parsing wheel platform"
    assert isinstance(exc_info.value.cause, ValueError)


def test_extract_sbom_from_wheel(tmp_path: Path) -> None:
    """Read the Red Hat SPDX SBOM from a wheel zip."""
    wheel = _wheel_with_sbom(tmp_path)
    assert epa.extract_sbom_from_wheel(wheel, "test_package", "1.0.0") == _SBOM


def test_extract_sbom_from_wheel_mixed_case_dist_info(tmp_path: Path) -> None:
    """Match dist-info by normalized name when the archive keeps mixed case."""
    wheel = _wheel_with_sbom(
        tmp_path,
        filename="My_Pkg-1.0.0-py3-none-any.whl",
        sbom_member="My_Pkg-1.0.0.dist-info/sboms/redhat.spdx.json",
    )
    assert epa.extract_sbom_from_wheel(wheel, "my_pkg", "1.0.0") == _SBOM


def test_extract_sbom_from_wheel_missing(tmp_path: Path) -> None:
    """Fail when the expected SBOM path is absent."""
    wheel = _write_wheel(tmp_path / "pkg-1.0.0-py3-none-any.whl", {"METADATA": "x"})
    with pytest.raises(tekton.CheckStepError, match="SBOM not found in wheel"):
        epa.extract_sbom_from_wheel(wheel, "pkg", "1.0.0")


def test_extract_pypi_purl() -> None:
    """Return the pkg:pypi PURL that matches the wheel name and version."""
    assert (
        epa.extract_pypi_purl(_SBOM, "test_package", "1.0.0", "wheel.whl")
        == "pkg:pypi/test_package@1.0.0"
    )


def test_extract_pypi_purl_missing() -> None:
    """Fail when the SBOM has no pkg:pypi PURL."""
    with pytest.raises(tekton.CheckStepError, match="No pkg:pypi PURL found"):
        epa.extract_pypi_purl(
            {"packages": [{"externalRefs": []}]}, "pkg", "1.0.0", "wheel.whl"
        )


def test_extract_pypi_purl_packages_not_list() -> None:
    """Treat a non-list packages field as empty."""
    with pytest.raises(tekton.CheckStepError, match="No pkg:pypi PURL found"):
        epa.extract_pypi_purl({"packages": "nope"}, "pkg", "1.0.0", "wheel.whl")


def test_extract_pypi_purl_skips_non_pypi_refs() -> None:
    """Ignore non-object rows, non-purl refs, and non-pypi locators."""
    sbom = {
        "packages": [
            "skip",
            {"externalRefs": "nope"},
            {
                "externalRefs": [
                    "skip",
                    {"referenceType": "cpe", "referenceLocator": "cpe:/a"},
                    {"referenceType": "purl", "referenceLocator": "pkg:generic/foo"},
                    {
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/real@1",
                    },
                ]
            },
        ]
    }
    assert epa.extract_pypi_purl(sbom, "real", "1", "wheel.whl") == "pkg:pypi/real@1"


def test_extract_pypi_purl_skips_preceding_dependency() -> None:
    """Ignore a dependency PURL that appears before the wheel package."""
    sbom = {
        "packages": [
            {
                "externalRefs": [
                    {
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/requests@2.0.0",
                    }
                ]
            },
            {
                "externalRefs": [
                    {
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/test-package@1.0.0",
                    }
                ]
            },
        ]
    }
    assert (
        epa.extract_pypi_purl(sbom, "test_package", "1.0.0", "wheel.whl")
        == "pkg:pypi/test-package@1.0.0"
    )


def test_extract_sbom_from_wheel_invalid_root(tmp_path: Path) -> None:
    """Reject an SBOM whose JSON root is not an object."""
    wheel = _write_wheel(
        tmp_path / "pkg-1.0.0-py3-none-any.whl",
        {"pkg-1.0.0.dist-info/sboms/redhat.spdx.json": "[1]"},
    )
    with pytest.raises(tekton.CheckStepError, match="SBOM root must be an object"):
        epa.extract_sbom_from_wheel(wheel, "pkg", "1.0.0")


def test_extract_sbom_from_wheel_rejects_oversized_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a ZIP member whose uncompressed size exceeds the limit."""
    wheel = _wheel_with_sbom(tmp_path)
    monkeypatch.setattr(epa, "_MAX_SBOM_UNCOMPRESSED_BYTES", 10)

    with pytest.raises(tekton.CheckStepError, match="uncompressed bytes") as exc_info:
        epa.extract_sbom_from_wheel(wheel, "test_package", "1.0.0")

    assert exc_info.value.action == "extracting SBOM from wheel"
    assert isinstance(exc_info.value.cause, ValueError)


def test_extract_sbom_from_wheel_rejects_malformed_archive(tmp_path: Path) -> None:
    """Wrap a non-zip wheel as CheckStepError."""
    wheel = tmp_path / "pkg-1.0.0-py3-none-any.whl"
    wheel.write_text("not-a-zip", encoding="utf-8")

    with pytest.raises(tekton.CheckStepError) as exc_info:
        epa.extract_sbom_from_wheel(wheel, "pkg", "1.0.0")

    assert exc_info.value.action == "extracting SBOM from wheel"
    assert isinstance(exc_info.value.cause, zipfile.BadZipFile)


def test_extract_sbom_from_wheel_rejects_invalid_json(tmp_path: Path) -> None:
    """Wrap malformed SBOM JSON as CheckStepError."""
    wheel = _write_wheel(
        tmp_path / "pkg-1.0.0-py3-none-any.whl",
        {"pkg-1.0.0.dist-info/sboms/redhat.spdx.json": "{not-json"},
    )

    with pytest.raises(tekton.CheckStepError) as exc_info:
        epa.extract_sbom_from_wheel(wheel, "pkg", "1.0.0")

    assert exc_info.value.action == "extracting SBOM from wheel"
    assert isinstance(exc_info.value.cause, json.JSONDecodeError)


def test_extract_sbom_from_wheel_rejects_decompressed_oversize(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wrap a bounded-reader limit violation as CheckStepError."""
    wheel = _wheel_with_sbom(tmp_path)

    def boom(*args: object, **kwargs: object) -> bytes:
        del args, kwargs
        raise ValueError("read data exceeds 10 bytes")

    monkeypatch.setattr(epa.file, "read_bounded", boom)

    with pytest.raises(tekton.CheckStepError, match="read data exceeds") as exc_info:
        epa.extract_sbom_from_wheel(wheel, "test_package", "1.0.0")

    assert exc_info.value.action == "extracting SBOM from wheel"
    assert isinstance(exc_info.value.cause, ValueError)


def test_collect_wheel_artifacts_happy_path(tmp_path: Path) -> None:
    """Build one artifact row from a valid wheel (catalog happy path)."""
    _wheel_with_sbom(tmp_path)
    (tmp_path / "test_package-1.0.0.tar.gz").write_text("sdist", encoding="utf-8")

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert artifacts == [
        {
            "component": "test_package",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "noarch",
            "os": "any",
        }
    ]


def test_collect_wheel_artifacts_mixed_case_dist_info(tmp_path: Path) -> None:
    """Collect artifacts from a mixed-case wheel whose dist-info keeps that spelling."""
    sbom = {
        "spdxVersion": "SPDX-2.3",
        "packages": [
            {
                "name": "My_Pkg",
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/My_Pkg@1.0.0",
                    }
                ],
            }
        ],
    }
    _wheel_with_sbom(
        tmp_path,
        filename="My_Pkg-1.0.0-py3-none-any.whl",
        sbom=sbom,
        sbom_member="My_Pkg-1.0.0.dist-info/sboms/redhat.spdx.json",
    )

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert artifacts == [
        {
            "component": "My_Pkg",
            "purl": "pkg:pypi/My_Pkg@1.0.0",
            "architecture": "noarch",
            "os": "any",
        }
    ]


def test_flatten_package_files_materializes_nested_packages(tmp_path: Path) -> None:
    """Copy nested wheels and source distributions to the files-directory root."""
    staging = tmp_path / "staging"
    files_dir = tmp_path / "files"
    _wheel_with_sbom(staging / "oci" / "python")
    (staging / "oci" / "python" / "test_package-1.0.0.tar.gz").write_text(
        "sdist", encoding="utf-8"
    )
    (staging / "oci" / "python" / "README").write_text("skip", encoding="utf-8")

    epa._flatten_package_files(staging, files_dir)

    assert (files_dir / "test_package-1.0.0-py3-none-any.whl").is_file()
    assert (files_dir / "test_package-1.0.0.tar.gz").read_text(encoding="utf-8") == "sdist"
    assert not (files_dir / "README").exists()
    assert not (files_dir / "oci").exists()


def test_flatten_package_files_keeps_identical_duplicates(tmp_path: Path) -> None:
    """Keep one root file when two nested packages have the same bytes."""
    staging = tmp_path / "staging"
    files_dir = tmp_path / "files"
    for directory in (staging / "linux", staging / "darwin"):
        directory.mkdir(parents=True)
        (directory / "test_package-1.0.0.tar.gz").write_bytes(b"same")

    epa._flatten_package_files(staging, files_dir)

    dest = files_dir / "test_package-1.0.0.tar.gz"
    assert dest.read_bytes() == b"same"
    assert list(files_dir.iterdir()) == [dest]


def test_flatten_package_files_rejects_conflicting_names(tmp_path: Path) -> None:
    """Fail when two nested packages would overwrite different bytes at the root."""
    staging = tmp_path / "staging"
    files_dir = tmp_path / "files"
    (staging / "linux").mkdir(parents=True)
    (staging / "darwin").mkdir(parents=True)
    (staging / "linux" / "pkg.whl").write_bytes(b"linux")
    (staging / "darwin" / "pkg.whl").write_bytes(b"darwin")

    with pytest.raises(tekton.CheckStepError, match="Conflicting package file 'pkg.whl'"):
        epa._flatten_package_files(staging, files_dir)


def test_flatten_package_files_rejects_source_symlink(tmp_path: Path) -> None:
    """Reject a package symlink that points outside the source tree."""
    staging = tmp_path / "staging"
    files_dir = tmp_path / "files"
    outside = tmp_path / "outside" / "secret.whl"
    staging.mkdir()
    outside.parent.mkdir()
    outside.write_bytes(b"secret")
    (staging / "pkg.whl").symlink_to(outside)

    with pytest.raises(tekton.CheckStepError, match="source path must stay under"):
        epa._flatten_package_files(staging, files_dir)

    assert not (files_dir / "pkg.whl").exists()
    assert outside.read_bytes() == b"secret"


def test_flatten_package_files_rejects_dangling_destination_symlink(
    tmp_path: Path,
) -> None:
    """Reject a dangling destination symlink that points outside files_dir."""
    staging = tmp_path / "staging"
    files_dir = tmp_path / "files"
    outside = tmp_path / "outside" / "escaped.whl"
    staging.mkdir()
    files_dir.mkdir()
    outside.parent.mkdir()
    (staging / "pkg.whl").write_bytes(b"payload")
    dest = files_dir / "pkg.whl"
    dest.symlink_to(outside)

    with pytest.raises(tekton.CheckStepError, match="Conflicting package file 'pkg.whl'"):
        epa._flatten_package_files(staging, files_dir)

    assert dest.is_symlink()
    assert dest.readlink() == outside
    assert not outside.exists()


def test_collect_wheel_artifacts_nested_directory(tmp_path: Path) -> None:
    """Discover a valid wheel extracted under an OCI artifact subdirectory."""
    _wheel_with_sbom(tmp_path / "oci" / "python")

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert artifacts == [
        {
            "component": "test_package",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "noarch",
            "os": "any",
        }
    ]


def test_collect_wheel_artifacts_rejects_source_symlink(tmp_path: Path) -> None:
    """Reject a wheel symlink that points outside the files directory."""
    files_dir = tmp_path / "files"
    outside = _wheel_with_sbom(tmp_path / "outside")
    files_dir.mkdir()
    (files_dir / "pkg.whl").symlink_to(outside)

    with pytest.raises(tekton.CheckStepError, match="source path must stay under"):
        epa.collect_wheel_artifacts(files_dir)


def test_collect_wheel_artifacts_no_wheels(tmp_path: Path) -> None:
    """Fail when no wheels are present (catalog fail-no-wheels test)."""
    (tmp_path / "test_package-1.0.0.tar.gz").write_text("sdist", encoding="utf-8")
    with pytest.raises(tekton.CheckStepError, match="No .whl files found"):
        epa.collect_wheel_artifacts(tmp_path)


def test_collect_wheel_artifacts_dedupes(tmp_path: Path) -> None:
    """Keep the first of duplicate (component, purl, arch, os) rows."""
    _wheel_with_sbom(tmp_path, "test_package-1.0.0-py3-none-any.whl")
    _wheel_with_sbom(tmp_path, "test_package-1.0.0-py2-none-any.whl")

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert len(artifacts) == 1
    assert artifacts[0]["component"] == "test_package"


def test_collect_wheel_artifacts_multi_arch_platform_tags(tmp_path: Path) -> None:
    """Emit one artifact row per distinct architecture on a multi-arch wheel."""
    _wheel_with_sbom(
        tmp_path,
        "test_package-1.0.0-py3-none-manylinux2014_x86_64.manylinux2014_aarch64.whl",
    )

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert artifacts == [
        {
            "component": "test_package",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "aarch64",
            "os": "linux",
        },
        {
            "component": "test_package",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "x86_64",
            "os": "linux",
        },
    ]


def test_collect_wheel_artifacts_universal2_mac_wheel(tmp_path: Path) -> None:
    """Emit one Darwin artifact row per architecture for a universal2 wheel."""
    _wheel_with_sbom(
        tmp_path,
        "test_package-1.0.0-py3-none-macosx_11_0_universal2.whl",
    )

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert artifacts == [
        {
            "component": "test_package",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "arm64",
            "os": "darwin",
        },
        {
            "component": "test_package",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "x86_64",
            "os": "darwin",
        },
    ]


def test_collect_wheel_artifacts_same_arch_platform_tags(tmp_path: Path) -> None:
    """Collapse multiple tags that map to the same architecture and OS."""
    _wheel_with_sbom(
        tmp_path,
        "test_package-1.0.0-py3-none-manylinux2014_x86_64.manylinux_2_17_x86_64.whl",
    )

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert artifacts == [
        {
            "component": "test_package",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "x86_64",
            "os": "linux",
        }
    ]


def test_update_release_notes_artifacts_appends() -> None:
    """Append new artifacts to any existing releaseNotes list."""
    data: dict = {"releaseNotes": {"content": {"artifacts": [{"purl": "old"}]}}}
    epa.update_release_notes_artifacts(data, [{"purl": "new"}])
    assert data["releaseNotes"]["content"]["artifacts"] == [{"purl": "old"}, {"purl": "new"}]


def test_update_release_notes_artifacts_replaces_invalid_content() -> None:
    """Recreate content/artifacts when the stored values are the wrong type."""
    data: dict = {"releaseNotes": {"content": "bad"}}
    epa.update_release_notes_artifacts(data, [{"purl": "new"}])
    assert data["releaseNotes"]["content"]["artifacts"] == [{"purl": "new"}]


def test_update_release_notes_artifacts_creates_missing_release_notes() -> None:
    """Create releaseNotes when data.json does not define the key."""
    data: dict = {}
    epa.update_release_notes_artifacts(data, [{"purl": "new"}])
    assert data["releaseNotes"]["content"]["artifacts"] == [{"purl": "new"}]


def test_update_release_notes_artifacts_replaces_null_release_notes() -> None:
    """Replace a null releaseNotes value with a dictionary before assigning."""
    data: dict = {"releaseNotes": None}
    epa.update_release_notes_artifacts(data, [{"purl": "new"}])
    assert data["releaseNotes"]["content"]["artifacts"] == [{"purl": "new"}]


def test_update_mapping_components_patches_existing() -> None:
    """Patch an existing mapping entry that has no content type."""
    data: dict = {"mapping": {"components": [{"name": "test_package"}]}}
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"] == [
        {"name": "test_package", "contentType": "generic"}
    ]


def test_update_mapping_components_does_not_duplicate() -> None:
    """Do not append a second mapping row for an already-listed component."""
    data: dict = {
        "mapping": {"components": [{"name": "test_package", "contentType": "binary"}]}
    }
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"] == [{"name": "test_package", "contentType": "binary"}]


def test_update_mapping_components_skips_content_gateway_type() -> None:
    """Leave rows that already have contentGateway.contentType unchanged."""
    data: dict = {
        "mapping": {
            "components": [
                {"name": "test_package", "contentGateway": {"contentType": "binary"}}
            ]
        }
    }
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"][0]["contentGateway"]["contentType"] == "binary"
    assert "contentType" not in data["mapping"]["components"][0]


def test_update_mapping_components_empty_gateway_uses_top_level() -> None:
    """Fall through to top-level contentType when contentGateway has none."""
    data: dict = {
        "mapping": {
            "components": [
                {
                    "name": "test_package",
                    "contentGateway": {},
                    "contentType": "binary",
                }
            ]
        }
    }
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"][0]["contentType"] == "binary"


def test_update_mapping_components_empty_nested_keeps_top_level() -> None:
    """Keep a top-level type when contentGateway.contentType is empty."""
    data: dict = {
        "mapping": {
            "components": [
                {
                    "name": "test_package",
                    "contentGateway": {"contentType": ""},
                    "contentType": "binary",
                }
            ]
        }
    }
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"][0]["contentType"] == "binary"
    assert data["mapping"]["components"][0]["contentGateway"]["contentType"] == ""


def test_update_mapping_components_empty_nested_type_sets_generic() -> None:
    """Set nested and top-level types to generic when both are empty."""
    data: dict = {
        "mapping": {
            "components": [
                {
                    "name": "test_package",
                    "contentGateway": {"contentType": ""},
                }
            ]
        }
    }
    epa.update_mapping_components(data, ["test_package"])
    component = data["mapping"]["components"][0]
    assert component["contentType"] == "generic"
    assert component["contentGateway"]["contentType"] == "generic"


def test_update_mapping_components_empty_gateway_gets_generic() -> None:
    """Set generic when both contentGateway and top-level types are empty."""
    data: dict = {"mapping": {"components": [{"name": "test_package", "contentGateway": {}}]}}
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"][0]["contentType"] == "generic"


def test_update_mapping_components_appends_missing() -> None:
    """Append generic mapping rows for artifact components not already listed."""
    data: dict = {"mapping": {"components": [{"name": "other"}]}}
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"] == [
        {"name": "other"},
        {"name": "test_package", "contentType": "generic"},
    ]


def test_update_mapping_components_replaces_invalid_mapping() -> None:
    """Recreate mapping.components when mapping is not an object."""
    data: dict = {"mapping": "bad"}
    epa.update_mapping_components(data, ["pkg"])
    assert data["mapping"]["components"] == [{"name": "pkg", "contentType": "generic"}]


def test_update_mapping_components_preserves_list_name() -> None:
    """Keep a mapping row whose name is a list and still append the artifact."""
    data: dict = {"mapping": {"components": [{"name": ["bad"]}]}}
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"] == [
        {"name": ["bad"]},
        {"name": "test_package", "contentType": "generic"},
    ]


def test_update_mapping_components_preserves_object_name() -> None:
    """Keep a mapping row whose name is an object and still append the artifact."""
    data: dict = {"mapping": {"components": [{"name": {"nested": True}}]}}
    epa.update_mapping_components(data, ["test_package"])
    assert data["mapping"]["components"] == [
        {"name": {"nested": True}},
        {"name": "test_package", "contentType": "generic"},
    ]


def test_pull_oci_artifacts(tmp_path: Path) -> None:
    """Pull into an isolated dir, then merge into files_dir via oras_pull."""
    image = f"quay.io/a@{_SHA256_1}"
    files_dir = tmp_path / "files"
    pulled = files_dir / "artifact.whl"

    def fake_pull(_pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "artifact.whl").write_text("wheel", encoding="utf-8")

    with patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull) as mock_pull:
        epa.pull_oci_artifacts([image], files_dir)

    mock_pull.assert_called_once()
    assert mock_pull.call_args.args == (image,)
    download_dir = mock_pull.call_args.kwargs["download_dir"]
    assert download_dir != files_dir
    assert pulled.is_file()
    assert pulled.read_text(encoding="utf-8") == "wheel"


def test_pull_oci_artifacts_retry_discards_partial_pull(tmp_path: Path) -> None:
    """Do not merge leftover files from a failed oras pull attempt."""
    image = f"quay.io/a@{_SHA256_1}"
    files_dir = tmp_path / "files"
    attempts = {"n": 0}

    def fake_pull(_pull_spec: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        attempts["n"] += 1
        if attempts["n"] == 1:
            (download_dir / "stale.whl").write_text("partial", encoding="utf-8")
            raise RuntimeError("oras pull failed")
        (download_dir / "good.whl").write_text("ok", encoding="utf-8")

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch("release_service_utils.helpers.retry.retry.time.sleep"),
    ):
        epa.pull_oci_artifacts([image], files_dir)

    assert attempts["n"] == 2
    assert (files_dir / "good.whl").read_text(encoding="utf-8") == "ok"
    assert not (files_dir / "stale.whl").exists()


def test_pull_oci_artifacts_keeps_identical_duplicates(tmp_path: Path) -> None:
    """Keep a relative path when two images extract the same bytes."""
    files_dir = tmp_path / "files"

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        nested = download_dir / "nested" / "pkg.whl"
        nested.parent.mkdir(parents=True, exist_ok=True)
        nested.write_bytes(b"same-bytes")

    with patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull):
        epa.pull_oci_artifacts(
            [f"quay.io/a@{_SHA256_1}", f"quay.io/b@{_SHA256_2}"],
            files_dir,
        )

    dest = files_dir / "nested" / "pkg.whl"
    assert dest.read_bytes() == b"same-bytes"


def test_pull_oci_artifacts_rejects_conflicting_paths(tmp_path: Path) -> None:
    """Fail when two images write different bytes at the same relative path."""
    files_dir = tmp_path / "files"
    payloads = {
        f"quay.io/a@{_SHA256_1}": b"one",
        f"quay.io/b@{_SHA256_2}": b"two",
    }

    def fake_pull(image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "pkg.whl").write_bytes(payloads[image])

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        pytest.raises(tekton.CheckStepError, match="pkg.whl"),
    ):
        epa.pull_oci_artifacts(
            [f"quay.io/a@{_SHA256_1}", f"quay.io/b@{_SHA256_2}"],
            files_dir,
        )

    assert (files_dir / "pkg.whl").read_bytes() == b"one"


def test_pull_oci_artifacts_merges_distinct_paths(tmp_path: Path) -> None:
    """Keep files from different images when their relative paths differ."""
    files_dir = tmp_path / "files"
    payloads = {
        f"quay.io/a@{_SHA256_1}": ("linux/a.whl", b"linux"),
        f"quay.io/b@{_SHA256_2}": ("darwin/b.whl", b"darwin"),
    }

    def fake_pull(image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        relative, content = payloads[image]
        dest = download_dir / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

    with patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull):
        epa.pull_oci_artifacts(
            [f"quay.io/a@{_SHA256_1}", f"quay.io/b@{_SHA256_2}"],
            files_dir,
        )

    assert (files_dir / "linux" / "a.whl").read_bytes() == b"linux"
    assert (files_dir / "darwin" / "b.whl").read_bytes() == b"darwin"


def test_pull_oci_artifacts_rejects_source_symlink(tmp_path: Path) -> None:
    """Reject a pulled symlink that points outside the extraction tree."""
    files_dir = tmp_path / "files"
    outside = tmp_path / "outside" / "secret.whl"
    outside.parent.mkdir()
    outside.write_bytes(b"secret")

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "pkg.whl").symlink_to(outside)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        pytest.raises(tekton.CheckStepError, match="source path must stay under"),
    ):
        epa.pull_oci_artifacts([f"quay.io/a@{_SHA256_1}"], files_dir)

    assert not (files_dir / "pkg.whl").exists()
    assert outside.read_bytes() == b"secret"


def test_pull_oci_artifacts_rejects_file_directory_prefix_collision(
    tmp_path: Path,
) -> None:
    """Fail when a later image needs a directory where an earlier file sits."""
    files_dir = tmp_path / "files"
    payloads = {
        f"quay.io/a@{_SHA256_1}": ("pkg", b"file"),
        f"quay.io/b@{_SHA256_2}": ("pkg/nested.whl", b"nested"),
    }

    def fake_pull(image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        relative, content = payloads[image]
        dest = download_dir / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        pytest.raises(tekton.CheckStepError, match="Conflicting path 'pkg'"),
    ):
        epa.pull_oci_artifacts(
            [f"quay.io/a@{_SHA256_1}", f"quay.io/b@{_SHA256_2}"],
            files_dir,
        )

    assert (files_dir / "pkg").read_bytes() == b"file"
    assert not (files_dir / "pkg" / "nested.whl").exists()


def test_provenance_filename_bounds_long_slash_heavy_repository() -> None:
    """Keep a hashed name under 255 bytes for a long slash-heavy repository."""
    digest = "sha256:" + "a" * 64
    segment = "very-long-repository-path-segment"
    repository = "/".join([segment] * 20)
    encoded = urllib.parse.quote(repository, safe="")
    assert len(f"{digest}--{encoded}.json".encode()) > epa._FILENAME_MAX_BYTES

    first = epa._provenance_filename(f"{repository}@{digest}")
    second = epa._provenance_filename(f"other/{repository}@{digest}")
    assert first.startswith(f"{digest}--")
    assert first.endswith(".json")
    assert first != second
    assert len(first.encode()) <= epa._FILENAME_MAX_BYTES
    assert len(second.encode()) <= epa._FILENAME_MAX_BYTES


def test_provenance_filename_keeps_json_when_digest_is_overlong(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep ``.json`` if a defensive truncate of an overlong digest stem is needed."""
    monkeypatch.setattr(
        epa,
        "_image_identity",
        lambda _image: ("quay.io/a", "sha256:" + "a" * 300),
    )
    name = epa._provenance_filename("ignored")
    assert name.startswith("sha256:")
    assert name.endswith(".json")
    assert "--" in name
    assert len(name.encode()) <= epa._FILENAME_MAX_BYTES


def test_fetch_chains_provenance(tmp_path: Path) -> None:
    """Write decoded SLSA provenance named after repository and digest."""
    image = f"quay.io/a@{_SHA256_ABC123}"

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            assert cmd[1] == "verify-attestation"
            assert "--insecure-ignore-tlog=true" in cmd
            assert epa.COSIGN_KEY in cmd
            return _completed(cmd, stdout=_envelope(image=image) + "\nextra\n")
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa.fetch_chains_provenance([image], tmp_path)

    dest = tmp_path / "chains-provenance" / epa._provenance_filename(image)
    assert dest.name.startswith(f"{_SHA256_ABC123}--")
    assert dest.name.endswith(".json")
    assert len(dest.name.encode()) <= epa._FILENAME_MAX_BYTES
    written = json.loads(dest.read_text(encoding="utf-8"))
    assert written["predicate"]["buildDefinition"]
    assert written["subject"] == _statement_for(image)["subject"]


@pytest.mark.parametrize(
    "statement",
    [
        _STATEMENT,
        {**_STATEMENT, "subject": []},
    ],
)
def test_fetch_chains_provenance_rejects_missing_subject(
    tmp_path: Path,
    statement: dict,
) -> None:
    """Fail when the decoded statement has no subject."""
    image = f"quay.io/a@{_SHA256_ABC123}"
    with pytest.raises(tekton.CheckStepError, match="missing a subject"):
        _provenance_with_statement(tmp_path, image, statement)


def test_fetch_chains_provenance_rejects_unrelated_subject(tmp_path: Path) -> None:
    """Fail when the subject identifies a different image."""
    image = f"quay.io/a@{_SHA256_ABC123}"
    unrelated = _statement_for(f"quay.io/other@{_SHA256_1}")
    with pytest.raises(tekton.CheckStepError, match="does not identify"):
        _provenance_with_statement(tmp_path, image, unrelated)


@pytest.mark.parametrize(
    "subject",
    [
        "quay.io/a",
        [{"name": "quay.io/a"}],
        [{"digest": {"sha256": _SHA256_ABC123.removeprefix("sha256:")}}],
        [{"name": "quay.io/a", "digest": _SHA256_ABC123}],
        [None],
    ],
)
def test_fetch_chains_provenance_rejects_malformed_subject(
    tmp_path: Path,
    subject: object,
) -> None:
    """Fail when subject entries are not repository and SHA-256 digest pairs."""
    image = f"quay.io/a@{_SHA256_ABC123}"
    statement = {**_STATEMENT, "subject": subject}
    with pytest.raises(tekton.CheckStepError, match="malformed subject"):
        _provenance_with_statement(tmp_path, image, statement)


def test_fetch_chains_provenance_replaces_stale_digest_files(tmp_path: Path) -> None:
    """Replace leftover digest JSON with only the current snapshot files."""
    provenance_dir = tmp_path / "chains-provenance"
    stale = provenance_dir / "sha256:def456.json"
    _write_json(stale, {"stale": True})

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC123}"], tmp_path)

    remaining = sorted(path.name for path in provenance_dir.iterdir())
    expected = epa._provenance_filename(f"quay.io/a@{_SHA256_ABC123}")
    assert remaining == [expected]
    assert json.loads((provenance_dir / expected).read_text(encoding="utf-8"))["predicate"][
        "buildDefinition"
    ]
    assert not stale.exists()


def test_fetch_chains_provenance_keeps_previous_on_fetch_failure(tmp_path: Path) -> None:
    """Leave the last successful attestations when a later fetch fails."""
    dest_dir = tmp_path / "chains-provenance"
    dest_dir.mkdir()
    previous = dest_dir / "keep.json"
    previous.write_text("old-attestation", encoding="utf-8")

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return _completed(cmd, stdout="")

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(tekton.CheckStepError, match="Failed to fetch Chains provenance"),
    ):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC123}"], tmp_path)

    assert previous.read_text(encoding="utf-8") == "old-attestation"
    assert list(tmp_path.glob(".chains-provenance-*")) == []


def test_fetch_chains_provenance_keeps_previous_on_swap_failure(tmp_path: Path) -> None:
    """Restore the previous provenance directory when the sibling swap fails."""
    dest_dir = tmp_path / "chains-provenance"
    dest_dir.mkdir()
    previous = dest_dir / "keep.json"
    previous.write_text("old-attestation", encoding="utf-8")
    real_rename = Path.rename

    def wrapped(self: Path, target: str | Path) -> Path:
        target_path = Path(target)
        if (
            target_path == dest_dir
            and self.parent == dest_dir.parent
            and self.name.startswith(".chains-provenance-")
            and "outgoing" not in self.name
        ):
            raise OSError("swap failed")
        return real_rename(self, target)

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        patch.object(Path, "rename", wrapped),
        pytest.raises(OSError, match="swap failed"),
    ):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC123}"], tmp_path)

    assert dest_dir.is_dir()
    assert previous.read_text(encoding="utf-8") == "old-attestation"
    assert list(tmp_path.glob(".chains-provenance-*")) == []


def test_fetch_chains_provenance_keeps_shared_digest_for_distinct_repos(
    tmp_path: Path,
) -> None:
    """Write one provenance file per repository when two images share a digest."""
    images = [
        f"quay.io/org/one@{_SHA256_ABC123}",
        f"quay.io/org/two@{_SHA256_ABC123}",
    ]

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            image = str(cmd[-1])
            return _completed(cmd, stdout=_envelope(image=image))
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa.fetch_chains_provenance(images, tmp_path)

    provenance_dir = tmp_path / "chains-provenance"
    names = sorted(path.name for path in provenance_dir.iterdir())
    assert names == sorted(epa._provenance_filename(image) for image in images)
    subjects = {
        json.loads((provenance_dir / name).read_text(encoding="utf-8"))["subject"][0]["name"]
        for name in names
    }
    assert subjects == {epa._image_identity(image)[0] for image in images}


def test_fetch_chains_provenance_reuses_identical_same_identity(tmp_path: Path) -> None:
    """Keep one file when tagged and digest refs share repository and digest."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa.fetch_chains_provenance(
            [
                f"quay.io/org/repo:1.0@{_SHA256_ABC123}",
                f"quay.io/org/repo@{_SHA256_ABC123}",
            ],
            tmp_path,
        )

    remaining = list((tmp_path / "chains-provenance").iterdir())
    assert [path.name for path in remaining] == [
        epa._provenance_filename(f"quay.io/org/repo@{_SHA256_ABC123}")
    ]


def test_fetch_chains_provenance_rejects_conflicting_same_identity(
    tmp_path: Path,
) -> None:
    """Fail when two refs that share a filename yield different statements."""
    image = f"quay.io/org/repo@{_SHA256_ABC123}"
    envelopes = iter(
        (
            _envelope(
                _statement_for(image, predicate={"buildDefinition": {"buildType": "first"}})
            ),
            _envelope(
                _statement_for(image, predicate={"buildDefinition": {"buildType": "second"}})
            ),
        )
    )

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=next(envelopes))
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(tekton.CheckStepError, match="Conflicting Chains provenance"),
    ):
        epa.fetch_chains_provenance(
            [
                f"quay.io/org/repo:1.0@{_SHA256_ABC123}",
                f"quay.io/org/repo@{_SHA256_ABC123}",
            ],
            tmp_path,
        )


def test_fetch_chains_provenance_rejects_tagged_image(tmp_path: Path) -> None:
    """Do not treat repository slashes in a tag as provenance path segments."""
    with pytest.raises(tekton.CheckStepError, match="digest-qualified"):
        epa.fetch_chains_provenance(["quay.io/org/repo:latest"], tmp_path)
    provenance_dir = tmp_path / "chains-provenance"
    assert not (provenance_dir / "quay.io").exists()
    assert list(provenance_dir.rglob("*.json")) == []


def test_fetch_chains_provenance_invalid_payload(tmp_path: Path) -> None:
    """Fail when the cosign payload decodes to a non-object."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        payload = base64.b64encode(b"[1]").decode()
        return _completed(cmd, stdout=json.dumps({"payload": payload}))

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(
            tekton.CheckStepError, match="cosign attestation payload must be a JSON object"
        ),
    ):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC}"], tmp_path)


@pytest.mark.parametrize(
    "stdout",
    [
        "{",
        "[]",
        "{}",
        json.dumps({"payload": "@@@"}),
        json.dumps({"payload": base64.b64encode(b"not-json").decode()}),
    ],
)
def test_fetch_chains_provenance_malformed_envelope(tmp_path: Path, stdout: str) -> None:
    """Wrap Cosign envelope and payload parse failures as CheckStepError."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        return _completed(cmd, stdout=stdout)

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(tekton.CheckStepError, match="fetching Chains provenance") as exc_info,
    ):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC}"], tmp_path)

    assert exc_info.value.__cause__ is not None


@pytest.mark.parametrize("failing_cmd", ["select-oci-auth", "cosign"])
def test_fetch_chains_provenance_wraps_command_exit(tmp_path: Path, failing_cmd: str) -> None:
    """Wrap nonzero provenance command exits as CheckStepError."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == failing_cmd:
            raise subprocess.CalledProcessError(1, [str(part) for part in cmd])
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(tekton.CheckStepError, match="fetching Chains provenance") as exc_info,
    ):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC}"], tmp_path)

    assert isinstance(exc_info.value.cause, subprocess.CalledProcessError)
    assert exc_info.value.__cause__ is exc_info.value.cause


def test_fetch_chains_provenance_wraps_command_launch_failure(tmp_path: Path) -> None:
    """Wrap a missing provenance executable as CheckStepError."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        raise FileNotFoundError(cmd[0])

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(tekton.CheckStepError, match="fetching Chains provenance") as exc_info,
    ):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC}"], tmp_path)

    assert isinstance(exc_info.value.cause, FileNotFoundError)
    assert exc_info.value.__cause__ is exc_info.value.cause


def test_fetch_chains_provenance_empty_output(tmp_path: Path) -> None:
    """Fail when cosign prints no attestation envelope."""

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return _completed(cmd, stdout="")

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(tekton.CheckStepError, match="Failed to fetch Chains provenance"),
    ):
        epa.fetch_chains_provenance([f"quay.io/a@{_SHA256_ABC}"], tmp_path)


def test_run_happy_path(tmp_path: Path) -> None:
    """End-to-end run matches the catalog happy-path assertions."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    _write_json(
        snapshot_path,
        {
            "application": "test-app",
            "components": [
                {
                    "name": "test-component",
                    "containerImage": f"quay.io/test/test-package@{_SHA256_ABC123}",
                }
            ],
        },
    )
    _write_json(
        data_path,
        {
            "mapping": {"components": [{"name": "test_package"}]},
            "releaseNotes": {
                "product_id": [123],
                "type": "RHBA",
                "synopsis": "Test advisory",
                "content": {"artifacts": []},
            },
        },
    )

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        _wheel_with_sbom(download_dir)
        (download_dir / "test_package-1.0.0.tar.gz").write_text("sdist", encoding="utf-8")

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    assert (files_dir / "test_package-1.0.0-py3-none-any.whl").is_file()
    assert (files_dir / "test_package-1.0.0.tar.gz").is_file()
    data = json.loads(data_path.read_text(encoding="utf-8"))
    assert len(data["releaseNotes"]["content"]["artifacts"]) == 1
    assert data["releaseNotes"]["content"]["artifacts"][0]["purl"].startswith("pkg:pypi/")
    assert len(data["mapping"]["components"]) == 1
    assert data["mapping"]["components"][0]["name"] == "test_package"
    assert data["mapping"]["components"][0]["contentType"] == "generic"
    provenance = (
        files_dir
        / "chains-provenance"
        / epa._provenance_filename(f"quay.io/test/test-package@{_SHA256_ABC123}")
    )
    assert json.loads(provenance.read_text(encoding="utf-8"))["predicate"]["buildDefinition"]


def test_run_nested_wheel_is_found_by_sbom_extraction(tmp_path: Path) -> None:
    """Keep a nested wheel in release notes and for later SBOM discovery."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    _write_json(
        snapshot_path,
        {"components": [{"containerImage": f"quay.io/test/test-package@{_SHA256_ABC123}"}]},
    )
    _write_json(
        data_path,
        {
            "mapping": {"components": [{"name": "test_package"}]},
            "releaseNotes": {"content": {"artifacts": []}},
        },
    )

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        _wheel_with_sbom(download_dir / "oci" / "python")

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    root_wheel = files_dir / "test_package-1.0.0-py3-none-any.whl"
    assert root_wheel.is_file()
    assert not (files_dir / "oci").exists()
    data = json.loads(data_path.read_text(encoding="utf-8"))
    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 1
    assert artifacts[0]["purl"].startswith("pkg:pypi/")
    assert artifacts[0]["component"] == "test_package"

    found = extract_sboms_from_wheels(tmp_path, "files")
    assert found == 1
    extracted = (
        tmp_path
        / "sboms"
        / _sbom_output_name(
            "test_package-1.0.0-py3-none-any.whl",
            "test_package-1.0.0.dist-info/sboms/redhat.spdx.json",
        )
    )
    assert extracted.is_file()


def test_run_ignores_preexisting_wheels_in_files_dir(tmp_path: Path) -> None:
    """Replace leftover files_dir packages so SBOM extraction cannot see them."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    stale_sbom = {
        "spdxVersion": "SPDX-2.3",
        "packages": [
            {
                "name": "stale_package",
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/stale_package@9.9.9",
                    }
                ],
            }
        ],
    }
    files_dir.mkdir()
    _wheel_with_sbom(
        files_dir,
        filename="stale_package-9.9.9-py3-none-any.whl",
        sbom=stale_sbom,
        sbom_member="stale_package-9.9.9.dist-info/sboms/redhat.spdx.json",
    )
    _wheel_with_sbom(
        files_dir / "nested",
        filename="nested_stale-1.0.0-py3-none-any.whl",
        sbom=stale_sbom,
        sbom_member="stale_package-9.9.9.dist-info/sboms/redhat.spdx.json",
    )
    _write_json(
        snapshot_path,
        {"components": [{"containerImage": f"quay.io/test/test-package@{_SHA256_ABC123}"}]},
    )
    _write_json(
        data_path,
        {
            "mapping": {"components": [{"name": "test_package"}]},
            "releaseNotes": {"content": {"artifacts": []}},
        },
    )

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        _wheel_with_sbom(download_dir)

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert [row["component"] for row in artifacts] == ["test_package"]
    assert all(row["component"] != "stale_package" for row in artifacts)
    assert [row["name"] for row in data["mapping"]["components"]] == ["test_package"]
    assert not (files_dir / "stale_package-9.9.9-py3-none-any.whl").exists()
    assert not (files_dir / "nested").exists()
    assert (files_dir / "test_package-1.0.0-py3-none-any.whl").is_file()
    assert list(files_dir.rglob("*.whl")) == [
        files_dir / "test_package-1.0.0-py3-none-any.whl"
    ]
    assert (files_dir / "chains-provenance").is_dir()
    assert list(tmp_path.glob(".extract-py-files-*")) == []


def test_run_updates_mixed_case_mapping_without_duplicate(tmp_path: Path) -> None:
    """Patch an existing My_Pkg mapping row instead of appending my_pkg."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    sbom = {
        "spdxVersion": "SPDX-2.3",
        "packages": [
            {
                "name": "My_Pkg",
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/My_Pkg@1.0.0",
                    }
                ],
            }
        ],
    }
    _write_json(
        snapshot_path,
        {"components": [{"containerImage": f"quay.io/test/my-pkg@{_SHA256_ABC123}"}]},
    )
    _write_json(
        data_path,
        {
            "mapping": {"components": [{"name": "My_Pkg"}]},
            "releaseNotes": {"content": {"artifacts": []}},
        },
    )

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        _wheel_with_sbom(
            download_dir,
            filename="My_Pkg-1.0.0-py3-none-any.whl",
            sbom=sbom,
            sbom_member="My_Pkg-1.0.0.dist-info/sboms/redhat.spdx.json",
        )

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    assert data["releaseNotes"]["content"]["artifacts"][0]["component"] == "My_Pkg"
    assert data["mapping"]["components"] == [{"name": "My_Pkg", "contentType": "generic"}]


def test_run_retries_without_duplicating_artifacts(tmp_path: Path) -> None:
    """Leave prior package files and data.json unchanged on provenance failure."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    prior = files_dir / "prior-1.0.0-py3-none-any.whl"
    prior.write_bytes(b"keep-me")
    extracted = files_dir / "test_package-1.0.0-py3-none-any.whl"
    original_mapping = [{"name": "test_package"}]
    _write_json(
        snapshot_path,
        {"components": [{"containerImage": f"quay.io/test/test-package@{_SHA256_ABC123}"}]},
    )
    _write_json(
        data_path,
        {
            "mapping": {"components": original_mapping},
            "releaseNotes": {"content": {"artifacts": []}},
        },
    )

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        _wheel_with_sbom(download_dir)

    def failing_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        return _completed(cmd, stdout="")

    def succeeding_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=failing_run_cmd),
        pytest.raises(tekton.CheckStepError, match="Failed to fetch Chains provenance"),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    failed = json.loads(data_path.read_text(encoding="utf-8"))
    assert failed["releaseNotes"]["content"]["artifacts"] == []
    assert failed["mapping"]["components"] == original_mapping
    assert prior.read_bytes() == b"keep-me"
    assert not extracted.exists()
    assert not (files_dir / "chains-provenance").exists()
    assert list(tmp_path.glob(".extract-py-files-*")) == []

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=succeeding_run_cmd),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    retried = json.loads(data_path.read_text(encoding="utf-8"))
    artifacts = retried["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 1
    assert artifacts[0]["purl"].startswith("pkg:pypi/")
    assert not prior.exists()
    assert extracted.is_file()
    assert list(files_dir.rglob("*.whl")) == [extracted]


def test_run_restores_outputs_if_data_replace_fails(tmp_path: Path) -> None:
    """Restore files_dir and data.json when the atomic data replace fails."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    prior = files_dir / "prior-1.0.0-py3-none-any.whl"
    prior.write_bytes(b"keep-me")
    original = {
        "mapping": {"components": [{"name": "test_package"}]},
        "releaseNotes": {"content": {"artifacts": []}},
    }
    _write_json(snapshot_path, {"components": [{"containerImage": f"quay.io/a@{_SHA256_1}"}]})
    _write_json(data_path, original)

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        _wheel_with_sbom(download_dir)

    def fake_run_cmd(
        cmd: Sequence[str | Path],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(image=str(cmd[-1])))
        raise AssertionError(cmd)

    real_replace = os.replace

    def failing_replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        if Path(dst) == data_path:
            raise OSError("disk full")
        real_replace(src, dst)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        patch(f"{TASK}.os.replace", failing_replace),
        pytest.raises(OSError, match="disk full"),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    assert prior.read_bytes() == b"keep-me"
    assert not (files_dir / "test_package-1.0.0-py3-none-any.whl").exists()
    assert json.loads(data_path.read_text(encoding="utf-8")) == original
    assert list(tmp_path.glob(".extract-py-files-*")) == []
    assert list(tmp_path.glob(".files-outgoing-*")) == []
    assert list(tmp_path.glob(".files-failed-*")) == []
    assert list(tmp_path.glob(".data.json-*")) == []


def test_run_missing_data_raises(tmp_path: Path) -> None:
    """Raise when the data file is absent and leave prior package files."""
    snapshot_path = tmp_path / "snapshot.json"
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    prior = files_dir / "prior-1.0.0-py3-none-any.whl"
    prior.write_bytes(b"keep-me")
    _write_json(
        snapshot_path,
        {"components": [{"containerImage": f"quay.io/a@{_SHA256_1}"}]},
    )

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        _wheel_with_sbom(download_dir)

    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        pytest.raises(FileNotFoundError),
    ):
        epa.run(
            snapshot_path=snapshot_path,
            data_path=tmp_path / "missing.json",
            files_dir=files_dir,
        )

    assert prior.read_bytes() == b"keep-me"
    assert not (files_dir / "test_package-1.0.0-py3-none-any.whl").exists()


def test_run_no_wheels_raises(tmp_path: Path) -> None:
    """Propagate the no-wheels error after a successful pull."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    _write_json(snapshot_path, {"components": [{"containerImage": f"quay.io/a@{_SHA256_1}"}]})
    _write_json(data_path, {"releaseNotes": {"content": {"artifacts": []}}})

    def fake_pull(_image: str, download_dir: Path, **kwargs: object) -> None:
        del kwargs
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "test_package-1.0.0.tar.gz").write_text("sdist", encoding="utf-8")

    files_dir = tmp_path / "files"
    with (
        patch(f"{TASK}.oras_utils.oras_pull", side_effect=fake_pull),
        pytest.raises(tekton.CheckStepError, match="No .whl files found"),
    ):
        epa.run(
            snapshot_path=snapshot_path,
            data_path=data_path,
            files_dir=files_dir,
        )

    assert not files_dir.exists() or not any(files_dir.iterdir())


def test_resolve_files_dir_relative(tmp_path: Path) -> None:
    """Resolve a relative files directory under data_dir."""
    assert epa.resolve_files_dir(tmp_path, "files") == (tmp_path / "files").resolve()


def test_resolve_files_dir_blank_defaults(tmp_path: Path) -> None:
    """Treat blank files_dir as the default relative path."""
    assert epa.resolve_files_dir(tmp_path, "  ") == (tmp_path / "files").resolve()


def test_resolve_files_dir_absolute_under_data(tmp_path: Path) -> None:
    """Accept the catalog's absolute files directory when it stays in-tree."""
    files_dir = tmp_path / "files"
    assert epa.resolve_files_dir(tmp_path, str(files_dir)) == files_dir.resolve()


def test_resolve_files_dir_absolute_outside_data(tmp_path: Path) -> None:
    """Reject an absolute files directory that escapes data_dir."""
    with pytest.raises(tekton.CheckStepError, match="must stay under") as exc_info:
        epa.resolve_files_dir(tmp_path, "/etc/passwd")
    assert exc_info.value.action == "resolving files directory"
    assert isinstance(exc_info.value.cause, ValueError)


def test_run_rejects_files_dir_equal_to_data_dir(tmp_path: Path) -> None:
    """Reject a files directory that is the data directory before staging."""
    data_path = tmp_path / "data.json"
    _write_json(data_path, {"releaseNotes": {"content": {"artifacts": []}}})

    with pytest.raises(tekton.CheckStepError, match="must not be the data") as exc_info:
        epa.run(
            snapshot_path=tmp_path / "snap.json",
            data_path=data_path,
            files_dir=tmp_path,
        )

    assert exc_info.value.action == "resolving files directory"
    assert not (tmp_path / "chains-provenance").exists()


def test_run_rejects_data_path_nested_under_files_dir(tmp_path: Path) -> None:
    """Reject a data file that lives beneath the selected files directory."""
    files_dir = tmp_path / "files"
    data_path = files_dir / "nested" / "data.json"
    _write_json(data_path, {"releaseNotes": {"content": {"artifacts": []}}})

    with pytest.raises(tekton.CheckStepError, match="must not be the data") as exc_info:
        epa.run(
            snapshot_path=tmp_path / "snap.json",
            data_path=data_path,
            files_dir=files_dir,
        )

    assert exc_info.value.action == "resolving files directory"
    assert data_path.is_file()


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads PARAM_* env vars and calls run()."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_FILES_DIR", "files")

    with patch(f"{TASK}.run") as mock_run:
        assert epa.main() == 0

    mock_run.assert_called_once_with(
        snapshot_path=tmp_path / "snap.json",
        data_path=tmp_path / "data.json",
        files_dir=tmp_path / "files",
    )


def test_main_defaults_files_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PARAM_FILES_DIR defaults to files when unset."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.delenv("PARAM_FILES_DIR", raising=False)

    with patch(f"{TASK}.run") as mock_run:
        assert epa.main() == 0

    assert mock_run.call_args.kwargs["files_dir"] == tmp_path / "files"


def test_main_rejects_files_dir_dot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject PARAM_FILES_DIR=. so the data file is not swapped away."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_FILES_DIR", ".")

    with (
        patch(f"{TASK}.run") as mock_run,
        pytest.raises(tekton.CheckStepError, match="must not be the data") as exc_info,
    ):
        epa.main()

    mock_run.assert_not_called()
    assert exc_info.value.action == "resolving files directory"


def test_main_rejects_nested_data_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a PARAM_DATA_PATH that sits under PARAM_FILES_DIR."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "files/data.json")
    monkeypatch.setenv("PARAM_FILES_DIR", "files")

    with (
        patch(f"{TASK}.run") as mock_run,
        pytest.raises(tekton.CheckStepError, match="must not be the data") as exc_info,
    ):
        epa.main()

    mock_run.assert_not_called()
    assert exc_info.value.action == "resolving files directory"


def test_main_accepts_absolute_files_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PARAM_FILES_DIR may be the catalog's absolute $(params.dataDir)/$(params.filesDir)."""
    files_dir = tmp_path / "files"
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_FILES_DIR", str(files_dir))

    with patch(f"{TASK}.run") as mock_run:
        assert epa.main() == 0

    assert mock_run.call_args.kwargs["files_dir"] == files_dir.resolve()


def test_main_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when a required PARAM_* env var is missing."""
    monkeypatch.delenv("PARAM_DATA_DIR", raising=False)
    monkeypatch.delenv("PARAM_SNAPSHOT_PATH", raising=False)
    monkeypatch.delenv("PARAM_DATA_PATH", raising=False)
    with pytest.raises(SystemExit):
        epa.main()


def test_main_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Execute the package as ``python -m ...extract_py_artifacts``."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")

    with (
        patch(f"{TASK}.run"),
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(
            "release_service_utils.tasks.managed.extract_py_artifacts",
            run_name="__main__",
        )
    assert exc.value.code == 0
