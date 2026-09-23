"""Test package URL identity helpers."""

from __future__ import annotations

import pytest

from release_service_utils.helpers.pypi_purl import pypi_purl


@pytest.mark.parametrize(
    ("candidate", "name", "version"),
    [
        ("pkg:pypi/test_package@1.0.0", "test_package", "1.0.0"),
        ("pkg:pypi/test-package@1.0.0", "test_package", "1.0.0"),
        ("pkg:pypi/foo@1.0?filename=x", "foo", "1.0"),
    ],
)
def test_pypi_purl_matches_accepts_matching_identities(
    candidate: str, name: str, version: str
) -> None:
    """Match pkg:pypi values whose normalized name and version agree."""
    assert pypi_purl.pypi_purl_matches(candidate, name, version) is True


@pytest.mark.parametrize(
    "candidate",
    [
        None,
        "",
        "placeholder",
        "not-a-purl",
        "pkg:pypi",
        "pkg:pypi/",
        "pkg:pypi/test_package",
        "pkg:pypi/test_package@",
        "pkg:pypi/@1.0.0",
        "pkg:generic/app@1.0",
    ],
)
def test_pypi_purl_matches_rejects_invalid_and_unversioned(candidate: object) -> None:
    """Treat malformed or incomplete pkg:pypi values as non-matches."""
    assert pypi_purl.pypi_purl_matches(candidate, "test_package", "1.0.0") is False


@pytest.mark.parametrize(
    ("candidate", "name", "version"),
    [
        ("pkg:pypi/other_package@1.0.0", "test_package", "1.0.0"),
        ("pkg:pypi/test_package@0.9.0", "test_package", "1.0.0"),
        ("pkg:pypi/test_package@1.0.0", "test_package", "not-a-version"),
    ],
)
def test_pypi_purl_matches_rejects_mismatched_name_or_version(
    candidate: str, name: str, version: str
) -> None:
    """Reject a complete pkg:pypi identity for a different package or version."""
    assert pypi_purl.pypi_purl_matches(candidate, name, version) is False
