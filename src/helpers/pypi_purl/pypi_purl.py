"""Parse and compare package URL identities."""

from __future__ import annotations

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version
from packageurl import PackageURL


def pypi_purl_matches(purl: object, name: str, version: str) -> bool:
    """Return True when *purl* is a ``pkg:pypi`` identity for *name* and *version*.

    Malformed, unversioned, or mismatched values return False.
    """
    if not isinstance(purl, str) or not purl:
        return False
    try:
        parsed = PackageURL.from_string(purl)
    except ValueError:
        return False
    if parsed.type != "pypi" or not parsed.name or not parsed.version:
        return False
    try:
        return canonicalize_name(parsed.name) == canonicalize_name(name) and Version(
            parsed.version
        ) == Version(version)
    except InvalidVersion:
        return False
