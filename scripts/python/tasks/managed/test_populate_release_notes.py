"""Tests for populate_release_notes."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import populate_release_notes
import pytest
import requests
from requests.auth import HTTPBasicAuth


@pytest.fixture()
def _patch_get_arch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock get_image_architectures to return amd64 and s390x."""
    monkeypatch.setattr(
        populate_release_notes,
        "get_image_architectures",
        lambda _ref: MOCK_ARCH_DIGESTS,
    )


MOCK_ARCH_DIGESTS = [
    {
        "platform": {"architecture": "amd64", "os": "linux"},
        "digest": "sha256:abcdefg",
    },
    {
        "platform": {"architecture": "s390x", "os": "linux"},
        "digest": "sha256:deadbeef",
    },
]

MOCK_AUTH = HTTPBasicAuth("test@test.com", "token123")


def _write_json(path: Path, data: dict) -> None:
    """Write *data* as JSON to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _load(path: Path) -> dict:
    """Read JSON from *path*."""
    return json.loads(path.read_text(encoding="utf-8"))


def _default_release_notes(**overrides: Any) -> dict[str, Any]:
    release_notes: dict[str, Any] = {
        "product_id": [123],
        "product_name": "test-product",
        "product_version": "1.0",
        "cpe": "cpe:/a:redhat:test:1.0",
        "type": "RHBA",
        "synopsis": "Test synopsis",
        "topic": "Test topic",
        "description": "Test description",
        "solution": "Test solution",
    }
    release_notes.update(overrides)
    return release_notes


def _default_data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"releaseNotes": _default_release_notes()}
    data.update(overrides)
    return data


def _default_snapshot(
    components: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if components is None:
        components = [
            {
                "name": "comp",
                "containerImage": "registry.io/image@sha256:123456",
                "repositories": [
                    {
                        "rh-registry-repo": "registry.redhat.io/product/repo",
                        "tags": ["foo", "bar"],
                    }
                ],
            }
        ]
    return {"application": "test-app", "components": components}


def _make_secret_dir(tmp_path: Path) -> Path:
    """Write dummy Jira credentials under *tmp_path*/secrets."""
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    (secret_dir / "token").write_text("tok", encoding="utf-8")
    (secret_dir / "email").write_text("e@t.com", encoding="utf-8")
    return secret_dir


def _jira_response(issue_type: str, cve_id: str | None = None) -> dict:
    """Return a fake Jira issue response."""
    fields: dict[str, Any] = {"issuetype": {"name": issue_type}}
    if cve_id is not None:
        fields[populate_release_notes.jira_helper.JIRA_CVE_CUSTOM_FIELD_ID] = cve_id
    return {"fields": fields}


def _mock_session(responses: dict[str, dict]) -> MagicMock:
    """Return a fake Session that matches URLs by substring."""
    session = MagicMock(spec=requests.Session)

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        for key, body in responses.items():
            if key in url:
                resp = MagicMock()
                resp.json.return_value = body
                resp.raise_for_status.return_value = None
                return resp
        raise requests.ConnectionError(f"Unexpected URL: {url}")

    session.get.side_effect = fake_get
    return session


def test_build_cves_filters_by_component() -> None:
    """Only includes CVEs for the named component."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[
                {"key": "CVE-1", "component": "a", "packages": ["p"]},
                {"key": "CVE-2", "component": "b"},
            ]
        )
    )
    result = populate_release_notes.build_cves_for_component(data, "a")
    assert "CVE-1" in result["cves"]["fixed"]
    assert "CVE-2" not in result["cves"]["fixed"]


def test_build_cves_empty_when_no_match() -> None:
    """Empty fixed dict when no CVEs match."""
    data = _default_data()
    result = populate_release_notes.build_cves_for_component(data, "x")
    assert result["cves"]["fixed"] == {}


def test_timestamp_tag_from_oci_label() -> None:
    """Converts image.created label to timestamp."""
    component = {
        "name": "c",
        "metadata": {
            "labels": [
                {
                    "name": "org.opencontainers.image.created",
                    "value": "2025-04-14T02:14:26Z",
                }
            ]
        },
    }
    tag = populate_release_notes.get_timestamp_tag(component)
    assert tag == "1744596866"


def test_timestamp_tag_missing_label() -> None:
    """Returns empty string when label is absent."""
    component = {"name": "c"}
    assert populate_release_notes.get_timestamp_tag(component) == ""


def test_timestamp_tag_unparseable_label() -> None:
    """Returns empty string when label cannot be parsed."""
    component = {
        "name": "c",
        "metadata": {
            "labels": [
                {
                    "name": "org.opencontainers.image.created",
                    "value": "not-a-date",
                }
            ]
        },
    }
    assert populate_release_notes.get_timestamp_tag(component) == ""


def test_unique_tag_from_tags_regex() -> None:
    """Picks the longest tag matching the regex."""
    tags = ["foo", "9.4-1723436855", "9.4.0-1723436855", "bar"]
    tag = populate_release_notes.get_unique_tag_from_tags(tags)
    assert tag == "9.4.0-1723436855"


def test_unique_tag_from_tags_no_match() -> None:
    """Returns empty string when no tags match."""
    tag = populate_release_notes.get_unique_tag_from_tags(["foo", "bar"])
    assert tag == ""


def test_parse_checksum_file(tmp_path: Path) -> None:
    """Parses checksums and skips manifest files."""
    checksum_file = tmp_path / "SHA256SUMS"
    checksum_file.write_text(
        "abc123 file1.tar.gz\n" "def456 file2.tar.gz\n" "ghi789 thing_manifest.json\n",
        encoding="utf-8",
    )
    result = populate_release_notes.parse_checksum_file(checksum_file)
    assert result == {"file1.tar.gz": "abc123", "file2.tar.gz": "def456"}


def test_parse_checksum_file_skips_bad_lines(tmp_path: Path) -> None:
    """Lines without two fields are skipped."""
    checksum_file = tmp_path / "SHA256SUMS"
    checksum_file.write_text(
        "abc123 file1.tar.gz\nbadline\n\ndef456 file2.tar.gz\n",
        encoding="utf-8",
    )
    result = populate_release_notes.parse_checksum_file(checksum_file)
    assert result == {"file1.tar.gz": "abc123", "file2.tar.gz": "def456"}


def test_cve_validation_pass() -> None:
    """Duplicates removed, non-Jira and non-Vulnerability skipped, CVE matches."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[{"key": "CVE-123", "component": "comp"}],
            issues={
                "fixed": [
                    {"source": "redhat.atlassian.net", "id": "VULN-123"},
                    {"source": "redhat.atlassian.net", "id": "VULN-123"},
                    {"source": "redhat.atlassian.net", "id": "FEATURE-456"},
                    {"source": "bugzilla.redhat.com", "id": "BZ-789"},
                ]
            },
        )
    )
    session = _mock_session(
        {
            "VULN-123": _jira_response("Vulnerability", "CVE-123"),
            "FEATURE-456": _jira_response("Feature"),
        }
    )

    populate_release_notes.validate_cve_issues(data, session, MOCK_AUTH)

    assert session.get.call_count == 2
    assert len(data["releaseNotes"]["issues"]["fixed"]) == 3


def test_cve_validation_fail() -> None:
    """Raises when CVE from Jira is not in releaseNotes.cves."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[{"key": "CVE-456", "component": "comp"}],
            issues={
                "fixed": [
                    {"source": "redhat.atlassian.net", "id": "VULN-MISSING"},
                ]
            },
        )
    )
    session = _mock_session(
        {
            "VULN-MISSING": _jira_response("Vulnerability", "CVE-MISSING-456"),
        }
    )

    with pytest.raises(RuntimeError, match="CVE-MISSING-456"):
        populate_release_notes.validate_cve_issues(data, session, MOCK_AUTH)


def test_cve_validation_fail_empty_cves() -> None:
    """Raises when cves list is empty but a Vulnerability issue exists."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[],
            issues={
                "fixed": [
                    {"source": "redhat.atlassian.net", "id": "VULN-123"},
                ]
            },
        )
    )
    session = _mock_session(
        {
            "VULN-123": _jira_response("Vulnerability", "CVE-123"),
        }
    )

    with pytest.raises(RuntimeError, match="CVE-123"):
        populate_release_notes.validate_cve_issues(data, session, MOCK_AUTH)


def test_cve_server_converted() -> None:
    """issues.redhat.com is converted to redhat.atlassian.net."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[{"key": "CVE-123", "component": "comp"}],
            issues={
                "fixed": [
                    {"source": "issues.redhat.com", "id": "VULN-123"},
                ]
            },
        )
    )
    session = _mock_session(
        {
            "VULN-123": _jira_response("Vulnerability", "CVE-123"),
        }
    )

    populate_release_notes.validate_cve_issues(data, session, MOCK_AUTH)
    url = session.get.call_args_list[0].args[0]
    assert "redhat.atlassian.net" in url
    assert "issues.redhat.com" not in url


def test_cve_validation_no_issues_skips() -> None:
    """Skips when issues.fixed is absent."""
    data = _default_data()
    session = MagicMock(spec=requests.Session)
    populate_release_notes.validate_cve_issues(data, session, MOCK_AUTH)
    session.get.assert_not_called()


def test_cve_validation_fetch_failure_skips(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Jira fetch failure logs a warning and skips."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[{"key": "CVE-123", "component": "comp"}],
            issues={
                "fixed": [
                    {"source": "redhat.atlassian.net", "id": "VULN-123"},
                ]
            },
        )
    )
    session = MagicMock(spec=requests.Session)
    session.get.side_effect = requests.ConnectionError("fail")

    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    try:
        with caplog.at_level(logging.WARNING, logger="release"):
            populate_release_notes.validate_cve_issues(data, session, MOCK_AUTH)
        assert "Could not fetch issue" in caplog.text
    finally:
        release_logger.propagate = False


def test_cve_validation_null_cve_id() -> None:
    """Skips when Vulnerability issue has null CVE ID."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[{"key": "CVE-123", "component": "comp"}],
            issues={
                "fixed": [
                    {"source": "redhat.atlassian.net", "id": "VULN-1"},
                ]
            },
        )
    )
    session = _mock_session({"VULN-1": _jira_response("Vulnerability", None)})
    populate_release_notes.validate_cve_issues(data, session, MOCK_AUTH)


def test_single_image(_patch_get_arch: None) -> None:
    """Two arch entries for a single component and repo."""
    data = _default_data()
    snapshot = _default_snapshot()

    populate_release_notes.populate_images(data, snapshot)

    images = data["releaseNotes"]["content"]["images"]
    assert len(images) == 2

    amd = images[0]
    assert amd["architecture"] == "amd64"
    assert amd["containerImage"] == ("registry.redhat.io/product/repo@sha256:abcdefg")
    assert amd["purl"] == (
        "pkg:oci/repo@sha256%3Aabcdefg"
        "?arch=amd64"
        "&repository_url=registry.redhat.io/product/repo"
    )
    assert amd["repository"] == "registry.redhat.io/product/repo"
    assert amd["tags"] == ["foo", "bar"]
    assert amd["component"] == "comp"


def test_multiple_images_tag_selection(_patch_get_arch: None) -> None:
    """Longest matching tag used, no tag when none match."""
    components = [
        {
            "name": "comp",
            "containerImage": "registry.io/img@sha256:aaa",
            "repositories": [
                {
                    "rh-registry-repo": "registry.redhat.io/product/repo",
                    "tags": ["9.4-1723436855", "9.4.0-1723436855", "foo", "bar"],
                }
            ],
        },
        {
            "name": "comp2",
            "containerImage": "registry.io/img2@sha256:bbb",
            "repositories": [
                {
                    "rh-registry-repo": "registry.stage.redhat.io/product2/repo2",
                    "tags": ["foo", "bar"],
                }
            ],
        },
    ]
    data = _default_data()

    populate_release_notes.populate_images(data, _default_snapshot(components))

    images = data["releaseNotes"]["content"]["images"]
    assert len(images) == 4
    assert "&tag=9.4.0-1723436855" in images[0]["purl"]
    assert "&tag=" not in images[2]["purl"]


def test_multiple_repositories(_patch_get_arch: None) -> None:
    """One entry per repo per arch."""
    components = [
        {
            "name": "comp",
            "containerImage": "registry.io/img@sha256:aaa",
            "repositories": [
                {
                    "rh-registry-repo": "registry.redhat.io/product/repo",
                    "tags": ["9.4.0-1723436855"],
                },
                {
                    "rh-registry-repo": "registry.redhat.io/product/repo2",
                    "tags": ["foo", "bar"],
                },
            ],
        }
    ]
    data = _default_data()

    populate_release_notes.populate_images(data, _default_snapshot(components))

    images = data["releaseNotes"]["content"]["images"]
    assert len(images) == 4
    assert images[0]["repository"] == "registry.redhat.io/product/repo"
    assert "&tag=9.4.0-1723436855" in images[0]["purl"]
    assert images[2]["repository"] == "registry.redhat.io/product/repo2"
    assert "&tag=" not in images[2]["purl"]


def test_no_overwrite(_patch_get_arch: None) -> None:
    """Pre-existing content.images entries are kept."""
    data = _default_data(
        releaseNotes=_default_release_notes(content={"images": [{"existing": "entry"}]})
    )
    populate_release_notes.populate_images(data, _default_snapshot())

    images = data["releaseNotes"]["content"]["images"]
    assert len(images) == 3
    assert images[0] == {"existing": "entry"}


def test_unique_tag_from_label(_patch_get_arch: None) -> None:
    """PURL tag from org.opencontainers.image.created."""
    components = [
        {
            "name": "comp",
            "containerImage": "registry.io/img@sha256:aaa",
            "metadata": {
                "labels": [
                    {
                        "name": "org.opencontainers.image.created",
                        "value": "2025-04-14T02:14:26Z",
                    }
                ]
            },
            "repositories": [
                {
                    "rh-registry-repo": "registry.redhat.io/product/repo",
                    "tags": ["foo", "bar"],
                }
            ],
        }
    ]
    data = _default_data()

    populate_release_notes.populate_images(data, _default_snapshot(components))
    assert "&tag=1744596866" in data["releaseNotes"]["content"]["images"][0]["purl"]


def test_canonical_name_in_purl(_patch_get_arch: None) -> None:
    """Uses canonicalName in the PURL path."""
    components = [
        {
            "name": "comp",
            "canonicalName": "my-product/my-image",
            "containerImage": "registry.io/img@sha256:aaa",
            "repositories": [
                {
                    "rh-registry-repo": "registry.redhat.io/product/repo",
                    "tags": ["foo"],
                }
            ],
        }
    ]
    data = _default_data()

    populate_release_notes.populate_images(data, _default_snapshot(components))
    assert "pkg:oci/my-product/my-image@" in (
        data["releaseNotes"]["content"]["images"][0]["purl"]
    )


def test_images_skips_github_release() -> None:
    """No images produced for github releases."""
    data = _default_data(github={"githubSecret": "s"})
    populate_release_notes.populate_images(data, _default_snapshot())
    assert "content" not in data.get("releaseNotes", {})


def test_images_skips_binary_content() -> None:
    """No images produced for binary content."""
    data = _default_data(
        mapping={"components": [{"contentGateway": {"contentType": "binary"}}]}
    )
    populate_release_notes.populate_images(data, _default_snapshot())
    assert "content" not in data.get("releaseNotes", {})


def test_images_not_created_when_no_repositories(_patch_get_arch: None) -> None:
    """No content.images key when components have no repositories (e.g. RPM)."""
    components = [{"name": "comp", "containerImage": "r@sha256:abc123"}]
    data = _default_data()
    populate_release_notes.populate_images(data, _default_snapshot(components))
    assert "images" not in data.get("releaseNotes", {}).get("content", {})


def test_invalid_container_image_sha(_patch_get_arch: None) -> None:
    """RuntimeError when containerImage is not a valid sha256 reference."""
    components = [
        {
            "name": "comp",
            "containerImage": "registry.io/image:latest",
            "repositories": [
                {
                    "rh-registry-repo": "registry.redhat.io/product/repo",
                    "tags": ["foo"],
                }
            ],
        }
    ]
    data = _default_data()
    with pytest.raises(RuntimeError, match="Failed to extract sha256"):
        populate_release_notes.populate_images(data, _default_snapshot(components))


def test_cves_added(_patch_get_arch: None) -> None:
    """CVEs attached to matching component images."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            cves=[
                {"key": "CVE-123", "component": "comp", "packages": ["pkg1", "pkg2"]},
                {"key": "CVE-456", "component": "comp", "packages": ["pkg3"]},
            ]
        )
    )
    populate_release_notes.populate_images(data, _default_snapshot())

    images = data["releaseNotes"]["content"]["images"]
    assert len(images) == 2
    fixed = images[0]["cves"]["fixed"]
    assert "CVE-123" in fixed
    assert "CVE-456" in fixed
    assert fixed["CVE-123"]["packages"] == ["pkg1", "pkg2"]


def test_mixed_cve_images(_patch_get_arch: None) -> None:
    """CVEs only on comp1, not comp2. Type set to RHSA."""
    components = [
        {
            "name": "comp1",
            "containerImage": "reg/a@sha256:aaa",
            "repositories": [{"rh-registry-repo": "reg.io/p/r1", "tags": ["t1"]}],
        },
        {
            "name": "comp2",
            "containerImage": "reg/b@sha256:bbb",
            "repositories": [{"rh-registry-repo": "reg.io/p/r2", "tags": ["t2"]}],
        },
    ]
    data = _default_data(
        releaseNotes=_default_release_notes(
            type="RHBA",
            cves=[
                {"key": "CVE-123", "component": "comp1", "packages": []},
                {"key": "CVE-456", "component": "comp1", "packages": []},
            ],
        )
    )
    populate_release_notes.populate_images(data, _default_snapshot(components))
    populate_release_notes.update_type_and_references(data)

    images = data["releaseNotes"]["content"]["images"]
    assert len(images) == 4
    assert "cves" in images[0]
    assert "cves" not in images[2]
    assert data["releaseNotes"]["type"] == "RHSA"


def test_artifacts_skips_non_artifact_content() -> None:
    """Skips when content type is not binary, disk-image, or rpm."""
    data = _default_data()
    populate_release_notes.populate_artifacts(data, _default_snapshot())
    assert "artifacts" not in data.get("releaseNotes", {}).get("content", {})


def test_binary_with_files() -> None:
    """Two binary entries with placeholder PURLs."""
    data = _default_data(
        mapping={
            "components": [
                {
                    "name": "prod",
                    "contentGateway": {"contentType": "binary"},
                    "files": [
                        {"arch": "amd64", "os": "linux"},
                        {"arch": "amd64", "os": "windows"},
                    ],
                }
            ]
        }
    )
    snapshot = _default_snapshot([{"name": "prod", "containerImage": "r@sha256:a"}])

    populate_release_notes.populate_artifacts(data, snapshot)

    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 2
    assert artifacts[0]["purl"] == "placeholder"
    assert artifacts[1]["os"] == "windows"


def test_binary_staged_files() -> None:
    """Falls back to staged.files when files is absent."""
    data = _default_data(
        mapping={
            "components": [
                {
                    "name": "odf-cli",
                    "contentType": "binary",
                    "staged": {
                        "files": [
                            {"arch": "amd64", "os": "linux"},
                            {"arch": "amd64", "os": "darwin"},
                            {"arch": "amd64", "os": "windows"},
                        ]
                    },
                }
            ]
        }
    )
    snapshot = _default_snapshot([{"name": "odf-cli", "containerImage": "r@sha256:a"}])

    populate_release_notes.populate_artifacts(data, snapshot)

    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 3
    assert sorted(a["os"] for a in artifacts) == ["darwin", "linux", "windows"]


def test_binary_with_cves() -> None:
    """CVEs attached to binary artifacts."""
    data = _default_data(
        mapping={
            "components": [
                {
                    "name": "prod",
                    "contentGateway": {"contentType": "binary"},
                    "files": [{"arch": "amd64", "os": "linux"}],
                }
            ]
        },
        releaseNotes=_default_release_notes(
            cves=[
                {"key": "CVE-123", "component": "prod", "packages": ["p1", "p2"]},
                {"key": "CVE-456", "component": "prod", "packages": ["p3"]},
            ]
        ),
    )
    snapshot = _default_snapshot([{"name": "prod", "containerImage": "r@sha256:a"}])

    populate_release_notes.populate_artifacts(data, snapshot)

    fixed = data["releaseNotes"]["content"]["artifacts"][0]["cves"]["fixed"]
    assert "CVE-123" in fixed
    assert "CVE-456" in fixed


def test_disk_image_with_cves() -> None:
    """Two disk-image entries with CVEs and x86_64 arch."""
    data = _default_data(
        mapping={
            "components": [
                {
                    "name": "iso-comp",
                    "contentGateway": {"contentType": "disk-image"},
                    "staged": {"files": [{"filename": "image-x86_64.iso"}]},
                },
                {
                    "name": "qcow-comp",
                    "contentGateway": {"contentType": "disk-image"},
                    "staged": {"files": [{"filename": "image-x86_64.qcow2"}]},
                },
            ]
        },
        releaseNotes=_default_release_notes(
            cves=[
                {"key": "CVE-123", "component": "iso-comp", "packages": ["p1", "p2"]},
                {"key": "CVE-456", "component": "qcow-comp", "packages": []},
            ]
        ),
    )
    snapshot = _default_snapshot(
        [
            {"name": "iso-comp", "containerImage": "r@sha256:a"},
            {"name": "qcow-comp", "containerImage": "r@sha256:b"},
        ]
    )

    populate_release_notes.populate_artifacts(data, snapshot)

    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 2
    assert artifacts[0]["architecture"] == "x86_64"
    assert artifacts[0]["os"] == "linux"
    assert "CVE-123" in artifacts[0]["cves"]["fixed"]
    assert "CVE-456" in artifacts[1]["cves"]["fixed"]


def test_marketplace_disk_image() -> None:
    """Marketplace PURL uses pkg:generic with version."""
    data = _default_data(
        mapping={
            "cloudMarketplacesSecret": "my-secret",
            "components": [
                {
                    "name": "azure-img",
                    "contentType": "disk-image",
                    "staged": {
                        "version": "1.5",
                        "files": [{"filename": "img-x86_64.vhd"}],
                    },
                }
            ],
        },
        releaseNotes=_default_release_notes(
            cves=[{"key": "CVE-123", "component": "azure-img", "packages": []}]
        ),
    )
    snapshot = _default_snapshot([{"name": "azure-img", "containerImage": "r@sha256:a"}])

    populate_release_notes.populate_artifacts(data, snapshot)

    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 1
    assert artifacts[0]["purl"] == "pkg:generic/azure-img@1.5"
    assert artifacts[0]["architecture"] == "x86_64"


def test_disk_image_aarch64_arch() -> None:
    """aarch64 detected from filename."""
    data = _default_data(
        mapping={
            "components": [
                {
                    "name": "comp",
                    "contentType": "disk-image",
                    "staged": {"files": [{"filename": "image-aarch64.qcow2"}]},
                }
            ]
        }
    )
    snapshot = _default_snapshot([{"name": "comp", "containerImage": "r@sha256:a"}])
    populate_release_notes.populate_artifacts(data, snapshot)
    assert data["releaseNotes"]["content"]["artifacts"][0]["architecture"] == "aarch64"


def test_rpms() -> None:
    """RPM entries with signing key, SBOM and attestation URLs."""
    data = _default_data(
        mapping={"components": [{"name": "hello--main", "contentType": "rpm"}]},
        pulp={"domain": "public-hummingbird"},
        signOptions={"signKeyAlias": {"key": "hummingbird-signing-key"}},
    )
    snapshot = _default_snapshot(
        [
            {
                "name": "hello--main",
                "containerImage": "r@sha256:a",
                "rpmsToPublish": [
                    {
                        "rpmname": "hello",
                        "arch": "x86_64",
                        "version": "1.0",
                        "release": "1.fc38",
                        "distro": "hummingbird",
                        "targetRepos": [
                            {
                                "repository_id": "hbird-x86_64-id",
                                "repository_name": "binary",
                                "distro": "hummingbird",
                            }
                        ],
                    },
                    {
                        "rpmname": "hello",
                        "arch": "x86_64",
                        "version": "1.0",
                        "release": "1.fc38",
                        "distro": "hummingbird",
                        "sbomPath": "sboms/hello--main/sha256-12345.sbom",
                        "attestationPath": ("attestations/hello--main/sha256-12345.att"),
                        "targetRepos": [
                            {
                                "repository_id": "hbird-src-id",
                                "repository_name": "source",
                                "distro": "hummingbird",
                            }
                        ],
                    },
                ],
            }
        ]
    )

    populate_release_notes.populate_artifacts(data, snapshot)

    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 2

    assert artifacts[0]["purl"] == (
        "pkg:rpm/redhat/hello@1.0-1.fc38"
        "?arch=x86_64&distro=hummingbird&repository_id=hbird-x86_64-id"
    )
    assert artifacts[0]["signingKey"] == "hummingbird-signing-key"

    assert artifacts[1]["architecture"] == "src"
    assert artifacts[1]["sbom"] == (
        "https://packages.redhat.com/api/pulp-content/"
        "public-hummingbird/sboms/hello--main/sha256-12345.sbom"
    )
    assert artifacts[1]["attestation"] == (
        "https://packages.redhat.com/api/pulp-content/"
        "public-hummingbird/attestations/hello--main/sha256-12345.att"
    )


def test_rpm_without_distro_field() -> None:
    """RPM entry without distro at root level does not error."""
    data = _default_data(
        mapping={"components": [{"name": "comp", "contentType": "rpm"}]},
        signOptions={"signKeyAlias": {"key": "k"}},
    )
    snapshot = _default_snapshot(
        [
            {
                "name": "comp",
                "containerImage": "r@sha256:a",
                "rpmsToPublish": [
                    {
                        "rpmname": "pkg",
                        "arch": "x86_64",
                        "version": "1.0",
                        "release": "1.el9",
                        "targetRepos": [
                            {
                                "repository_id": "repo-id",
                                "repository_name": "binary",
                                "distro": "rhel",
                            }
                        ],
                    }
                ],
            }
        ]
    )

    populate_release_notes.populate_artifacts(data, snapshot)
    assert len(data["releaseNotes"]["content"]["artifacts"]) == 1


def test_rpm_no_rpms_to_publish() -> None:
    """RuntimeError when no component has rpmsToPublish."""
    data = _default_data(mapping={"components": [{"name": "comp", "contentType": "rpm"}]})
    snapshot = _default_snapshot([{"name": "comp", "containerImage": "r@sha256:a"}])

    with pytest.raises(RuntimeError, match="No rpmsToPublish"):
        populate_release_notes.populate_artifacts(data, snapshot)


def test_rpm_mixed_components() -> None:
    """Components without rpmsToPublish are skipped, not errored."""
    data = _default_data(
        mapping={"components": [{"name": "has-rpms", "contentType": "rpm"}]},
        signOptions={"signKeyAlias": {"key": "k"}},
    )
    snapshot = _default_snapshot(
        [
            {
                "name": "has-rpms",
                "containerImage": "r@sha256:a",
                "rpmsToPublish": [
                    {
                        "rpmname": "pkg",
                        "arch": "x86_64",
                        "version": "1.0",
                        "release": "1.el9",
                        "distro": "rhel",
                        "targetRepos": [
                            {
                                "repository_id": "repo-id",
                                "repository_name": "binary",
                                "distro": "rhel",
                            }
                        ],
                    }
                ],
            },
            {"name": "no-rpms", "containerImage": "r@sha256:b"},
        ]
    )

    populate_release_notes.populate_artifacts(data, snapshot)

    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 1
    assert artifacts[0]["component"] == "has-rpms"


def test_rpm_no_target_repos_fallback() -> None:
    """RPM without targetRepos uses best-effort purl."""
    data = _default_data(
        mapping={"components": [{"name": "comp", "contentType": "rpm"}]},
        signOptions={"signKeyAlias": {"key": "k"}},
    )
    snapshot = _default_snapshot(
        [
            {
                "name": "comp",
                "containerImage": "r@sha256:a",
                "rpmsToPublish": [
                    {
                        "rpmname": "pkg",
                        "arch": "x86_64",
                        "version": "1.0",
                        "release": "1.el9",
                        "distro": "rhel",
                    }
                ],
            }
        ]
    )
    populate_release_notes.populate_artifacts(data, snapshot)
    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 1
    assert "pkg:rpm/redhat/pkg@1.0-1.el9" in artifacts[0]["purl"]
    assert artifacts[0]["signingKey"] == "k"


def test_github_release(tmp_path: Path) -> None:
    """GitHub artifacts with v-prefixed download URLs and CVEs."""
    binaries = tmp_path / "releases"
    binaries.mkdir()
    (binaries / "product_1.0.0_SHA256SUMS").write_text(
        "aaa111 binary-linux-amd64\nbbb222 binary-windows-amd64\n"
        "ccc333 product_1.0.0_manifest.json\n",
        encoding="utf-8",
    )
    data = _default_data(
        github={"githubSecret": "s"},
        mapping={
            "components": [
                {
                    "name": "product",
                    "files": [
                        {"source": "path/binary-linux-amd64", "arch": "amd64", "os": "linux"},
                        {
                            "source": "path/binary-windows-amd64",
                            "arch": "amd64",
                            "os": "windows",
                        },
                    ],
                }
            ]
        },
        releaseNotes=_default_release_notes(
            cves=[
                {"key": "CVE-123", "component": "product", "packages": ["p1", "p2"]},
                {"key": "CVE-456", "component": "product", "packages": ["p3"]},
            ]
        ),
    )
    snapshot = _default_snapshot([{"name": "product", "containerImage": "r@sha256:a"}])

    populate_release_notes.populate_github(
        data,
        snapshot,
        "1.0.0",
        "https://github.com/some-org/some-repo",
        str(binaries),
    )

    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 2
    assert "download/v1.0.0/" in artifacts[0]["purl"]
    assert artifacts[0]["purl"].startswith("pkg:generic/product@1.0.0?")
    assert "CVE-123" in artifacts[0]["cves"]["fixed"]


def test_github_skips_manifest(tmp_path: Path) -> None:
    """Manifest files are skipped."""
    binaries = tmp_path / "releases"
    binaries.mkdir()
    (binaries / "prod_1.0_SHA256SUMS").write_text(
        "aaa binary1\nbbb prod_1.0_manifest.json\n",
        encoding="utf-8",
    )
    data = _default_data(
        github={"githubSecret": "s"},
        mapping={
            "components": [
                {
                    "name": "prod",
                    "files": [
                        {"source": "p/binary1", "arch": "amd64", "os": "linux"},
                        {"source": "p/prod_1.0_manifest.json", "arch": "amd64", "os": "linux"},
                    ],
                }
            ]
        },
    )
    snapshot = _default_snapshot([{"name": "prod", "containerImage": "r@sha256:a"}])

    populate_release_notes.populate_github(
        data,
        snapshot,
        "1.0",
        "https://github.com/o/r",
        str(binaries),
    )
    assert len(data["releaseNotes"]["content"]["artifacts"]) == 1


def test_github_skips_when_no_github() -> None:
    """Skips when .github field is absent."""
    data = _default_data()
    populate_release_notes.populate_github(data, _default_snapshot(), "", "", "")
    assert "content" not in data.get("releaseNotes", {})


def test_github_missing_binaries_dir() -> None:
    """RuntimeError when binaries dir does not exist."""
    data = _default_data(github={"githubSecret": "s"})
    with pytest.raises(RuntimeError, match="Binaries directory"):
        populate_release_notes.populate_github(
            data,
            _default_snapshot(),
            "1.0",
            "https://github.com/o/r",
            "/nonexistent",
        )


def test_github_empty_binaries_dir() -> None:
    """RuntimeError when binaries dir is empty string."""
    data = _default_data(github={"githubSecret": "s"})
    with pytest.raises(RuntimeError, match="Binaries directory"):
        populate_release_notes.populate_github(
            data,
            _default_snapshot(),
            "1.0",
            "https://github.com/o/r",
            "",
        )


def test_github_v_prefix_preserved(tmp_path: Path) -> None:
    """Existing v prefix is not doubled."""
    binaries = tmp_path / "releases"
    binaries.mkdir()
    (binaries / "p_v2.0_SHA256SUMS").write_text("aaa f1\n", encoding="utf-8")
    data = _default_data(
        github={"githubSecret": "s"},
        mapping={
            "components": [
                {
                    "name": "p",
                    "files": [{"source": "x/f1", "arch": "amd64", "os": "linux"}],
                }
            ]
        },
    )
    snapshot = _default_snapshot([{"name": "p", "containerImage": "r@sha256:a"}])

    populate_release_notes.populate_github(
        data,
        snapshot,
        "v2.0",
        "https://github.com/o/r",
        str(binaries),
    )
    purl = data["releaseNotes"]["content"]["artifacts"][0]["purl"]
    assert "download/v2.0/" in purl
    assert "download/vv2.0/" not in purl


def test_github_no_checksum_file(tmp_path: Path) -> None:
    """RuntimeError when no SHA256SUMS file exists."""
    binaries = tmp_path / "releases"
    binaries.mkdir()
    data = _default_data(github={"githubSecret": "s"})
    with pytest.raises(RuntimeError, match="No checksum file"):
        populate_release_notes.populate_github(
            data,
            _default_snapshot(),
            "1.0",
            "https://github.com/o/r",
            str(binaries),
        )


def test_github_bad_url(tmp_path: Path) -> None:
    """RuntimeError when release URL cannot be parsed."""
    binaries = tmp_path / "releases"
    binaries.mkdir()
    (binaries / "x_SHA256SUMS").write_text("aaa f1\n", encoding="utf-8")
    data = _default_data(github={"githubSecret": "s"})
    with pytest.raises(RuntimeError, match="Could not parse owner/repo"):
        populate_release_notes.populate_github(
            data,
            _default_snapshot(),
            "1.0",
            "https://not-github.com/bad",
            str(binaries),
        )


def test_rhsa_references() -> None:
    """CVE and classification URLs added to references."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            type="RHSA",
            references=["https://docs.example.com/rn"],
            content={
                "images": [
                    {
                        "cves": {
                            "fixed": {
                                "CVE-123": {"packages": []},
                                "CVE-456": {"packages": []},
                            }
                        }
                    }
                ]
            },
        )
    )
    populate_release_notes.update_type_and_references(data)

    references = data["releaseNotes"]["references"]
    assert "https://access.redhat.com/security/cve/CVE-123" in references
    assert "https://access.redhat.com/security/cve/CVE-456" in references
    assert "https://access.redhat.com/security/updates/classification/" in references
    assert "https://docs.example.com/rn" in references


def test_non_rhsa_references() -> None:
    """Empty references when no CVEs present."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            type="RHBA", content={"images": [{"component": "c"}]}
        )
    )
    populate_release_notes.update_type_and_references(data)
    assert data["releaseNotes"]["type"] == "RHBA"
    assert data["releaseNotes"].get("references", []) == []


def test_overwrite_type() -> None:
    """Type changed from RHEA to RHSA when CVEs exist."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            type="RHEA",
            content={"images": [{"cves": {"fixed": {"CVE-123": {"packages": []}}}}]},
        )
    )
    populate_release_notes.update_type_and_references(data)
    assert data["releaseNotes"]["type"] == "RHSA"


def test_cross_image_cve_dedup() -> None:
    """Same CVE across images only appears once in references."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            content={
                "images": [
                    {
                        "cves": {
                            "fixed": {
                                "CVE-123": {"packages": []},
                                "CVE-456": {"packages": []},
                            }
                        }
                    },
                    {
                        "cves": {
                            "fixed": {
                                "CVE-123": {"packages": []},
                                "CVE-789": {"packages": []},
                            }
                        }
                    },
                ]
            },
        )
    )
    populate_release_notes.update_type_and_references(data)

    references = data["releaseNotes"]["references"]
    cve_references = [r for r in references if "/cve/" in r]
    assert len(cve_references) == 3
    assert references == sorted(references)


def test_references_no_cve_dup() -> None:
    """Pre-existing references are not duplicated."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            references=[
                "https://access.redhat.com/security/cve/CVE-123",
                "https://access.redhat.com/security/updates/classification/",
                "https://docs.example.com/existing-page",
            ],
            content={
                "images": [
                    {
                        "cves": {
                            "fixed": {
                                "CVE-123": {"packages": []},
                                "CVE-456": {"packages": []},
                            }
                        }
                    }
                ]
            },
        )
    )
    populate_release_notes.update_type_and_references(data)

    references = data["releaseNotes"]["references"]
    assert len(references) == len(set(references))
    assert len(references) == 4


def test_no_content_noop() -> None:
    """No references key when no content exists."""
    data = _default_data()
    populate_release_notes.update_type_and_references(data)
    assert "references" not in data.get("releaseNotes", {})


def test_artifacts_references() -> None:
    """CVE references work with artifacts, not just images."""
    data = _default_data(
        releaseNotes=_default_release_notes(
            content={"artifacts": [{"cves": {"fixed": {"CVE-100": {"packages": []}}}}]},
        )
    )
    populate_release_notes.update_type_and_references(data)
    assert (
        "https://access.redhat.com/security/cve/CVE-100" in data["releaseNotes"]["references"]
    )


def test_fail_missing_data(tmp_path: Path) -> None:
    """Error when data.json does not exist."""
    snapshot_file = tmp_path / "snapshot.json"
    _write_json(snapshot_file, _default_snapshot())
    secret_dir = _make_secret_dir(tmp_path)

    with pytest.raises((FileNotFoundError, RuntimeError)):
        populate_release_notes.run(
            data_file=tmp_path / "missing.json",
            snapshot_file=snapshot_file,
            jira_secret_path=secret_dir,
            binaries_dir="",
            github_release_version="",
            github_release_url="",
        )


def test_fail_missing_snapshot(tmp_path: Path) -> None:
    """Error when snapshot.json does not exist."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, _default_data())
    secret_dir = _make_secret_dir(tmp_path)

    with pytest.raises((FileNotFoundError, RuntimeError)):
        populate_release_notes.run(
            data_file=data_file,
            snapshot_file=tmp_path / "missing.json",
            jira_secret_path=secret_dir,
            binaries_dir="",
            github_release_version="",
            github_release_url="",
        )


def test_run_happy_path(tmp_path: Path, _patch_get_arch: None) -> None:
    """run() writes updated data.json with content.images."""
    data_file = tmp_path / "data.json"
    snapshot_file = tmp_path / "snapshot.json"
    secret_dir = _make_secret_dir(tmp_path)

    _write_json(data_file, _default_data())
    _write_json(snapshot_file, _default_snapshot())

    rc = populate_release_notes.run(
        data_file=data_file,
        snapshot_file=snapshot_file,
        jira_secret_path=secret_dir,
        binaries_dir="",
        github_release_version="",
        github_release_url="",
    )

    assert rc == 0
    result = _load(data_file)
    assert len(result["releaseNotes"]["content"]["images"]) == 2


def test_main_happy_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_get_arch: None,
) -> None:
    """main() reads env vars and writes data.json."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_json(data_dir / "data.json", _default_data())
    _write_json(data_dir / "snapshot.json", _default_snapshot())

    secret_dir = _make_secret_dir(tmp_path)

    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.setenv("SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("JIRA_SECRET_PATH", str(secret_dir))

    assert populate_release_notes.main() == 0
    result = _load(data_dir / "data.json")
    assert len(result["releaseNotes"]["content"]["images"]) == 2


def test_main_missing_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """SystemExit when a required env var is missing."""
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.delenv("DATA_PATH", raising=False)
    monkeypatch.delenv("SNAPSHOT_PATH", raising=False)

    with pytest.raises(SystemExit):
        populate_release_notes.main()
