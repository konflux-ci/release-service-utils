from io import StringIO
import json
from typing import List, Union
from collections import namedtuple

import pytest
from packageurl import PackageURL
from spdx_tools.spdx.writer.json.json_writer import write_document_to_stream

from sbom.create_product_sbom import (
    ReleaseNotes,
    create_sbom,
    get_filename,
    parse_release_notes,
)
from sbom.sbomlib import Component, Image, IndexImage, Snapshot

Digests = namedtuple("Digests", ["single_arch", "multi_arch"])
DIGESTS = Digests(
    single_arch="sha256:8f2e5e7f92d8e8d2e9b3e9c1a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
    multi_arch="sha256:e4d2f37a563fcfa4d3a1ab476ded714c56f75f916d30c3a33815d64d41f78534",
)


@pytest.mark.parametrize(
    ["data", "expected_rn"],
    [
        pytest.param(
            {
                "unrelated": "field",
                "releaseNotes": {
                    "product_name": "Product",
                    "product_version": "1.0",
                    "cpe": "cpe",
                },
            },
            ReleaseNotes(
                product_name="Product",
                product_version="1.0",
                cpe="cpe",
            ),
            id="cpe-single",
        ),
        pytest.param(
            {
                "unrelated": "field",
                "releaseNotes": {
                    "product_name": "Product",
                    "product_version": "1.0",
                    "cpe": ["cpe1", "cpe2"],
                },
            },
            ReleaseNotes(
                product_name="Product",
                product_version="1.0",
                cpe=["cpe1", "cpe2"],
            ),
            id="cpe-list",
        ),
    ],
)
def test_parse_release_notes(data: dict, expected_rn: ReleaseNotes) -> None:
    actual = parse_release_notes(json.dumps(data))
    assert expected_rn == actual


def verify_cpe(sbom, expected_cpe: Union[str, List[str]]) -> None:
    """
    Verify that all CPE externalRefs are in the first package.
    """
    all_cpes = expected_cpe if isinstance(expected_cpe, list) else [expected_cpe]
    for cpe in all_cpes:
        assert {
            "referenceCategory": "SECURITY",
            "referenceLocator": cpe,
            "referenceType": "cpe22Type",
        } in sbom["packages"][0]["externalRefs"]


def verify_purls(sbom, expected: List[str]) -> None:
    """
    Verify that the actual purls in the SBOM match the expected purls.
    """
    actual_purls = []
    for package in sbom["packages"]:
        refs = package["externalRefs"]
        actual_purls.extend(
            [ref["referenceLocator"] for ref in refs if ref["referenceType"] == "purl"]
        )

    assert sorted(actual_purls) == sorted(expected), print(
        f"Actual: {actual_purls}, Expected: {expected}"
    )


def verify_checksums(sbom) -> None:
    """
    Verify that if there is an OCI purl in a package, the version can also be
    found in the checksums of the package.
    """
    for package in sbom["packages"]:
        refs = package["externalRefs"]
        purls = {
            PackageURL.from_string(ref["referenceLocator"])
            for ref in refs
            if ref["referenceType"] == "purl"
        }

        expected_checksums = {
            f"sha256:{checksum['checksumValue']}"
            for checksum in package.get("checksums", [])
            if checksum["algorithm"] == "SHA256"
        }

        actual_checksums = {purl.version or "" for purl in purls if purl.type == "oci"}

        assert actual_checksums == expected_checksums


def verify_relationships(sbom, components: List[Component]) -> None:
    """
    Verify that the correct relationships exist for each component and the product.
    """
    for component in components:
        assert {
            "spdxElementId": f"SPDXRef-component-{component.name}",
            "relatedSpdxElement": "SPDXRef-product",
            "relationshipType": "PACKAGE_OF",
        } in sbom["relationships"]

    # verify the relationship for the product
    assert {
        "spdxElementId": "SPDXRef-DOCUMENT",
        "relatedSpdxElement": "SPDXRef-product",
        "relationshipType": "DESCRIBES",
    } in sbom["relationships"]


def verify_supplier(sbom) -> None:
    # verify suppliers are set
    for package in sbom["packages"]:
        assert package["supplier"] == "Organization: Red Hat"


def verify_package_licenses(sbom) -> None:
    for package in sbom["packages"]:
        assert package["licenseDeclared"] == "NOASSERTION"


@pytest.mark.parametrize(
    "cpe",
    [
        pytest.param("cpe:/a:redhat:discovery:1.0::el9", id="cpe-single"),
        pytest.param(
            [
                "cpe:/a:redhat:discovery:1.0::el9",
                "cpe:/a:redhat:discovery:1.0::el10",
            ],
            id="cpe-list",
        ),
    ],
)
@pytest.mark.parametrize(
    ["snapshot", "purls"],
    [
        pytest.param(
            Snapshot(
                components=[
                    Component(
                        name="component",
                        image=Image("quay.io/repo", digest=DIGESTS.single_arch),
                        tags=["1.0", "latest"],
                    )
                ]
            ),
            [
                f"pkg:oci/repo@{DIGESTS.single_arch}?repository_url=quay.io/repo&tag=1.0",
                f"pkg:oci/repo@{DIGESTS.single_arch}?repository_url=quay.io/repo&tag=latest",
            ],
            id="single-component-single-arch",
        ),
        pytest.param(
            Snapshot(
                components=[
                    Component(
                        name="component",
                        image=IndexImage(
                            repository="quay.io/repo",
                            digest=DIGESTS.multi_arch,
                            children=[
                                Image("quay.io/repo", "sha256:aaa"),
                                Image("quay.io/repo", "sha256:bbb"),
                            ],
                        ),
                        tags=["1.0", "latest"],
                    )
                ]
            ),
            [
                f"pkg:oci/repo@{DIGESTS.multi_arch}?repository_url=quay.io/repo&tag=1.0",
                f"pkg:oci/repo@{DIGESTS.multi_arch}?repository_url=quay.io/repo&tag=latest",
            ],
            id="single-component-multi-arch",
        ),
        pytest.param(
            Snapshot(
                components=[
                    Component(
                        name="multiarch-component",
                        image=IndexImage(
                            repository="quay.io/repo",
                            digest=DIGESTS.multi_arch,
                            children=[
                                Image("quay.io/repo", "sha256:aaa"),
                                Image("quay.io/repo", "sha256:bbb"),
                            ],
                        ),
                        tags=["1.0", "latest"],
                    ),
                    Component(
                        name="singlearch-component",
                        image=Image("quay.io/another-repo", digest=DIGESTS.single_arch),
                        tags=["2.0", "production"],
                    ),
                ]
            ),
            [
                f"pkg:oci/repo@{DIGESTS.multi_arch}?repository_url=quay.io/repo&tag=1.0",
                f"pkg:oci/repo@{DIGESTS.multi_arch}?repository_url=quay.io/repo&tag=latest",
                f"pkg:oci/another-repo@{DIGESTS.single_arch}"
                "?repository_url=quay.io/another-repo&tag=2.0",
                f"pkg:oci/another-repo@{DIGESTS.single_arch}"
                "?repository_url=quay.io/another-repo&tag=production",
            ],
            id="multi-component-mixed-arch",
        ),
    ],
)
def test_create_sbom(snapshot: Snapshot, purls: List[str], cpe: Union[str, List[str]]):
    """
    Create an SBOM from release notes and a snapshot and verify that the
    expected properties hold.
    """
    release_notes = ReleaseNotes(
        product_name="Product",
        product_version="1.0",
        cpe=cpe,
    )

    sbom = create_sbom(release_notes, snapshot)
    output = StringIO()

    write_document_to_stream(sbom, output)  # type: ignore
    output.seek(0)

    sbom_dict = json.load(output)

    verify_cpe(sbom_dict, cpe)
    verify_purls(sbom_dict, purls)
    verify_relationships(sbom_dict, snapshot.components)
    verify_checksums(sbom_dict)
    verify_supplier(sbom_dict)
    verify_package_licenses(sbom_dict)

    assert sbom_dict["dataLicense"] == "CC0-1.0"


def test_get_filename() -> None:
    notes = ReleaseNotes(product_name="Amazing Red Hat Product", product_version="1.0", cpe="")
    assert get_filename(notes) == "Amazing-Red-Hat-Product-1.0.json"
