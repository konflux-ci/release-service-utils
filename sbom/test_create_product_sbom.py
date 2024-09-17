import datetime
import json

import unittest
from unittest.mock import MagicMock, patch, mock_open

from create_product_sbom import (
    create_sbom,
)


class TestCreateSBOM(unittest.TestCase):
    @patch("create_product_sbom.uuid")
    @patch("create_product_sbom.datetime")
    def test_create_sbom_no_components(self, mock_datetime: MagicMock, mock_uuid: MagicMock):
        mock_uuid.uuid4.return_value = "039f091d-8790-41bc-b63e-251ec860e3db"

        time = datetime.datetime.now()
        mock_datetime.now.return_value = time

        data = json.dumps(
            {
                "releaseNotes": {
                    "cpe": "cpe",
                    "product_name": "product",
                    "product_version": "1.2.3",
                    "images": [],
                }
            }
        )

        with patch("builtins.open", mock_open(read_data=data)):
            sbom = create_sbom("./data.json")
            assert sbom == {
                "spdxVersion": "SPDX-2.3",
                "SPDXID": "SPDXRef-DOCUMENT",
                "dataLicense": "CC0-1.0",
                "documentNamespace": "https://redhat.com/spdxdocs/"
                "product-1.2.3-039f091d-8790-41bc-b63e-251ec860e3db",
                "creationInfo": {
                    "created": time.isoformat(),
                    "creator": "Organization: Red Hat",
                },
                "name": "product",
                "packages": [
                    {
                        "SPDXID": "SPDXRef-product",
                        "name": "product",
                        "versionInfo": "1.2.3",
                        "supplier": "Organization: Red Hat",
                        "downloadLocation": "NOASSERTION",
                        "externalRefs": [
                            {
                                "referenceCategory": "SECURITY",
                                "referenceType": "cpe22Type",
                                "referenceLocator": "cpe",
                            }
                        ],
                    }
                ],
                "relationships": [
                    {
                        "spdxElementId": "SPDXRef-DOCUMENT",
                        "relationshipType": "DESCRIBES",
                        "relatedSpdxElement": "SPDXRef-product",
                    }
                ],
            }

    @patch("create_product_sbom.uuid")
    @patch("create_product_sbom.datetime")
    def test_create_sbom_single_component(
        self, mock_datetime: MagicMock, mock_uuid: MagicMock
    ):
        mock_uuid.uuid4.return_value = "039f091d-8790-41bc-b63e-251ec860e3db"

        time = datetime.datetime.now()
        mock_datetime.now.return_value = time

        data = json.dumps(
            {
                "releaseNotes": {
                    "cpe": "cpe",
                    "product_name": "product",
                    "product_version": "1.2.3",
                    "images": [{"component": "comp1", "purl": "purl1"}],
                }
            }
        )

        with patch("builtins.open", mock_open(read_data=data)):
            sbom = create_sbom("./data.json")
            assert sbom == {
                "spdxVersion": "SPDX-2.3",
                "SPDXID": "SPDXRef-DOCUMENT",
                "dataLicense": "CC0-1.0",
                "documentNamespace": "https://redhat.com/spdxdocs/"
                "product-1.2.3-039f091d-8790-41bc-b63e-251ec860e3db",
                "creationInfo": {
                    "created": time.isoformat(),
                    "creator": "Organization: Red Hat",
                },
                "name": "product",
                "packages": [
                    {
                        "SPDXID": "SPDXRef-product",
                        "name": "product",
                        "versionInfo": "1.2.3",
                        "supplier": "Organization: Red Hat",
                        "downloadLocation": "NOASSERTION",
                        "externalRefs": [
                            {
                                "referenceCategory": "SECURITY",
                                "referenceType": "cpe22Type",
                                "referenceLocator": "cpe",
                            }
                        ],
                    },
                    {
                        "SPDXID": "SPDXRef-component-0",
                        "name": "comp1",
                        "downloadLocation": "NOASSERTION",
                        "externalRefs": [
                            {
                                "referenceCategory": "PACKAGE-MANAGER",
                                "referenceType": "purl",
                                "referenceLocator": "purl1",
                            }
                        ],
                    },
                ],
                "relationships": [
                    {
                        "spdxElementId": "SPDXRef-DOCUMENT",
                        "relationshipType": "DESCRIBES",
                        "relatedSpdxElement": "SPDXRef-product",
                    },
                    {
                        "spdxElementId": "SPDXRef-product",
                        "relationshipType": "PACKAGE_OF",
                        "relatedSpdxElement": "SPDXRef-component-0",
                    },
                ],
            }

    @patch("create_product_sbom.uuid")
    @patch("create_product_sbom.datetime")
    def test_create_sbom_multiple_components_multiple_purls(
        self, mock_datetime: MagicMock, mock_uuid: MagicMock
    ):
        mock_uuid.uuid4.return_value = "039f091d-8790-41bc-b63e-251ec860e3db"

        time = datetime.datetime.now()
        mock_datetime.now.return_value = time

        data = json.dumps(
            {
                "releaseNotes": {
                    "cpe": "cpe",
                    "product_name": "product",
                    "product_version": "1.2.3",
                    "images": [
                        {"component": "comp1", "purl": "purl1"},
                        {"component": "comp1", "purl": "purl2"},
                        {"component": "comp2", "purl": "purl3"},
                    ],
                }
            }
        )

        with patch("builtins.open", mock_open(read_data=data)):
            sbom = create_sbom("./data.json")
            assert sbom == {
                "spdxVersion": "SPDX-2.3",
                "SPDXID": "SPDXRef-DOCUMENT",
                "dataLicense": "CC0-1.0",
                "documentNamespace": "https://redhat.com/spdxdocs/"
                "product-1.2.3-039f091d-8790-41bc-b63e-251ec860e3db",
                "creationInfo": {
                    "created": time.isoformat(),
                    "creator": "Organization: Red Hat",
                },
                "name": "product",
                "packages": [
                    {
                        "SPDXID": "SPDXRef-product",
                        "name": "product",
                        "versionInfo": "1.2.3",
                        "supplier": "Organization: Red Hat",
                        "downloadLocation": "NOASSERTION",
                        "externalRefs": [
                            {
                                "referenceCategory": "SECURITY",
                                "referenceType": "cpe22Type",
                                "referenceLocator": "cpe",
                            }
                        ],
                    },
                    {
                        "SPDXID": "SPDXRef-component-0",
                        "name": "comp1",
                        "downloadLocation": "NOASSERTION",
                        "externalRefs": [
                            {
                                "referenceCategory": "PACKAGE-MANAGER",
                                "referenceType": "purl",
                                "referenceLocator": "purl1",
                            },
                            {
                                "referenceCategory": "PACKAGE-MANAGER",
                                "referenceType": "purl",
                                "referenceLocator": "purl2",
                            },
                        ],
                    },
                    {
                        "SPDXID": "SPDXRef-component-1",
                        "name": "comp2",
                        "downloadLocation": "NOASSERTION",
                        "externalRefs": [
                            {
                                "referenceCategory": "PACKAGE-MANAGER",
                                "referenceType": "purl",
                                "referenceLocator": "purl3",
                            },
                        ],
                    },
                ],
                "relationships": [
                    {
                        "spdxElementId": "SPDXRef-DOCUMENT",
                        "relationshipType": "DESCRIBES",
                        "relatedSpdxElement": "SPDXRef-product",
                    },
                    {
                        "spdxElementId": "SPDXRef-product",
                        "relationshipType": "PACKAGE_OF",
                        "relatedSpdxElement": "SPDXRef-component-0",
                    },
                    {
                        "spdxElementId": "SPDXRef-product",
                        "relationshipType": "PACKAGE_OF",
                        "relatedSpdxElement": "SPDXRef-component-1",
                    },
                ],
            }
