import json
import unittest
from unittest.mock import MagicMock, patch, mock_open

from update_component_sbom import (
    get_component_to_purls_map,
    update_cyclonedx_sbom,
    update_spdx_sbom,
    update_sboms,
)


class TestUpdateComponentSBOM(unittest.TestCase):
    def test_get_component_to_purls_map_single_arch(self) -> None:
        release_note_images = [
            {"component": "comp1", "purl": "purl1"},
            {"component": "comp1", "purl": "purl2"},
            {"component": "comp2", "purl": "purl3"},
        ]

        result = get_component_to_purls_map(release_note_images)
        assert result == {
            "comp1": ["purl1", "purl2"],
            "comp2": ["purl3"],
        }

    def test_get_component_to_purls_map_multi_arch(self) -> None:
        release_note_images = [
            {
                "component": "comp1",
                "purl": "pkg:oci/bar@sha256%3Aabcde?arch=amd64&repository_url=registry.io/foo",
                "multiarch": True,
                "arch": "amd64",
                "imageSha": "foosha1",
            },
        ]

        result = get_component_to_purls_map(release_note_images)
        assert result == {
            "comp1": ["pkg:oci/bar@sha256%3Afoosha1?repository_url=registry.io/foo"],
            "comp1_amd64": [
                "pkg:oci/bar@sha256%3Afoosha1?arch=amd64&repository_url=registry.io/foo",
                "pkg:oci/bar@sha256%3Aabcde?repository_url=registry.io/foo",
            ],
        }

    def test_update_cyclonedx_sbom(self) -> None:
        sbom = {
            "metadata": {
                "component": {
                    "name": "comp1",
                    "purl": "purl1",
                }
            },
            "components": [
                {"name": "comp1", "purl": "purl1"},
                {"name": "comp2", "purl": "purl2"},
            ],
        }
        mapping = {
            "comp1": ["updated_purl1"],
            "comp2": ["updated_purl2"],
        }
        update_cyclonedx_sbom(sbom, mapping)
        assert sbom == {
            "metadata": {
                "component": {
                    "name": "comp1",
                    "purl": "updated_purl1",
                }
            },
            "components": [
                {"name": "comp1", "purl": "updated_purl1"},
                {"name": "comp2", "purl": "updated_purl2"},
            ],
        }

    def test_update_spdx_sbom(self) -> None:
        sbom = {
            "packages": [
                {
                    "name": "comp1",
                    "externalRefs": [
                        {
                            "referenceCategory": "PACKAGE-MANAGER",
                            "referenceType": "purl",
                            "referenceLocator": "pkg:oci/package@sha256:123",
                        }
                    ],
                },
                {
                    "name": "comp2",
                    "externalRefs": [
                        {
                            "referenceCategory": "PACKAGE-MANAGER",
                            "referenceType": "purl",
                            "referenceLocator": "pkg:oci/package@sha256:456",
                        }
                    ],
                },
            ]
        }
        mapping = {
            "comp1": [
                "pkg:oci/package@sha256:123?repository_url=quay.io/foo/bar",
                "pkg:oci/package@sha256:234?repository_url=quay.io/foo/bar",
            ],
            "comp2": [
                "pkg:oci/package@sha256:456?repository_url=quay.io/foo/bar",
                "pkg:oci/package@sha256:567?repository_url=quay.io/foo/bar",
            ],
        }

        update_spdx_sbom(sbom, mapping)
        assert sbom == {
            "name": "quay.io/foo/bar@sha256:456",
            "packages": [
                {
                    "name": "comp1",
                    "externalRefs": [
                        {
                            "referenceCategory": "PACKAGE-MANAGER",
                            "referenceType": "purl",
                            "referenceLocator": "pkg:oci/package@sha256:123"
                            "?repository_url=quay.io/foo/bar",
                        },
                        {
                            "referenceCategory": "PACKAGE-MANAGER",
                            "referenceType": "purl",
                            "referenceLocator": "pkg:oci/package@sha256:234"
                            "?repository_url=quay.io/foo/bar",
                        },
                    ],
                },
                {
                    "name": "comp2",
                    "externalRefs": [
                        {
                            "referenceCategory": "PACKAGE-MANAGER",
                            "referenceType": "purl",
                            "referenceLocator": "pkg:oci/package@sha256:456"
                            "?repository_url=quay.io/foo/bar",
                        },
                        {
                            "referenceCategory": "PACKAGE-MANAGER",
                            "referenceType": "purl",
                            "referenceLocator": "pkg:oci/package@sha256:567"
                            "?repository_url=quay.io/foo/bar",
                        },
                    ],
                },
            ],
        }

    @patch("update_component_sbom.glob.glob")
    @patch("update_component_sbom.get_component_to_purls_map")
    @patch("update_component_sbom.update_cyclonedx_sbom")
    @patch("update_component_sbom.update_spdx_sbom")
    def test_update_sboms_with_cyclonedex_format(
        self,
        mock_spdx_sbom: MagicMock,
        mock_cyclonedx_sbom: MagicMock,
        mock_mapping: MagicMock,
        mock_glob: MagicMock,
    ) -> None:
        # combining the content of data.json and sbom, since there can only be one read_data
        # defined in the mock_open
        test_cyclonedx_sbom = {
            "bomFormat": "CycloneDX",
            "releaseNotes": {"content": {"images": "foo"}},
        }

        with patch(
            "builtins.open", mock_open(read_data=json.dumps(test_cyclonedx_sbom))
        ) as mock_fs:
            mock_glob.return_value = ["sbom1"]
            update_sboms("data_path", "input_path", "output_path")
            mock_mapping.assert_called_once_with("foo")
            mock_spdx_sbom.assert_not_called()
            mock_cyclonedx_sbom.assert_called_once_with(
                test_cyclonedx_sbom, mock_mapping.return_value
            )
            assert mock_fs.call_count == 3

    @patch("update_component_sbom.glob.glob")
    @patch("update_component_sbom.get_component_to_purls_map")
    @patch("update_component_sbom.update_cyclonedx_sbom")
    @patch("update_component_sbom.update_spdx_sbom")
    def test_update_sboms_with_spdx_format(
        self,
        mock_spdx_sbom: MagicMock,
        mock_cyclonedx_sbom: MagicMock,
        mock_mapping: MagicMock,
        mock_glob: MagicMock,
    ) -> None:
        # combining the content of data.json and sbom, since there can only be one read_data
        # defined in the mock_open
        test_spdx_sbom = {"spdxVersion": "2.3", "releaseNotes": {"content": {"images": "foo"}}}

        with patch(
            "builtins.open", mock_open(read_data=json.dumps(test_spdx_sbom))
        ) as mock_fs:
            mock_glob.return_value = ["sbom1"]
            update_sboms("data_path", "input_path", "output_path")
            mock_mapping.assert_called_once_with("foo")
            mock_cyclonedx_sbom.assert_not_called()
            mock_spdx_sbom.assert_called_once_with(test_spdx_sbom, mock_mapping.return_value)
            assert mock_fs.call_count == 3

    @patch("update_component_sbom.glob.glob")
    @patch("update_component_sbom.get_component_to_purls_map")
    @patch("update_component_sbom.update_cyclonedx_sbom")
    @patch("update_component_sbom.update_spdx_sbom")
    def test_update_sboms_with_wrong_format(
        self,
        mock_spdx_sbom: MagicMock,
        mock_cyclonedx_sbom: MagicMock,
        mock_mapping: MagicMock,
        mock_glob: MagicMock,
    ) -> None:
        # combining the content of data.json and sbom, since there can only be one read_data
        # defined in the mock_open
        test_spdx_sbom = {
            "notSbom": "NoSbomVersion",
            "releaseNotes": {"content": {"images": "foo"}},
        }

        with patch(
            "builtins.open", mock_open(read_data=json.dumps(test_spdx_sbom))
        ) as mock_fs:
            mock_glob.return_value = ["not-sbom"]
            update_sboms("data_path", "input_path", "output_path")
            mock_mapping.assert_called_once_with("foo")
            mock_spdx_sbom.assert_not_called()
            mock_cyclonedx_sbom.assert_not_called()
            assert mock_fs.call_count == 2
