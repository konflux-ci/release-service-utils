import json
import tempfile
from unittest.mock import patch, AsyncMock, call, ANY
from typing import Optional
from packageurl import PackageURL
import pytest
from pathlib import Path

from sbom.handlers.cyclonedx1 import CDXSpec
from sbom.update_component_sbom import update_sboms
from sbom.sbomlib import Component, Image, IndexImage, Snapshot, get_purl_digest

TESTDATA_PATH = Path(__file__).parent.joinpath("testdata")


class TestSPDXVersion23:
    @pytest.mark.asyncio
    @patch("sbom.update_component_sbom.write_sbom")
    async def test_single_component_single_arch(self, mock_write_sbom: AsyncMock) -> None:
        data_path = TESTDATA_PATH.joinpath("single-component-single-arch/spdx")

        async def fake_load_sbom(reference: str, _) -> tuple[dict, str]:
            with open(data_path.joinpath("build_sbom.json")) as f:
                return json.load(f), ""

        snapshot = Snapshot(
            components=[
                Component(
                    name="component",
                    release_repository="registry.redhat.io/org/tenant/test",
                    image=Image("sha256:deadbeef"),
                    tags=["1.0", "latest"],
                    repository="quay.io/repo",
                )
            ],
        )

        with open(data_path.joinpath("release_sbom.json")) as fp:
            expected_sbom = json.load(fp)

        with patch("sbom.update_component_sbom.load_sbom", side_effect=fake_load_sbom):
            await update_sboms(snapshot, Path("dummy"))
            mock_write_sbom.assert_awaited_with(expected_sbom, ANY)

    @pytest.mark.asyncio
    @patch("sbom.update_component_sbom.write_sbom")
    async def test_single_component_multiarch(self, mock_write_sbom: AsyncMock) -> None:
        data_path = TESTDATA_PATH.joinpath("single-component-multiarch/spdx")

        index_digest = (
            "sha256:fae7e52c95ee8d24ad9e64b54e693047b94e1b1ef98be3e3b4b9859f986e5b1d"
        )
        child_digest = (
            "sha256:84fb3b3c3cef7283a9c5172f25cf00c53274eea4972a9366e24e483ef2507921"
        )

        async def fake_load_sbom(reference: str, _) -> tuple[dict, str]:
            if index_digest in reference:
                with open(data_path.joinpath("build_index_sbom.json")) as f:
                    return json.load(f), ""

            with open(data_path.joinpath("build_image_sbom.json")) as f:
                return json.load(f), ""

        snapshot = Snapshot(
            components=[
                Component(
                    name="component",
                    release_repository="registry.redhat.io/org/tenant/test",
                    image=IndexImage(
                        index_digest,
                        children=[Image(child_digest)],
                    ),
                    tags=["1.0", "latest"],
                    repository="quay.io/repo",
                )
            ],
        )

        with open(data_path.joinpath("release_index_sbom.json")) as fp:
            expected_index_sbom = json.load(fp)

        with open(data_path.joinpath("release_image_sbom.json")) as fp:
            expected_image_sbom = json.load(fp)

        with patch("sbom.update_component_sbom.load_sbom", side_effect=fake_load_sbom):
            await update_sboms(snapshot, Path("dummy"))

            mock_write_sbom.assert_has_awaits(
                [
                    call(expected_index_sbom, ANY),
                    call(expected_image_sbom, ANY),
                ]
            )

    @pytest.mark.asyncio
    @patch("sbom.update_component_sbom.write_sbom")
    async def test_multi_component_multiarch(self, mock_write_sbom: AsyncMock) -> None:
        data_path = TESTDATA_PATH.joinpath("single-component-multiarch/spdx")

        index_digest = (
            "sha256:fae7e52c95ee8d24ad9e64b54e693047b94e1b1ef98be3e3b4b9859f986e5b1d"
        )
        child_digest = (
            "sha256:84fb3b3c3cef7283a9c5172f25cf00c53274eea4972a9366e24e483ef2507921"
        )

        num_components = 250

        async def fake_load_sbom(reference: str, _) -> tuple[dict, str]:
            if index_digest in reference:
                with open(data_path.joinpath("build_index_sbom.json")) as f:
                    return json.load(f), ""

            with open(data_path.joinpath("build_image_sbom.json")) as f:
                return json.load(f), ""

        snapshot = Snapshot(
            components=[
                Component(
                    name="component",
                    release_repository="registry.redhat.io/org/tenant/test",
                    image=IndexImage(
                        index_digest,
                        children=[Image(child_digest)],
                    ),
                    tags=["1.0", "latest"],
                    repository="quay.io/repo",
                )
            ]
            * num_components,
        )

        with open(data_path.joinpath("release_index_sbom.json")) as fp:
            expected_index_sbom = json.load(fp)

        with open(data_path.joinpath("release_image_sbom.json")) as fp:
            expected_image_sbom = json.load(fp)

        with patch("sbom.update_component_sbom.load_sbom", side_effect=fake_load_sbom):
            await update_sboms(snapshot, Path("dummy"))

            mock_write_sbom.assert_has_awaits(
                [
                    call(expected_index_sbom, ANY),
                    call(expected_image_sbom, ANY),
                ]
                * num_components
            )


class TestCycloneDX:
    @staticmethod
    def verify_purl(purl: PackageURL, kflx_component: Component) -> None:
        assert purl.qualifiers is not None
        assert (
            purl.qualifiers.get("repository_url") == kflx_component.repository  # type: ignore
        )
        assert purl.name == kflx_component.repository.split("/")[-1]

    @staticmethod
    def verify_tags(kflx_component: Component, cdx_component: dict) -> None:
        """
        Verify that all tags are present in PURLs in the evidence.identity field
        if there are more than one.
        """
        if len(kflx_component.tags) == 1:
            # in this case, we don't populate the evidence.identity field so
            # let's make sure we add the tag to the component.purl field
            purl = PackageURL.from_string(cdx_component["purl"])
            assert purl.qualifiers is not None
            assert purl.qualifiers.get("tag") == kflx_component.tags[0]  # type: ignore
            return

        tags = set(kflx_component.tags)

        try:
            identity = cdx_component["evidence"]["identity"]
        except KeyError:
            raise AssertionError("CDX component is missing evidence.identity field.")

        for id_item in identity:
            if id_item.get("field") != "purl":
                continue
            purl = PackageURL.from_string(id_item["concludedValue"])
            TestCycloneDX.verify_purl(purl, kflx_component)

            purl_tag = purl.qualifiers.get("tag")  # type: ignore
            assert isinstance(purl_tag, str), f"Missing tag in identity purl {purl}."
            tags.remove(purl_tag)

        assert len(tags) == 0, f"Not all tags present in identity purls, missing {tags}."

    @staticmethod
    def find_matching_konflux_component(
        snapshot: Snapshot, digest: str
    ) -> Optional[Component]:
        for component in snapshot.components:
            if component.image.digest == digest:
                return component

        return None

    @staticmethod
    def verify_component_updated(
        snapshot: Snapshot,
        cdx_component: dict,
        verify_tags: bool,
    ) -> None:
        if (purl_str := cdx_component.get("purl")) is None:
            return

        digest = get_purl_digest(purl_str)
        kflx_component = TestCycloneDX.find_matching_konflux_component(snapshot, digest)
        if kflx_component is None:
            return

        TestCycloneDX.verify_purl(PackageURL.from_string(purl_str), kflx_component)

        if verify_tags:
            TestCycloneDX.verify_tags(kflx_component, cdx_component)

    @staticmethod
    def verify_components_updated(snapshot: Snapshot, sbom: dict) -> None:
        """
        This method verifies that all CycloneDX container components that have a
        matching Konflux component in the release are updated.
        """
        TestCycloneDX.verify_component_updated(
            snapshot, sbom["metadata"]["component"], verify_tags=False
        )

        for component in sbom.get("components", []):
            TestCycloneDX.verify_component_updated(snapshot, component, verify_tags=True)

    @pytest.mark.asyncio
    @patch("sbom.update_component_sbom.write_sbom")
    @pytest.mark.parametrize(
        "spec",
        [
            pytest.param(CDXSpec.v1_4, id="cdx-1.4"),
            pytest.param(CDXSpec.v1_5, id="cdx-1.5"),
            pytest.param(CDXSpec.v1_6, id="cdx-1.6"),
        ],
    )
    @pytest.mark.parametrize(
        "tags",
        [
            pytest.param(["1.0"], id="single-tag"),
            pytest.param(["1.0", "latest"], id="multiple-tags"),
        ],
    )
    async def test_single_component_single_arch(
        self, mock_write_sbom: AsyncMock, spec: CDXSpec, tags: list[str]
    ) -> None:
        data_path = TESTDATA_PATH.joinpath("single-component-single-arch/cdx")

        async def fake_load_sbom(reference: str, _) -> tuple[dict, str]:
            with open(data_path.joinpath("build_sbom.json")) as f:
                build_sbom = json.load(f)
                # we can do this, because our build sbom should not contain any
                # version-specific structure
                build_sbom["specVersion"] = spec.value
                return build_sbom, ""

        snapshot = Snapshot(
            components=[
                Component(
                    name="component",
                    release_repository="registry.redhat.io/org/tenant/test",
                    image=Image("sha256:deadbeef"),
                    tags=tags,
                    repository="quay.io/repo",
                )
            ],
        )

        with patch("sbom.update_component_sbom.load_sbom", side_effect=fake_load_sbom):
            await update_sboms(snapshot, Path("dummy"))

            # get the SBOM that was written
            sbom, _ = mock_write_sbom.call_args[0]

            try:
                TestCycloneDX.verify_components_updated(snapshot, sbom)
            except Exception as err:
                with tempfile.NamedTemporaryFile("w", delete=False) as tmpf:
                    json.dump(sbom, tmpf)
                    raise AssertionError(
                        f"Failed verification of SBOM: {err}."
                        " Writing generated SBOM to {tmpf.name}"
                    ) from err
