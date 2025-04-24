import json
from unittest.mock import patch, AsyncMock, call, ANY
import pytest
from pathlib import Path

from sbom.update_component_sbom import update_sboms
from sbom.sbomlib import Component, Image, IndexImage, Snapshot

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
                    repository="registry.redhat.io/org/tenant/test",
                    image=Image("sha256:deadbeef"),
                    tags=["1.0", "latest"],
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
                    repository="registry.redhat.io/org/tenant/test",
                    image=IndexImage(
                        index_digest,
                        children=[Image(child_digest)],
                    ),
                    tags=["1.0", "latest"],
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
                    repository="registry.redhat.io/org/tenant/test",
                    image=IndexImage(
                        index_digest,
                        children=[Image(child_digest)],
                    ),
                    tags=["1.0", "latest"],
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
