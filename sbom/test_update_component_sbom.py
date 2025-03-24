import json
import unittest
from unittest.mock import MagicMock, patch, mock_open, AsyncMock, call, ANY
import pytest
from pathlib import Path

from update_component_sbom import update_sboms
from sbomlib import Component, Image, IndexImage, Snapshot

TESTDATA_PATH = Path(__file__).parent.joinpath("testdata")


@pytest.mark.asyncio
@patch("update_component_sbom.write_sbom")
async def test_spdx_single_component_single_arch(mock_write_sbom: AsyncMock) -> None:

    async def fake_load_sbom(reference: str, _) -> tuple[dict, str]:
        with open(TESTDATA_PATH.joinpath("single-component-single-arch/build_sbom.json")) as f:
            return json.load(f), ""

    snapshot = Snapshot(
        cpe="",
        tags=[],
        components=[
            Component(
                repository="registry.redhat.io/org/tenant/test",
                image=Image("sha256:deadbeef"),
            )
        ],
    )

    with open(TESTDATA_PATH.joinpath("single-component-single-arch/release_sbom.json")) as fp:
        expected_sbom = json.load(fp)

    with patch("update_component_sbom.load_sbom", side_effect=fake_load_sbom):
        await update_sboms(snapshot, Path("dummy"))
        mock_write_sbom.assert_awaited_with(expected_sbom, ANY)


@pytest.mark.asyncio
@patch("update_component_sbom.write_sbom")
async def test_spdx_single_component_multiarch(mock_write_sbom: AsyncMock) -> None:

    async def fake_load_sbom(reference: str, _) -> tuple[dict, str]:
        if "sha256:fae" in reference:
            with open(
                TESTDATA_PATH.joinpath("single-component-multiarch/build_index_sbom.json")
            ) as f:
                return json.load(f), ""

        with open(
            TESTDATA_PATH.joinpath("single-component-multiarch/build_image_sbom.json")
        ) as f:
            return json.load(f), ""

    snapshot = Snapshot(
        cpe="",
        tags=[],
        components=[
            Component(
                repository="registry.redhat.io/org/tenant/test",
                image=IndexImage(
                    "sha256:fae7e52c95ee8d24ad9e64b54e693047b94e1b1ef98be3e3b4b9859f986e5b1d",
                    children=[
                        Image(
                            "sha256:84fb3b3c3cef7283a9c5172f25cf00c53274eea4972a9366e24e483ef2507921"
                        )
                    ],
                ),
            )
        ],
    )

    with open(
        TESTDATA_PATH.joinpath("single-component-multiarch/release_index_sbom.json")
    ) as fp:
        expected_index_sbom = json.load(fp)

    with open(
        TESTDATA_PATH.joinpath("single-component-multiarch/release_image_sbom.json")
    ) as fp:
        expected_image_sbom = json.load(fp)

    with patch("update_component_sbom.load_sbom", side_effect=fake_load_sbom):
        await update_sboms(snapshot, Path("dummy"))

        mock_write_sbom.assert_has_awaits(
            [
                call(expected_index_sbom, ANY),
                call(expected_image_sbom, ANY),
            ]
        )
