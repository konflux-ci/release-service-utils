from io import StringIO
from typing import Any
import json
import tempfile
import pytest
from pathlib import Path

from unittest.mock import mock_open, patch, AsyncMock, call

import sbomlib
from sbomlib import IndexImage, Snapshot, Component, Image


@pytest.mark.parametrize(
    ["auths", "reference", "expected_auths"],
    [
        pytest.param(
            {
                "registry.local/repo": {"auth": "some_token"},
                "another.io/repo": {"auth": "some_token"},
            },
            "registry.local/repo@sha256:deadbeef",
            {"registry.local": {"auth": "some_token"}},
            id="simple",
        ),
        pytest.param(
            {"registry.local/org/repo": {"auth": "some_token"}},
            "registry.local/org/repo@sha256:deadbeef",
            {"registry.local": {"auth": "some_token"}},
            id="nested",
        ),
    ],
)
def test_get_oci_auth_file(auths, reference, expected_auths):
    test_config = {"auths": auths}

    with tempfile.NamedTemporaryFile(mode="w") as config:
        json.dump(test_config, config)
        config.flush()

        fp = StringIO()

        assert sbomlib.get_oci_auth_file(reference, Path(config.name), fp) is True

        fp.seek(0)

        data = json.loads(fp.read())
        assert data["auths"] == expected_auths


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["index_manifest"],
    [
        pytest.param({"mediaType": "application/vnd.oci.image.index.v1+json"}, id="oci-index"),
        pytest.param(
            {"mediaType": "application/vnd.docker.distribution.manifest.list.v2+json"},
            id="docker-manifest-list",
        ),
    ],
)
async def test_make_snapshot(index_manifest: dict[str, str]) -> None:
    snapshot_raw = json.dumps(
        {
            "components": [
                {
                    "containerImage": "quay.io/repo1@sha256:deadbeef",
                    "rh-registry-repo": "registry.redhat.io/repo1",
                },
                {
                    "containerImage": "quay.io/repo2@sha256:ffffffff",
                    "rh-registry-repo": "registry.redhat.io/repo2",
                },
            ]
        }
    )

    expected_snapshot = Snapshot(
        components=[
            Component(
                "registry.redhat.io/repo1",
                image=IndexImage("sha256:deadbeef", children=[Image("sha256:aaaaffff")]),
            ),
            Component(
                "registry.redhat.io/repo2",
                image=IndexImage("sha256:ffffffff", children=[Image("sha256:bbbbffff")]),
            ),
        ],
        tags=[],
        cpe="",
    )

    def fake_get_image_manifest(repository: str, _: str) -> dict[str, Any]:
        if repository == "registry.redhat.io/repo1":
            child_digest = "sha256:aaaaffff"

            return {
                **index_manifest,
                "manifests": [{"digest": child_digest}],
            }

        else:
            child_digest = "sha256:bbbbffff"
            return {
                **index_manifest,
                "manifests": [{"digest": child_digest}],
            }

    with patch("sbomlib.get_image_manifest", side_effect=fake_get_image_manifest):
        with patch("builtins.open", mock_open(read_data=snapshot_raw)):
            snapshot = await sbomlib.make_snapshot(Path(""), Path(""))
            assert snapshot == expected_snapshot
