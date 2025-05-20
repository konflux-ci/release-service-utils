from typing import Any, Optional
import json
import tempfile
import pytest
from pathlib import Path

from unittest.mock import mock_open, patch

import sbom.sbomlib as sbomlib
from sbom.sbomlib import IndexImage, Snapshot, Component, Image, construct_purl


@pytest.mark.parametrize(
    ["auths", "image", "expected_auths"],
    [
        pytest.param(
            {
                "registry.local/repo": {"auth": "some_token"},
                "another.io/repo": {"auth": "some_token"},
            },
            Image("registry.local/repo", "sha256:deadbeef"),
            {"registry.local": {"auth": "some_token"}},
            id="simple",
        ),
        pytest.param(
            {"registry.local/org/repo": {"auth": "some_token"}},
            Image("registry.local/org/repo", "sha256:deadbeef"),
            {"registry.local": {"auth": "some_token"}},
            id="nested",
        ),
    ],
)
def test_get_oci_auth_file(auths, image, expected_auths):
    test_config = {"auths": auths}

    with tempfile.NamedTemporaryFile(mode="w") as config:
        json.dump(test_config, config)
        config.flush()

        with sbomlib.make_oci_auth_file(image, auth=Path(config.name)) as authfile:
            with open(authfile, "r") as fp:
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
                    "name": "comp-1",
                    "containerImage": "quay.io/repo1@sha256:deadbeef",
                    "rh-registry-repo": "registry.redhat.io/repo1",
                    "tags": ["1.0"],
                },
                {
                    "name": "comp-2",
                    "containerImage": "quay.io/repo2@sha256:ffffffff",
                    "rh-registry-repo": "registry.redhat.io/repo2",
                    "tags": ["2.0", "latest"],
                },
            ]
        }
    )

    expected_snapshot = Snapshot(
        components=[
            Component(
                name="comp-1",
                image=IndexImage(
                    "registry.redhat.io/repo1",
                    "sha256:deadbeef",
                    children=[Image("registry.redhat.io/repo1", "sha256:aaaaffff")],
                ),
                tags=["1.0"],
            ),
            Component(
                name="comp-2",
                image=IndexImage(
                    "registry.redhat.io/repo2",
                    "sha256:ffffffff",
                    children=[Image("registry.redhat.io/repo2", "sha256:bbbbffff")],
                ),
                tags=["2.0", "latest"],
            ),
        ],
    )

    def fake_get_image_manifest(image: Image) -> dict[str, Any]:
        if image.repository == "registry.redhat.io/repo1":
            child_digest = "sha256:aaaaffff"

            return {
                **index_manifest,
                "manifests": [{"digest": child_digest}],
            }

        child_digest = "sha256:bbbbffff"
        return {
            **index_manifest,
            "manifests": [{"digest": child_digest}],
        }

    with patch("sbom.sbomlib.get_image_manifest", side_effect=fake_get_image_manifest):
        with patch("builtins.open", mock_open(read_data=snapshot_raw)):
            snapshot = await sbomlib.make_snapshot(Path(""))
            assert snapshot == expected_snapshot


@pytest.mark.parametrize(
    ["repository", "digest", "arch", "tag", "expected"],
    [
        pytest.param(
            "registry.redhat.io/test",
            "sha256:deadbeef",
            "amd64",
            "1.0",
            "pkg:oci/test@sha256:deadbeef?arch=amd64&"
            "repository_url=registry.redhat.io/test&tag=1.0",
        ),
        pytest.param(
            "registry.redhat.io/test",
            "sha256:deadbeef",
            None,
            None,
            "pkg:oci/test@sha256:deadbeef?repository_url=registry.redhat.io/test",
        ),
        pytest.param(
            "registry.redhat.io/org/test",
            "sha256:deadbeef",
            None,
            None,
            "pkg:oci/test@sha256:deadbeef?repository_url=registry.redhat.io/org/test",
        ),
    ],
)
def test_construct_purl(
    repository: str, digest: str, arch: Optional[str], tag: Optional[str], expected: str
) -> None:
    assert construct_purl(Image(repository, digest), arch, tag) == expected
