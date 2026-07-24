"""Tests for image_architectures helper."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import patch

import pytest

import image_architectures


def _skopeo_result(stdout: str, rc: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["skopeo"], rc, stdout=stdout, stderr="" if rc == 0 else "error"
    )


def test_oci_artifact_with_digest_in_ref() -> None:
    """OCI artifact images return a synthetic amd64/linux entry."""
    raw = json.dumps(
        {
            "artifactType": "application/vnd.oci.artifact.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.artifact.config.v1+json"},
        }
    )
    with patch("image_architectures.skopeo.inspect", return_value=_skopeo_result(raw)):
        result = image_architectures.get_image_architectures("registry.io/img@sha256:abc123")
    assert len(result) == 1
    assert result[0]["platform"] == {"architecture": "amd64", "os": "linux"}
    assert result[0]["digest"] == "sha256:abc123"
    assert result[0]["multiarch"] is False
    assert result[0]["configMediaType"] == "application/vnd.oci.artifact.config.v1+json"


def test_helm_chart_returns_synthetic_entry() -> None:
    """Helm chart config mediaType triggers OCI artifact path."""
    raw = json.dumps(
        {
            "config": {"mediaType": "application/vnd.cncf.helm.config.v1+json"},
        }
    )
    with patch("image_architectures.skopeo.inspect", return_value=_skopeo_result(raw)):
        result = image_architectures.get_image_architectures("registry.io/chart@sha256:helm1")
    assert result[0]["configMediaType"] == "application/vnd.cncf.helm.config.v1+json"


def test_oci_artifact_without_sha_digest_resolves_via_oras() -> None:
    """When the image ref has no sha256 digest, oras_resolve is called."""
    raw = json.dumps(
        {
            "artifactType": "something",
            "config": {"mediaType": "foo"},
        }
    )
    with (
        patch("image_architectures.skopeo.inspect", return_value=_skopeo_result(raw)),
        patch(
            "image_architectures.oras_utils.oras_resolve",
            return_value="sha256:resolved",
        ) as mock_resolve,
    ):
        result = image_architectures.get_image_architectures("registry.io/img:tag")
    mock_resolve.assert_called_once_with("registry.io/img:tag")
    assert result[0]["digest"] == "sha256:resolved"


def test_single_arch_oci_manifest() -> None:
    """Single-arch OCI image manifest triggers a second skopeo inspect."""
    raw = json.dumps(
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
        }
    )
    inspect_out = json.dumps(
        {
            "Architecture": "arm64",
            "Os": "linux",
            "Digest": "sha256:singleoci",
        }
    )
    with patch("image_architectures.skopeo.inspect") as mock_inspect:
        mock_inspect.side_effect = [
            _skopeo_result(raw),
            _skopeo_result(inspect_out),
        ]
        result = image_architectures.get_image_architectures(
            "registry.io/img@sha256:singleoci"
        )
    assert len(result) == 1
    assert result[0]["platform"] == {"architecture": "arm64", "os": "linux"}
    assert result[0]["digest"] == "sha256:singleoci"
    assert result[0]["multiarch"] is False


def test_single_arch_docker_v2_defaults_to_amd64_linux() -> None:
    """Docker v2 single manifest defaults arch/os to amd64/linux when empty."""
    raw = json.dumps(
        {
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        }
    )
    inspect_out = json.dumps(
        {
            "Architecture": "",
            "Os": "",
            "Digest": "sha256:dockerv2",
        }
    )
    with patch("image_architectures.skopeo.inspect") as mock_inspect:
        mock_inspect.side_effect = [
            _skopeo_result(raw),
            _skopeo_result(inspect_out),
        ]
        result = image_architectures.get_image_architectures("registry.io/img@sha256:dockerv2")
    assert result[0]["platform"] == {"architecture": "amd64", "os": "linux"}


def test_multi_arch_manifest_list() -> None:
    """Multi-arch manifest lists return one entry per platform with multiarch=True."""
    raw = json.dumps(
        {
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
                {
                    "platform": {"architecture": "amd64", "os": "linux"},
                    "digest": "sha256:amd",
                },
                {
                    "platform": {"architecture": "arm64", "os": "linux"},
                    "digest": "sha256:arm",
                },
            ],
        }
    )
    with patch("image_architectures.skopeo.inspect", return_value=_skopeo_result(raw)):
        result = image_architectures.get_image_architectures("registry.io/img@sha256:multi")
    assert len(result) == 2
    assert result[0]["multiarch"] is True
    assert result[1]["multiarch"] is True
    assert result[0]["digest"] == "sha256:amd"
    assert result[1]["digest"] == "sha256:arm"


def test_skopeo_raw_failure_raises() -> None:
    """RuntimeError when the initial skopeo inspect --raw fails."""
    with patch(
        "image_architectures.skopeo.inspect",
        return_value=_skopeo_result("", rc=1),
    ):
        with pytest.raises(RuntimeError, match="skopeo inspect --raw failed"):
            image_architectures.get_image_architectures("registry.io/bad@sha256:x")


def test_skopeo_inspect_failure_raises() -> None:
    """RuntimeError when the second skopeo inspect (no --raw) fails."""
    raw = json.dumps(
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
        }
    )
    with patch("image_architectures.skopeo.inspect") as mock_inspect:
        mock_inspect.side_effect = [
            _skopeo_result(raw),
            _skopeo_result("", rc=1),
        ]
        with pytest.raises(RuntimeError, match="skopeo inspect failed"):
            image_architectures.get_image_architectures("registry.io/img@sha256:x")


def test_digest_from_ref_without_at_returns_empty() -> None:
    """_digest_from_ref returns empty string when ref has no @."""
    assert image_architectures._digest_from_ref("registry.io/img:tag") == ""
