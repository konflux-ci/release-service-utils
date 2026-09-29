"""Tests for FBC fragment OCP version inspection."""

from __future__ import annotations

import json
import subprocess
from typing import Any
from unittest.mock import patch

import pytest
from release_service_utils.helpers import ocp_version

HELPER = "release_service_utils.helpers.ocp_version.ocp_version"


def _completed(stdout: str = "") -> subprocess.CompletedProcess[str]:
    """Build a subprocess result like ``skopeo.inspect`` returns."""
    return subprocess.CompletedProcess(
        args=["skopeo", "inspect"],
        returncode=0,
        stdout=stdout,
        stderr="",
    )


def _manifest(media_type: str, base_name: str | None = None) -> str:
    """Build a raw skopeo inspect manifest JSON string."""
    manifest: dict[str, Any] = {"mediaType": media_type}
    if base_name is not None:
        manifest["annotations"] = {"org.opencontainers.image.base.name": base_name}
    return json.dumps(manifest)


def test_base_name_tag_extracts_tag_after_last_colon() -> None:
    """Return the tag after the last colon in the base.name annotation."""
    manifest = {
        "annotations": {
            "org.opencontainers.image.base.name": (
                "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.12"
            )
        }
    }
    assert ocp_version.base_name_tag(manifest) == "v4.12"


def test_base_name_tag_missing_annotations_returns_empty() -> None:
    """Return an empty string when the manifest has no annotations."""
    assert ocp_version.base_name_tag({}) == ""


def test_base_name_tag_missing_base_name_returns_empty() -> None:
    """Return empty when other annotations are present but base.name is not."""
    assert ocp_version.base_name_tag({"annotations": {"other": "value"}}) == ""


def test_base_name_tag_empty_base_name_returns_empty() -> None:
    """Return empty when the base.name annotation is an empty string."""
    manifest = {"annotations": {"org.opencontainers.image.base.name": ""}}
    assert ocp_version.base_name_tag(manifest) == ""


def test_resolve_ocp_version_single_arch() -> None:
    """Read the base-image tag from a single-arch manifest without a second inspect."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_manifest(
                "application/vnd.oci.image.manifest.v1+json",
                "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.12",
            )
        ),
    ) as mock_inspect:
        version = ocp_version.resolve_ocp_version(
            "quay.io/fbc/test-fbc-component@sha256:manifest"
        )
    assert version == "v4.12"
    mock_inspect.assert_called_once_with(
        "quay.io/fbc/test-fbc-component@sha256:manifest", raw=True, check=True
    )


def test_resolve_ocp_version_oci_index() -> None:
    """Resolve an OCI index via get-image-architectures, then re-inspect."""
    arch_output = (
        json.dumps({"digest": "sha256:manifest", "platform": {"architecture": "amd64"}})
        + "\n"
        + json.dumps({"digest": "sha256:manifest", "platform": {"architecture": "ppc64le"}})
    )
    with (
        patch(
            f"{HELPER}.skopeo.inspect",
            side_effect=[
                _completed(
                    stdout=json.dumps({"mediaType": "application/vnd.oci.image.index.v1+json"})
                ),
                _completed(
                    stdout=_manifest(
                        "application/vnd.oci.image.manifest.v1+json",
                        "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.12",
                    )
                ),
            ],
        ) as mock_inspect,
        patch(f"{HELPER}.run_cmd_text", return_value=arch_output) as mock_run_cmd,
    ):
        version = ocp_version.resolve_ocp_version("quay.io/fbc/multi-arch@sha256:index")
    assert version == "v4.12"
    mock_run_cmd.assert_called_once_with(
        ["get-image-architectures", "quay.io/fbc/multi-arch@sha256:index"]
    )
    assert mock_inspect.call_args_list[1].args == ("quay.io/fbc/multi-arch@sha256:manifest",)
    assert mock_inspect.call_args_list[1].kwargs == {"raw": True, "check": True}


def test_resolve_ocp_version_docker_manifest_list() -> None:
    """Resolve a Docker v2 schema 2 manifest-list the same way as an OCI index."""
    arch_output = json.dumps(
        {"digest": "sha256:dockerv2s2manifest", "platform": {"architecture": "amd64"}}
    )
    with (
        patch(
            f"{HELPER}.skopeo.inspect",
            side_effect=[
                _completed(
                    stdout=json.dumps(
                        {
                            "mediaType": (
                                "application/vnd.docker.distribution.manifest.list.v2+json"
                            )
                        }
                    )
                ),
                _completed(
                    stdout=_manifest(
                        "application/vnd.docker.distribution.manifest.v2+json",
                        "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v4.15",
                    )
                ),
            ],
        ),
        patch(f"{HELPER}.run_cmd_text", return_value=arch_output),
    ):
        version = ocp_version.resolve_ocp_version(
            "quay.io/hacbs-release-tests/test-ocp-version/test-fbc-component-docker-v2s2"
            "@sha256:dockerv2s2index"
        )
    assert version == "v4.15"


def test_resolve_ocp_version_returns_tag_without_v_prefix() -> None:
    """Leave a bare X.Y tag unchanged; callers decide whether to prefix v."""
    arch_output = json.dumps({"digest": "sha256:plat"}) + "\n\n"
    with (
        patch(
            f"{HELPER}.skopeo.inspect",
            side_effect=[
                _completed(
                    stdout=json.dumps(
                        {
                            "mediaType": (
                                "application/vnd.docker.distribution.manifest.list.v2+json"
                            )
                        }
                    )
                ),
                _completed(
                    stdout=_manifest(
                        "application/vnd.oci.image.manifest.v1+json",
                        "registry.access.redhat.com/ubi9/ubi:4.16",
                    )
                ),
            ],
        ),
        patch(f"{HELPER}.run_cmd_text", return_value=arch_output),
    ):
        assert ocp_version.resolve_ocp_version("quay.io/fbc/dockerv2@sha256:idx") == "4.16"


def test_resolve_ocp_version_skopeo_failure_propagates() -> None:
    """A failing skopeo inspect (check=True) raises CalledProcessError."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        side_effect=subprocess.CalledProcessError(1, "skopeo"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            ocp_version.resolve_ocp_version("reg.io/img@sha256:missing")


def _inspect_labels(labels: Any) -> str:
    """Build a non-raw skopeo inspect JSON string with *labels*."""
    return json.dumps({"Labels": labels})


def test_read_openshift_version_label_missing_labels_key() -> None:
    """Return None when the inspect payload has no Labels object."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(stdout=json.dumps({})),
    ) as mock_inspect:
        assert ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc") is None
    mock_inspect.assert_called_once_with(
        "quay.io/fbc/img@sha256:abc", no_tags=True, check=True
    )


def test_read_openshift_version_label_labels_not_object() -> None:
    """Return None when Labels is present but is not a JSON object."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(stdout=_inspect_labels(None)),
    ):
        assert ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc") is None


def test_read_openshift_version_label_key_absent() -> None:
    """Return None when other labels exist but openshift.version does not."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(stdout=_inspect_labels({"other": "value"})),
    ):
        assert ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc") is None


def test_read_openshift_version_label_valid_string_array() -> None:
    """Parse a JSON-encoded array stored as a Docker label string."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: '["v4.21"]',
                }
            )
        ),
    ):
        assert ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc") == [
            "v4.21"
        ]


def test_read_openshift_version_label_native_array() -> None:
    """Accept a native JSON array if the inspect payload already decoded it."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: ["v4.18", "v4.21"],
                }
            )
        ),
    ):
        assert ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc") == [
            "v4.18",
            "v4.21",
        ]


def test_read_openshift_version_label_null_value() -> None:
    """Raise when the label is present with a JSON null value."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: None,
                }
            )
        ),
    ):
        with pytest.raises(ValueError, match="invalid value 'null'"):
            ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc")


def test_read_openshift_version_label_empty_array() -> None:
    """Raise when the label is an empty JSON array."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: "[]",
                }
            )
        ),
    ):
        with pytest.raises(ValueError, match="invalid value '\\[\\]'"):
            ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc")


def test_read_openshift_version_label_not_array() -> None:
    """Raise when the label is a plain version string instead of an array."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: "v4.21",
                }
            )
        ),
    ):
        with pytest.raises(ValueError, match="invalid value 'v4.21'"):
            ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc")


def test_read_openshift_version_label_json_non_array() -> None:
    """Raise when the label decodes to JSON that is not an array."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: "true",
                }
            )
        ),
    ):
        with pytest.raises(ValueError, match="invalid value 'true'"):
            ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc")


def test_read_openshift_version_label_malformed_json() -> None:
    """Raise when the label string is not valid JSON."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: "not-json",
                }
            )
        ),
    ):
        with pytest.raises(ValueError, match="invalid value 'not-json'") as exc_info:
            ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc")
    assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)


def test_read_openshift_version_label_native_empty_array() -> None:
    """Raise when a native empty array is present (covers non-string error formatting)."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: [],
                }
            )
        ),
    ):
        with pytest.raises(ValueError, match="invalid value '\\[\\]'"):
            ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc")


def test_read_openshift_version_label_stringifies_items() -> None:
    """Coerce non-string array items so callers always receive strings."""
    with patch(
        f"{HELPER}.skopeo.inspect",
        return_value=_completed(
            stdout=_inspect_labels(
                {
                    ocp_version.FBC_OPENSHIFT_VERSION_LABEL: ["v4.21", 4],
                }
            )
        ),
    ):
        assert ocp_version.read_openshift_version_label("quay.io/fbc/img@sha256:abc") == [
            "v4.21",
            "4",
        ]
