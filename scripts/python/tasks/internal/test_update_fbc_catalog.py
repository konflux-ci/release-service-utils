"""Tests for ``update_fbc_catalog``."""

from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path
from unittest import mock

import pytest
import requests

import authentication
import iib
import tekton
import update_fbc_catalog

# ---------------------------------------------------------------------------
# parse_fbc_fragments
# ---------------------------------------------------------------------------


def test_parse_fbc_fragments_valid() -> None:
    """A valid JSON array is parsed and sorted."""
    assert update_fbc_catalog.parse_fbc_fragments('["b", "a", "c"]') == ["a", "b", "c"]


def test_parse_fbc_fragments_not_array() -> None:
    """Non-array JSON raises ``ValueError``."""
    with pytest.raises(ValueError, match="JSON array"):
        update_fbc_catalog.parse_fbc_fragments('{"a": 1}')


def test_parse_fbc_fragments_empty_array() -> None:
    """An empty array raises ``ValueError``."""
    with pytest.raises(ValueError, match="empty"):
        update_fbc_catalog.parse_fbc_fragments("[]")


def test_parse_fbc_fragments_non_string_item() -> None:
    """Non-string items raise ``ValueError``."""
    with pytest.raises(ValueError, match="non-empty strings"):
        update_fbc_catalog.parse_fbc_fragments("[1, 2]")


def test_parse_fbc_fragments_blank_item() -> None:
    """Whitespace-only items raise ``ValueError``."""
    with pytest.raises(ValueError, match="non-empty strings"):
        update_fbc_catalog.parse_fbc_fragments('["ok", "  "]')


def test_parse_fbc_fragments_invalid_json() -> None:
    """Invalid JSON raises ``JSONDecodeError``."""
    with pytest.raises(json.JSONDecodeError):
        update_fbc_catalog.parse_fbc_fragments("not json")


# ---------------------------------------------------------------------------
# create_container_auth_config
# ---------------------------------------------------------------------------


def test_create_auth_config_normal_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Auth entry is written with base64-encoded credentials."""
    monkeypatch.setenv("HOME", str(tmp_path))
    authentication.create_container_auth_config("registry.example.com/repo:v1", "user:pass")
    auth_file = tmp_path / ".config" / "containers" / "auth.json"
    data = json.loads(auth_file.read_text(encoding="utf-8"))
    expected_token = base64.b64encode(b"user:pass").decode("ascii")
    assert data["auths"]["registry.example.com/repo"]["auth"] == expected_token


def test_create_auth_config_no_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Empty credentials produce an empty auth.json."""
    monkeypatch.setenv("HOME", str(tmp_path))
    authentication.create_container_auth_config("registry.example.com/repo:v1", "")
    auth_file = tmp_path / ".config" / "containers" / "auth.json"
    assert json.loads(auth_file.read_text(encoding="utf-8")) == {}


@pytest.mark.parametrize(
    "host",
    [
        "registry-proxy.engineering.redhat.com",
        "registry-proxy-stage.engineering.redhat.com",
    ],
)
def test_create_auth_config_registry_proxy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    host: str,
) -> None:
    """registry-proxy hosts get empty auth (Kerberos, not token auth)."""
    monkeypatch.setenv("HOME", str(tmp_path))
    authentication.create_container_auth_config(
        f"{host}/rh-osbs/iib:v4.12",
        "some-cred",
    )
    auth_file = tmp_path / ".config" / "containers" / "auth.json"
    assert json.loads(auth_file.read_text(encoding="utf-8")) == {}


def test_create_auth_config_registry_proxy_removes_existing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Existing auth entry for registry-proxy is removed."""
    monkeypatch.setenv("HOME", str(tmp_path))
    auth_dir = tmp_path / ".config" / "containers"
    auth_dir.mkdir(parents=True)
    auth_file = auth_dir / "auth.json"
    auth_file.write_text(
        json.dumps(
            {
                "auths": {
                    "registry-proxy.engineering.redhat.com/rh-osbs/iib": {"auth": "old-token"},
                    "other-registry.com/repo": {"auth": "keep-me"},
                }
            }
        ),
        encoding="utf-8",
    )
    authentication.create_container_auth_config(
        "registry-proxy.engineering.redhat.com/rh-osbs/iib:v4.12",
        "some-cred",
    )
    data = json.loads(auth_file.read_text(encoding="utf-8"))
    assert "registry-proxy.engineering.redhat.com/rh-osbs/iib" not in data["auths"]
    assert data["auths"]["other-registry.com/repo"]["auth"] == "keep-me"


# ---------------------------------------------------------------------------
# inspect_image_created
# ---------------------------------------------------------------------------


def _skopeo_result(
    stdout: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=""
    )


def test_inspect_image_created_success() -> None:
    """A valid skopeo response returns the created date string."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result('{"created": "2024-01-15T10:30:00Z"}'),
    ):
        assert update_fbc_catalog.inspect_image_created("img:v1") == "2024-01-15T10:30:00Z"


def test_inspect_image_created_skopeo_failure() -> None:
    """Non-zero skopeo exit returns ``None``."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result(returncode=1),
    ):
        assert update_fbc_catalog.inspect_image_created("img:v1") is None


def test_inspect_image_created_no_created_field() -> None:
    """Missing ``created`` field returns ``None``."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result("{}"),
    ):
        assert update_fbc_catalog.inspect_image_created("img:v1") is None


def test_inspect_image_created_invalid_json() -> None:
    """Invalid JSON stdout returns ``None``."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result("not json"),
    ):
        assert update_fbc_catalog.inspect_image_created("img:v1") is None


# ---------------------------------------------------------------------------
# is_build_newer_than_index
# ---------------------------------------------------------------------------


def test_is_build_newer_than_index_newer() -> None:
    """Return ``True`` when the build is newer than from_index."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image_resolved": "registry/idx@sha256:new",
    }
    with mock.patch(
        "update_fbc_catalog.inspect_image_created",
        side_effect=[
            "2024-06-01T00:00:00Z",
            "2024-01-01T00:00:00Z",
        ],
    ):
        assert update_fbc_catalog.is_build_newer_than_index(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_than_index_older() -> None:
    """Return ``False`` when the build is older than from_index."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image_resolved": "registry/idx@sha256:old",
    }
    with mock.patch(
        "update_fbc_catalog.inspect_image_created",
        side_effect=[
            "2024-01-01T00:00:00Z",
            "2024-06-01T00:00:00Z",
        ],
    ):
        assert not update_fbc_catalog.is_build_newer_than_index(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_than_index_no_resolved() -> None:
    """Return ``False`` when ``index_image_resolved`` is missing."""
    build: iib.IIBBuild = {"id": 1, "state": "complete"}
    assert not update_fbc_catalog.is_build_newer_than_index(
        build, "registry/idx:v4.12", "https://iib", "user"
    )


def test_is_build_newer_than_index_falls_back_to_resolved() -> None:
    """Use ``from_index_resolved`` when direct from_index inspect fails."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image_resolved": "registry/idx@sha256:new",
        "from_index_resolved": "registry/idx@sha256:old-resolved",
    }
    with mock.patch(
        "update_fbc_catalog.inspect_image_created",
        side_effect=[
            "2024-06-01T00:00:00Z",
            None,
            "2024-01-01T00:00:00Z",
        ],
    ):
        assert update_fbc_catalog.is_build_newer_than_index(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_than_index_skopeo_fails_uses_iib() -> None:
    """Fall back to IIB query when skopeo fails for both images."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image_resolved": "registry/idx@sha256:x",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-06-01T00:00:00Z",
    }
    with (
        mock.patch(
            "update_fbc_catalog.inspect_image_created",
            return_value=None,
        ),
        mock.patch(
            "iib.query_builds",
            return_value={"items": []},
        ),
    ):
        assert not update_fbc_catalog.is_build_newer_than_index(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


# ---------------------------------------------------------------------------
# _is_build_newer_via_iib
# ---------------------------------------------------------------------------


def test_is_build_newer_via_iib_no_newer_builds() -> None:
    """Return ``True`` when no newer build exists in IIB."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-06-01T00:00:00Z",
    }
    with mock.patch(
        "iib.query_builds",
        return_value={
            "items": [
                {
                    "id": 1,
                    "distribution_scope": "prod",
                    "updated": "2024-06-01T00:00:00Z",
                }
            ],
        },
    ):
        assert update_fbc_catalog._is_build_newer_via_iib(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_via_iib_newer_build_exists() -> None:
    """Return ``False`` when a newer build exists for the same from_index."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-01-01T00:00:00Z",
    }
    with mock.patch(
        "iib.query_builds",
        return_value={
            "items": [
                {
                    "id": 2,
                    "distribution_scope": "prod",
                    "updated": "2024-06-01T00:00:00Z",
                }
            ],
        },
    ):
        assert not update_fbc_catalog._is_build_newer_via_iib(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_via_iib_query_fails() -> None:
    """Return ``False`` when the IIB query fails."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-06-01T00:00:00Z",
    }
    with mock.patch(
        "iib.query_builds",
        side_effect=requests.ConnectionError("timeout"),
    ):
        assert not update_fbc_catalog._is_build_newer_via_iib(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_via_iib_mismatched_from_index() -> None:
    """Return ``False`` when the build's from_index doesn't match."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.11",
        "updated": "2024-06-01T00:00:00Z",
    }
    assert not update_fbc_catalog._is_build_newer_via_iib(
        build, "registry/idx:v4.12", "https://iib", "user"
    )


# ---------------------------------------------------------------------------
# check_previous_build
# ---------------------------------------------------------------------------


def test_check_previous_build_finds_fresh_complete() -> None:
    """Return a fresh completed build when one exists."""
    complete_build: iib.IIBBuild = {
        "id": 10,
        "state": "complete",
        "fbc_fragments": ["b", "a"],
        "distribution_scope": "prod",
        "updated": "2024-06-01T00:00:00Z",
    }
    with (
        mock.patch(
            "iib.query_builds",
            return_value={"items": [complete_build]},
        ),
        mock.patch(
            "update_fbc_catalog.is_build_newer_than_index",
            return_value=True,
        ),
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a", "b"],
            [],
        )
    assert result == complete_build


def test_check_previous_build_stale_complete_finds_in_progress() -> None:
    """Fall through to in-progress when completed build is stale."""
    ip_build: iib.IIBBuild = {
        "id": 20,
        "state": "in_progress",
        "fbc_fragments": ["a", "b"],
        "distribution_scope": "prod",
        "updated": "2024-06-01T00:00:00Z",
    }
    with (
        mock.patch(
            "iib.query_builds",
            side_effect=[
                {
                    "items": [
                        {
                            "id": 10,
                            "state": "complete",
                            "fbc_fragments": ["a", "b"],
                            "distribution_scope": "prod",
                            "updated": "2024-01-01T00:00:00Z",
                        }
                    ]
                },
                {"items": [ip_build]},
            ],
        ),
        mock.patch(
            "update_fbc_catalog.is_build_newer_than_index",
            return_value=False,
        ),
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a", "b"],
            [],
        )
    assert result == ip_build


def test_check_previous_build_filters_by_build_tags() -> None:
    """In-progress builds are filtered by build_tags when provided."""
    with mock.patch(
        "iib.query_builds",
        side_effect=[
            {"items": []},
            {
                "items": [
                    {
                        "id": 1,
                        "state": "in_progress",
                        "fbc_fragments": ["a"],
                        "distribution_scope": "prod",
                        "updated": "2024-01-01T00:00:00Z",
                        "build_tags": ["other-plr"],
                    },
                    {
                        "id": 2,
                        "state": "in_progress",
                        "fbc_fragments": ["a"],
                        "distribution_scope": "prod",
                        "updated": "2024-06-01T00:00:00Z",
                        "build_tags": ["my-plr"],
                    },
                ]
            },
        ],
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            ["my-plr"],
        )
    assert result is not None
    assert result["id"] == 2


def test_check_previous_build_no_match() -> None:
    """Return ``None`` when no builds match."""
    with mock.patch(
        "iib.query_builds",
        return_value={"items": []},
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            [],
        )
    assert result is None


def test_check_previous_build_query_error() -> None:
    """Return ``None`` when the IIB query fails."""
    with mock.patch(
        "iib.query_builds",
        side_effect=requests.ConnectionError("timeout"),
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            [],
        )
    assert result is None


# ---------------------------------------------------------------------------
# poll_build_status
# ---------------------------------------------------------------------------


def test_poll_build_status_immediate_complete() -> None:
    """Return immediately when the build is already complete."""
    build: iib.IIBBuild = {"id": 1, "state": "complete"}
    with mock.patch("iib.get_build", return_value=build):
        result = update_fbc_catalog.poll_build_status(
            "https://iib",
            1,
            3600,
            sleep_fn=lambda _: None,
            clock_fn=mock.MagicMock(side_effect=[0.0, 0.0, 0.0]),
        )
    assert result["state"] == "complete"


def test_poll_build_status_polls_until_complete() -> None:
    """Poll multiple times before the build completes."""
    builds = [
        {"id": 1, "state": "in_progress"},
        {"id": 1, "state": "in_progress"},
        {"id": 1, "state": "complete"},
    ]
    clock_values = [0.0, 0.0, 0.0, 30.0, 30.0, 60.0, 60.0]
    with mock.patch("iib.get_build", side_effect=builds):
        result = update_fbc_catalog.poll_build_status(
            "https://iib",
            1,
            3600,
            sleep_fn=lambda _: None,
            clock_fn=mock.MagicMock(side_effect=clock_values),
        )
    assert result["state"] == "complete"


def test_poll_build_status_timeout() -> None:
    """Raise ``TimeoutError`` when timeout is exceeded."""
    with mock.patch(
        "iib.get_build",
        return_value={"id": 1, "state": "in_progress"},
    ):
        with pytest.raises(TimeoutError, match="Timeout after"):
            update_fbc_catalog.poll_build_status(
                "https://iib",
                1,
                60,
                sleep_fn=lambda _: None,
                clock_fn=mock.MagicMock(side_effect=[0.0, 0.0, 0.0, 61.0]),
            )


def test_poll_build_status_failed_state() -> None:
    """Return build info when the build fails."""
    build: iib.IIBBuild = {"id": 1, "state": "failed", "state_reason": "oops"}
    with mock.patch("iib.get_build", return_value=build):
        result = update_fbc_catalog.poll_build_status(
            "https://iib",
            1,
            3600,
            sleep_fn=lambda _: None,
            clock_fn=mock.MagicMock(side_effect=[0.0, 0.0, 0.0]),
        )
    assert result["state"] == "failed"


def test_poll_build_status_removes_state_history() -> None:
    """``state_history`` is removed from the returned build info."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "state_history": [{"state": "in_progress"}],
    }
    with mock.patch("iib.get_build", return_value=build):
        result = update_fbc_catalog.poll_build_status(
            "https://iib",
            1,
            3600,
            sleep_fn=lambda _: None,
            clock_fn=mock.MagicMock(side_effect=[0.0, 0.0, 0.0]),
        )
    assert "state_history" not in result


def test_poll_build_status_retries_on_fetch_error() -> None:
    """Transient fetch errors are retried."""
    builds = [
        requests.ConnectionError("fail"),
        {"id": 1, "state": "complete"},
    ]
    clock_values = [0.0, 0.0, 0.0, 30.0, 30.0, 30.0]

    def _get_build(_url: str, _id: int) -> iib.IIBBuild:
        val = builds.pop(0)
        if isinstance(val, Exception):
            raise val
        return val

    with mock.patch("iib.get_build", side_effect=_get_build):
        result = update_fbc_catalog.poll_build_status(
            "https://iib",
            1,
            3600,
            sleep_fn=lambda _: None,
            clock_fn=mock.MagicMock(side_effect=clock_values),
        )
    assert result["state"] == "complete"


# ---------------------------------------------------------------------------
# validate_index_image
# ---------------------------------------------------------------------------


def test_validate_index_image_overwrite_and_publish_match() -> None:
    """No error when index_image matches from_index."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image": "registry/idx:v4.12",
        "from_index": "registry/idx:v4.12",
    }
    update_fbc_catalog.validate_index_image(build, True, True)


def test_validate_index_image_overwrite_and_publish_mismatch() -> None:
    """Raise ``ValueError`` when index_image doesn't match from_index."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image": "registry/idx:wrong",
        "from_index": "registry/idx:v4.12",
    }
    with pytest.raises(ValueError, match="Index image mismatch"):
        update_fbc_catalog.validate_index_image(build, True, True)


def test_validate_index_image_overwrite_without_publish() -> None:
    """Raise ``ValueError`` for invalid overwrite+no-publish combination."""
    build: iib.IIBBuild = {"id": 1, "state": "complete"}
    with pytest.raises(ValueError, match="Invalid combination"):
        update_fbc_catalog.validate_index_image(build, True, False)


def test_validate_index_image_no_overwrite_passes() -> None:
    """No error for non-overwrite strategies."""
    build: iib.IIBBuild = {"id": 1, "state": "complete"}
    update_fbc_catalog.validate_index_image(build, False, True)
    update_fbc_catalog.validate_index_image(build, False, False)


# ---------------------------------------------------------------------------
# get_manifest_digests
# ---------------------------------------------------------------------------


def test_get_manifest_digests_success() -> None:
    """Return space-separated digests from a manifest list."""
    manifest = {
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": "sha256:aaa",
            },
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": "sha256:bbb",
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:ccc",
            },
        ],
    }
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result(json.dumps(manifest)),
    ):
        result = update_fbc_catalog.get_manifest_digests("img:v1")
    assert result == "sha256:aaa sha256:bbb"


def test_get_manifest_digests_not_multiarch() -> None:
    """Raise ``RuntimeError`` when no v2 manifests are found."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result('{"manifests": []}'),
    ):
        with pytest.raises(RuntimeError, match="not multi-arch"):
            update_fbc_catalog.get_manifest_digests("img:v1")


def test_get_manifest_digests_skopeo_failure() -> None:
    """Raise ``RuntimeError`` when skopeo fails."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result(returncode=1),
    ):
        with pytest.raises(RuntimeError, match="skopeo inspect --raw failed"):
            update_fbc_catalog.get_manifest_digests("img:v1")


# ---------------------------------------------------------------------------
# _poll_and_collect
# ---------------------------------------------------------------------------


def test_poll_and_collect_complete_success() -> None:
    """Successful complete build returns exit_code=0 with digests."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "internal_index_image_copy": "internal/img:v1",
        "index_image": "registry/idx:v4.12",
        "from_index": "registry/idx:v4.12",
        "logs": {"url": "https://logs/1"},
    }
    manifest = {
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": "sha256:abc",
            }
        ],
    }
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result(json.dumps(manifest)),
    ):
        result = update_fbc_catalog._poll_and_collect(
            "https://iib",
            build,
            3600,
            True,
            True,
        )
    assert result.exit_code == 0
    assert result.state == "complete"
    assert result.index_image_digests == "sha256:abc"
    assert result.iib_log_url == "https://logs/1"


def test_poll_and_collect_failed_build() -> None:
    """Failed build returns exit_code=1."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "failed",
        "state_reason": "something broke",
    }
    result = update_fbc_catalog._poll_and_collect(
        "https://iib",
        build,
        3600,
        False,
        False,
    )
    assert result.exit_code == 1
    assert result.state == "failed"


def test_poll_and_collect_timeout() -> None:
    """Timeout returns exit_code=124."""
    build: iib.IIBBuild = {"id": 1, "state": "in_progress"}
    with mock.patch(
        "update_fbc_catalog.poll_build_status",
        side_effect=TimeoutError("timed out"),
    ):
        result = update_fbc_catalog._poll_and_collect(
            "https://iib",
            build,
            60,
            False,
            False,
        )
    assert result.exit_code == 124
    assert result.state == "failed"
    assert result.state_reason == "Build timeout"


def test_poll_and_collect_missing_id() -> None:
    """Raise ``ValueError`` when build has no id."""
    build: iib.IIBBuild = {"state": "complete"}
    with pytest.raises(ValueError, match="missing 'id'"):
        update_fbc_catalog._poll_and_collect(
            "https://iib",
            build,
            3600,
            False,
            False,
        )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _setup_result_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> dict[str, Path]:
    """Set up Tekton result env vars and return paths."""
    paths = {
        "RESULT_JSON_BUILD_INFO": tmp_path / "json_build_info",
        "RESULT_BUILD_STATE": tmp_path / "build_state",
        "RESULT_INDEX_IMAGE_DIGESTS": tmp_path / "digests",
        "RESULT_IIB_LOG": tmp_path / "iib_log",
        "RESULT_EXIT_CODE": tmp_path / "exit_code",
    }
    for name, path in paths.items():
        monkeypatch.setenv(name, str(path))
    return paths


def test_main_invalid_fbc_fragments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Invalid fbc_fragments writes failure and exits."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    with pytest.raises(SystemExit, match="update_fbc_catalog.py"):
        update_fbc_catalog.main(
            [
                "--fbc-fragments",
                "not-json",
                "--from-index",
                "idx:v1",
            ]
        )
    state = json.loads(paths["RESULT_BUILD_STATE"].read_text(encoding="utf-8"))
    assert state["state"] == "failed"
    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "1"


def test_main_empty_fbc_fragments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Empty fbc_fragments array writes failure and exits."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    with pytest.raises(SystemExit, match="empty"):
        update_fbc_catalog.main(
            [
                "--fbc-fragments",
                "[]",
                "--from-index",
                "idx:v1",
            ]
        )
    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "1"


def test_main_writes_results_on_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Successful run writes all result files and returns 0."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    monkeypatch.setenv("IIB_SERVICE_ACCOUNT_MOUNT", str(tmp_path / "sa"))
    monkeypatch.setenv("IIB_SERVICES_CONFIG_MOUNT", str(tmp_path / "cfg"))
    monkeypatch.setenv(
        "IIB_OVERWRITE_FROMIMAGE_CREDENTIALS_MOUNT",
        str(tmp_path / "ow"),
    )
    monkeypatch.setenv(
        "PUBLISHING_CREDENTIALS_MOUNT",
        str(tmp_path / "pub"),
    )

    fake_result = update_fbc_catalog.RunResult(
        build_info={"id": 1, "state": "complete"},
        state="complete",
        state_reason="",
        index_image_digests="sha256:abc sha256:def",
        iib_log_url="https://logs/1",
        exit_code=0,
    )
    with mock.patch(
        "update_fbc_catalog.run",
        return_value=fake_result,
    ):
        rc = update_fbc_catalog.main(
            [
                "--fbc-fragments",
                '["frag-a"]',
                "--from-index",
                "idx:v1",
            ]
        )

    assert rc == 0
    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "0"
    assert (
        paths["RESULT_INDEX_IMAGE_DIGESTS"].read_text(encoding="utf-8")
        == "sha256:abc sha256:def"
    )
    state = json.loads(paths["RESULT_BUILD_STATE"].read_text(encoding="utf-8"))
    assert state["state"] == "complete"
    compressed = paths["RESULT_JSON_BUILD_INFO"].read_text(encoding="utf-8")
    assert iib.decompress_build_info(compressed) == {
        "id": 1,
        "state": "complete",
    }
    assert (
        paths["RESULT_IIB_LOG"].read_text(encoding="utf-8") == "IIB log url is: https://logs/1"
    )


def test_main_check_step_error_writes_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """``CheckStepError`` from ``run`` writes failure and exits."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    monkeypatch.setenv("IIB_SERVICE_ACCOUNT_MOUNT", str(tmp_path / "sa"))
    monkeypatch.setenv("IIB_SERVICES_CONFIG_MOUNT", str(tmp_path / "cfg"))
    monkeypatch.setenv(
        "IIB_OVERWRITE_FROMIMAGE_CREDENTIALS_MOUNT",
        str(tmp_path / "ow"),
    )
    monkeypatch.setenv(
        "PUBLISHING_CREDENTIALS_MOUNT",
        str(tmp_path / "pub"),
    )

    with mock.patch(
        "update_fbc_catalog.run",
        side_effect=tekton.CheckStepError(
            "kinit",
            subprocess.CalledProcessError(1, "kinit"),
        ),
    ):
        with pytest.raises(SystemExit, match="update_fbc_catalog.py"):
            update_fbc_catalog.main(
                [
                    "--fbc-fragments",
                    '["frag-a"]',
                    "--from-index",
                    "idx:v1",
                ]
            )

    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "1"
    state = json.loads(paths["RESULT_BUILD_STATE"].read_text(encoding="utf-8"))
    assert state["state"] == "failed"


def test_main_missing_result_env_exits() -> None:
    """Missing result env vars raise ``SystemExit``."""
    with pytest.raises(SystemExit):
        update_fbc_catalog.main(
            [
                "--fbc-fragments",
                '["a"]',
                "--from-index",
                "idx:v1",
            ]
        )


def test_main_invalid_build_tags_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Invalid build-tags JSON writes failure and exits."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    with pytest.raises(SystemExit, match="Invalid JSON"):
        update_fbc_catalog.main(
            [
                "--fbc-fragments",
                '["a"]',
                "--from-index",
                "idx:v1",
                "--build-tags",
                "not-json",
            ]
        )
    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "1"


def test_main_returns_nonzero_on_failed_build(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Failed build result returns non-zero exit code from main."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    _setup_mount_env(monkeypatch, tmp_path)

    fake_result = update_fbc_catalog.RunResult(
        build_info={"id": 1, "state": "failed"},
        state="failed",
        state_reason="Build failed with exit code 1",
        index_image_digests="",
        iib_log_url="",
        exit_code=1,
    )
    with mock.patch(
        "update_fbc_catalog.run",
        return_value=fake_result,
    ):
        rc = update_fbc_catalog.main(
            [
                "--fbc-fragments",
                '["a"]',
                "--from-index",
                "idx:v1",
            ]
        )

    assert rc == 1
    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "1"
    state = json.loads(paths["RESULT_BUILD_STATE"].read_text(encoding="utf-8"))
    assert state["state"] == "failed"
    assert state["state_reason"] == "Build failed with exit code 1"


def test_main_timeout_returns_124(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Timeout result returns exit code 124 from main."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    _setup_mount_env(monkeypatch, tmp_path)

    fake_result = update_fbc_catalog.RunResult(
        build_info={"id": 1, "state": "in_progress"},
        state="failed",
        state_reason="Build timeout",
        index_image_digests="",
        iib_log_url="",
        exit_code=124,
    )
    with mock.patch(
        "update_fbc_catalog.run",
        return_value=fake_result,
    ):
        rc = update_fbc_catalog.main(
            [
                "--fbc-fragments",
                '["a"]',
                "--from-index",
                "idx:v1",
            ]
        )

    assert rc == 124
    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "124"


def _setup_mount_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Set up mount path env vars."""
    monkeypatch.setenv("IIB_SERVICE_ACCOUNT_MOUNT", str(tmp_path / "sa"))
    monkeypatch.setenv("IIB_SERVICES_CONFIG_MOUNT", str(tmp_path / "cfg"))
    monkeypatch.setenv(
        "IIB_OVERWRITE_FROMIMAGE_CREDENTIALS_MOUNT",
        str(tmp_path / "ow"),
    )
    monkeypatch.setenv(
        "PUBLISHING_CREDENTIALS_MOUNT",
        str(tmp_path / "pub"),
    )


# ---------------------------------------------------------------------------
# poll_build_status — new behavior tests
# ---------------------------------------------------------------------------


def test_poll_build_status_skips_error_responses() -> None:
    """Responses with an ``error`` field are retried."""
    responses = [
        {"error": "service unavailable"},
        {"id": 1, "state": "complete"},
    ]
    clock_values = [0.0, 0.0, 30.0, 30.0, 30.0]

    def _get_build(_url: str, _id: int) -> iib.IIBBuild:
        return responses.pop(0)

    with mock.patch("iib.get_build", side_effect=_get_build):
        result = update_fbc_catalog.poll_build_status(
            "https://iib",
            1,
            3600,
            sleep_fn=lambda _: None,
            clock_fn=mock.MagicMock(side_effect=clock_values),
        )
    assert result["state"] == "complete"


def test_poll_build_status_writes_log_url(tmp_path: Path) -> None:
    """Log URL is written to iib_log_path each iteration."""
    builds = [
        {"id": 1, "state": "in_progress", "logs": {"url": "https://logs/1"}},
        {"id": 1, "state": "complete", "logs": {"url": "https://logs/2"}},
    ]
    clock_values = [0.0, 0.0, 0.0, 30.0, 30.0, 30.0]
    log_path = tmp_path / "iib_log"

    with mock.patch("iib.get_build", side_effect=builds):
        update_fbc_catalog.poll_build_status(
            "https://iib",
            1,
            3600,
            iib_log_path=log_path,
            sleep_fn=lambda _: None,
            clock_fn=mock.MagicMock(side_effect=clock_values),
        )

    assert log_path.read_text(encoding="utf-8") == "IIB log url is: https://logs/2"


# ---------------------------------------------------------------------------
# _poll_and_collect — additional paths
# ---------------------------------------------------------------------------


def test_poll_and_collect_validation_error() -> None:
    """Index image validation failure returns exit_code=1."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image": "registry/idx:wrong",
        "from_index": "registry/idx:v4.12",
    }
    result = update_fbc_catalog._poll_and_collect(
        "https://iib",
        build,
        3600,
        True,
        True,
    )
    assert result.exit_code == 1
    assert result.state == "failed"
    assert "Index image mismatch" in result.state_reason


def test_poll_and_collect_missing_internal_copy() -> None:
    """Missing internal_index_image_copy returns exit_code=1."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image": "registry/idx:v4.12",
        "from_index": "registry/idx:v4.12",
    }
    result = update_fbc_catalog._poll_and_collect(
        "https://iib",
        build,
        3600,
        True,
        True,
    )
    assert result.exit_code == 1
    assert "Missing internal_index_image_copy" in result.state_reason


def test_poll_and_collect_digest_extraction_fails() -> None:
    """Failed manifest digest extraction returns exit_code=1."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "internal_index_image_copy": "internal/img:v1",
    }
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result(returncode=1),
    ):
        result = update_fbc_catalog._poll_and_collect(
            "https://iib",
            build,
            3600,
            False,
            False,
        )
    assert result.exit_code == 1
    assert "Failed to get manifest digests" in result.state_reason


def test_poll_and_collect_polls_in_progress_build() -> None:
    """In-progress build is polled until complete."""
    ip_build: iib.IIBBuild = {"id": 1, "state": "in_progress"}
    complete_build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "internal_index_image_copy": "internal/img:v1",
    }
    manifest = {
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": "sha256:abc",
            }
        ],
    }
    with (
        mock.patch(
            "update_fbc_catalog.poll_build_status",
            return_value=complete_build,
        ),
        mock.patch(
            "skopeo.subprocess.run",
            return_value=_skopeo_result(json.dumps(manifest)),
        ),
    ):
        result = update_fbc_catalog._poll_and_collect(
            "https://iib",
            ip_build,
            3600,
            False,
            False,
        )
    assert result.exit_code == 0
    assert result.index_image_digests == "sha256:abc"


def test_poll_and_collect_failed_state_reason() -> None:
    """Failed build uses state_reason from IIB or fallback message."""
    build_with_reason: iib.IIBBuild = {
        "id": 1,
        "state": "failed",
        "state_reason": "IIB internal error",
    }
    result = update_fbc_catalog._poll_and_collect(
        "https://iib",
        build_with_reason,
        3600,
        False,
        False,
    )
    assert result.state_reason == "IIB internal error"

    build_no_reason: iib.IIBBuild = {
        "id": 2,
        "state": "failed",
    }
    result2 = update_fbc_catalog._poll_and_collect(
        "https://iib",
        build_no_reason,
        3600,
        False,
        False,
    )
    assert result2.state_reason == "Build failed with exit code 1"


# ---------------------------------------------------------------------------
# _is_build_newer_via_iib — additional paths
# ---------------------------------------------------------------------------


def test_is_build_newer_via_iib_empty_items() -> None:
    """Return ``False`` when IIB returns empty items list."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-06-01T00:00:00Z",
    }
    with mock.patch(
        "iib.query_builds",
        return_value={"items": []},
    ):
        assert not update_fbc_catalog._is_build_newer_via_iib(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_via_iib_no_updated_field() -> None:
    """Return ``False`` when the build has no updated timestamp."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.12",
    }
    assert not update_fbc_catalog._is_build_newer_via_iib(
        build, "registry/idx:v4.12", "https://iib", "user"
    )


def test_is_build_newer_via_iib_unparseable_timestamps() -> None:
    """Unparseable timestamps default to 0 (matching bash behavior)."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.12",
        "updated": "not-a-date",
    }
    with mock.patch(
        "iib.query_builds",
        return_value={
            "items": [
                {
                    "id": 2,
                    "distribution_scope": "prod",
                    "updated": "also-not-a-date",
                }
            ],
        },
    ):
        assert update_fbc_catalog._is_build_newer_via_iib(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_is_build_newer_via_iib_filters_invalid_scope() -> None:
    """Builds with non-valid distribution_scope are excluded."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-06-01T00:00:00Z",
    }
    with mock.patch(
        "iib.query_builds",
        return_value={
            "items": [
                {
                    "id": 2,
                    "distribution_scope": "dev",
                    "updated": "2024-12-01T00:00:00Z",
                }
            ],
        },
    ):
        assert not update_fbc_catalog._is_build_newer_via_iib(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


# ---------------------------------------------------------------------------
# check_previous_build — additional paths
# ---------------------------------------------------------------------------


def test_check_previous_build_ignores_non_matching_fragments() -> None:
    """Builds with different fbc_fragments are not returned."""
    with mock.patch(
        "iib.query_builds",
        side_effect=[
            {
                "items": [
                    {
                        "id": 1,
                        "state": "complete",
                        "fbc_fragments": ["x", "y"],
                        "distribution_scope": "prod",
                        "updated": "2024-06-01T00:00:00Z",
                    }
                ]
            },
            {"items": []},
        ],
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a", "b"],
            [],
        )
    assert result is None


def test_check_previous_build_ignores_null_fragments() -> None:
    """Builds with null fbc_fragments are skipped."""
    with mock.patch(
        "iib.query_builds",
        side_effect=[
            {
                "items": [
                    {
                        "id": 1,
                        "state": "complete",
                        "fbc_fragments": None,
                        "distribution_scope": "prod",
                        "updated": "2024-06-01T00:00:00Z",
                    }
                ]
            },
            {"items": []},
        ],
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            [],
        )
    assert result is None


def test_check_previous_build_ignores_dev_scope() -> None:
    """Builds with distribution_scope 'dev' are excluded."""
    with mock.patch(
        "iib.query_builds",
        side_effect=[
            {
                "items": [
                    {
                        "id": 1,
                        "state": "complete",
                        "fbc_fragments": ["a"],
                        "distribution_scope": "dev",
                        "updated": "2024-06-01T00:00:00Z",
                    }
                ]
            },
            {"items": []},
        ],
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            [],
        )
    assert result is None


def test_check_previous_build_picks_latest_completed() -> None:
    """When multiple completed builds match, the latest is picked."""
    with (
        mock.patch(
            "iib.query_builds",
            return_value={
                "items": [
                    {
                        "id": 1,
                        "state": "complete",
                        "fbc_fragments": ["a"],
                        "distribution_scope": "prod",
                        "updated": "2024-01-01T00:00:00Z",
                    },
                    {
                        "id": 2,
                        "state": "complete",
                        "fbc_fragments": ["a"],
                        "distribution_scope": "prod",
                        "updated": "2024-06-01T00:00:00Z",
                    },
                ]
            },
        ),
        mock.patch(
            "update_fbc_catalog.is_build_newer_than_index",
            return_value=True,
        ),
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            [],
        )
    assert result is not None
    assert result["id"] == 2


def test_check_previous_build_no_build_tags_matches_all_ip() -> None:
    """Without build_tags, any in-progress build with matching fragments is returned."""
    with mock.patch(
        "iib.query_builds",
        side_effect=[
            {"items": []},
            {
                "items": [
                    {
                        "id": 1,
                        "state": "in_progress",
                        "fbc_fragments": ["a"],
                        "distribution_scope": "prod",
                        "updated": "2024-06-01T00:00:00Z",
                        "build_tags": ["some-plr"],
                    }
                ]
            },
        ],
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            [],
        )
    assert result is not None
    assert result["id"] == 1


def test_check_previous_build_in_progress_query_error() -> None:
    """Return ``None`` when the in-progress query fails."""
    with mock.patch(
        "iib.query_builds",
        side_effect=[
            {"items": []},
            requests.ConnectionError("timeout"),
        ],
    ):
        result = update_fbc_catalog.check_previous_build(
            "https://iib",
            "user",
            "idx:v4.12",
            ["a"],
            [],
        )
    assert result is None


# ---------------------------------------------------------------------------
# is_build_newer_than_index — additional paths
# ---------------------------------------------------------------------------


def test_is_build_newer_catalog_date_unparseable() -> None:
    """Unparseable catalog date falls through to IIB check."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "index_image_resolved": "registry/idx@sha256:x",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-06-01T00:00:00Z",
    }
    with (
        mock.patch(
            "update_fbc_catalog.inspect_image_created",
            side_effect=["bad-date", "2024-01-01T00:00:00Z"],
        ),
        mock.patch(
            "update_fbc_catalog._is_build_newer_via_iib",
            return_value=True,
        ) as iib_mock,
    ):
        assert update_fbc_catalog.is_build_newer_than_index(
            build, "registry/idx:v4.12", "https://iib", "user"
        )
    iib_mock.assert_called_once()


# ---------------------------------------------------------------------------
# inspect_image_created — additional paths
# ---------------------------------------------------------------------------


def test_inspect_image_created_null_string() -> None:
    """A ``created`` value of literal ``"null"`` returns ``None``."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result('{"created": "null"}'),
    ):
        assert update_fbc_catalog.inspect_image_created("img:v1") is None


# ---------------------------------------------------------------------------
# Scenarios from bash test suite
# ---------------------------------------------------------------------------


def test_auth_failure_falls_back_to_iib_and_reuses() -> None:
    """When skopeo fails for both images, IIB fallback confirms reuse."""
    build: iib.IIBBuild = {
        "id": 10,
        "state": "complete",
        "index_image_resolved": "registry/idx@sha256:resolved",
        "from_index": "registry/idx:v4.12",
        "updated": "2024-06-01T00:00:00Z",
    }
    with (
        mock.patch(
            "update_fbc_catalog.inspect_image_created",
            return_value=None,
        ),
        mock.patch(
            "iib.query_builds",
            return_value={
                "items": [
                    {
                        "id": 10,
                        "distribution_scope": "prod",
                        "updated": "2024-06-01T00:00:00Z",
                    }
                ],
            },
        ),
    ):
        assert update_fbc_catalog.is_build_newer_than_index(
            build, "registry/idx:v4.12", "https://iib", "user"
        )


def test_main_empty_from_index(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Empty ``--from-index`` writes failure and exits."""
    paths = _setup_result_env(monkeypatch, tmp_path)
    with pytest.raises(SystemExit, match="from-index is required"):
        update_fbc_catalog.main(
            [
                "--fbc-fragments",
                '["a"]',
                "--from-index",
                "",
            ]
        )
    assert paths["RESULT_EXIT_CODE"].read_text(encoding="utf-8") == "1"


def test_poll_and_collect_removes_state_history_without_polling() -> None:
    """``state_history`` is removed even when polling is skipped."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "internal_index_image_copy": "internal/img:v1",
        "state_history": [{"state": "in_progress"}],
    }
    manifest = {
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": "sha256:abc",
            }
        ],
    }
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_skopeo_result(json.dumps(manifest)),
    ):
        result = update_fbc_catalog._poll_and_collect(
            "https://iib",
            build,
            3600,
            False,
            False,
        )
    assert result.exit_code == 0
    assert "state_history" not in result.build_info
