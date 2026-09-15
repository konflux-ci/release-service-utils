"""Tests for `filter_published_fbc_images` task logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import requests
from release_service_utils.tasks.managed.filter_published_fbc_images import (
    filter_published_fbc_images as fbc,
)

TASK = (
    "release_service_utils.tasks.managed.filter_published_fbc_images"
    ".filter_published_fbc_images"
)


def _component(name: str, digest: str) -> dict[str, str]:
    """Build a snapshot component dict."""
    return {"name": name, "containerImage": f"quay.io/test/{name}@{digest}"}


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _workspace(
    tmp_path: Path,
    snapshot: dict[str, Any],
    data: dict[str, Any],
) -> tuple[Path, Path, Path, Path]:
    """Create snapshot, data, result, and cert-mount paths under *tmp_path*."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    result_path = tmp_path / "result.txt"
    cert_mount = tmp_path / "secrets"
    cert_mount.mkdir()
    (cert_mount / "cert").write_text("cert", encoding="utf-8")
    (cert_mount / "key").write_text("key", encoding="utf-8")
    _write_json(snapshot_path, snapshot)
    _write_json(data_path, data)
    return snapshot_path, data_path, result_path, cert_mount


def _run(
    tmp_path: Path,
    snapshot: dict[str, Any],
    data: dict[str, Any],
) -> dict[str, Any]:
    """Run the filter workflow and return the written snapshot."""
    snapshot_path, data_path, result_path, cert_mount = _workspace(
        tmp_path,
        snapshot,
        data,
    )
    fbc.run(
        data_dir=tmp_path,
        snapshot_path=snapshot_path,
        data_path=data_path,
        result_path=result_path,
        pyxis_secret_mount=cert_mount,
    )
    assert result_path.read_text(encoding="utf-8") == fbc.FILTERED_SNAPSHOT_FILENAME
    return json.loads((tmp_path / fbc.FILTERED_SNAPSHOT_FILENAME).read_text())


# -- helpers -----------------------------------------------------------------


def test_last_update_date_filter_is_iso_date() -> None:
    """Return a YYYY-MM-DD date string."""
    value = fbc.last_update_date_filter()
    assert len(value) == 10
    assert value[4] == "-" and value[7] == "-"


def test_resolve_pyxis_api_url_prefers_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use PYXIS_URL when the test harness sets a mock server URL."""
    monkeypatch.setenv("PYXIS_URL", "http://127.0.0.1:8080/v1/")
    assert fbc.resolve_pyxis_api_url("stage") == "http://127.0.0.1:8080/v1"


def test_resolve_pyxis_api_url_maps_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Map a Pyxis server name when PYXIS_URL is unset."""
    monkeypatch.delenv("PYXIS_URL", raising=False)
    assert fbc.resolve_pyxis_api_url("production") == "https://pyxis.api.redhat.com/v1"


def test_resolve_pyxis_api_url_rejects_invalid_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise when the server name is not a known Pyxis environment."""
    monkeypatch.delenv("PYXIS_URL", raising=False)
    with pytest.raises(ValueError, match="Invalid server"):
        fbc.resolve_pyxis_api_url("not-a-server")


def test_pyxis_server_name_defaults() -> None:
    """Default to production when pyxis.server is missing."""
    assert fbc.pyxis_server_name({}) == "production"
    assert fbc.pyxis_server_name({"pyxis": {}}) == "production"
    assert fbc.pyxis_server_name({"pyxis": {"server": "stage"}}) == "stage"


def test_is_staged_index_bool_and_string() -> None:
    """Treat JSON true and the string true as staged."""
    assert fbc.is_staged_index({"fbc": {"stagedIndex": True}}) is True
    assert fbc.is_staged_index({"fbc": {"stagedIndex": "true"}}) is True
    assert fbc.is_staged_index({"fbc": {"stagedIndex": "True"}}) is True
    assert fbc.is_staged_index({"fbc": {"stagedIndex": False}}) is False
    assert fbc.is_staged_index({"fbc": {}}) is False
    assert fbc.is_staged_index({}) is False


def test_fbc_target_index_reads_template() -> None:
    """Return the targetIndex template or an empty string."""
    assert fbc.fbc_target_index({"fbc": {"targetIndex": "catalog:{{ OCP_VERSION }}"}}) == (
        "catalog:{{ OCP_VERSION }}"
    )
    assert fbc.fbc_target_index({"fbc": {"targetIndex": ""}}) == ""
    assert fbc.fbc_target_index({"fbc": {"targetIndex": None}}) == ""
    assert fbc.fbc_target_index({"fbc": {}}) == ""
    assert fbc.fbc_target_index({}) == ""


def test_extract_component_ocp_version_adds_v_prefix() -> None:
    """Prefix a bare X.Y tag with v."""
    with patch(f"{TASK}.resolve_ocp_version", return_value="4.15"):
        assert fbc.extract_component_ocp_version(_component("comp", "sha256:abc")) == "v4.15"


def test_extract_component_ocp_version_keeps_existing_prefix() -> None:
    """Leave a tag that already starts with v unchanged."""
    with patch(f"{TASK}.resolve_ocp_version", return_value="v4.15"):
        assert fbc.extract_component_ocp_version(_component("comp", "sha256:abc")) == "v4.15"


def test_extract_component_ocp_version_missing_annotation() -> None:
    """Raise when the base-image annotation is absent."""
    with patch(f"{TASK}.resolve_ocp_version", return_value=""):
        with pytest.raises(ValueError, match="not found"):
            fbc.extract_component_ocp_version(_component("comp", "sha256:abc"))


def test_extract_component_ocp_version_invalid_format() -> None:
    """Raise when the version is not vX.Y with a one- or two-digit minor."""
    with patch(f"{TASK}.resolve_ocp_version", return_value="4.14.1"):
        with pytest.raises(ValueError, match="Invalid OCP version format"):
            fbc.extract_component_ocp_version(_component("comp", "sha256:abc"))


def test_resolve_target_index_spacing_variants() -> None:
    """Replace OCP_VERSION placeholders regardless of interior spacing."""
    assert (
        fbc.resolve_target_index("quay.io/cat:{{ OCP_VERSION }}", "v4.15")
        == "quay.io/cat:v4.15"
    )
    assert (
        fbc.resolve_target_index("quay.io/cat:{{OCP_VERSION}}", "v4.15") == "quay.io/cat:v4.15"
    )
    assert (
        fbc.resolve_target_index("quay.io/cat:{{  OCP_VERSION  }}", "v4.15")
        == "quay.io/cat:v4.15"
    )


def test_fragment_digest_with_and_without_at() -> None:
    """Split on the last @ when present; otherwise return the whole reference."""
    assert fbc.fragment_digest("quay.io/test/img@sha256:abc") == "sha256:abc"
    assert fbc.fragment_digest("quay.io/test/img:tag") == "quay.io/test/img:tag"


def test_unique_target_indexes_preserves_order() -> None:
    """Deduplicate resolved indexes while keeping first-seen order."""
    template = "catalog:{{ OCP_VERSION }}"
    indexes = fbc.unique_target_indexes(
        [["v4.14", "v4.16"], ["v4.14"]],
        template,
    )
    assert indexes == ["catalog:v4.14", "catalog:v4.16"]


def test_extract_published_digests_from_related_images_and_bundles() -> None:
    """Collect unique digests from related_images and nested bundles."""
    records = [
        {
            "related_images": [
                {"digest": "sha256:abc", "image": "quay.io/a@sha256:abc"},
                {"image": "quay.io/b@sha256:def"},
                {"digest": ""},
                {"image": "quay.io/no-digest"},
                "skip-me",
            ],
            "bundles": [
                {
                    "related_images": [
                        {"digest": "sha256:bun"},
                        {"image": "quay.io/c@sha256:bun2"},
                    ]
                },
                "skip-bundle",
            ],
        },
        "skip-record",
        {"bundles": {"not": "a list"}},
    ]
    assert fbc.extract_published_digests(records) == {
        "sha256:abc",
        "sha256:def",
        "sha256:bun",
        "sha256:bun2",
    }


def test_digests_from_related_images_rejects_non_list() -> None:
    """Return no digests when related_images is not a list."""
    assert fbc._digests_from_related_images(None) == []
    assert fbc._digests_from_related_images({"digest": "sha256:x"}) == []


def test_digests_from_related_images_skips_empty_digest_suffix() -> None:
    """Ignore an image reference whose digest after @ is empty."""
    assert fbc._digests_from_related_images([{"image": "quay.io/foo@"}]) == []


def test_pyxis_images_url_includes_filter_and_page_size() -> None:
    """Include a date filter and page_size=500 in the Pyxis query URL."""
    with patch(
        f"{TASK}.last_update_date_filter",
        return_value="2026-07-28",
    ):
        url = fbc.pyxis_images_url(
            "https://pyxis.api.redhat.com/v1",
            "quay.io/redhat-pending/catalog:v4.14-published",
        )
    assert "page_size=500" in url
    assert "last_update_date" in url
    assert "v4.14-published" in url
    assert url.startswith("https://pyxis.api.redhat.com/v1/images?filter=")


def test_query_published_digests_success() -> None:
    """Return digests from a successful Pyxis JSON body."""
    body = json.dumps(
        {
            "data": [
                {"related_images": [{"digest": "sha256:abc", "image": "quay.io/a@sha256:abc"}]}
            ]
        }
    )
    with patch(
        f"{TASK}.http_client.get_text",
        return_value=body,
    ) as mock_get:
        result = fbc.query_published_digests(
            "http://pyxis/v1",
            "catalog:v4.14",
            ("cert", "key"),
        )
    assert result == {"sha256:abc"}
    assert mock_get.call_args.kwargs["cert"] == ("cert", "key")
    assert mock_get.call_args.kwargs["timeout"] == 60


def test_query_published_digests_http_error() -> None:
    """Return None when the Pyxis HTTP call raises."""
    with patch(
        f"{TASK}.http_client.get_text",
        side_effect=requests.HTTPError("500"),
    ):
        assert (
            fbc.query_published_digests("http://pyxis/v1", "catalog:v4.14", ("c", "k")) is None
        )


def test_query_published_digests_invalid_json() -> None:
    """Return None when Pyxis returns a truncated JSON body."""
    with patch(
        f"{TASK}.http_client.get_text",
        return_value='{"data": [{"broken"',
    ):
        assert (
            fbc.query_published_digests("http://pyxis/v1", "catalog:v4.14", ("c", "k")) is None
        )


def test_query_published_digests_missing_data() -> None:
    """Return None when the JSON object has no data field."""
    with patch(
        f"{TASK}.http_client.get_text",
        return_value='{"ok": true}',
    ):
        assert (
            fbc.query_published_digests("http://pyxis/v1", "catalog:v4.14", ("c", "k")) is None
        )


def test_query_published_digests_data_not_list() -> None:
    """Return None when data is present but not a JSON array."""
    with patch(
        f"{TASK}.http_client.get_text",
        return_value='{"data": {"unexpected": true}}',
    ):
        assert (
            fbc.query_published_digests("http://pyxis/v1", "catalog:v4.14", ("c", "k")) is None
        )


def test_query_published_digests_empty_data() -> None:
    """Return an empty set when Pyxis has no index images yet."""
    with patch(
        f"{TASK}.http_client.get_text",
        return_value='{"data": []}',
    ):
        assert (
            fbc.query_published_digests("http://pyxis/v1", "catalog:v4.14", ("c", "k"))
            == set()
        )


def test_query_all_target_indexes_stops_on_failure() -> None:
    """Return None when any unique targetIndex query fails."""
    with patch(
        f"{TASK}.query_published_digests",
        side_effect=[{"sha256:abc"}, None],
    ):
        assert (
            fbc.query_all_target_indexes(
                "http://pyxis/v1",
                [["v4.14"], ["v4.16"]],
                "catalog:{{ OCP_VERSION }}",
                ("c", "k"),
            )
            is None
        )


def test_query_all_target_indexes_success() -> None:
    """Map each unique targetIndex to its published digest set."""
    with patch(
        f"{TASK}.query_published_digests",
        side_effect=[{"sha256:a"}, {"sha256:b"}],
    ) as mock_query:
        result = fbc.query_all_target_indexes(
            "http://pyxis/v1",
            [["v4.14"], ["v4.16"], ["v4.14"]],
            "catalog:{{ OCP_VERSION }}",
            ("c", "k"),
        )
    assert result == {
        "catalog:v4.14": {"sha256:a"},
        "catalog:v4.16": {"sha256:b"},
    }
    assert mock_query.call_count == 2


def test_filter_unpublished_components_drops_published() -> None:
    """Drop a published fragment and keep an unpublished one."""
    snapshot = {
        "application": "app",
        "components": [
            _component("published", "sha256:partial"),
            _component("new", "sha256:new"),
        ],
    }
    filtered = fbc.filter_unpublished_components(
        snapshot,
        [["v4.17"], ["v4.18"]],
        "catalog:{{ OCP_VERSION }}",
        {
            "catalog:v4.17": {"sha256:partial"},
            "catalog:v4.18": set(),
        },
    )
    assert [c["name"] for c in filtered["components"]] == ["new"]
    assert filtered["components"][0]["ocpVersion"] == ["v4.18"]


def test_filter_unpublished_components_unknown_index_keeps_version() -> None:
    """Keep a version whose resolved targetIndex was not queried."""
    snapshot = {"components": [_component("comp", "sha256:abc")]}
    filtered = fbc.filter_unpublished_components(
        snapshot,
        [["v4.15"]],
        "catalog:{{ OCP_VERSION }}",
        {},
    )
    assert filtered["components"][0]["ocpVersion"] == ["v4.15"]


def test_filter_unpublished_components_keeps_unpublished_versions_only() -> None:
    """Keep a multi-version component with only unpublished versions attached."""
    snapshot = {"components": [_component("multi", "sha256:frag")]}
    filtered = fbc.filter_unpublished_components(
        snapshot,
        [["v4.17", "v4.18"]],
        "catalog:{{ OCP_VERSION }}",
        {
            "catalog:v4.17": {"sha256:frag"},
            "catalog:v4.18": set(),
        },
    )
    assert [c["name"] for c in filtered["components"]] == ["multi"]
    assert filtered["components"][0]["ocpVersion"] == ["v4.18"]


def test_filter_unpublished_components_drops_when_all_versions_published() -> None:
    """Drop a component whose fragment is published for every OCP version."""
    snapshot = {"components": [_component("done", "sha256:frag")]}
    filtered = fbc.filter_unpublished_components(
        snapshot,
        [["v4.17", "v4.18"]],
        "catalog:{{ OCP_VERSION }}",
        {
            "catalog:v4.17": {"sha256:frag"},
            "catalog:v4.18": {"sha256:frag"},
        },
    )
    assert filtered["components"] == []


def test_filter_unpublished_components_keeps_all_unpublished_versions() -> None:
    """Keep every OCP version when none of them are in the catalog."""
    snapshot = {"components": [_component("new", "sha256:frag")]}
    filtered = fbc.filter_unpublished_components(
        snapshot,
        [["v4.17", "v4.18"]],
        "catalog:{{ OCP_VERSION }}",
        {
            "catalog:v4.17": set(),
            "catalog:v4.18": set(),
        },
    )
    assert filtered["components"][0]["ocpVersion"] == ["v4.17", "v4.18"]


def test_filter_unpublished_components_mixed_multi_and_single() -> None:
    """Keep unpublished versions of one component and drop a fully published one."""
    snapshot = {
        "components": [
            _component("multi", "sha256:frag"),
            _component("published", "sha256:gone"),
        ],
    }
    filtered = fbc.filter_unpublished_components(
        snapshot,
        [["v4.14", "v4.16"], ["v4.14"]],
        "catalog:{{ OCP_VERSION }}",
        {
            "catalog:v4.14": {"sha256:frag", "sha256:gone"},
            "catalog:v4.16": set(),
        },
    )
    assert [c["name"] for c in filtered["components"]] == ["multi"]
    assert filtered["components"][0]["ocpVersion"] == ["v4.16"]


def test_attach_ocp_versions_preserves_multi_version_lists() -> None:
    """Copy each component's version list onto ocpVersion without wrapping again."""
    snapshot = {
        "application": "app",
        "components": [_component("multi", "sha256:frag"), _component("single", "sha256:x")],
    }
    attached = fbc.attach_ocp_versions(snapshot, [["v4.17", "v4.18"], ["v4.19"]])
    assert attached["components"][0]["ocpVersion"] == ["v4.17", "v4.18"]
    assert attached["components"][1]["ocpVersion"] == ["v4.19"]


# -- run() / main() ----------------------------------------------------------


def test_run_empty_snapshot(tmp_path: Path) -> None:
    """Copy an empty snapshot through without inspecting images."""
    snapshot = {"application": "app", "components": []}
    result = _run(tmp_path, snapshot, {"fbc": {"targetIndex": "catalog:v4.15"}})
    assert result["components"] == []
    assert "ocpVersion" not in result


def test_run_empty_components_null(tmp_path: Path) -> None:
    """Treat a missing components list as empty."""
    snapshot = {"application": "app"}
    result = _run(tmp_path, snapshot, {})
    assert result.get("components") in (None, [])


def test_run_staged_index_keeps_all(tmp_path: Path) -> None:
    """Attach ocpVersion and skip Pyxis when stagedIndex is true."""
    snapshot = {"components": [_component("comp", "sha256:abc")]}
    data = {"fbc": {"stagedIndex": True, "targetIndex": "catalog:v4.15"}}
    with patch(
        f"{TASK}.resolve_ocp_version",
        return_value="4.15",
    ):
        result = _run(tmp_path, snapshot, data)
    assert len(result["components"]) == 1
    assert result["components"][0]["ocpVersion"] == ["v4.15"]


def test_run_staged_index_string_true(tmp_path: Path) -> None:
    """Honor stagedIndex when it is the string true."""
    snapshot = {"components": [_component("comp", "sha256:abc")]}
    data = {"fbc": {"stagedIndex": "true"}}
    with (
        patch(
            f"{TASK}.resolve_ocp_version",
            return_value="v4.15",
        ),
        patch(f"{TASK}.http_client.get_text") as mock_get,
    ):
        result = _run(tmp_path, snapshot, data)
    mock_get.assert_not_called()
    assert result["components"][0]["ocpVersion"] == ["v4.15"]


def test_run_missing_target_index_keeps_all(tmp_path: Path) -> None:
    """Attach ocpVersion and skip Pyxis when targetIndex is absent."""
    snapshot = {"components": [_component("comp", "sha256:abc")]}
    with patch(
        f"{TASK}.resolve_ocp_version",
        return_value="4.15",
    ):
        result = _run(tmp_path, snapshot, {"pyxis": {"server": "production"}})
    assert result["components"][0]["ocpVersion"] == ["v4.15"]


def test_run_filters_published_fragments(tmp_path: Path) -> None:
    """Drop components whose digests are listed in the Pyxis index."""
    snapshot = {
        "application": "test-app",
        "components": [
            _component("comp1-published", "sha256:abc123"),
            _component("comp2-new", "sha256:def456"),
            _component("comp3-published", "sha256:ghi789"),
        ],
    }
    data = {
        "pyxis": {"server": "production"},
        "fbc": {"targetIndex": "quay.io/redhat-pending/catalog:v4.14-published"},
    }
    body = json.dumps(
        {
            "data": [
                {
                    "related_images": [
                        {"digest": "sha256:abc123", "image": "quay.io/a@sha256:abc123"},
                        {"digest": "sha256:ghi789", "image": "quay.io/c@sha256:ghi789"},
                    ]
                }
            ]
        }
    )
    with (
        patch(
            f"{TASK}.resolve_ocp_version",
            return_value="4.15",
        ),
        patch(
            f"{TASK}.http_client.get_text",
            return_value=body,
        ),
    ):
        result = _run(tmp_path, snapshot, data)
    names = [c["name"] for c in result["components"]]
    assert names == ["comp2-new"]
    assert result["components"][0]["ocpVersion"] == ["v4.15"]


def test_run_all_published_clears_components(tmp_path: Path) -> None:
    """Write an empty components list when every fragment is already published."""
    snapshot = {
        "components": [
            _component("comp1", "sha256:mno345"),
            _component("comp2", "sha256:pqr678"),
        ]
    }
    data = {"fbc": {"targetIndex": "catalog:v4.16-all-published"}}
    body = json.dumps(
        {
            "data": [
                {
                    "related_images": [
                        {"digest": "sha256:mno345"},
                        {"digest": "sha256:pqr678"},
                    ]
                }
            ]
        }
    )
    with (
        patch(
            f"{TASK}.resolve_ocp_version",
            return_value="v4.16",
        ),
        patch(
            f"{TASK}.http_client.get_text",
            return_value=body,
        ),
    ):
        result = _run(tmp_path, snapshot, data)
    assert result["components"] == []


def test_run_first_release_keeps_all(tmp_path: Path) -> None:
    """Keep every component when Pyxis returns no index images."""
    snapshot = {"components": [_component("comp", "sha256:new")]}
    data = {"fbc": {"targetIndex": "catalog:v4.17-empty"}}
    with (
        patch(
            f"{TASK}.resolve_ocp_version",
            return_value="4.15",
        ),
        patch(
            f"{TASK}.http_client.get_text",
            return_value='{"data": []}',
        ),
    ):
        result = _run(tmp_path, snapshot, data)
    assert len(result["components"]) == 1
    assert result["components"][0]["ocpVersion"] == ["v4.15"]


def test_run_pyxis_failure_keeps_all(tmp_path: Path) -> None:
    """Keep every component when the Pyxis query fails."""
    snapshot = {"components": [_component("comp", "sha256:abc")]}
    data = {"fbc": {"targetIndex": "catalog:v4.14"}}
    with (
        patch(
            f"{TASK}.resolve_ocp_version",
            return_value="4.15",
        ),
        patch(
            f"{TASK}.http_client.get_text",
            side_effect=requests.ConnectionError("down"),
        ),
    ):
        result = _run(tmp_path, snapshot, data)
    assert len(result["components"]) == 1
    assert result["components"][0]["ocpVersion"] == ["v4.15"]


def test_run_groups_components_by_resolved_index(tmp_path: Path) -> None:
    """Issue one Pyxis query per unique resolved targetIndex."""
    snapshot = {
        "components": [
            _component("comp-a", "sha256:aaa"),
            _component("comp-b", "sha256:bbb"),
        ]
    }
    data = {"fbc": {"targetIndex": "catalog:{{ OCP_VERSION }}"}}
    body = json.dumps({"data": [{"related_images": [{"digest": "sha256:aaa"}]}]})

    def version_for_image(image_ref: str) -> str:
        return "4.14" if "comp-a" in image_ref else "4.16"

    with (
        patch(
            f"{TASK}.resolve_ocp_version",
            side_effect=version_for_image,
        ),
        patch(
            f"{TASK}.http_client.get_text",
            return_value=body,
        ) as mock_get,
    ):
        result = _run(tmp_path, snapshot, data)
    assert mock_get.call_count == 2
    names = [c["name"] for c in result["components"]]
    assert "comp-b" in names
    assert "comp-a" in names or names == ["comp-b"]


def test_run_bundles_field(tmp_path: Path) -> None:
    """Treat bundle related_images as published fragments."""
    snapshot = {
        "components": [
            _component("bundle1", "sha256:bun111"),
            _component("kept", "sha256:new"),
        ]
    }
    data = {"fbc": {"targetIndex": "catalog:v4.18-bundles"}}
    body = json.dumps(
        {
            "data": [
                {
                    "bundles": [
                        {"related_images": [{"digest": "sha256:bun111"}]},
                    ]
                }
            ]
        }
    )
    with (
        patch(
            f"{TASK}.resolve_ocp_version",
            return_value="4.15",
        ),
        patch(
            f"{TASK}.http_client.get_text",
            return_value=body,
        ),
    ):
        result = _run(tmp_path, snapshot, data)
    assert [c["name"] for c in result["components"]] == ["kept"]


def test_main_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit zero after a successful empty-snapshot run."""
    _write_json(tmp_path / "snapshot.json", {"components": []})
    _write_json(tmp_path / "data.json", {})
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("RESULT_FILTERED_SNAPSHOT_PATH", str(tmp_path / "result.txt"))
    monkeypatch.setenv("PYXIS_SECRET_MOUNT", str(tmp_path / "secrets"))
    (tmp_path / "secrets").mkdir()
    assert fbc.main() == 0
    assert (tmp_path / "result.txt").read_text(encoding="utf-8") == (
        fbc.FILTERED_SNAPSHOT_FILENAME
    )


def test_main_uses_default_secret_mount(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall back to /etc/secrets when PYXIS_SECRET_MOUNT is unset."""
    _write_json(tmp_path / "snapshot.json", {"components": []})
    _write_json(tmp_path / "data.json", {})
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("RESULT_FILTERED_SNAPSHOT_PATH", str(tmp_path / "result.txt"))
    monkeypatch.delenv("PYXIS_SECRET_MOUNT", raising=False)
    assert fbc.main() == 0
