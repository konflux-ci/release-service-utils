"""Tests for `advisory_data`."""

from __future__ import annotations

import base64
import gzip
import json
import os
import time
from pathlib import Path

import advisory_data
import pytest


def _gzip_b64(obj: dict) -> str:
    raw = json.dumps(obj).encode("utf-8")
    gz = gzip.compress(raw)
    return base64.standard_b64encode(gz).decode("ascii")


def test_advisory_secret_name_stage() -> None:
    """Return the stage secret name for the 'stage' environment."""
    assert advisory_data.advisory_secret_name("stage") == advisory_data.ADVISORY_SECRET_STAGE


def test_advisory_secret_name_production() -> None:
    """Return the prod secret name for the 'production' environment."""
    assert (
        advisory_data.advisory_secret_name("production") == advisory_data.ADVISORY_SECRET_PROD
    )


def test_spec_content_json_pointer_image() -> None:
    """Map `image` content type to `.content.images`."""
    assert advisory_data.spec_content_json_pointer("image") == ".content.images"


def test_spec_content_json_pointer_artifacts() -> None:
    """Map artifact content types to `.content.artifacts`."""
    for t in ("binary", "generic", "rpm", "disk-image"):
        assert advisory_data.spec_content_json_pointer(t) == ".content.artifacts"


def test_spec_content_json_pointer_rejects_unknown() -> None:
    """Reject unsupported content types."""
    with pytest.raises(ValueError, match="Unsupported"):
        advisory_data.spec_content_json_pointer("unknown-type")


def test_advisory_url_prefix_stage() -> None:
    """Use the stage portal URL for the rhtap-release repo."""
    assert (
        advisory_data.advisory_url_prefix("https://gitlab.com/foo/rhtap-release/bar.git")
        == "https://access.stage.redhat.com/errata"
    )


def test_advisory_url_prefix_prod() -> None:
    """Use the production portal URL for other repos."""
    assert (
        advisory_data.advisory_url_prefix("https://gitlab.com/org/repo.git")
        == "https://access.redhat.com/errata"
    )


def test_decode_advisory_param_roundtrip() -> None:
    """Round-trip advisory JSON through base64 gzip encoding."""
    data = {"type": "RHSA", "live_id": None, "content": {"images": []}}
    out = advisory_data.decode_advisory_param(_gzip_b64(data))
    assert out == data


def test_content_array_from_decoded() -> None:
    """Read content arrays from decoded advisory JSON."""
    d = {"content": {"images": [{"x": 1}]}}
    assert advisory_data.content_array_from_decoded(d, ".content.images") == [{"x": 1}]
    assert advisory_data.content_array_from_decoded({}, ".content.images") == []


def test_set_decoded_content_array() -> None:
    """Write a content array back into decoded advisory JSON."""
    d: dict = {"type": "RHSA"}
    advisory_data.set_decoded_content_array(d, ".content.images", [{"a": 1}])
    assert d["content"]["images"] == [{"a": 1}]


def test_load_advisory_yaml_roundtrip(tmp_path: Path) -> None:
    """Load advisory YAML and read metadata, spec, and content paths."""
    p = tmp_path / "a.yaml"
    p.write_text(
        "metadata:\n  name: '2024:1'\nspec:\n  type: RHSA\n  content:\n" "    images: []\n",
        encoding="utf-8",
    )
    doc = advisory_data.load_advisory_yaml(p)
    assert advisory_data.get_advisory_metadata_name(doc) == "2024:1"
    assert advisory_data.get_advisory_spec_type(doc) == "RHSA"
    assert advisory_data.spec_content_array_from_advisory_yaml(doc, ".content.images") == []


def test_template_context_merge_order() -> None:
    """Let `tmpl_data` override duplicate top-level template keys."""
    base = {"advisory": {"spec": {"k": 1}}}
    out = advisory_data.template_context_merge(base, "n", "d")
    assert out["advisory_name"] == "n"
    assert out["advisory_ship_date"] == "d"
    assert out["advisory"]["spec"]["k"] == 1


def test_json_dict_to_yaml_text_roundtrip() -> None:
    """Serialize advisory dicts to readable multi-line YAML."""
    document = {"spec": {"type": "RHSA", "content": {"images": [{"tags": ["a"]}]}}}
    yml = advisory_data.json_dict_to_yaml_text(document)
    assert "RHSA" in yml
    assert "tags:" in yml


def test_list_existing_advisory_subdirs_order(tmp_path: Path) -> None:
    """List advisory subdirs with newest leaf mtime first."""
    base = tmp_path / "t"
    d_old = base / "2024" / "0001"
    d_new = base / "2025" / "0002"
    d_old.mkdir(parents=True)
    d_new.mkdir(parents=True)
    t_old = time.time() - 100
    t_new = time.time()
    os.utime(d_old, (t_old, t_old))
    os.utime(d_new, (t_new, t_new))
    rel = advisory_data.list_existing_advisory_subdirs(base)
    assert rel == ["2025/0002", "2024/0001"]


def test_list_existing_advisory_subdirs_missing(tmp_path: Path) -> None:
    """Return an empty list when the advisory base path is absent."""
    assert advisory_data.list_existing_advisory_subdirs(tmp_path / "nope") == []


def test_filter_content_by_existing_image(tmp_path: Path) -> None:
    """Drop image rows that already exist in the existing content file."""
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    content.write_text(
        json.dumps(
            [
                {
                    "containerImage": "q.io/i@sha256:a",
                    "tags": ["t1"],
                    "repository": "r1",
                }
            ]
        ),
        encoding="utf-8",
    )
    existing.write_text(
        json.dumps(
            [
                {
                    "containerImage": "q.io/i@sha256:a",
                    "tags": ["t1"],
                    "repository": "r1",
                }
            ]
        ),
        encoding="utf-8",
    )
    out = advisory_data.filter_content_by_existing(
        "image", content, existing, stderr_path=None
    )
    assert json.loads(out) == []


def test_filter_image_keeps_when_no_match(tmp_path: Path) -> None:
    """Keep image rows when no existing row matches."""
    rows = [{"containerImage": "a", "tags": ["1"], "repository": "r"}]
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    content.write_text(json.dumps(rows), encoding="utf-8")
    existing.write_text(
        json.dumps([{"containerImage": "b", "tags": ["1"], "repository": "r"}]),
        encoding="utf-8",
    )
    out = advisory_data.filter_content_by_existing(
        "image", content, existing, stderr_path=None
    )
    assert json.loads(out) == rows


def test_filter_rpm_exact_purl(tmp_path: Path) -> None:
    """Drop rpm rows whose purl exactly matches an existing row."""
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    p = "pkg:golang/example@1.0"
    content.write_text(json.dumps([{"purl": p}]), encoding="utf-8")
    existing.write_text(json.dumps([{"purl": p}]), encoding="utf-8")
    out = advisory_data.filter_content_by_existing("rpm", content, existing, stderr_path=None)
    assert json.loads(out) == []


def test_filter_disk_image_exact_purl(tmp_path: Path) -> None:
    """Drop disk-image rows whose purl exactly matches an existing row."""
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    p = "pkg:golang/example@1.0"
    content.write_text(json.dumps([{"purl": p}]), encoding="utf-8")
    existing.write_text(json.dumps([{"purl": p}]), encoding="utf-8")
    out = advisory_data.filter_content_by_existing(
        "disk-image", content, existing, stderr_path=None
    )
    assert json.loads(out) == []


def test_filter_generic_strips_checksum_for_match(tmp_path: Path) -> None:
    """Match generic rows after stripping checksum query parameters."""
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    content.write_text(
        json.dumps([{"purl": "pkg:x?checksum=new123"}]),
        encoding="utf-8",
    )
    existing.write_text(
        json.dumps([{"purl": "pkg:x?checksum=old456"}]),
        encoding="utf-8",
    )
    out = advisory_data.filter_content_by_existing(
        "generic", content, existing, stderr_path=None
    )
    assert json.loads(out) == []


def test_filter_invalid_json_appends_stderr(tmp_path: Path) -> None:
    """Append parse errors to the stderr log and re-raise."""
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    content.write_text("not-json", encoding="utf-8")
    existing.write_text("[]", encoding="utf-8")
    err = tmp_path / "err.log"
    with pytest.raises(json.JSONDecodeError):
        advisory_data.filter_content_by_existing("image", content, existing, stderr_path=err)
    assert "invalid JSON" in err.read_text(encoding="utf-8")


def test_filter_requires_arrays(tmp_path: Path) -> None:
    """Require both content files to contain JSON arrays."""
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    content.write_text('"x"', encoding="utf-8")
    existing.write_text("[]", encoding="utf-8")
    with pytest.raises(TypeError, match="arrays"):
        advisory_data.filter_content_by_existing("image", content, existing, stderr_path=None)


def test_append_signing_key_to_content() -> None:
    """Set signingKey on every content row."""
    root = {"content": {"images": [{"a": 1}, {"b": 2}]}}
    advisory_data.append_signing_key_to_content(root, ".content.images", "k1")
    for row in root["content"]["images"]:
        assert row["signingKey"] == "k1"


def test_append_signing_key_skips_existing() -> None:
    """Do not overwrite non-empty signingKey values."""
    root = {
        "content": {
            "images": [
                {"signingKey": "existing"},
                {"signingKey": ""},
                {"a": 1},
            ]
        }
    }
    advisory_data.append_signing_key_to_content(root, ".content.images", "k1")
    assert root["content"]["images"][0]["signingKey"] == "existing"
    assert root["content"]["images"][1]["signingKey"] == "k1"
    assert root["content"]["images"][2]["signingKey"] == "k1"


def test_filter_image_skips_non_dict_rows() -> None:
    """Ignore non-dict rows when filtering images."""
    out = advisory_data._filter_image(
        ["x", {"containerImage": "a", "tags": [], "repository": "r"}],
        [],
    )
    assert len(out) == 1


def test_filter_image_skips_non_dict_existing() -> None:
    """Ignore non-dict rows in the existing image list."""
    rows = [{"containerImage": "a", "tags": ["t"], "repository": "r"}]
    out = advisory_data._filter_image(rows, ["bad", {"containerImage": "b"}])
    assert out == rows


def test_filter_rpm_skips_non_dict_and_keeps_unknown_purl() -> None:
    """Keep rows with missing purl and skip invalid existing entries."""
    out = advisory_data._filter_rpm(
        ["x", {"purl": None}, {"purl": "pkg:a"}],
        [{"purl": "pkg:b"}],
    )
    assert out == [{"purl": None}, {"purl": "pkg:a"}]


def test_filter_generic_skips_invalid_rows() -> None:
    """Ignore invalid rows when filtering generic or binary content."""
    out = advisory_data._filter_generic_binary(
        ["x", {"purl": None}, {"purl": "pkg:new"}],
        [{"purl": "pkg:base?checksum=1"}],
    )
    assert len(out) == 1


def test_content_array_from_decoded_none_segment() -> None:
    """Return an empty list when a path segment is null."""
    d = {"content": {"images": None}}
    assert advisory_data.content_array_from_decoded(d, ".content.images") == []


def test_set_decoded_content_array_bad_path() -> None:
    """Reject unsupported content list paths."""
    with pytest.raises(ValueError, match="unsupported"):
        advisory_data.set_decoded_content_array({}, ".bad.path", [])


def test_load_advisory_yaml_empty_document(tmp_path: Path) -> None:
    """Treat an empty YAML file as an empty mapping."""
    p = tmp_path / "empty.yaml"
    p.write_text("", encoding="utf-8")
    assert advisory_data.load_advisory_yaml(p) == {}


def test_load_advisory_yaml_rejects_non_mapping(tmp_path: Path) -> None:
    """Reject YAML documents whose root is not a mapping."""
    p = tmp_path / "list.yaml"
    p.write_text("- a\n", encoding="utf-8")
    with pytest.raises(TypeError, match="mapping"):
        advisory_data.load_advisory_yaml(p)


def test_spec_content_array_from_advisory_yaml_paths() -> None:
    """Walk spec content paths and return lists or empty results."""
    doc = {"spec": {"content": {"images": [{"i": 1}]}}}
    assert advisory_data.spec_content_array_from_advisory_yaml(doc, ".content.images") == [
        {"i": 1}
    ]
    assert advisory_data.spec_content_array_from_advisory_yaml({}, ".content.images") == []
    bad = {"spec": "not-a-dict"}
    assert advisory_data.spec_content_array_from_advisory_yaml(bad, ".content.images") == []
    assert (
        advisory_data.spec_content_array_from_advisory_yaml(
            {"spec": {"content": []}}, ".content.images"
        )
        == []
    )
    assert (
        advisory_data.spec_content_array_from_advisory_yaml(
            {"spec": {"content": {}}}, ".content.images"
        )
        == []
    )


def test_get_advisory_spec_type_and_name_defaults() -> None:
    """Return empty strings for missing spec type and metadata name."""
    assert advisory_data.get_advisory_spec_type({}) == ""
    assert advisory_data.get_advisory_metadata_name({}) == ""


def test_template_data_for_apply() -> None:
    """Wrap advisory spec data for apply_template."""
    assert advisory_data.template_data_for_apply({"type": "RHSA"}) == {
        "advisory": {"spec": {"type": "RHSA"}}
    }


def test_list_existing_advisory_subdirs_skips_files(tmp_path: Path) -> None:
    """Ignore plain files when listing advisory subdirectories."""
    base = tmp_path / "t"
    (base / "2024").mkdir(parents=True)
    (base / "2024" / "notadir.txt").write_text("x", encoding="utf-8")
    (base / "file.txt").write_text("y", encoding="utf-8")
    assert advisory_data.list_existing_advisory_subdirs(base) == []


def test_encode_advisory_param_round_trips_decode() -> None:
    """Gzip/base64 encoding round-trips through decode_advisory_param."""
    payload = {"type": "RHBA", "content": {"artifacts": []}}
    encoded = advisory_data.encode_advisory_param(payload)
    assert advisory_data.decode_advisory_param(encoded) == payload


def test_first_mapping_content_type_reads_component_rows() -> None:
    """Return the first mapping.components content type."""
    data = {
        "mapping": {
            "components": [
                {"contentType": "generic"},
                {"contentType": "image"},
            ],
        },
    }
    assert advisory_data.first_mapping_content_type(data) == "generic"


def test_first_mapping_content_type_reads_content_gateway_rows() -> None:
    """Prefer contentGateway.contentType over top-level contentType."""
    data = {
        "mapping": {
            "components": [
                {
                    "contentGateway": {"contentType": "binary"},
                    "contentType": "image",
                },
            ],
        },
    }
    assert advisory_data.first_mapping_content_type(data) == "binary"
