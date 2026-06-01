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


def test_spec_content_json_pointer_image() -> None:
    assert advisory_data.spec_content_json_pointer("image") == ".content.images"


def test_spec_content_json_pointer_artifacts() -> None:
    for t in ("binary", "generic", "rpm"):
        assert advisory_data.spec_content_json_pointer(t) == ".content.artifacts"


def test_spec_content_json_pointer_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        advisory_data.spec_content_json_pointer("disk-image")


def test_advisory_url_prefix_stage() -> None:
    assert (
        advisory_data.advisory_url_prefix("https://gitlab.com/foo/rhtap-release/bar.git")
        == "https://access.stage.redhat.com/errata"
    )


def test_advisory_url_prefix_prod() -> None:
    assert (
        advisory_data.advisory_url_prefix("https://gitlab.com/org/repo.git")
        == "https://access.redhat.com/errata"
    )


def test_decode_advisory_param_roundtrip() -> None:
    data = {"type": "RHSA", "live_id": None, "content": {"images": []}}
    out = advisory_data.decode_advisory_param(_gzip_b64(data))
    assert out == data


def test_content_array_from_decoded() -> None:
    d = {"content": {"images": [{"x": 1}]}}
    assert advisory_data.content_array_from_decoded(d, ".content.images") == [{"x": 1}]
    assert advisory_data.content_array_from_decoded({}, ".content.images") == []


def test_set_decoded_content_array() -> None:
    d: dict = {"type": "RHSA"}
    advisory_data.set_decoded_content_array(d, ".content.images", [{"a": 1}])
    assert d["content"]["images"] == [{"a": 1}]


def test_load_advisory_yaml_roundtrip(tmp_path: Path) -> None:
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
    # `tmpl_data` is merged after fixed keys, so it wins on duplicate top-level keys.
    base = {"advisory": {"spec": {"k": 1}}}
    out = advisory_data.template_context_merge(base, "n", "d")
    assert out["advisory_name"] == "n"
    assert out["advisory_ship_date"] == "d"
    assert out["advisory"]["spec"]["k"] == 1


def test_json_dict_to_yaml_text_roundtrip() -> None:
    document = {"spec": {"type": "RHSA", "content": {"images": [{"tags": ["a"]}]}}}
    yml = advisory_data.json_dict_to_yaml_text(document)
    assert "RHSA" in yml
    assert "tags:" in yml


def test_list_existing_advisory_subdirs_order(tmp_path: Path) -> None:
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
    assert advisory_data.list_existing_advisory_subdirs(tmp_path / "nope") == []


def test_filter_content_by_existing_image(tmp_path: Path) -> None:
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
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    p = "pkg:golang/example@1.0"
    content.write_text(json.dumps([{"purl": p}]), encoding="utf-8")
    existing.write_text(json.dumps([{"purl": p}]), encoding="utf-8")
    out = advisory_data.filter_content_by_existing("rpm", content, existing, stderr_path=None)
    assert json.loads(out) == []


def test_filter_generic_strips_checksum_for_match(tmp_path: Path) -> None:
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
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    content.write_text("not-json", encoding="utf-8")
    existing.write_text("[]", encoding="utf-8")
    err = tmp_path / "err.log"
    with pytest.raises(json.JSONDecodeError):
        advisory_data.filter_content_by_existing("image", content, existing, stderr_path=err)
    assert "invalid JSON" in err.read_text(encoding="utf-8")


def test_filter_requires_arrays(tmp_path: Path) -> None:
    content = tmp_path / "c.json"
    existing = tmp_path / "e.json"
    content.write_text('"x"', encoding="utf-8")
    existing.write_text("[]", encoding="utf-8")
    with pytest.raises(TypeError, match="arrays"):
        advisory_data.filter_content_by_existing("image", content, existing, stderr_path=None)


def test_append_signing_key_to_content() -> None:
    root = {"content": {"images": [{"a": 1}, {"b": 2}]}}
    advisory_data.append_signing_key_to_content(root, ".content.images", "k1")
    for row in root["content"]["images"]:
        assert row["signingKey"] == "k1"


def test_append_signing_key_skips_existing() -> None:
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
    out = advisory_data._filter_image(
        ["x", {"containerImage": "a", "tags": [], "repository": "r"}],
        [],
    )
    assert len(out) == 1


def test_filter_image_skips_non_dict_existing() -> None:
    rows = [{"containerImage": "a", "tags": ["t"], "repository": "r"}]
    out = advisory_data._filter_image(rows, ["bad", {"containerImage": "b"}])
    assert out == rows


def test_filter_rpm_skips_non_dict_and_keeps_unknown_purl() -> None:
    out = advisory_data._filter_rpm(
        ["x", {"purl": None}, {"purl": "pkg:a"}],
        [{"purl": "pkg:b"}],
    )
    assert out == [{"purl": None}, {"purl": "pkg:a"}]


def test_filter_generic_skips_invalid_rows() -> None:
    out = advisory_data._filter_generic_binary(
        ["x", {"purl": None}, {"purl": "pkg:new"}],
        [{"purl": "pkg:base?checksum=1"}],
    )
    assert len(out) == 1


def test_content_array_from_decoded_none_segment() -> None:
    d = {"content": {"images": None}}
    assert advisory_data.content_array_from_decoded(d, ".content.images") == []


def test_set_decoded_content_array_bad_path() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        advisory_data.set_decoded_content_array({}, ".bad.path", [])


def test_load_advisory_yaml_empty_document(tmp_path: Path) -> None:
    p = tmp_path / "empty.yaml"
    p.write_text("", encoding="utf-8")
    assert advisory_data.load_advisory_yaml(p) == {}


def test_load_advisory_yaml_rejects_non_mapping(tmp_path: Path) -> None:
    p = tmp_path / "list.yaml"
    p.write_text("- a\n", encoding="utf-8")
    with pytest.raises(TypeError, match="mapping"):
        advisory_data.load_advisory_yaml(p)


def test_spec_content_array_from_advisory_yaml_paths() -> None:
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
    assert advisory_data.get_advisory_spec_type({}) == ""
    assert advisory_data.get_advisory_metadata_name({}) == ""


def test_template_data_for_apply() -> None:
    assert advisory_data.template_data_for_apply({"type": "RHSA"}) == {
        "advisory": {"spec": {"type": "RHSA"}}
    }


def test_list_existing_advisory_subdirs_skips_files(tmp_path: Path) -> None:
    base = tmp_path / "t"
    (base / "2024").mkdir(parents=True)
    (base / "2024" / "notadir.txt").write_text("x", encoding="utf-8")
    (base / "file.txt").write_text("y", encoding="utf-8")
    assert advisory_data.list_existing_advisory_subdirs(base) == []
