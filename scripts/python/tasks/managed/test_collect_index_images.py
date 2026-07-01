"""Test collect_index_images task logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import collect_index_images

_TRANSLATED_FBC = [
    {
        "repo": "redhat.io",
        "url": "registry.redhat.io/redhat/fbc-target-index:v4.12",
    },
    {
        "repo": "access.redhat.com",
        "url": "registry.access.redhat.com/redhat/fbc-target-index:v4.12",
    },
]

_TRANSLATED_PREVIEW = [
    {
        "repo": "redhat.io",
        "url": "registry.redhat.io/redhat/preview-operator-index:v4.13",
    },
    {
        "repo": "access.redhat.com",
        "url": "registry.access.redhat.com/redhat/preview-operator-index:v4.13",
    },
]


def _write_results(path: Path, components: list[dict[str, Any]]) -> None:
    """Write an internal-request results JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"components": components}) + "\n", encoding="utf-8")


def test_split_target_index_parses_repository_and_tag() -> None:
    """Split a target index reference at the last colon."""
    assert collect_index_images.split_target_index(
        "quay.io/redhat/redhat----fbc-target-index:v4.12",
    ) == ("quay.io/redhat/redhat----fbc-target-index", "v4.12")


def test_split_target_index_rejects_missing_tag() -> None:
    """Reject target index values without a tag."""
    with pytest.raises(ValueError, match="repository and tag"):
        collect_index_images.split_target_index("quay.io/redhat/repo")


def test_build_tags_appends_timestamp_for_bare_ocp_version() -> None:
    """Append buildTimestamp for bare OCP version tags."""
    assert collect_index_images.build_tags("v4.12", "2468") == [
        "v4.12",
        "v4.12-2468",
    ]


def test_build_tags_keeps_hotfix_tag_only() -> None:
    """Do not append buildTimestamp for hotfix tags."""
    assert collect_index_images.build_tags("v4.12-12345-6789", "9999") == [
        "v4.12-12345-6789",
    ]


def test_build_tags_keeps_pre_ga_tag_only() -> None:
    """Do not append buildTimestamp for pre-GA tags."""
    tag = "v4.13-myproduct-1.0-20250220143022"
    assert collect_index_images.build_tags(tag, "9999") == [tag]


def test_translation_repo_url_strips_tag_suffix() -> None:
    """Return the repository URL without the tag portion."""
    url = collect_index_images.translation_repo_url(_TRANSLATED_FBC, "redhat.io")
    assert url == "registry.redhat.io/redhat/fbc-target-index"


def test_translation_repo_url_returns_empty_for_unknown_repo() -> None:
    """Return an empty string when the repo key is absent."""
    assert collect_index_images.translation_repo_url(_TRANSLATED_FBC, "missing") == ""


def test_translation_repo_url_returns_empty_for_blank_url() -> None:
    """Return an empty string when the matched entry has no URL."""
    translated = [{"repo": "redhat.io", "url": ""}]
    assert collect_index_images.translation_repo_url(translated, "redhat.io") == ""


def test_build_repo_object_includes_optional_delivery_repos() -> None:
    """Include translated delivery-repo fields when present."""
    repo_object = collect_index_images.build_repo_object(
        "quay.io/redhat/redhat----fbc-target-index",
        ["v4.12", "v4.12-2468"],
        "registry.redhat.io/redhat/fbc-target-index",
        "registry.access.redhat.com/redhat/fbc-target-index",
    )
    assert repo_object["rh-registry-repo"] == "registry.redhat.io/redhat/fbc-target-index"
    assert repo_object["registry-access-repo"] == (
        "registry.access.redhat.com/redhat/fbc-target-index"
    )


def test_build_repo_object_omits_empty_delivery_repos() -> None:
    """Omit optional delivery-repo fields when translation is blank."""
    repo_object = collect_index_images.build_repo_object(
        "quay.io/example/repo",
        ["v1"],
        "",
        "",
    )
    assert "rh-registry-repo" not in repo_object
    assert "registry-access-repo" not in repo_object


def test_collect_index_image_components_single_version() -> None:
    """Build one component with timestamp tag for a bare OCP version."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results, "2468")

    component = snapshot["components"][0]
    assert component["containerImage"] == "redhat.com/rh-stage/iib@sha256:abcdefghijk"
    assert component["repository"] == "quay.io/redhat/redhat----fbc-target-index"
    assert component["tags"] == ["v4.12", "v4.12-2468"]
    assert component["repositories"][0]["tags"] == ["v4.12", "v4.12-2468"]
    assert "registry.redhat.io" in component["repositories"][0]["rh-registry-repo"]
    assert "registry.access.redhat.com" in (
        component["repositories"][0]["registry-access-repo"]
    )


def test_collect_index_image_components_multiple_versions() -> None:
    """Build separate components for each bare OCP version."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.13",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:lmnopqrstuv",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results, "1357")

    assert len(snapshot["components"]) == 2
    assert snapshot["components"][0]["tags"] == ["v4.12", "v4.12-1357"]
    assert snapshot["components"][1]["tags"] == ["v4.13", "v4.13-1357"]


def test_collect_index_image_components_hotfix_and_pre_ga() -> None:
    """Keep hotfix and pre-GA tags without appending buildTimestamp."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12-12345-6789",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
            {
                "target_index": (
                    "quay.io/redhat/redhat----preview-operator-index:"
                    "v4.13-myproduct-1.0-20250220143022"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:lmnopqrstuv",
            },
        ],
    }

    def _translate(target_index: str) -> list[dict[str, Any]]:
        if "preview-operator-index" in target_index:
            return _TRANSLATED_PREVIEW
        return _TRANSLATED_FBC

    with mock.patch(
        "collect_index_images.image_ref.translate_delivery_repo",
        side_effect=_translate,
    ):
        snapshot = collect_index_images.collect_index_image_components(results, "9999")

    assert snapshot["components"][0]["tags"] == ["v4.12-12345-6789"]
    assert snapshot["components"][1]["tags"] == ["v4.13-myproduct-1.0-20250220143022"]


def test_collect_index_image_components_includes_image_digests() -> None:
    """Copy image_digests from the internal-request results when present."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
                "image_digests": ["sha256:one", "sha256:two"],
            },
        ],
    }
    with mock.patch(
        "collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results, "2468")

    assert snapshot["components"][0]["imageDigests"] == ["sha256:one", "sha256:two"]


def test_collect_index_image_components_defaults_image_digests_to_empty() -> None:
    """Use an empty imageDigests list when image_digests is absent."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results, "2468")

    assert snapshot["components"][0]["imageDigests"] == []


def test_collect_index_image_components_rejects_invalid_row() -> None:
    """Fail when a components entry is not a JSON object."""
    with pytest.raises(ValueError, match="components\\[0\\] must be a JSON object"):
        collect_index_images.collect_index_image_components({"components": ["bad"]}, "1")


def test_collect_index_image_components_rejects_missing_target_index() -> None:
    """Fail when target_index is missing from a component row."""
    row = {"index_image_resolved": "img@sha256:abc"}
    with pytest.raises(ValueError, match="target_index must be a non-empty string"):
        collect_index_images.collect_index_image_components({"components": [row]}, "1")


def test_collect_index_image_components_rejects_missing_source_index() -> None:
    """Fail when index_image_resolved is missing from a component row."""
    row = {"target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12"}
    with pytest.raises(ValueError, match="index_image_resolved must be a non-empty string"):
        collect_index_images.collect_index_image_components({"components": [row]}, "1")


def test_collect_index_image_components_rejects_invalid_image_digests() -> None:
    """Fail when image_digests is not a JSON array."""
    row = {
        "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
        "index_image_resolved": "img@sha256:abc",
        "image_digests": "bad",
    }
    with pytest.raises(ValueError, match="image_digests must be a JSON array"):
        collect_index_images.collect_index_image_components({"components": [row]}, "1")


def test_collect_index_image_components_rejects_invalid_components_type() -> None:
    """Fail when the results components field is not an array."""
    with pytest.raises(ValueError, match="components must be a JSON array"):
        collect_index_images.collect_index_image_components(
            {"components": "bad"},
            "2468",
        )


def test_run_collect_index_images_writes_snapshot_and_result(
    tmp_path: Path,
) -> None:
    """Write the snapshot JSON file and Tekton result path."""
    _write_results(
        tmp_path / "internal-requests-results.json",
        [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
            },
        ],
    )
    result_path = tmp_path / "result.txt"
    with mock.patch(
        "collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        collect_index_images.run_collect_index_images(
            data_dir=tmp_path,
            internal_request_results_file=Path("internal-requests-results.json"),
            build_timestamp="2468",
            snapshot_path=tmp_path / collect_index_images.SNAPSHOT_FILENAME,
            index_image_snapshot_result_path=result_path,
        )

    snapshot = json.loads(
        (tmp_path / collect_index_images.SNAPSHOT_FILENAME).read_text(encoding="utf-8"),
    )
    assert len(snapshot["components"]) == 1
    assert result_path.read_text(encoding="utf-8") == collect_index_images.SNAPSHOT_FILENAME


def test_run_collect_index_images_missing_results_file(tmp_path: Path) -> None:
    """Fail when the internal request results file is missing."""
    with pytest.raises(FileNotFoundError, match="internal request results file not found"):
        collect_index_images.run_collect_index_images(
            data_dir=tmp_path,
            internal_request_results_file=Path("missing.json"),
            build_timestamp="2468",
            snapshot_path=tmp_path / collect_index_images.SNAPSHOT_FILENAME,
            index_image_snapshot_result_path=tmp_path / "result.txt",
        )


def test_module_main_guard_propagates_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Executing the module as `__main__` propagates failures from main()."""
    import runpy

    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_RESULTS_FILE", "missing.json")
    monkeypatch.setenv("PARAM_BUILD_TIMESTAMP", "2468")
    monkeypatch.setenv("RESULT_INDEX_IMAGE_SNAPSHOT_PATH", str(tmp_path / "result.txt"))
    with pytest.raises(FileNotFoundError, match="internal request results file not found"):
        runpy.run_module("collect_index_images", run_name="__main__")


def test_main_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit zero after a successful run."""
    _write_results(
        tmp_path / "internal-requests-results.json",
        [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
            },
        ],
    )
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_RESULTS_FILE", "internal-requests-results.json")
    monkeypatch.setenv("PARAM_BUILD_TIMESTAMP", "2468")
    monkeypatch.setenv("RESULT_INDEX_IMAGE_SNAPSHOT_PATH", str(tmp_path / "result.txt"))

    with mock.patch(
        "collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        assert collect_index_images.main() == 0
