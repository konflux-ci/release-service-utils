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
        "v4.12",
        "registry.redhat.io/redhat/fbc-target-index",
        "registry.access.redhat.com/redhat/fbc-target-index",
    )
    assert repo_object["tags"] == ["v4.12"]
    assert repo_object["rh-registry-repo"] == "registry.redhat.io/redhat/fbc-target-index"
    assert repo_object["registry-access-repo"] == (
        "registry.access.redhat.com/redhat/fbc-target-index"
    )


def test_build_repo_object_omits_empty_delivery_repos() -> None:
    """Omit optional delivery-repo fields when translation is blank."""
    repo_object = collect_index_images.build_repo_object(
        "quay.io/example/repo",
        "v1",
        "",
        "",
    )
    assert "rh-registry-repo" not in repo_object
    assert "registry-access-repo" not in repo_object


def test_collect_index_image_components_creates_two_components() -> None:
    """Build two separate components for floating and timestamped tags."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.12-1234567890"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results)

    assert len(snapshot["components"]) == 2
    # First component: floating tag
    assert (
        snapshot["components"][0]["containerImage"]
        == "redhat.com/rh-stage/iib@sha256:abcdefghijk"
    )
    assert (
        snapshot["components"][0]["repository"] == "quay.io/redhat/redhat----fbc-target-index"
    )
    assert snapshot["components"][0]["tags"] == ["v4.12"]
    # Second component: timestamped tag
    assert (
        snapshot["components"][1]["containerImage"]
        == "redhat.com/rh-stage/iib@sha256:abcdefghijk"
    )
    assert (
        snapshot["components"][1]["repository"] == "quay.io/redhat/redhat----fbc-target-index"
    )
    assert snapshot["components"][1]["tags"] == ["v4.12-1234567890"]


def test_collect_index_image_components_multiple_versions() -> None:
    """Build separate components for each index image."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.12-1111111111"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.13",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.13-2222222222"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:lmnopqrstuv",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results)

    assert len(snapshot["components"]) == 4
    assert snapshot["components"][0]["tags"] == ["v4.12"]
    assert snapshot["components"][1]["tags"] == ["v4.12-1111111111"]
    assert snapshot["components"][2]["tags"] == ["v4.13"]
    assert snapshot["components"][3]["tags"] == ["v4.13-2222222222"]


def test_collect_index_image_components_hotfix_single_component() -> None:
    """Keep hotfix tag only (no separate timestamped component when they match)."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12-12345-6789",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.12-12345-6789"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results)

    assert len(snapshot["components"]) == 1
    assert snapshot["components"][0]["tags"] == ["v4.12-12345-6789"]


def test_collect_index_image_components_pre_ga_single_component() -> None:
    """Keep pre-GA tag only (no separate timestamped component when they match)."""
    tag = "v4.13-myproduct-1.0-20250220143022"
    results = {
        "components": [
            {
                "target_index": f"quay.io/redhat/redhat----preview-operator-index:{tag}",
                "target_index_with_timestamp": (
                    f"quay.io/redhat/redhat----preview-operator-index:{tag}"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:lmnopqrstuv",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_PREVIEW,
    ):
        snapshot = collect_index_images.collect_index_image_components(results)

    assert len(snapshot["components"]) == 1
    assert snapshot["components"][0]["tags"] == [tag]


def test_collect_index_image_components_staged_release_empty_target() -> None:
    """Skip empty target_index for staged releases."""
    results = {
        "components": [
            {
                "target_index": "",
                "target_index_with_timestamp": "",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
        ],
    }
    snapshot = collect_index_images.collect_index_image_components(results)

    # No components created when both target indices are empty
    assert len(snapshot["components"]) == 0


def test_collect_index_image_components_includes_image_digests() -> None:
    """Copy image_digests from the internal-request results when present."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.12-1234567890"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
                "image_digests": ["sha256:one", "sha256:two"],
            },
        ],
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results)

    # Both components should have the same image_digests
    assert snapshot["components"][0]["imageDigests"] == ["sha256:one", "sha256:two"]
    assert snapshot["components"][1]["imageDigests"] == ["sha256:one", "sha256:two"]


def test_collect_index_image_components_defaults_image_digests_to_empty() -> None:
    """Use an empty imageDigests list when image_digests is absent."""
    results = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.12-1234567890"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
            },
        ],
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(results)

    assert snapshot["components"][0]["imageDigests"] == []
    assert snapshot["components"][1]["imageDigests"] == []


def test_collect_index_image_components_rejects_invalid_row() -> None:
    """Fail when a components entry is not a JSON object."""
    with pytest.raises(ValueError, match="components\\[0\\] must be a JSON object"):
        collect_index_images.collect_index_image_components({"components": ["bad"]})


def test_collect_index_image_components_rejects_missing_source_index() -> None:
    """Fail when index_image_resolved is missing from a component row."""
    row = {"target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12"}
    with pytest.raises(ValueError, match="index_image_resolved must be a non-empty string"):
        collect_index_images.collect_index_image_components({"components": [row]})


def test_collect_index_image_components_rejects_invalid_image_digests() -> None:
    """Fail when image_digests is not a JSON array."""
    row = {
        "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
        "index_image_resolved": "img@sha256:abc",
        "image_digests": "bad",
    }
    with pytest.raises(ValueError, match="image_digests must be a JSON array"):
        collect_index_images.collect_index_image_components({"components": [row]})


def test_collect_index_image_components_rejects_invalid_components_type() -> None:
    """Fail when the results components field is not an array."""
    with pytest.raises(ValueError, match="components must be a JSON array"):
        collect_index_images.collect_index_image_components({"components": "bad"})


def test_collect_index_image_components_rejects_non_string_target_index() -> None:
    """Fail when target_index is not a string."""
    row = {
        "target_index": 123,
        "target_index_with_timestamp": "quay.io/redhat/repo:v1-123",
        "index_image_resolved": "img@sha256:abc",
    }
    with pytest.raises(ValueError, match="target_index must be a string"):
        collect_index_images.collect_index_image_components({"components": [row]})


def test_collect_index_image_components_rejects_non_string_target_index_with_ts() -> None:
    """Fail when target_index_with_timestamp is not a string."""
    row = {
        "target_index": "quay.io/redhat/repo:v1",
        "target_index_with_timestamp": 456,
        "index_image_resolved": "img@sha256:abc",
    }
    with pytest.raises(ValueError, match="target_index_with_timestamp must be a string"):
        collect_index_images.collect_index_image_components({"components": [row]})


def test_collect_index_image_components_only_target_index_populated() -> None:
    """Create one component when only target_index is present."""
    row = {
        "target_index": "quay.io/redhat/redhat----preview-operator-index:v4.13",
        "target_index_with_timestamp": "",
        "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_PREVIEW,
    ):
        snapshot = collect_index_images.collect_index_image_components(
            {"components": [row]},
        )
    assert len(snapshot["components"]) == 1
    assert snapshot["components"][0]["tags"] == ["v4.13"]


def test_collect_index_image_components_only_target_index_with_ts_populated() -> None:
    """Create one component when only target_index_with_timestamp is present."""
    row = {
        "target_index": "",
        "target_index_with_timestamp": "quay.io/redhat/redhat----fbc-target-index:v4.12-123",
        "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        snapshot = collect_index_images.collect_index_image_components(
            {"components": [row]},
        )
    assert len(snapshot["components"]) == 1
    assert snapshot["components"][0]["tags"] == ["v4.12-123"]


def test_collect_index_image_components_missing_ts_field() -> None:
    """Create one component when target_index_with_timestamp key is absent."""
    row = {
        "target_index": "quay.io/redhat/redhat----preview-operator-index:v4.13",
        "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_PREVIEW,
    ):
        snapshot = collect_index_images.collect_index_image_components(
            {"components": [row]},
        )
    assert len(snapshot["components"]) == 1
    assert snapshot["components"][0]["tags"] == ["v4.13"]


def test_collect_index_image_components_null_ts_with_populated_target() -> None:
    """Create one component when target_index_with_timestamp is null."""
    row = {
        "target_index": "quay.io/redhat/redhat----preview-operator-index:v4.13",
        "target_index_with_timestamp": None,
        "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
    }
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_PREVIEW,
    ):
        snapshot = collect_index_images.collect_index_image_components(
            {"components": [row]},
        )
    assert len(snapshot["components"]) == 1
    assert snapshot["components"][0]["tags"] == ["v4.13"]


def test_run_collect_index_images_writes_snapshot_and_result(
    tmp_path: Path,
) -> None:
    """Write the snapshot JSON file and Tekton result path."""
    _write_results(
        tmp_path / "internal-requests-results.json",
        [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.12",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.12-1234567890"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
            },
        ],
    )
    result_path = tmp_path / "result.txt"
    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        collect_index_images.run_collect_index_images(
            data_dir=tmp_path,
            internal_request_results_file=Path("internal-requests-results.json"),
            snapshot_path=tmp_path / collect_index_images.SNAPSHOT_FILENAME,
            index_image_snapshot_result_path=result_path,
        )

    snapshot = json.loads(
        (tmp_path / collect_index_images.SNAPSHOT_FILENAME).read_text(encoding="utf-8"),
    )
    assert len(snapshot["components"]) == 2
    assert result_path.read_text(encoding="utf-8") == collect_index_images.SNAPSHOT_FILENAME


def test_run_collect_index_images_missing_results_file(tmp_path: Path) -> None:
    """Fail when the internal request results file is missing."""
    with pytest.raises(FileNotFoundError, match="internal request results file not found"):
        collect_index_images.run_collect_index_images(
            data_dir=tmp_path,
            internal_request_results_file=Path("missing.json"),
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
    monkeypatch.setenv("RESULT_INDEX_IMAGE_SNAPSHOT_PATH", str(tmp_path / "result.txt"))
    with pytest.raises(FileNotFoundError, match="internal request results file not found"):
        runpy.run_module(
            "release_service_utils.tasks.managed" ".collect_index_images.collect_index_images",
            run_name="__main__",
        )


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
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.12-1234567890"
                ),
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abc",
            },
        ],
    )
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_RESULTS_FILE", "internal-requests-results.json")
    monkeypatch.setenv("RESULT_INDEX_IMAGE_SNAPSHOT_PATH", str(tmp_path / "result.txt"))

    with mock.patch(
        "collect_index_images.collect_index_images.image_ref.translate_delivery_repo",
        return_value=_TRANSLATED_FBC,
    ):
        assert collect_index_images.main() == 0
