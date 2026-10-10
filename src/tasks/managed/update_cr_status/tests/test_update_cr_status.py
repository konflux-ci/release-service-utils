"""Test for update_cr_status."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest
from release_service_utils.tasks.managed.update_cr_status import update_cr_status

TASK = "release_service_utils.tasks.managed.update_cr_status.update_cr_status"


def _write(path: Path, content: object) -> None:
    """Write content to path as JSON, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content), encoding="utf-8")


@pytest.fixture
def captured_patch() -> Iterator[list[dict]]:
    """Mock kubectl.patch_resource and record each call's args and patch file contents."""
    calls: list[dict] = []

    def _fake(resource_type: str, name: str, **kwargs: object) -> None:
        patch_file = Path(str(kwargs["patch_file"]))
        calls.append(
            {
                "resource_type": resource_type,
                "name": name,
                "payload": json.loads(patch_file.read_text(encoding="utf-8")),
                "patch_file": patch_file,
                **kwargs,
            }
        )

    with mock.patch(f"{TASK}.kubectl.patch_resource", side_effect=_fake):
        yield calls


def test_merge_results_dir_single_file(tmp_path: Path) -> None:
    """A single file is returned as is."""
    _write(tmp_path / "test.json", {"foo": "bar"})
    assert update_cr_status.merge_results_dir(tmp_path) == {"foo": "bar"}


def test_merge_results_dir_nested_objects(tmp_path: Path) -> None:
    """Two files nested objects get merged, unique keys just get added."""
    _write(tmp_path / "one.json", {"one": {"foo": "bar"}, "two": {"a": "b"}})
    _write(tmp_path / "two.json", {"one": {"union": "value"}, "z": {"cat": "dog"}})
    assert update_cr_status.merge_results_dir(tmp_path) == {
        "one": {"foo": "bar", "union": "value"},
        "two": {"a": "b"},
        "z": {"cat": "dog"},
    }


def test_merge_results_dir_array_concat(tmp_path: Path) -> None:
    """Same array key in two files ends up as both arrays concatenated."""
    _write(tmp_path / "one.json", {"test": ["one", "two"]})
    _write(tmp_path / "two.json", {"test": ["three", "four"]})
    assert sorted(update_cr_status.merge_results_dir(tmp_path)["test"]) == [
        "four",
        "one",
        "three",
        "two",
    ]


def test_merge_results_dir_array_concat_keeps_order_and_duplicates(tmp_path: Path) -> None:
    """Repeated values and file order are kept, not deduped or sorted."""
    _write(tmp_path / "one.json", {"test": ["b", "a"]})
    _write(tmp_path / "two.json", {"test": ["c", "a"]})
    assert update_cr_status.merge_results_dir(tmp_path)["test"] == ["b", "a", "c", "a"]


def test_merge_results_dir_advisory_results(tmp_path: Path) -> None:
    """A real filter-already-released-advisory-images results file merges fine too."""
    advisory = {
        "advisory": {
            "url": "https://access.redhat.com/errata/RHSA-2024:12345",
            "internal_url": "https://errata.devel.redhat.com/advisory/12345",
        }
    }
    _write(tmp_path / "filter-already-released-advisory-images-results.json", advisory)
    assert update_cr_status.merge_results_dir(tmp_path) == advisory


def test_merge_results_dir_subdirectories(tmp_path: Path) -> None:
    """Files in nested subdirectories are included too."""
    _write(tmp_path / "a" / "b" / "deep.json", {"deep": True})
    _write(tmp_path / "top.json", {"top": True})
    assert update_cr_status.merge_results_dir(tmp_path) == {"deep": True, "top": True}


def test_merge_results_dir_sorted_order(
    tmp_path: Path,
) -> None:
    """When two files set the same key, the one that sorts later wins."""
    _write(tmp_path / "b.json", {"key": "second"})
    _write(tmp_path / "a.json", {"key": "first"})
    assert update_cr_status.merge_results_dir(tmp_path) == {"key": "second"}


def test_merge_results_dir_missing_directory(tmp_path: Path) -> None:
    """A missing results directory returns an empty object."""
    assert update_cr_status.merge_results_dir(tmp_path / "nonexistent") == {}


def test_merge_results_dir_empty_directory(tmp_path: Path) -> None:
    """A results directory with no files returns an empty object."""
    assert update_cr_status.merge_results_dir(tmp_path) == {}


def test_merge_results_dir_non_object_raises(tmp_path: Path) -> None:
    """A JSON list instead of an object raises a TypeError."""
    _write(tmp_path / "list.json", ["a", "b"])
    with pytest.raises(TypeError):
        update_cr_status.merge_results_dir(tmp_path)


def test_patch_status_empty_results(captured_patch: list[dict]) -> None:
    """An empty results object is still patched."""
    update_cr_status.patch_status("ns/name", "release", "artifacts", {})
    assert captured_patch[0]["payload"] == {"status": {"artifacts": {}}}


def test_patch_status_splits_resource(
    captured_patch: list[dict],
) -> None:
    """A valid namespace/name resource splits into namespace and name."""
    update_cr_status.patch_status("ns/name", "release", "artifacts", {})
    assert captured_patch[0]["namespace"] == "ns"
    assert captured_patch[0]["name"] == "name"


def test_patch_status_custom_status_key(
    captured_patch: list[dict],
) -> None:
    """A custom statusKey and resourceType are used instead of the defaults."""
    update_cr_status.patch_status(
        "default/releaseplan-missing-rbac",
        "releasePlan",
        "releasePlanAdmission",
        {"name": "foo", "active": False},
    )
    call = captured_patch[0]
    assert call["resource_type"] == "releasePlan"
    assert call["payload"] == {
        "status": {"releasePlanAdmission": {"name": "foo", "active": False}}
    }


def test_patch_status_kubectl_failure() -> None:
    """A failing kubectl call raises out of patch_status."""
    with mock.patch(f"{TASK}.kubectl.patch_resource", side_effect=RuntimeError("forbidden")):
        with pytest.raises(RuntimeError, match="forbidden"):
            update_cr_status.patch_status(
                "default/my-release", "release", "artifacts", {"automated": True}
            )


def test_run_single_result_file(tmp_path: Path, captured_patch: list[dict]) -> None:
    """One result file gets merged and patched under the default statusKey."""
    _write(tmp_path / "uid" / "results" / "test.json", {"foo": "bar"})

    update_cr_status.run(
        tmp_path, "uid/results", "default/release-cr-status", "release", "artifacts"
    )

    assert captured_patch[0]["payload"] == {"status": {"artifacts": {"foo": "bar"}}}
    assert captured_patch[0]["name"] == "release-cr-status"


def test_run_no_results_dir(tmp_path: Path, captured_patch: list[dict]) -> None:
    """A missing results dir still patches an empty object."""
    update_cr_status.run(
        tmp_path,
        "uid/nonexistent",
        "default/release-cr-no-results",
        "release",
        "artifacts",
    )
    assert captured_patch[0]["payload"] == {"status": {"artifacts": {}}}


def test_run_bad_json(tmp_path: Path) -> None:
    """Bad JSON fails the run and nothing gets patched."""
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "test.json").write_text(
        "this\nis\n not\njson\n}\n", encoding="utf-8"
    )
    with mock.patch(f"{TASK}.kubectl.patch_resource") as mock_patch:
        with pytest.raises(json.JSONDecodeError, match="File is not valid JSON"):
            update_cr_status.run(
                tmp_path, "results", "default/my-release", "release", "artifacts"
            )
    mock_patch.assert_not_called()


def test_run_absolute_results_dir_path_rejected(tmp_path: Path) -> None:
    """An absolute resultsDirPath is rejected, not read."""
    outside = tmp_path.parent / "outside.json"
    _write(outside, {"secret": "value"})
    with mock.patch(f"{TASK}.kubectl.patch_resource") as mock_patch:
        with pytest.raises(ValueError, match="must be relative"):
            update_cr_status.run(
                tmp_path, str(outside.parent), "default/my-release", "release", "artifacts"
            )
    mock_patch.assert_not_called()


def test_run_results_dir_path_traversal_rejected(tmp_path: Path) -> None:
    """A resultsDirPath that escapes dataDir with .. is rejected."""
    outside = tmp_path.parent / "outside"
    _write(outside / "secret.json", {"secret": "value"})
    with mock.patch(f"{TASK}.kubectl.patch_resource") as mock_patch:
        with pytest.raises(ValueError, match="must stay under"):
            update_cr_status.run(
                tmp_path, "../outside", "default/my-release", "release", "artifacts"
            )
    mock_patch.assert_not_called()


def test_main_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Required env vars get read, optional ones fall back to their defaults."""
    monkeypatch.setenv("PARAM_DATA_DIR", "/data")
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "uid/results")
    monkeypatch.setenv("PARAM_RESOURCE", "default/rel")
    monkeypatch.delenv("PARAM_RESOURCE_TYPE", raising=False)
    monkeypatch.delenv("PARAM_STATUS_KEY", raising=False)

    with mock.patch(f"{TASK}.run") as mock_run:
        assert update_cr_status.main() == 0

    mock_run.assert_called_once_with(
        data_dir=Path("/data"),
        results_dir_path="uid/results",
        resource="default/rel",
        resource_type="release",
        status_key="artifacts",
    )
