"""Tests for the ``json_merge`` helper module."""

from __future__ import annotations

import json

import pytest

import json_merge

# ---------------------------------------------------------------------------
# unique_sorted
# ---------------------------------------------------------------------------


def test_unique_sorted_strings() -> None:
    """Duplicate strings are removed and the result is sorted."""
    assert json_merge.unique_sorted(["b", "a", "b", "c", "a"]) == ["a", "b", "c"]


def test_unique_sorted_numbers() -> None:
    """Numbers are sorted numerically and deduplicated."""
    assert json_merge.unique_sorted([3, 1, 2, 1, 3]) == [1, 2, 3]


def test_unique_sorted_mixed_types_ordering() -> None:
    """Mixed JSON types follow jq's null < bool < number < string < array < object order."""
    values = [1, "a", None, True, False, [1], {"a": 1}]
    result = json_merge.unique_sorted(values)
    assert result == [None, False, True, 1, "a", [1], {"a": 1}]


def test_unique_sorted_empty_list() -> None:
    """An empty list returns an empty list."""
    assert json_merge.unique_sorted([]) == []


def test_unique_sorted_unsupported_type_raises() -> None:
    """A value with no JSON equivalent raises ``TypeError``."""
    with pytest.raises(TypeError, match="not JSON serializable"):
        json_merge.unique_sorted([{1, 2, 3}])


def test_unique_sorted_dicts_deduplicated() -> None:
    """Equal dicts are deduplicated even though they are unhashable."""
    values = [{"a": 1}, {"a": 1}, {"b": 2}]
    assert json_merge.unique_sorted(values) == [{"a": 1}, {"b": 2}]


def test_unique_sorted_nested_arrays() -> None:
    """Arrays are compared and sorted element-wise."""
    values = [[2, 1], [1, 2], [1, 1]]
    assert json_merge.unique_sorted(values) == [[1, 1], [1, 2], [2, 1]]


# ---------------------------------------------------------------------------
# jq_multiply
# ---------------------------------------------------------------------------


def test_jq_multiply_disjoint_keys_are_combined() -> None:
    """Keys unique to either side are kept."""
    assert json_merge.jq_multiply({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


def test_jq_multiply_scalar_conflict_b_wins() -> None:
    """When both sides define a scalar, ``b``'s value wins."""
    assert json_merge.jq_multiply({"a": 1}, {"a": 2}) == {"a": 2}


def test_jq_multiply_nested_objects_merge_recursively() -> None:
    """Nested objects present on both sides are merged recursively."""
    a = {"a": 1, "b": {"c": 1, "d": 1}}
    b = {"b": {"c": 2, "e": 1}, "f": 3}
    assert json_merge.jq_multiply(a, b) == {"a": 1, "b": {"c": 2, "d": 1, "e": 1}, "f": 3}


def test_jq_multiply_arrays_are_overwritten_not_merged() -> None:
    """Unlike merge_deep_union_arrays, arrays are replaced wholesale."""
    a = {"tags": ["v1", "v2"]}
    b = {"tags": ["v3"]}
    assert json_merge.jq_multiply(a, b) == {"tags": ["v3"]}


def test_jq_multiply_object_vs_non_object_b_wins() -> None:
    """If one side's value is not an object, ``b``'s value replaces it entirely."""
    a = {"a": {"nested": True}}
    b = {"a": "scalar"}
    assert json_merge.jq_multiply(a, b) == {"a": "scalar"}


def test_jq_multiply_incompatible_top_level_types_raise() -> None:
    """Real ``jq`` can't multiply two arrays, two strings, or an object by a string."""
    with pytest.raises(ValueError, match="cannot be multiplied"):
        json_merge.jq_multiply([1, 2], [3])
    with pytest.raises(ValueError, match="cannot be multiplied"):
        json_merge.jq_multiply("a", "b")
    with pytest.raises(ValueError, match="cannot be multiplied"):
        json_merge.jq_multiply({"a": 1}, "b")


def test_jq_multiply_numbers_multiplies_arithmetically() -> None:
    """``jq``'s ``*`` on two numbers is regular multiplication, not object merge."""
    assert json_merge.jq_multiply(3, 4) == 12


def test_jq_multiply_empty_dict_identity() -> None:
    """Multiplying with an empty dict on the left copies ``b``'s contents."""
    b = {"a": 1, "b": {"c": 2}}
    assert json_merge.jq_multiply({}, b) == b


def test_jq_multiply_does_not_mutate_inputs() -> None:
    """Neither input dict is mutated."""
    a = {"a": {"c": 1}}
    b = {"a": {"d": 2}}
    a_copy, b_copy = json.loads(json.dumps(a)), json.loads(json.dumps(b))
    json_merge.jq_multiply(a, b)
    assert a == a_copy
    assert b == b_copy


# ---------------------------------------------------------------------------
# merge_deep_union_arrays
# ---------------------------------------------------------------------------


def test_merge_deep_union_arrays_disjoint_keys() -> None:
    """Keys unique to either side are kept."""
    assert json_merge.merge_deep_union_arrays({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


def test_merge_deep_union_arrays_concatenates_and_dedupes() -> None:
    """Array values are concatenated, deduplicated, and sorted."""
    a = {"tags": ["v1", "v2"]}
    b = {"tags": ["v2", "v3"]}
    assert json_merge.merge_deep_union_arrays(a, b) == {"tags": ["v1", "v2", "v3"]}


def test_merge_deep_union_arrays_nested_objects_merge_recursively() -> None:
    """Nested objects are merged recursively rather than replaced."""
    a = {"settings": {"accountId": ["1"], "publish": True}}
    b = {"settings": {"accountId": ["2"]}}
    result = json_merge.merge_deep_union_arrays(a, b)
    assert result == {"settings": {"accountId": ["1", "2"], "publish": True}}


def test_merge_deep_union_arrays_scalar_conflict_b_wins() -> None:
    """When both sides define an incompatible scalar, ``b``'s value wins."""
    assert json_merge.merge_deep_union_arrays({"a": 1}, {"a": 2}) == {"a": 2}


def test_merge_deep_union_arrays_null_b_falls_back_to_a() -> None:
    """A ``None`` value in ``b`` does not clobber ``a``'s value."""
    assert json_merge.merge_deep_union_arrays({"a": 1}, {"a": None}) == {"a": 1}


def test_merge_deep_union_arrays_preserves_false() -> None:
    """A literal ``False`` in ``b`` is preserved and not treated as missing."""
    assert json_merge.merge_deep_union_arrays({"a": True}, {"a": False}) == {"a": False}


def test_merge_deep_union_arrays_key_only_in_a() -> None:
    """A key present only in ``a`` is preserved."""
    assert json_merge.merge_deep_union_arrays({"a": 1}, {}) == {"a": 1}


def test_merge_deep_union_arrays_key_only_in_b() -> None:
    """A key present only in ``b`` is added."""
    assert json_merge.merge_deep_union_arrays({}, {"a": 1}) == {"a": 1}


def test_merge_deep_union_arrays_empty_both() -> None:
    """Merging two empty objects returns an empty object."""
    assert json_merge.merge_deep_union_arrays({}, {}) == {}


def test_merge_deep_union_arrays_type_mismatch_array_vs_scalar() -> None:
    """A type mismatch (array vs scalar) falls through to ``b`` wins."""
    assert json_merge.merge_deep_union_arrays({"a": [1, 2]}, {"a": "x"}) == {"a": "x"}


def test_merge_deep_union_arrays_does_not_mutate_inputs() -> None:
    """Neither input dict is mutated."""
    a = {"tags": ["v1"]}
    b = {"tags": ["v2"]}
    json_merge.merge_deep_union_arrays(a, b)
    assert a == {"tags": ["v1"]}
    assert b == {"tags": ["v2"]}


def test_merge_deep_union_arrays_named_objects_merged_by_name() -> None:
    """Array elements with the same ``name`` are merged rather than duplicated."""
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": [{"name": "foo", "y": 2}]}
    assert json_merge.merge_deep_union_arrays(a, b) == {
        "components": [{"name": "foo", "x": 1, "y": 2}]
    }


def test_merge_deep_union_arrays_duplicate_named_entries_in_a_combined() -> None:
    """Duplicate named entries in ``a`` are combined before merging with ``b``."""
    a = {"components": [{"name": "foo", "x": 1}, {"name": "foo", "y": 2}]}
    b = {"components": [{"name": "foo", "z": 3}]}
    result = json_merge.merge_deep_union_arrays(a, b)
    assert result == {"components": [{"name": "foo", "x": 1, "y": 2, "z": 3}]}


def test_merge_deep_union_arrays_named_object_scalar_b_wins() -> None:
    """When same-named objects define the same scalar key, ``b``'s value wins."""
    a = {"components": [{"name": "foo", "version": "1.0"}]}
    b = {"components": [{"name": "foo", "version": "2.0"}]}
    assert json_merge.merge_deep_union_arrays(a, b) == {
        "components": [{"name": "foo", "version": "2.0"}]
    }


def test_merge_deep_union_arrays_named_object_null_b_falls_back_to_a() -> None:
    """A ``None`` value in ``b`` does not clobber ``a``'s value for a named object."""
    a = {"components": [{"name": "foo", "version": "1.0"}]}
    b = {"components": [{"name": "foo", "version": None}]}
    assert json_merge.merge_deep_union_arrays(a, b) == {
        "components": [{"name": "foo", "version": "1.0"}]
    }


def test_merge_deep_union_arrays_named_objects_empty_a() -> None:
    """Named entries from ``b`` are kept when ``a``'s array is empty."""
    a = {"components": []}
    b = {"components": [{"name": "foo", "x": 1}]}
    assert json_merge.merge_deep_union_arrays(a, b) == {
        "components": [{"name": "foo", "x": 1}]
    }


def test_merge_deep_union_arrays_named_objects_empty_b() -> None:
    """Named entries from ``a`` are kept when ``b``'s array is empty."""
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": []}
    assert json_merge.merge_deep_union_arrays(a, b) == {
        "components": [{"name": "foo", "x": 1}]
    }


def test_merge_deep_union_arrays_named_objects_different_names_kept() -> None:
    """Named objects with different names are both kept."""
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": [{"name": "bar", "y": 2}]}
    result = json_merge.merge_deep_union_arrays(a, b)
    assert len(result["components"]) == 2
    assert {"name": "foo", "x": 1} in result["components"]
    assert {"name": "bar", "y": 2} in result["components"]


def test_merge_deep_union_arrays_unnamed_items_pass_through() -> None:
    """Items without a ``name`` key are appended unchanged."""
    a = {"components": [{"name": "foo", "x": 1}, {"role": "sidecar"}]}
    b = {"components": [{"name": "foo", "y": 2}]}
    result = json_merge.merge_deep_union_arrays(a, b)
    assert {"name": "foo", "x": 1, "y": 2} in result["components"]
    assert {"role": "sidecar"} in result["components"]


def test_merge_deep_union_arrays_named_object_nested_merge_recursively() -> None:
    """Nested objects inside same-named entries are merged recursively."""
    a = {"components": [{"name": "foo", "staged": {"destination": "dest", "files": ["f1"]}}]}
    b = {"components": [{"name": "foo", "staged": {"version": "1.0"}}]}
    result = json_merge.merge_deep_union_arrays(a, b)
    assert result == {
        "components": [
            {
                "name": "foo",
                "staged": {"destination": "dest", "files": ["f1"], "version": "1.0"},
            }
        ]
    }


def test_merge_deep_union_arrays_named_object_disjoint_nested_keys_combined() -> None:
    """Disjoint nested keys from same named entries are combined."""
    a = {"mapping": {"components": [{"name": "main", "staged": {"version": "4.11.5"}}]}}
    b = {
        "mapping": {
            "components": [
                {
                    "contentType": "binary",
                    "name": "main",
                    "staged": {"destination": "rhacs-files", "files": [{"arch": "amd64"}]},
                }
            ]
        }
    }
    result = json_merge.merge_deep_union_arrays(a, b)
    components = result["mapping"]["components"]
    assert len(components) == 1
    assert components[0]["contentType"] == "binary"
    assert components[0]["staged"]["version"] == "4.11.5"
    assert components[0]["staged"]["destination"] == "rhacs-files"


def test_merge_deep_union_arrays_named_component_scalar_b_wins() -> None:
    """A scalar conflict inside a same named component is won by ``b``."""
    a = {
        "mapping": {
            "components": [
                {"name": "main-4-11", "contentType": "source", "staged": {"version": "4.11.5"}}
            ]
        }
    }
    b = {"mapping": {"components": [{"name": "main-4-11", "contentType": "binary"}]}}
    result = json_merge.merge_deep_union_arrays(a, b)
    components = result["mapping"]["components"]
    assert len(components) == 1
    assert components[0]["contentType"] == "binary"
    assert components[0]["staged"]["version"] == "4.11.5"


def test_merge_deep_union_arrays_references_from_both_sides_combined() -> None:
    """References from both sides are combined into one list."""
    a = {"releaseNotes": {"references": ["https://access.redhat.com/downloads/content/837"]}}
    b = {
        "releaseNotes": {
            "references": ["https://docs.redhat.com/en/documentation/rhacs/4.11/"]
        }
    }
    result = json_merge.merge_deep_union_arrays(a, b)
    refs = result["releaseNotes"]["references"]
    assert "https://access.redhat.com/downloads/content/837" in refs
    assert "https://docs.redhat.com/en/documentation/rhacs/4.11/" in refs
    assert len(refs) == 2


def test_merge_deep_union_arrays_named_component_only_in_b_is_added() -> None:
    """A component present only in ``b`` is added alongside shared entries."""
    a = {"mapping": {"components": [{"name": "main-4-11", "staged": {"version": "4.11.5"}}]}}
    b = {
        "mapping": {
            "components": [
                {
                    "contentType": "binary",
                    "name": "main-4-11",
                    "staged": {"destination": "rhacs-4-for-rhel-9-x86_64-files"},
                },
                {
                    "contentType": "binary",
                    "name": "sidecar-4-11",
                    "staged": {"destination": "rhacs-4-sidecar-files"},
                },
            ]
        }
    }
    result = json_merge.merge_deep_union_arrays(a, b)
    by_name = {c["name"]: c for c in result["mapping"]["components"]}
    assert len(by_name) == 2
    assert by_name["main-4-11"]["staged"]["version"] == "4.11.5"
    assert by_name["main-4-11"]["staged"]["destination"] == "rhacs-4-for-rhel-9-x86_64-files"
    assert by_name["sidecar-4-11"]["staged"]["destination"] == "rhacs-4-sidecar-files"


def test_merge_deep_union_arrays_rpm_repositories_merged_by_name() -> None:
    """rpm-repositories entries with the same name are merged, not duplicated."""
    a = {
        "mapping": {
            "rpm-repositories": [{"name": "rhel-9-baseos", "baseurl": "https://a.example.com"}]
        }
    }
    b = {"mapping": {"rpm-repositories": [{"name": "rhel-9-baseos", "gpgcheck": True}]}}
    result = json_merge.merge_deep_union_arrays(a, b)
    repos = result["mapping"]["rpm-repositories"]
    assert len(repos) == 1
    assert repos[0]["name"] == "rhel-9-baseos"
    assert repos[0]["baseurl"] == "https://a.example.com"
    assert repos[0]["gpgcheck"] is True


def test_merge_deep_union_arrays_named_objects_successive_calls() -> None:
    """Named entries accumulate correctly across successive merge calls."""
    release = {
        "mapping": {"components": [{"name": "main-4-11", "staged": {"version": "4.11.5"}}]},
        "releaseNotes": {"type": "RHBA", "synopsis": "RHACS 4.11.5 CLI update"},
    }
    rp = {
        "releaseNotes": {
            "description": "Binary release of the roxctl CLI.",
            "solution": "Download the roxctl binary for your platform.",
        }
    }
    rpa = {
        "cdn": {"env": "stage"},
        "mapping": {
            "components": [
                {
                    "contentType": "binary",
                    "name": "main-4-11",
                    "staged": {
                        "destination": "rhacs-4-for-rhel-9-x86_64-files",
                        "files": [
                            {"arch": "amd64", "filename": "roxctl-linux", "os": "linux"}
                        ],
                    },
                }
            ]
        },
    }
    merged = json_merge.merge_deep_union_arrays({}, release)
    merged = json_merge.merge_deep_union_arrays(merged, rp)
    merged = json_merge.merge_deep_union_arrays(merged, rpa)
    components = merged["mapping"]["components"]
    assert len(components) == 1
    assert components[0]["contentType"] == "binary"
    assert components[0]["staged"]["version"] == "4.11.5"
    assert components[0]["staged"]["destination"] == "rhacs-4-for-rhel-9-x86_64-files"
    assert merged["cdn"]["env"] == "stage"
    assert merged["releaseNotes"]["type"] == "RHBA"
    assert merged["releaseNotes"]["description"] == "Binary release of the roxctl CLI."
