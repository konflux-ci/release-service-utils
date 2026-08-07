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
