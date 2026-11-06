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
    assert json_merge.jq_multiply(a, b) == {
        "a": 1,
        "b": {"c": 2, "d": 1, "e": 1},
        "f": 3,
    }


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


# -- plain scalars, objects, and arrays --


def test_merge_deep_union_arrays_disjoint_keys() -> None:
    """Keys unique to either side are kept."""
    assert json_merge.merge_deep_union_arrays({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


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


def test_merge_deep_union_arrays_type_mismatch_object_vs_array() -> None:
    """A type mismatch (object vs array) also falls through to ``b`` wins."""
    assert json_merge.merge_deep_union_arrays({"a": {"x": 1}}, {"a": [1]}) == {"a": [1]}


def test_merge_deep_union_arrays_does_not_mutate_inputs() -> None:
    """Neither input dict is mutated."""
    a = {"tags": ["v1"]}
    b = {"tags": ["v2"]}
    json_merge.merge_deep_union_arrays(a, b)
    assert a == {"tags": ["v1"]}
    assert b == {"tags": ["v2"]}


def test_merge_deep_union_arrays_concatenates_and_dedupes() -> None:
    """A plain array is concatenated and deduplicated."""
    a = {"tags": ["v1", "v2"]}
    b = {"tags": ["v2", "v3"]}
    assert json_merge.merge_deep_union_arrays(a, b) == {"tags": ["v1", "v2", "v3"]}


def test_merge_deep_union_arrays_without_merge_keys_objects_not_matched() -> None:
    """With no merge_keys entry for the field, objects are never matched by identity."""
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": [{"name": "foo", "y": 2}]}
    result = json_merge.merge_deep_union_arrays(a, b)
    assert result["components"] == [{"name": "foo", "x": 1}, {"name": "foo", "y": 2}]


# -- merge_keys set, but no entries carry the identity field --


def test_merge_deep_union_arrays_merge_keys_field_not_array_merges_as_scalar() -> None:
    """A field listed in merge_keys that isn't actually an array just follows scalar rules."""
    merge_keys = {"components": ["name"]}
    result = json_merge.merge_deep_union_arrays(
        {"components": "a"}, {"components": "b"}, merge_keys
    )
    assert result == {"components": "b"}


def test_merge_deep_union_arrays_merge_keys_no_matchable_entries_falls_back() -> None:
    """If neither side's entries carry the identity field, it's merged like a plain array."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"role": "sidecar"}]}
    b = {"components": [{"role": "init"}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [{"role": "init"}, {"role": "sidecar"}]


# -- identity merge --


def test_merge_deep_union_arrays_merged_by_identity() -> None:
    """Entries sharing an identity are merged into one instead of duplicated."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": [{"name": "foo", "y": 2}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [{"name": "foo", "x": 1, "y": 2}]


def test_merge_deep_union_arrays_duplicates_in_a_combined_before_b() -> None:
    """Same identity duplicates within ``a`` are folded together before merging with ``b``."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "x": 1}, {"name": "foo", "y": 2}]}
    b = {"components": [{"name": "foo", "z": 3}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [{"name": "foo", "x": 1, "y": 2, "z": 3}]


def test_merge_deep_union_arrays_identity_match_scalar_b_wins() -> None:
    """Within a matched entry, a scalar conflict is still won by ``b``."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "version": "1.0"}]}
    b = {"components": [{"name": "foo", "version": "2.0"}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [{"name": "foo", "version": "2.0"}]


def test_merge_deep_union_arrays_identity_match_null_b_falls_back_to_a() -> None:
    """Within a matched entry, a ``None`` in ``b`` still doesn't clobber ``a``'s value."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "version": "1.0"}]}
    b = {"components": [{"name": "foo", "version": None}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [{"name": "foo", "version": "1.0"}]


def test_merge_deep_union_arrays_identity_match_empty_a() -> None:
    """Entries from ``b`` are kept when ``a``'s array is empty."""
    merge_keys = {"components": ["name"]}
    result = json_merge.merge_deep_union_arrays(
        {"components": []}, {"components": [{"name": "foo", "x": 1}]}, merge_keys
    )
    assert result["components"] == [{"name": "foo", "x": 1}]


def test_merge_deep_union_arrays_identity_match_empty_b() -> None:
    """Entries from ``a`` are kept when ``b``'s array is empty."""
    merge_keys = {"components": ["name"]}
    result = json_merge.merge_deep_union_arrays(
        {"components": [{"name": "foo", "x": 1}]}, {"components": []}, merge_keys
    )
    assert result["components"] == [{"name": "foo", "x": 1}]


def test_merge_deep_union_arrays_identity_match_different_identities_both_kept() -> None:
    """Entries with different identity values are both kept, not merged."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": [{"name": "bar", "y": 2}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert len(result["components"]) == 2
    assert {"name": "foo", "x": 1} in result["components"]
    assert {"name": "bar", "y": 2} in result["components"]


def test_merge_deep_union_arrays_entries_without_identity_both_kept() -> None:
    """Different entries missing the identity field are kept from both sides, not merged."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "x": 1}, {"role": "sidecar"}]}
    b = {"components": [{"name": "foo", "y": 2}, {"role": "init"}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert {"name": "foo", "x": 1, "y": 2} in result["components"]
    assert {"role": "sidecar"} in result["components"]
    assert {"role": "init"} in result["components"]


def test_merge_deep_union_arrays_entries_without_identity_dedupe_if_identical() -> None:
    """Identical entries missing the identity field still dedupe, same as a plain array."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "x": 1}, {"role": "sidecar"}]}
    b = {"components": [{"name": "foo", "y": 2}, {"role": "sidecar"}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert {"name": "foo", "x": 1, "y": 2} in result["components"]
    assert result["components"].count({"role": "sidecar"}) == 1


def test_merge_deep_union_arrays_identity_match_nested_merge_recursively() -> None:
    """Nested objects inside matched entries are merged recursively."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "staged": {"destination": "dest", "files": ["f1"]}}]}
    b = {"components": [{"name": "foo", "staged": {"version": "1.0"}}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [
        {
            "name": "foo",
            "staged": {"destination": "dest", "files": ["f1"], "version": "1.0"},
        }
    ]


def test_merge_deep_union_arrays_identity_match_only_in_b_added() -> None:
    """A new entry only in ``b`` is added alongside one matched by identity."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": [{"name": "foo", "y": 2}, {"name": "bar", "z": 3}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    by_name = {c["name"]: c for c in result["components"]}
    assert by_name == {
        "foo": {"name": "foo", "x": 1, "y": 2},
        "bar": {"name": "bar", "z": 3},
    }


def test_merge_deep_union_arrays_composite_identity_matches_all_fields() -> None:
    """A composite identity only merges entries that share every one of its fields."""
    merge_keys = {"components": ["name", "arch"]}
    a = {"components": [{"name": "foo", "arch": "x86_64", "x": 1}]}
    b = {"components": [{"name": "foo", "arch": "x86_64", "y": 2}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [{"name": "foo", "arch": "x86_64", "x": 1, "y": 2}]


def test_merge_deep_union_arrays_composite_identity_mismatch_kept_separate() -> None:
    """Same name but a different value for the second field means a different identity."""
    merge_keys = {"components": ["name", "arch"]}
    a = {"components": [{"name": "foo", "arch": "x86_64"}]}
    b = {"components": [{"name": "foo", "arch": "aarch64"}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert len(result["components"]) == 2


def test_merge_deep_union_arrays_composite_identity_missing_field_kept_separate() -> None:
    """One side missing a composite key field is a different identity from one that has it."""
    merge_keys = {"components": ["name", "arch"]}
    a = {"components": [{"name": "foo"}]}
    b = {"components": [{"name": "foo", "arch": "x86_64"}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert len(result["components"]) == 2


def test_merge_deep_union_arrays_entries_without_first_key_field_not_merged() -> None:
    """Missing the first identity field means no match, even with a shared second field."""
    merge_keys = {"items": ["primary", "secondary"]}
    a = {"items": [{"secondary": "s1", "x": 1}]}
    b = {"items": [{"secondary": "s1", "y": 2}]}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert {"secondary": "s1", "x": 1} in result["items"]
    assert {"secondary": "s1", "y": 2} in result["items"]


def test_merge_deep_union_arrays_identity_match_works_when_nested() -> None:
    """An array matched by merge_keys is found by field name no matter how deeply nested."""
    merge_keys = {"repositories": ["name"]}
    a = {"mapping": {"repositories": [{"name": "foo", "baseurl": "https://a.example.com"}]}}
    b = {"mapping": {"repositories": [{"name": "foo", "gpgcheck": True}]}}
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["mapping"]["repositories"] == [
        {"name": "foo", "baseurl": "https://a.example.com", "gpgcheck": True}
    ]


def test_merge_deep_union_arrays_multiple_merge_keys_fields_independent() -> None:
    """Two merge_keys entries each apply by their own key, even nested one inside the other."""
    merge_keys = {"components": ["name"], "repositories": ["url"]}
    a = {
        "mapping": {
            "components": [{"name": "foo", "repositories": [{"url": "y", "tags": ["a", "b"]}]}]
        }
    }
    b = {
        "mapping": {
            "components": [{"name": "foo", "repositories": [{"url": "y", "tags": ["b", "c"]}]}]
        }
    }
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["mapping"]["components"] == [
        {"name": "foo", "repositories": [{"url": "y", "tags": ["a", "b", "c"]}]}
    ]


def test_merge_deep_union_arrays_identity_match_does_not_mutate_inputs() -> None:
    """Entries still merge by identity, but neither input itself is mutated."""
    merge_keys = {"components": ["name"]}
    a = {"components": [{"name": "foo", "x": 1}]}
    b = {"components": [{"name": "foo", "y": 2}]}
    a_copy, b_copy = json.loads(json.dumps(a)), json.loads(json.dumps(b))
    result = json_merge.merge_deep_union_arrays(a, b, merge_keys)
    assert result["components"] == [{"name": "foo", "x": 1, "y": 2}]
    assert a == a_copy
    assert b == b_copy


def test_merge_deep_union_arrays_identity_match_accumulates_across_calls() -> None:
    """Entries keep merging by identity across repeated merge_deep_union_arrays calls."""
    merge_keys = {"components": ["name"]}
    first = {"components": [{"name": "foo", "x": 1}]}
    second = {"other": "value"}
    third = {"components": [{"name": "foo", "y": 2}, {"name": "bar", "z": 3}]}
    merged = json_merge.merge_deep_union_arrays({}, first, merge_keys)
    merged = json_merge.merge_deep_union_arrays(merged, second, merge_keys)
    merged = json_merge.merge_deep_union_arrays(merged, third, merge_keys)
    assert merged == {
        "components": [{"name": "foo", "x": 1, "y": 2}, {"name": "bar", "z": 3}],
        "other": "value",
    }


# ---------------------------------------------------------------------------
# merge_concat_arrays
# ---------------------------------------------------------------------------


def test_merge_concat_arrays_disjoint_keys() -> None:
    """Keys unique to either side are kept."""
    assert json_merge.merge_concat_arrays({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


def test_merge_concat_arrays_concatenates_in_order_with_duplicates() -> None:
    """Arrays keep their order and duplicates."""
    a = {"tags": ["v2", "v1"]}
    b = {"tags": ["v3", "v1"]}
    assert json_merge.merge_concat_arrays(a, b) == {"tags": ["v2", "v1", "v3", "v1"]}


def test_merge_concat_arrays_nested_objects_merge_recursively() -> None:
    """Nested objects are merged recursively rather than replaced."""
    a = {"settings": {"publish": True}}
    b = {"settings": {"env": "prod"}}
    result = json_merge.merge_concat_arrays(a, b)
    assert result == {"settings": {"publish": True, "env": "prod"}}


def test_merge_concat_arrays_only_concatenates_top_level_arrays() -> None:
    """An array nested inside a merged object is overwritten, not concatenated."""
    a = {"settings": {"accountId": ["1"], "publish": True}}
    b = {"settings": {"accountId": ["2"]}}
    result = json_merge.merge_concat_arrays(a, b)
    assert result == {"settings": {"accountId": ["2"], "publish": True}}


def test_merge_concat_arrays_scalar_conflict_b_wins() -> None:
    """When both sides define an incompatible scalar, ``b``'s value wins."""
    assert json_merge.merge_concat_arrays({"a": 1}, {"a": 2}) == {"a": 2}


def test_merge_concat_arrays_null_b_falls_back_to_a() -> None:
    """A ``None`` value in ``b`` does not clobber ``a``'s value."""
    assert json_merge.merge_concat_arrays({"a": 1}, {"a": None}) == {"a": 1}


def test_merge_concat_arrays_false_b_falls_back_to_a() -> None:
    """A literal ``False`` in ``b`` falls back to ``a`` (unlike merge_deep_union_arrays)."""
    assert json_merge.merge_concat_arrays({"a": True}, {"a": False}) == {"a": True}


def test_merge_concat_arrays_key_only_in_a() -> None:
    """A key present only in ``a`` is preserved."""
    assert json_merge.merge_concat_arrays({"a": 1}, {}) == {"a": 1}


def test_merge_concat_arrays_key_only_in_b() -> None:
    """A key present only in ``b`` is added."""
    assert json_merge.merge_concat_arrays({}, {"a": 1}) == {"a": 1}


def test_merge_concat_arrays_empty_both() -> None:
    """Merging two empty objects returns an empty object."""
    assert json_merge.merge_concat_arrays({}, {}) == {}


def test_merge_concat_arrays_type_mismatch_array_vs_scalar() -> None:
    """A type mismatch (array vs scalar) falls through to ``b`` wins."""
    assert json_merge.merge_concat_arrays({"a": [1, 2]}, {"a": "x"}) == {"a": "x"}


def test_merge_concat_arrays_does_not_mutate_inputs() -> None:
    """Neither input dict is mutated."""
    a = {"tags": ["v1"]}
    b = {"tags": ["v2"]}
    json_merge.merge_concat_arrays(a, b)
    assert a == {"tags": ["v1"]}
    assert b == {"tags": ["v2"]}
