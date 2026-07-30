"""Tests for collect_task_params."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from release_service_utils.tasks.managed.collect_task_params import (
    collect_task_params,
    extract_value_from_data,
    main,
    parse_jq_key_path,
    parse_keys_to_extract,
    run_collect_task_params,
    validate_key_spec,
)


def _write_json(path: Path, data: dict) -> None:
    """Write a dict as JSON to path, creating parent dirs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _sample_data() -> dict:
    """Return a sample data dict with nested structures."""
    return {
        "releaseNotes": {
            "summary": "Test release summary",
            "product_name": "Test Product",
        },
        "foo": "bar",
        "nested": {
            "deep": {
                "value": "deeply-nested-value",
            },
        },
        "items": ["first", "second", "third"],
        "numeric": 42,
        "boolean": True,
    }


# -- parse_keys_to_extract --


def test_parse_keys_to_extract_valid_array() -> None:
    """Parse a valid JSON array."""
    keys_json = '[{"resultIndex": 0, "key": ".foo"}]'
    result = parse_keys_to_extract(keys_json)
    assert result == [{"resultIndex": 0, "key": ".foo"}]


def test_parse_keys_to_extract_multiple_items() -> None:
    """Parse a JSON array with multiple items."""
    keys_json = '[{"resultIndex": 0, "key": ".foo"}, {"resultIndex": 1, "key": ".bar"}]'
    result = parse_keys_to_extract(keys_json)
    assert len(result) == 2


def test_parse_keys_to_extract_invalid_json() -> None:
    """Raise JSONDecodeError for invalid JSON."""
    with pytest.raises(json.JSONDecodeError):
        parse_keys_to_extract("not valid json")


def test_parse_keys_to_extract_not_array() -> None:
    """Raise ValueError when JSON is not an array."""
    with pytest.raises(ValueError, match="must be a valid JSON array"):
        parse_keys_to_extract('{"resultIndex": 0, "key": ".foo"}')


def test_parse_keys_to_extract_empty_array() -> None:
    """Raise ValueError when array is empty."""
    with pytest.raises(ValueError, match="array is empty"):
        parse_keys_to_extract("[]")


# -- validate_key_spec --


def test_validate_key_spec_valid() -> None:
    """Return result_index and key for valid spec."""
    spec = {"resultIndex": 0, "key": ".foo"}
    result_index, key = validate_key_spec(spec, 0, 1)
    assert result_index == 0
    assert key == ".foo"


def test_validate_key_spec_missing_result_index() -> None:
    """Raise ValueError when resultIndex is missing."""
    spec = {"key": ".foo"}
    with pytest.raises(ValueError, match="missing resultIndex or key"):
        validate_key_spec(spec, 0, 1)


def test_validate_key_spec_missing_key() -> None:
    """Raise ValueError when key is missing."""
    spec = {"resultIndex": 0}
    with pytest.raises(ValueError, match="missing resultIndex or key"):
        validate_key_spec(spec, 0, 1)


def test_validate_key_spec_negative_result_index() -> None:
    """Raise ValueError for negative resultIndex."""
    spec = {"resultIndex": -1, "key": ".foo"}
    with pytest.raises(ValueError, match="must be a non-negative integer"):
        validate_key_spec(spec, 0, 1)


def test_validate_key_spec_non_integer_result_index() -> None:
    """Raise ValueError for non-integer resultIndex."""
    spec = {"resultIndex": "zero", "key": ".foo"}
    with pytest.raises(ValueError, match="must be a non-negative integer"):
        validate_key_spec(spec, 0, 1)


def test_validate_key_spec_out_of_bounds() -> None:
    """Raise ValueError when resultIndex exceeds key count."""
    spec = {"resultIndex": 5, "key": ".foo"}
    with pytest.raises(ValueError, match="out of bounds"):
        validate_key_spec(spec, 0, 3)


# -- parse_jq_key_path --


def test_parse_jq_key_path_simple() -> None:
    """Parse a simple single-segment key."""
    assert parse_jq_key_path(".foo") == ["foo"]


def test_parse_jq_key_path_nested() -> None:
    """Parse a nested key path."""
    assert parse_jq_key_path(".foo.bar") == ["foo", "bar"]


def test_parse_jq_key_path_deeply_nested() -> None:
    """Parse a deeply nested key path."""
    assert parse_jq_key_path(".a.b.c.d") == ["a", "b", "c", "d"]


def test_parse_jq_key_path_with_brackets() -> None:
    """Parse a key with bracket notation."""
    assert parse_jq_key_path('.foo["bar"]') == ["foo", "bar"]


def test_parse_jq_key_path_with_hyphen() -> None:
    """Parse a key containing hyphens."""
    assert parse_jq_key_path(".release-notes.summary") == ["release-notes", "summary"]


def test_parse_jq_key_path_without_leading_dot() -> None:
    """Return key as-is when no leading dot."""
    assert parse_jq_key_path("foo") == ["foo"]


def test_parse_jq_key_path_empty_after_dot() -> None:
    """Return empty list for just a dot."""
    assert parse_jq_key_path(".") == []


# -- extract_value_from_data --


def test_extract_value_simple_key() -> None:
    """Extract a top-level value."""
    data = {"foo": "bar"}
    assert extract_value_from_data(data, ".foo") == "bar"


def test_extract_value_nested_key() -> None:
    """Extract a nested value."""
    data = {"nested": {"deep": {"value": "found"}}}
    assert extract_value_from_data(data, ".nested.deep.value") == "found"


def test_extract_value_missing_key() -> None:
    """Return None for missing key."""
    data = {"foo": "bar"}
    assert extract_value_from_data(data, ".missing") is None


def test_extract_value_missing_nested_key() -> None:
    """Return None for missing nested key."""
    data = {"foo": {"bar": "baz"}}
    assert extract_value_from_data(data, ".foo.missing") is None


def test_extract_value_numeric() -> None:
    """Extract a numeric value."""
    data = {"count": 42}
    assert extract_value_from_data(data, ".count") == 42


def test_extract_value_boolean() -> None:
    """Extract a boolean value."""
    data = {"enabled": True}
    assert extract_value_from_data(data, ".enabled") is True


def test_extract_value_dict() -> None:
    """Extract a dict value."""
    data = {"config": {"key": "value"}}
    assert extract_value_from_data(data, ".config") == {"key": "value"}


def test_extract_value_list() -> None:
    """Extract a list value."""
    data = {"items": ["a", "b", "c"]}
    assert extract_value_from_data(data, ".items") == ["a", "b", "c"]


def test_extract_value_from_list_index() -> None:
    """Extract a value from a list by index."""
    data = {"items": ["first", "second", "third"]}
    assert extract_value_from_data(data, ".items[1]") == "second"


# -- collect_task_params --


def test_collect_task_params_single_key(tmp_path: Path) -> None:
    """Extract a single value."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"foo": "bar"})

    keys_json = '[{"resultIndex": 0, "key": ".foo"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == ["bar"]


def test_collect_task_params_multiple_keys(tmp_path: Path) -> None:
    """Extract multiple values with different result indices."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, _sample_data())

    keys_json = """[
        {"resultIndex": 1, "key": ".releaseNotes.summary"},
        {"resultIndex": 0, "key": ".foo"}
    ]"""
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result[0] == "bar"
    assert result[1] == "Test release summary"


def test_collect_task_params_with_default(tmp_path: Path) -> None:
    """Use default value when key is missing."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"foo": "bar"})

    keys_json = '[{"resultIndex": 0, "key": ".missing", "default": "fallback"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == ["fallback"]


def test_collect_task_params_missing_key_no_default(tmp_path: Path) -> None:
    """Raise KeyError when key is missing and no default."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"foo": "bar"})

    keys_json = '[{"resultIndex": 0, "key": ".missing"}]'
    with pytest.raises(KeyError, match="not found in data file"):
        collect_task_params(data_file=data_file, keys_json=keys_json)


def test_collect_task_params_nested_key(tmp_path: Path) -> None:
    """Extract a deeply nested value."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, _sample_data())

    keys_json = '[{"resultIndex": 0, "key": ".nested.deep.value"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == ["deeply-nested-value"]


def test_collect_task_params_numeric_value(tmp_path: Path) -> None:
    """Extract and stringify a numeric value."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, _sample_data())

    keys_json = '[{"resultIndex": 0, "key": ".numeric"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == ["42"]


def test_collect_task_params_boolean_value(tmp_path: Path) -> None:
    """Extract and stringify a boolean value."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, _sample_data())

    keys_json = '[{"resultIndex": 0, "key": ".boolean"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == ["True"]


def test_collect_task_params_dict_value(tmp_path: Path) -> None:
    """Extract and JSON-stringify a dict value."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, _sample_data())

    keys_json = '[{"resultIndex": 0, "key": ".releaseNotes"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    parsed = json.loads(result[0])
    assert parsed["summary"] == "Test release summary"


def test_collect_task_params_list_value(tmp_path: Path) -> None:
    """Extract and JSON-stringify a list value."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, _sample_data())

    keys_json = '[{"resultIndex": 0, "key": ".items"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    parsed = json.loads(result[0])
    assert parsed == ["first", "second", "third"]


def test_collect_task_params_missing_data_file(tmp_path: Path) -> None:
    """Raise FileNotFoundError when data file is missing."""
    keys_json = '[{"resultIndex": 0, "key": ".foo"}]'
    with pytest.raises(FileNotFoundError):
        collect_task_params(data_file=tmp_path / "missing.json", keys_json=keys_json)


# -- run_collect_task_params --


def test_run_collect_task_params_writes_result(tmp_path: Path) -> None:
    """Write JSON array to result file."""
    data_file = tmp_path / "data.json"
    result_file = tmp_path / "result"
    _write_json(data_file, {"foo": "bar", "baz": "qux"})

    keys_json = """[
        {"resultIndex": 0, "key": ".foo"},
        {"resultIndex": 1, "key": ".baz"}
    ]"""

    rc = run_collect_task_params(
        data_file=data_file,
        keys_json=keys_json,
        result_path=result_file,
    )

    assert rc == 0
    result = json.loads(result_file.read_text())
    assert result == ["bar", "qux"]


def test_run_collect_task_params_out_of_order_indices(tmp_path: Path) -> None:
    """Handle resultIndex values that are out of input order."""
    data_file = tmp_path / "data.json"
    result_file = tmp_path / "result"
    _write_json(data_file, {"a": "alpha", "b": "beta", "c": "gamma"})

    keys_json = """[
        {"resultIndex": 2, "key": ".a"},
        {"resultIndex": 0, "key": ".b"},
        {"resultIndex": 1, "key": ".c"}
    ]"""

    rc = run_collect_task_params(
        data_file=data_file,
        keys_json=keys_json,
        result_path=result_file,
    )

    assert rc == 0
    result = json.loads(result_file.read_text())
    assert result == ["beta", "gamma", "alpha"]


# -- main --


def test_main_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return 0 and write result file when fully configured."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_json(data_dir / "data.json", {"foo": "bar"})

    result_file = tmp_path / "result"

    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.setenv("KEYS_JSON", '[{"resultIndex": 0, "key": ".foo"}]')
    monkeypatch.setenv("RESULT_EXTRACTED_VALUES", str(result_file))

    rc = main()

    assert rc == 0
    result = json.loads(result_file.read_text())
    assert result == ["bar"]


def test_main_multiple_extractions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Extract multiple values through main()."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_json(
        data_dir / "data.json",
        {
            "releaseNotes": {"summary": "Test summary"},
            "version": "1.2.3",
        },
    )

    result_file = tmp_path / "result"

    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.setenv(
        "KEYS_JSON",
        '[{"resultIndex": 0, "key": ".releaseNotes.summary"}, '
        '{"resultIndex": 1, "key": ".version"}]',
    )
    monkeypatch.setenv("RESULT_EXTRACTED_VALUES", str(result_file))

    rc = main()

    assert rc == 0
    result = json.loads(result_file.read_text())
    assert result == ["Test summary", "1.2.3"]


def test_main_missing_data_dir_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when DATA_DIR is missing."""
    monkeypatch.delenv("DATA_DIR", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_missing_data_path_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when DATA_PATH is missing."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("DATA_PATH", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_missing_keys_json_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when KEYS_JSON is missing."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.delenv("KEYS_JSON", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_missing_result_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when RESULT_EXTRACTED_VALUES is missing."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.setenv("KEYS_JSON", '[{"resultIndex": 0, "key": ".foo"}]')
    monkeypatch.delenv("RESULT_EXTRACTED_VALUES", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_with_default_value(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Use default value through main() when key is missing."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_json(data_dir / "data.json", {"existing": "value"})

    result_file = tmp_path / "result"

    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.setenv(
        "KEYS_JSON",
        '[{"resultIndex": 0, "key": ".missing", "default": "default_value"}]',
    )
    monkeypatch.setenv("RESULT_EXTRACTED_VALUES", str(result_file))

    rc = main()

    assert rc == 0
    result = json.loads(result_file.read_text())
    assert result == ["default_value"]


# -- Catalog integration tests --
# These tests mirror the Tekton pipeline tests from release-service-catalog


def test_catalog_happy_path(tmp_path: Path) -> None:
    """Mirror test-collect-task-params.yaml from catalog.

    Test that collect-task-params correctly extracts keys with and without defaults.
    """
    data_file = tmp_path / "data.json"
    result_file = tmp_path / "result"
    _write_json(
        data_file,
        {
            "foo": {"bar": "nested test value"},
            "arr": ["str"],
            "simpleValue": "hello world",
        },
    )

    keys_json = """[
        {"resultIndex": 0, "key": ".arr[0]"},
        {"resultIndex": 1, "key": ".foo.bar"},
        {"resultIndex": 2, "key": ".simpleValue"},
        {"resultIndex": 3, "key": ".missingKey", "default": "default_value"},
        {"resultIndex": 4, "key": ".numberValue", "default": "42"},
        {"resultIndex": 5, "key": ".missingemptydefault", "default": ""}
    ]"""

    rc = run_collect_task_params(
        data_file=data_file,
        keys_json=keys_json,
        result_path=result_file,
    )

    assert rc == 0
    result = json.loads(result_file.read_text())

    assert result[0] == "str"
    assert result[1] == "nested test value"
    assert result[2] == "hello world"
    assert result[3] == "default_value"
    assert result[4] == "42"
    assert result[5] == ""


def test_catalog_fail_empty_keys(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-empty-keys.yaml from catalog.

    Test that collect-task-params fails when keysToExtract is empty.
    """
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"foo": {"bar": "nested test value"}})

    with pytest.raises(ValueError, match="array is empty"):
        collect_task_params(data_file=data_file, keys_json="[]")


def test_catalog_fail_invalid_index(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-invalid-index.yaml from catalog.

    Test that collect-task-params fails when resultIndex is out of bounds.
    """
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"foo": {"bar": "nested test value"}, "simpleValue": "hello"})

    keys_json = """[
        {"resultIndex": 0, "key": ".foo.bar"},
        {"resultIndex": 9, "key": ".simpleValue"}
    ]"""

    with pytest.raises(ValueError, match="out of bounds"):
        collect_task_params(data_file=data_file, keys_json=keys_json)


def test_catalog_fail_missing_resultindex(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-missing-resultindex.yaml from catalog.

    Test that collect-task-params fails when resultIndex or key is missing.
    """
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"foo": {"bar": "nested test value"}})

    keys_json = """[
        {"key": ".foo.bar"},
        {"resultIndex": 2}
    ]"""

    with pytest.raises(ValueError, match="missing resultIndex or key"):
        collect_task_params(data_file=data_file, keys_json=keys_json)


def test_catalog_fail_no_data(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-no-data.yaml from catalog.

    Test that collect-task-params fails when no data file is provided.
    """
    keys_json = '[{"resultIndex": 0, "key": ".foo.bar"}]'

    with pytest.raises(FileNotFoundError):
        collect_task_params(data_file=tmp_path / "nonexistent.json", keys_json=keys_json)


def test_catalog_fail_no_default(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-no-default.yaml from catalog.

    Test that collect-task-params fails when key is not in data file and no default.
    """
    data_file = tmp_path / "data.json"
    _write_json(
        data_file,
        {"foo": {"bar": "nested test value"}, "simpleValue": "hello world"},
    )

    keys_json = """[
        {"resultIndex": 0, "key": ".foo.bar"},
        {"resultIndex": 1, "key": ".simpleValue"},
        {"resultIndex": 2, "key": ".test"}
    ]"""

    with pytest.raises(KeyError, match="not found in data file"):
        collect_task_params(data_file=data_file, keys_json=keys_json)


def test_catalog_fail_non_integer_index(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-non-integer-index.yaml from catalog.

    Test that collect-task-params fails when resultIndex is not an integer.
    """
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"booleanValue": True, "foo": {"bar": "nested"}})

    keys_json = """[
        {"resultIndex": "abc", "key": ".booleanValue"},
        {"resultIndex": 1, "key": ".foo.bar"},
        {"resultIndex": 2, "key": ".simpleValue"}
    ]"""

    with pytest.raises(ValueError, match="must be a non-negative integer"):
        collect_task_params(data_file=data_file, keys_json=keys_json)


def test_catalog_fail_not_array(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-not-array.yaml from catalog.

    Test that collect-task-params fails when keysToExtract is not an array.
    """
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"simpleValue": "hello world"})

    keys_json = '{"resultIndex": 0, "key": ".simpleValue"}'

    with pytest.raises(ValueError, match="must be a valid JSON array"):
        collect_task_params(data_file=data_file, keys_json=keys_json)


def test_catalog_fail_not_json(tmp_path: Path) -> None:
    """Mirror test-collect-task-params-fail-not-json.yaml from catalog.

    Test that collect-task-params fails when keysToExtract is not valid JSON.
    """
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"simpleValue": "hello world"})

    keys_json = "invalid json string"

    with pytest.raises(json.JSONDecodeError):
        collect_task_params(data_file=data_file, keys_json=keys_json)


def test_collect_task_params_empty_default(tmp_path: Path) -> None:
    """Use empty string as default value when key is missing."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"foo": "bar"})

    keys_json = '[{"resultIndex": 0, "key": ".missing", "default": ""}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == [""]


def test_collect_task_params_array_first_element(tmp_path: Path) -> None:
    """Extract first element from array using bracket notation."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"arr": ["first", "second", "third"]})

    keys_json = '[{"resultIndex": 0, "key": ".arr[0]"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == ["first"]


def test_parse_jq_key_path_array_index() -> None:
    """Parse key with array index notation."""
    assert parse_jq_key_path(".arr[0]") == ["arr", "0"]
    assert parse_jq_key_path(".items[2]") == ["items", "2"]


def test_extract_value_invalid_list_index() -> None:
    """Return None for out-of-bounds list index."""
    data = {"items": ["a", "b"]}
    assert extract_value_from_data(data, ".items[99]") is None


def test_extract_value_non_numeric_list_index() -> None:
    """Return None for non-numeric list index."""
    data = {"items": ["a", "b"]}
    assert extract_value_from_data(data, ".items[abc]") is None


def test_collect_task_params_invalid_array_index_with_default(tmp_path: Path) -> None:
    """Use default value when array index is out of bounds."""
    data_file = tmp_path / "data.json"
    _write_json(data_file, {"arr": ["first", "second"]})

    keys_json = '[{"resultIndex": 0, "key": ".arr[99]", "default": "not_found"}]'
    result = collect_task_params(data_file=data_file, keys_json=keys_json)

    assert result == ["not_found"]
