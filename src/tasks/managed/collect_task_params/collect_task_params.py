#!/usr/bin/env python3
"""Extract values from a data JSON file based on specified keys and result indices.

This script takes an array of resultIndex/key pairs and extracts the corresponding
values from a JSON data file. Each extracted value is placed at the specified
resultIndex in the output array for use by downstream Tekton tasks. Optional default
values can be provided for keys that may not exist in the data file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from release_service_utils.helpers import tekton
from release_service_utils.helpers.file import load_json_dict
from release_service_utils.helpers.logger import logger


def parse_keys_to_extract(keys_json: str) -> list[dict[str, Any]]:
    """Parse and validate the keysToExtract JSON string.

    The input must be a non-empty JSON array of objects. Each object should have
    'resultIndex' and 'key' fields, with an optional 'default' field.
    """
    parsed = json.loads(keys_json)

    if not isinstance(parsed, list):
        raise ValueError(
            f"keysToExtract must be a valid JSON array. Received value: {keys_json}"
        )

    if len(parsed) == 0:
        raise ValueError("keysToExtract array is empty")

    return parsed


def validate_key_spec(spec: dict[str, Any], index: int, key_count: int) -> tuple[int, str]:
    """Validate a single key extraction specification.

    Return the validated (result_index, key) tuple, or raise ValueError.
    """
    result_index = spec.get("resultIndex")
    key = spec.get("key")

    if result_index is None or key is None:
        raise ValueError(
            f"Invalid key extraction specification at index {index}: "
            "missing resultIndex or key"
        )

    if not isinstance(result_index, int) or result_index < 0:
        raise ValueError(
            f"resultIndex at position {index} must be a non-negative integer, "
            f"got: {result_index}"
        )

    if result_index >= key_count:
        raise ValueError(
            f"resultIndex {result_index} at position {index} is out of bounds. "
            f"Valid range is 0 to {key_count - 1}"
        )

    return result_index, str(key)


def parse_jq_key_path(key: str) -> list[str]:
    """Parse a jq-style key path into segments for traversing a JSON structure.

    Keys in collect-task-params use jq dot notation: a leading '.', dot-separated
    object keys, bracket notation for array indices (``.arr[0]``), and quoted
    bracket keys for special characters (``.foo["bar-baz"]``).

    Return a list of segments used to walk a parsed JSON dict/list.

    Examples::

        ".foo.bar"               -> ["foo", "bar"]
        ".release-notes.summary" -> ["release-notes", "summary"]
        '.foo["bar"]'            -> ["foo", "bar"]
        ".arr[0]"                -> ["arr", "0"]
        "plainKey"               -> ["plainKey"]
        "."                      -> []
    """
    if not key.startswith("."):
        return [key]

    path = key[1:]
    if not path:
        return []

    segments: list[str] = []
    current = ""
    in_bracket = False
    in_quote = False

    i = 0
    while i < len(path):
        char = path[i]
        if char == "[" and not in_quote:
            if current:
                segments.append(current)
                current = ""
            in_bracket = True
        elif char == "]" and in_bracket and not in_quote:
            if current:
                segments.append(current)
                current = ""
            in_bracket = False
        elif char == '"' and in_bracket:
            in_quote = not in_quote
        elif char == "." and not in_bracket and not in_quote:
            if current:
                segments.append(current)
                current = ""
        else:
            current += char
        i += 1

    if current:
        segments.append(current)

    return segments


def extract_value_from_data(data: dict[str, Any], key: str) -> Any:
    """Extract a value from the data dict using a jq-style key path.

    Return None if the key path does not exist in the data.
    """
    segments = parse_jq_key_path(key)

    current: Any = data
    for segment in segments:
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        elif isinstance(current, list):
            try:
                idx = int(segment)
                current = current[idx]
            except (ValueError, IndexError):
                return None
        else:
            return None

    return current


def collect_task_params(
    *,
    data_file: Path,
    keys_json: str,
) -> list[str]:
    """Extract values from the data file based on the keys specification.

    Return a list of extracted string values positioned according to resultIndex.
    """
    keys_to_extract = parse_keys_to_extract(keys_json)
    key_count = len(keys_to_extract)

    logger.info("Extracting %d value(s) from %s", key_count, data_file)

    data = load_json_dict(data_file)

    result_array: list[str] = [""] * key_count

    for i, spec in enumerate(keys_to_extract):
        result_index, key = validate_key_spec(spec, i, key_count)
        default_value = spec.get("default")

        value = extract_value_from_data(data, key)

        if value is None:
            if default_value is not None:
                logger.info(
                    "Key %s not found in data file, using default value: %s",
                    key,
                    default_value,
                )
                value = default_value
            else:
                raise KeyError(
                    f"Key {key} not found in data file and no default value was provided"
                )

        if isinstance(value, (dict, list)):
            result_array[result_index] = json.dumps(value)
        else:
            result_array[result_index] = str(value)

    return result_array


def run_collect_task_params(
    *,
    data_file: Path,
    keys_json: str,
    result_path: Path,
) -> int:
    """Run the collect-task-params workflow and write the result file."""
    result_array = collect_task_params(data_file=data_file, keys_json=keys_json)

    logger.info("Writing extracted values to %s", result_path)
    result_path.write_text(json.dumps(result_array) + "\n", encoding="utf-8")

    return 0


def main() -> int:
    """Read environment variables, extract values, and write results."""
    data_dir = Path(tekton.require_env("DATA_DIR"))
    data_path = tekton.require_env("DATA_PATH")
    keys_json = tekton.require_env("KEYS_JSON")

    (result_path,) = tekton.result_paths_from_env("RESULT_EXTRACTED_VALUES")

    return run_collect_task_params(
        data_file=data_dir / data_path,
        keys_json=keys_json,
        result_path=result_path,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
