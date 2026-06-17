#!/usr/bin/env python3
"""Validate release data JSON keys against the dataKeys schema."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import file
import tekton
from check_jsonschema.checker import SchemaChecker
from check_jsonschema.formats import FormatOptions
from check_jsonschema.instance_loader import CustomLazyFile, InstanceLoader
from check_jsonschema.regex_variants import RegexImplementation, RegexVariantName
from check_jsonschema.reporter import TextReporter
from check_jsonschema.schema_loader import SchemaLoader
from logger import logger

DEFAULT_SCHEMA_PATH = Path("/home/schemas/dataKeys.json")


def resolve_schema_path(schema_path: Path) -> Path:
    """Return *schema_path* when the file exists."""
    if not schema_path.is_file():
        msg = f"schema file not found: {schema_path}"
        raise FileNotFoundError(msg)
    return schema_path


def parse_systems_param(systems_json: str) -> list[dict[str, Any]]:
    """Parse the Tekton `systems` param as a JSON array of system objects."""
    text = systems_json.strip()
    if not text:
        return []
    data = json.loads(text)
    if not isinstance(data, list):
        msg = "systems parameter must be a JSON array"
        raise ValueError(msg)
    return data


def merge_systems_into_data(
    data: dict[str, Any],
    systems: list[dict[str, Any]],
) -> dict[str, Any]:
    """Append *systems* to the `systems` array in *data*."""
    existing = data.get("systems")
    if existing is None:
        merged: list[dict[str, Any]] = []
    elif isinstance(existing, list):
        merged = list(existing)
    else:
        msg = "data systems must be a JSON array"
        raise ValueError(msg)
    merged.extend(systems)
    data["systems"] = merged
    return data


def validate_data_against_schema(schema_path: Path, data_path: Path) -> None:
    """Validate *data_path* against *schema_path* using check-jsonschema."""
    regex_impl = RegexImplementation(RegexVariantName.default)
    checker = SchemaChecker(
        SchemaLoader(str(schema_path)),
        InstanceLoader([CustomLazyFile(str(data_path))]),
        TextReporter(verbosity=1, stream=sys.stderr),
        format_opts=FormatOptions(regex_impl=regex_impl),
        regex_impl=regex_impl,
    )
    if checker.run() != 0:
        msg = f"schema validation failed for {data_path}"
        raise ValueError(msg)


def run_check_data_keys(
    *,
    data_dir: Path,
    data_path: Path,
    schema_path: Path,
    systems_json: str,
) -> None:
    """Load data, merge systems, and validate against the schema."""
    data_file = data_dir / data_path
    if not data_file.is_file():
        msg = "No data JSON was provided."
        raise FileNotFoundError(msg)

    logger.info("Loading data from %s", data_file)
    data = file.load_json_dict(data_file)
    systems = parse_systems_param(systems_json)
    if systems:
        logger.info("Merging %d required system(s) into data", len(systems))
    data = merge_systems_into_data(data, systems)
    data_file.write_text(json.dumps(data) + "\n", encoding="utf-8")

    schema = resolve_schema_path(schema_path)
    logger.info("Validating %s against schema %s", data_file, schema)
    validate_data_against_schema(schema, data_file)
    logger.info("Schema validation succeeded")


def main() -> int:
    """Run the check-data-keys workflow; exit non-zero on failure."""
    run_check_data_keys(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        data_path=Path(tekton.require_env("PARAM_DATA_PATH")),
        schema_path=file.path_from_env_variable("SCHEMA_FILE", DEFAULT_SCHEMA_PATH),
        systems_json=os.environ.get("PARAM_SYSTEMS", ""),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
