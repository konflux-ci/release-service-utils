#!/usr/bin/env python3
"""
Transform advisory data from input data JSON to output JSON format.
This script performs the following tasks:
1. Accepts advisory data as a JSON string or from a JSON file.
2. Optionally accepts a JSON schema for validation.
3. Builds an advisory JSON structure.
4. Renders Jinja templates in the advisory spec fields.
5. Wraps long text fields for better YAML conversion.
6. Validates the final advisory against the provided schema if passed.
7. Writes the final advisory JSON to the specified output file.
"""

import argparse
import json
import logging
import sys
import textwrap
from typing import Any

from jinja2 import StrictUndefined, exceptions
from jinja2.nativetypes import NativeEnvironment
from jsonschema import Draft7Validator

LOGGER = logging.getLogger("apply_advisory")
# Maximum line width for text wrapping
TEXT_WRAP_WIDTH = 120
# Fields that should have their text wrapped for better readability
MULTILINE_FIELDS = {"synopsis", "topic", "description", "solution"}
# Jinja2 environment for template rendering
ENV = NativeEnvironment(undefined=StrictUndefined, trim_blocks=True, lstrip_blocks=True)


def setup_argparser() -> argparse.Namespace:
    """Set up command line argument parser.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description="Transform advisory data to JSON format.")

    data_group = parser.add_mutually_exclusive_group(required=True)
    data_group.add_argument("--data", help="JSON string containing advisory data")
    data_group.add_argument("--data-file", help="Path to JSON file containing advisory data")

    schema_group = parser.add_mutually_exclusive_group(required=False)
    schema_group.add_argument("--schema", help="JSON schema string for validation")
    schema_group.add_argument("--schema-file", help="Path to JSON schema file for validation")

    parser.add_argument("-o", "--output", required=True, help="Output filename")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()


def setup_logger(level: int = logging.INFO, log_format: Any = None):
    """Set up and configure logger with stdout and stderr handlers.

    Args:
        level: Logging level for stdout handler
        log_format: Custom log format string
    """
    if log_format is None:
        log_format = "%(asctime)s [%(name)s] %(levelname)s %(message)s"

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    formatter = logging.Formatter(log_format)

    # Add stdout and stderr handlers for Tekton result readability
    for stream, handler_level in [(sys.stdout, level), (sys.stderr, logging.ERROR)]:
        handler = logging.StreamHandler(stream)
        handler.setLevel(handler_level)
        handler.setFormatter(formatter)
        root.addHandler(handler)


def load_data(arg: argparse.Namespace) -> tuple[dict, dict]:
    """Load input data and schema from command line arguments.

    Args:
        arg: Parsed command line arguments

    :return: Tuple containing data dictionary and schema dictionary or none if not provided
    """
    # Load required data
    if arg.data:
        data = json.loads(arg.data)
    elif arg.data_file:
        with open(arg.data_file, encoding="utf-8") as f:
            data = json.load(f)
    else:
        raise ValueError("No advisory data provided")
    if not isinstance(data, dict):
        raise ValueError("Advisory data must be a JSON object")
    # Load schema if provided
    if arg.schema:
        schema = json.loads(arg.schema)
    elif arg.schema_file:
        with open(arg.schema_file, encoding="utf-8") as f:
            schema = json.load(f)
    else:
        LOGGER.info("No schema provided for validation")
        return data, None

    if not isinstance(schema, dict):
        raise ValueError("Schema must be a JSON object")
    return data, schema


def build_advisory(data: dict) -> dict:
    """Build advisory JSON structure from JSON data.

    Args:
        data: Input data dictionary

    :return: Advisory dictionary
    """
    if not isinstance(data, dict):
        raise TypeError("Input data must be a dictionary")
    if "advisory" not in data:
        raise KeyError("Missing advisory key in input data")
    if "spec" not in data["advisory"]:
        raise KeyError("Missing advisory.spec key in input data")
    spec = data["advisory"]["spec"]
    advisory = {
        "apiVersion": "rhtap.redhat.com/v1alpha1",
        "kind": "Advisory",
        "metadata": {
            "name": data.get("advisory_name"),
            "ship_date": data.get("advisory_ship_date"),
        },
        "spec": {
            "product_id": spec.get("product_id"),
            "product_name": spec.get("product_name"),
            "product_version": spec.get("product_version"),
            "product_stream": spec.get("product_stream"),
            "cpe": spec.get("cpe"),
            "type": spec.get("type"),
            "skip_customer_notifications": spec.get("skip_customer_notifications", False),
            "content": spec.get("content"),
            "synopsis": spec.get("synopsis"),
            "topic": spec.get("topic"),
            "description": spec.get("description"),
            "solution": spec.get("solution"),
            "references": spec.get("references"),
        },
    }
    if "severity" in spec:
        advisory["spec"]["severity"] = spec["severity"]
    if "issues" in spec and isinstance(spec["issues"], dict) and "fixed" in spec["issues"]:
        advisory["spec"]["issues"] = {
            "fixed": [
                {
                    "id": issue.get("id"),
                    "source": issue.get("source"),
                    **({"public": issue["public"]} if "public" in issue else {}),
                }
                for issue in spec["issues"]["fixed"]
                if isinstance(issue, dict)
            ]
        }
    return advisory


def has_jinja_syntax(value: Any) -> bool:
    """Check if value is a string that contains Jinja syntax.

    Args:
        value: Value to check

    :return: True if string contains {{, {%, or {# Jinja syntax false otherwise
    """
    return isinstance(value, str) and any(s in value for s in ["{{", "{%", "{#"])


def process_jinja_in_values(
    value: Any, env: NativeEnvironment, context: dict, path: str = ""
) -> Any:
    """Recursively render Jinja syntax found in string values.

    Args:
        value: Value to process
        env: Jinja environment
        context: Template rendering context
        path: Current path in the data structure used for error messages

    :return: Processed value with Jinja templates rendered
    """

    # Render strings that contain Jinja syntax
    if isinstance(value, str) and has_jinja_syntax(value):
        try:
            rendered = env.from_string(value).render(context)
            return rendered.strip()
        except exceptions.TemplateError as e:
            raise RuntimeError(f"Jinja rendering error at {path or 'root'}: {e}") from e

    # Handle nested dictionaries
    if isinstance(value, dict):
        out = {}
        for key, child in value.items():
            key_str = str(key)
            child_path = f"{path}.{key_str}" if path else key_str
            out[key] = process_jinja_in_values(child, env, context, child_path)
        return out

    # Handle nested lists
    if isinstance(value, list):
        out_list = []
        for index, item in enumerate(value):
            child_path = f"{path}[{index}]"
            out_list.append(process_jinja_in_values(item, env, context, child_path))
        return out_list

    # Return original value
    return value


def wrap_value(value: Any, width: int = TEXT_WRAP_WIDTH):
    """Wrap string for prettier output.

    Args:
        value: Input string to wrap or other type
        width: Maximum line width

    :return: Wrapped string or original value if not a string
    """
    if not isinstance(value, str):
        return value

    wrapper = textwrap.TextWrapper(width=width, break_long_words=False, break_on_hyphens=False)
    # Preserve string with existing newlines
    if "\n" in value:
        lines = value.split("\n")
        wrapped = []
        for line in lines:
            if line.strip():
                wrapped.extend(wrapper.wrap(line))
            else:
                wrapped.append("")
        return "\n".join(wrapped)
    # Wrap string if too long
    if len(value) > width:
        return wrapper.fill(value)
    return value


def wrap_multiline_strings(spec: dict):
    """Wrap long text in designated multiline fields for better readability.

    Args:
        spec: Advisory spec dictionary
    """
    if not isinstance(spec, dict):
        return
    for field in MULTILINE_FIELDS:
        if field in spec:
            spec[field] = wrap_value(spec[field])


def validate_against_schema(advisory: dict, schema: dict):
    """Validate advisory against JSON schema.

    Args:
        advisory: Advisory dictionary to validate
        schema: JSON schema dictionary
    """
    Draft7Validator.check_schema(schema)
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(advisory), key=lambda e: e.path)
    if errors:
        error_messages = []
        for error in errors:
            loc = "/".join(map(str, error.path)) or "root"
            error_messages.append(f"{loc}: {error.message}")
        raise ValueError(
            f"Schema validation failed with {len(errors)} error(s):\n"
            + "\n".join(error_messages)
        )
    LOGGER.info("Schema validation passed")


def main():
    """Main entry point."""
    args = setup_argparser()
    setup_logger(level=logging.DEBUG if args.verbose else logging.INFO)
    try:
        data, schema = load_data(args)
        LOGGER.info("Building advisory JSON")
        advisory = build_advisory(data)
        LOGGER.debug("Built advisory JSON: \n%s", json.dumps(advisory, indent=2))
        LOGGER.info("Rendering Jinja templates")
        context = {"advisory": advisory}
        advisory["spec"] = process_jinja_in_values(advisory["spec"], ENV, context)
        LOGGER.debug(
            "Rendered Jinja advisory spec: \n%s", json.dumps(advisory["spec"], indent=2)
        )
        LOGGER.info("Wrapping multiline strings")
        wrap_multiline_strings(advisory["spec"])
        LOGGER.debug("Wrapped Advisory spec: \n%s", json.dumps(advisory["spec"], indent=2))
        if schema is not None:
            LOGGER.info("Validating against schema")
            validate_against_schema(advisory, schema)
        with open(args.output, "w", encoding="utf-8") as output_file:
            json.dump(advisory, output_file, indent=2)
        LOGGER.info("Successfully wrote Advisory JSON to: %s", args.output)
    except Exception:
        LOGGER.exception("Advisory generation failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
