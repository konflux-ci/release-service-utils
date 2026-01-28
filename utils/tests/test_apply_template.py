import json
from pathlib import Path
import tempfile

import pytest
from unittest.mock import MagicMock

from apply_template import (
    load_data,
    build_advisory,
    has_jinja_syntax,
    process_jinja_in_values,
    wrap_value,
    wrap_multiline_strings,
    validate_against_schema,
    TEXT_WRAP_WIDTH,
    ENV,
)


@pytest.fixture
def input_data():
    """Valid input data for building an advisory."""
    return {
        "advisory_name": "2024:1234",
        "advisory_ship_date": "2024-12-12T00:00:00Z",
        "advisory": {
            "spec": {
                "product_id": 123,
                "product_name": "Test Product",
                "product_version": "1.0",
                "product_stream": "stream1",
                "cpe": "cpe:/a:test:product",
                "type": "RHEA",
                "content": {
                    "images": [
                        {
                            "containerImage": "quay.io/example/openstack@sha256:abdeNEW",
                            "repository": "rhosp16-rhel8/openstack",
                            "tags": ["latest", "tp1"],
                            "architecture": "amd64",
                            "purl": (
                                "pkg:example/openstack@256:abcde?"
                                "repository_url=quay.io/example/rhosp16-rhel8"
                            ),
                            "cves": {
                                "fixed": {
                                    "CVE-2022-1234": {
                                        "packages": [
                                            "pkg:golang/golang.org/x/net/http2@1.11.1"
                                        ]
                                    }
                                }
                            },
                            "signingKey": "key1",
                        }
                    ]
                },
                "synopsis": "Test synopsis",
                "topic": "Test topic",
                "description": "Test description",
                "solution": "Test solution",
                "references": ["https://example.com"],
            }
        },
    }


@pytest.fixture
def advisory_schema():
    """JSON schema for advisory validation."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["apiVersion", "kind", "metadata", "spec"],
        "properties": {
            "apiVersion": {"type": "string", "const": "rhtap.redhat.com/v1alpha1"},
            "kind": {"type": "string", "const": "Advisory"},
            "metadata": {
                "type": "object",
                "required": ["name", "ship_date"],
                "properties": {
                    "name": {"type": "string"},
                    "ship_date": {"type": "string"},
                },
            },
            "spec": {
                "type": "object",
                "required": [
                    "product_id",
                    "product_name",
                    "type",
                    "synopsis",
                    "topic",
                    "description",
                    "solution",
                    "references",
                ],
            },
        },
    }


def test_load_data__from_string(input_data, advisory_schema):
    """Test loading data from JSON string arguments"""
    args = MagicMock()
    args.data = json.dumps(input_data)
    args.data_file = None
    args.schema = json.dumps(advisory_schema)
    args.schema_file = None

    data, schema = load_data(args)
    assert data == input_data
    assert schema == advisory_schema


def test_load_data__from_files(input_data, advisory_schema):
    """Test loading data from JSON files"""
    temp_dir = tempfile.TemporaryDirectory()
    data_file = Path(temp_dir.name) / "input_data.json"
    schema_file = Path(temp_dir.name) / "advisory_schema.json"
    data_file.write_text(json.dumps(input_data))
    schema_file.write_text(json.dumps(advisory_schema))

    args = MagicMock()
    args.data = None
    args.data_file = str(data_file)
    args.schema = None
    args.schema_file = str(schema_file)

    data, schema = load_data(args)
    assert data == input_data
    assert schema == advisory_schema


@pytest.mark.parametrize(
    "data_input,expected_error,match_text",
    [
        ("{invalid json", json.JSONDecodeError, None),
        (None, ValueError, "No advisory data provided"),
    ],
)
def test_load_data__errors(data_input, expected_error, match_text):
    """Test load_data raises errors for invalid input"""
    args = MagicMock()
    args.data = data_input
    args.data_file = None
    args.schema = None
    args.schema_file = None

    if match_text:
        with pytest.raises(expected_error, match=match_text):
            load_data(args)
    else:
        with pytest.raises(expected_error):
            load_data(args)


def test_build_advisory__basic(input_data):
    """Test building advisory with all required fields"""
    advisory = build_advisory(input_data)
    spec = advisory["spec"]

    assert advisory["apiVersion"] == "rhtap.redhat.com/v1alpha1"
    assert advisory["kind"] == "Advisory"
    assert advisory["metadata"]["name"] == "2024:1234"
    assert advisory["metadata"]["ship_date"] == "2024-12-12T00:00:00Z"

    assert spec["product_id"] == 123
    assert spec["product_name"] == "Test Product"
    assert spec["product_version"] == "1.0"
    assert spec["product_stream"] == "stream1"
    assert spec["cpe"] == "cpe:/a:test:product"
    assert spec["type"] == "RHEA"
    assert spec["skip_customer_notifications"] is False
    assert spec["content"] == input_data["advisory"]["spec"]["content"]
    assert spec["synopsis"] == "Test synopsis"
    assert spec["topic"] == "Test topic"
    assert spec["description"] == "Test description"
    assert spec["solution"] == "Test solution"
    assert spec["references"] == ["https://example.com"]


def test_build_advisory__with_severity(input_data):
    """Test building advisory with optional severity field"""
    input_data["advisory"]["spec"]["severity"] = "Critical"
    advisory = build_advisory(input_data)
    assert advisory["spec"]["severity"] == "Critical"


def test_build_advisory__with_issues(input_data):
    """Test building advisory with issues field"""
    input_data["advisory"]["spec"]["issues"] = {
        "fixed": [
            {"id": "BUG-123", "source": "bugzilla.redhat.com", "public": True},
            {"id": "BUG-456", "source": "bugzilla.redhat.com", "public": False},
        ]
    }
    advisory = build_advisory(input_data)
    assert len(advisory["spec"]["issues"]["fixed"]) == 2
    assert advisory["spec"]["issues"]["fixed"][0]["id"] == "BUG-123"
    assert advisory["spec"]["issues"]["fixed"][0]["public"] is True


@pytest.mark.parametrize(
    "bad_data,expected_error,match_text",
    [
        ({"some": "data"}, KeyError, "Missing advisory key"),
        ({"advisory": {}}, KeyError, "Missing advisory.spec key"),
        ("not a dict", TypeError, "Input data must be a dictionary"),
    ],
)
def test_build_advisory__errors(bad_data, expected_error, match_text):
    """Test build_advisory raises appropriate errors for invalid input"""
    with pytest.raises(expected_error, match=match_text):
        build_advisory(bad_data)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("{{ variable }}", True),
        ("{% if condition %}text{% endif %}", True),
        ("{# comment #}", True),
        ("plain text", False),
    ],
)
def test_has_jinja_syntax__detection(value, expected):
    """Test detection of Jinja2 syntax in strings"""
    assert has_jinja_syntax(value) is expected


@pytest.mark.parametrize(
    "template,context,expected",
    [
        ("Hello {{ name }}", {"name": "Test"}, "Hello Test"),
        (
            "{% if type == 'RHSA' %}Security{% else %}Other{% endif %}",
            {"type": "RHSA"},
            "Security",
        ),
        ("plain text", {}, "plain text"),
    ],
)
def test_process_jinja_in_values__strings(template, context, expected):
    """Test rendering Jinja fields in strings"""
    result = process_jinja_in_values(template, ENV, context)
    assert result == expected


def test_process_jinja_in_values__nested_dict():
    """Test rendering Jinja fields in nested dictionary"""
    context = {"product": "TestProd"}
    data = {"field1": "{{ product }}", "field2": "static"}
    result = process_jinja_in_values(data, ENV, context)
    assert result["field1"] == "TestProd"
    assert result["field2"] == "static"


def test_process_jinja_in_values__list():
    """Test rendering Jinja fields in a list"""
    context = {"item": "ListItem"}
    data = ["Item 1", "{{ item }}", "Item 3"]
    result = process_jinja_in_values(data, ENV, context)
    assert result[0] == "Item 1"
    assert result[1] == "ListItem"
    assert result[2] == "Item 3"


@pytest.mark.parametrize(
    "template,context,error_match",
    [
        (
            "{{ undefined_var }}",
            {},
            "Jinja rendering error at root: 'undefined_var' is undefined",
        ),
        (
            "{{ invalid syntax }",
            {"product": "TestProd"},
            (
                "Jinja rendering error at root: expected token "
                "'end of print statement', got 'syntax'"
            ),
        ),
    ],
)
def test_process_jinja_in_values__errors(template, context, error_match):
    """Test that Jinja rendering errors are raised for invalid templates."""
    with pytest.raises(RuntimeError, match=error_match):
        process_jinja_in_values(template, ENV, context)


def test_wrap_text__short_string():
    """Test that short strings are not wrapped"""
    text = "Short text"
    result = wrap_value(text)
    assert result == "Short text"


def test_wrap_text__long_string():
    """Test that long strings are wrapped at specified width"""
    text = ("This is a long string with spaces " * 10).strip()
    result = wrap_value(text)
    assert "\n" in result
    lines = result.split("\n")
    for line in lines:
        assert len(line) <= TEXT_WRAP_WIDTH


def test_wrap_text__preserves_newlines():
    """Test that existing newlines in text are preserved."""
    text = "Line 1\nLine 2\nLine 3"
    result = wrap_value(text)
    assert result.count("\n") == 2


def test_wrap_multiline_strings__wraps_fields():
    """Test that specific fields are wrapped while others are left unchanged."""
    long_text = ("This is a long string with spaces " * 10).strip()
    spec = {
        "synopsis": long_text,
        "topic": "Short topic",
        "description": long_text,
        "solution": "Short solution",
        "other_field": long_text,
    }
    wrap_multiline_strings(spec)

    assert "\n" in spec["synopsis"]
    assert "\n" not in spec["topic"]
    assert "\n" in spec["description"]
    assert "\n" not in spec["solution"]


def test_validate_against_schema__valid(input_data, advisory_schema):
    """Test that valid advisory passes schema validation."""
    advisory = build_advisory(input_data)
    validate_against_schema(advisory, advisory_schema)


def test_validate_against_schema__errors(input_data, advisory_schema):
    """Test that schema validation catches validation errors."""
    advisory = build_advisory(input_data)
    del advisory["metadata"]["name"]
    del advisory["spec"]["references"]

    with pytest.raises(ValueError, match=r"Schema validation failed with 2 error\(s\)"):
        validate_against_schema(advisory, advisory_schema)
