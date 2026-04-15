"""Tests for subst_template module."""

import unittest.mock as mock
import importlib
from io import StringIO
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
subst_template = importlib.import_module("subst_template")

# flake8: noqa: E402
from subst_template import (  # noqa: F402
    subst_template,
    setup_jinja,
    load_input_data,
    write_output,
    load_template,
    main,
)


def test_subst_template_default():
    """Test basic template substitution."""
    data = {"name": "World"}
    env = setup_jinja(data, labels_ext=False, strict=False)
    template = "Hello, {{ name }}!"
    result = subst_template(env, template, data)
    assert result == "Hello, World!"


def test_subst_template_allow_empty_inputs():
    """Test template substitution with empty inputs allowed."""
    template = "Hello, {{ name }} {{name2}}!"
    data = {"name": None, "name2": ""}
    env = setup_jinja(data, labels_ext=False, strict=False)
    result = subst_template(env, template, data, allow_empty_inputs=True)
    assert result == "Hello, None !"


def test_subst_template_disallow_empty_inputs():
    """Test template substitution raises error when empty inputs are disallowed."""
    template = "Hello, {{ name }} {{name2}}!"
    data = {"name": None, "name2": ""}
    env = setup_jinja(data, labels_ext=True, strict=False)
    with pytest.raises(ValueError):
        subst_template(env, template, data, allow_empty_inputs=False)


def test_subst_template_ext_label():
    """Test label extension with simple label access."""
    template = "Hello, {{name}}: {{ labels.good.label }}!"
    data = {"name": "World", "labels": {"good.label": "label1"}}
    env = setup_jinja(data, labels_ext=True, strict=False)
    result = subst_template(env, template, data)
    assert result == "Hello, World: label1!"


def test_subst_template_ext_label_dashes():
    """Test label extension with dashes in label names."""
    template = "Hello, {{name}}: {{ labels.good.label-dash }}!"
    data = {
        "name": "World",
        "labels": {"good.label-dash": "label1", "good.label-with.two-dashes": "label2"},
    }
    env = setup_jinja(data, labels_ext=True, strict=False)
    result = subst_template(env, template, data)
    assert result == "Hello, World: label1!"

    template = "Hello, {{name}}: {{ labels.good.label-with.two-dashes }}!"
    result = subst_template(env, template, data)
    assert result == "Hello, World: label2!"


def test_subst_template_ext_label_starts_with_underscore():
    """Test label extension with labels starting with underscore."""
    template = "Hello, {{name}}: {{ labels._good.label-dash }}!"
    data = {"name": "World", "labels": {"_good.label-dash": "label1"}}
    env = setup_jinja(data, labels_ext=True, strict=False)
    result = subst_template(env, template, data)
    assert result == "Hello, World: label1!"


def test_subst_template_ext_label_missing():
    """Test label extension returns empty string for missing labels."""
    template = "Hello, {{name}}: {{ labels.missing.label-dash }}!"
    data = {"name": "World", "labels": {"good.label-dash": "label1"}}
    env = setup_jinja(data, labels_ext=True, strict=False)
    result = subst_template(env, template, data)
    assert result == "Hello, World: !"


def test_subst_template_ext_label_missing_strict():
    """Test label extension raises error for missing labels in strict mode."""
    template = "Hello, {{name}}: {{ labels.missing.label-dash }}!"
    data = {"name": "World", "labels": {"good.label-dash": "label1"}}
    env = setup_jinja(data, labels_ext=True, strict=True)
    with pytest.raises(KeyError, match="Label 'missing.label-dash' not found in labels"):
        subst_template(env, template, data)


def test_subst_template_ext_no_label():
    """Test label extension raises error when no label path is specified."""
    template = "Hello, {{name}}: {{ labels }}!"
    data = {"name": "World", "labels": {"good.label-dash": "label1"}}
    env = setup_jinja(data, labels_ext=True, strict=False)
    with pytest.raises(KeyError, match="No label specified in path ''"):
        subst_template(env, template, data)


def test_subst_template_dashes_without_labels():
    """Test dashed variables outside labels extension raise errors."""
    template = "Hello, {{name}}: {{ dashed-variable }}!"
    data = {"name": "World", "dashed-variable": "dashed-value"}
    env = setup_jinja(data, labels_ext=True, strict=False)
    with pytest.raises(Exception):
        subst_template(env, template, data)


def test_subst_template_no_strict_missing():
    """Test non-strict mode renders missing variables as-is."""
    template = "Hello, {{ name }}!"
    data = {}
    env = setup_jinja(data, labels_ext=True, strict=False)
    result = subst_template(env, template, data)
    assert result == "Hello, {{ name }}!"


def test_subst_template_strict_missing():
    """Test strict mode raises error for missing variables."""
    template = "Hello, {{ name }}!"
    data = {}
    env = setup_jinja(data, labels_ext=True, strict=True)
    with pytest.raises(Exception):
        subst_template(env, template, data)


@pytest.fixture
def fix_setup_jinja():
    """Mock setup_jinja function."""
    with mock.patch("subst_template.setup_jinja") as mock_setup_jinja:
        yield mock_setup_jinja


@pytest.fixture
def fix_subst_template():
    """Mock subst_template function."""
    with mock.patch("subst_template.subst_template") as mock_subst_template:
        yield mock_subst_template


@pytest.fixture
def fix_load_input_data():
    """Mock load_input_data function."""
    with mock.patch("subst_template.load_input_data") as mock_load_input_data:
        yield mock_load_input_data


@pytest.fixture
def fix_load_template():
    """Mock load_template function."""
    with mock.patch("subst_template.load_template") as mock_load_template:
        yield mock_load_template


@pytest.fixture
def fix_write_output():
    """Mock write_output function."""
    with mock.patch("subst_template.write_output") as mock_write_output:
        yield mock_write_output


def test_subst_main(
    fix_setup_jinja,
    fix_subst_template,
    fix_load_input_data,
    fix_load_template,
    fix_write_output,
):
    """Test main function integration with all components."""
    main(["--data", "data_file", "--template", "template_file", "-o", "output_file"])
    fix_load_input_data.assert_called_with("data_file")
    fix_setup_jinja.assert_called_with(fix_load_input_data(), labels_ext=False, strict=False)
    fix_load_template.assert_called_with("template_file")
    fix_write_output.assert_called_with(fix_subst_template(), "output_file")


def test_load_input_data():
    """Test loading input data from JSON file."""
    with mock.patch(
        "builtins.open", mock.mock_open(read_data='{"key": "value"}')
    ) as mock_file:
        result = load_input_data("data_file")
        mock_file.assert_called_with("data_file", "r")
        assert result == {"key": "value"}


def test_load_template_file():
    """Test loading template from file."""
    with mock.patch(
        "builtins.open", mock.mock_open(read_data="template content")
    ) as mock_file:
        result = load_template("template_file")
        mock_file.assert_called_with("template_file", "r")
        assert result == "template content"


def test_load_template_stdin():
    """Test loading template from stdin."""
    template_content = "template content from stdin"
    with mock.patch("sys.stdin", StringIO(template_content)):
        result = load_template("")
        assert result == "template content from stdin"


def test_write_output_file():
    """Test writing output to file."""
    with mock.patch("builtins.open", mock.mock_open()) as mock_file:
        write_output("output content", "output_file")
        mock_file.assert_called_with("output_file", "w")
        mock_file().write.assert_called_with("output content")


def test_write_output_stdout():
    """Test writing output to stdout."""
    with mock.patch("builtins.print") as mock_print:
        write_output("output content", None)
        mock_print.assert_called_with("output content")
