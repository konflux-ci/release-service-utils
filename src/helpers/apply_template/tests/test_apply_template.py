"""Tests for apply_template helper."""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from jinja2 import TemplateSyntaxError
from jinja2.exceptions import SecurityError

from release_service_utils.helpers.apply_template import main, setup_argparser


@patch(
    "argparse._sys.argv",
    ["apply_template", "--data", "{}", "--template", "somefile", "-o", "newfile"],
)
def test_setup_argparser_proper_args():
    """Parse data, template, and output arguments correctly."""
    args_out = setup_argparser()
    assert args_out.data == "{}"
    assert args_out.template == "somefile"
    assert args_out.output == "newfile"


@patch(
    "argparse._sys.argv",
    [
        "apply_template",
        "--data-file",
        "datafile.json",
        "--template",
        "somefile",
        "-o",
        "newfile",
    ],
)
def test_setup_argparser_data_file_arg():
    """Parse data-file argument instead of inline data."""
    args_out = setup_argparser()
    assert args_out.data_file == "datafile.json"
    assert args_out.data is None
    assert args_out.template == "somefile"
    assert args_out.output == "newfile"


def test_setup_argparser_improper_args():
    """Exit with code 2 when required arguments are missing."""
    with pytest.raises(SystemExit) as e:
        setup_argparser()
    assert e.value.code == 2


@patch("builtins.open")
@patch("jinja2.Template.render")
@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_advisory_template(
    mock_argparser: MagicMock, mock_render: MagicMock, mock_open: MagicMock
):
    """Render template and write output as JSON."""
    args = MagicMock()
    args.template = "templates/advisory.yaml.jinja"
    args.data = "{}"
    args.output = "somefile"
    args.verbose = True
    mock_argparser.return_value = args
    mock_render.return_value = "foo: bar"
    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2]
    mock_open1.__enter__.return_value.read.return_value = "foo: bar"
    file = mock_open2.__enter__.return_value

    # Act
    main()

    # Verify output is JSON
    assert file.write.called
    written = "".join(call.args[0] for call in file.write.call_args_list)
    assert json.loads(written) == {"foo": "bar"}


@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_with_data_file(mock_argparser: MagicMock):
    """Load template data from file and render advisory template."""
    _, data_filename = tempfile.mkstemp(suffix=".json")
    _, output_filename = tempfile.mkstemp()

    test_data = {
        "advisory_name": "test_advisory",
        "advisory_ship_date": "2024-01-01",
        "advisory": {
            "spec": {
                "product_id": 1,
                "product_name": "Test Product",
                "product_version": "1.0.0",
                "product_stream": "test_stream",
                "cpe": "cpe:/test:id",
                "type": "RHEA",
                "topic": "Test topic",
                "description": "Test description",
                "solution": "Test solution",
                "synopsis": "Test synopsis",
                "references": ["test_ref"],
                "content": {},
                "issues": {
                    "fixed": [
                        {
                            "id": "TEST-1",
                            "source": "issues.redhat.com",
                            "summary": "Test issue",
                            "public": "true",
                        }
                    ]
                },
            }
        },
    }

    try:
        # Write test data to the data file
        with open(data_filename, "w") as f:
            json.dump(test_data, f)

        args = MagicMock()
        args.template = "templates/advisory.yaml.jinja"
        args.data = None
        args.data_file = data_filename
        args.output = output_filename
        args.verbose = False
        mock_argparser.return_value = args

        # Act
        main()

        # Verify the output file was created and contains expected content
        with open(output_filename, "r") as f:
            result = json.load(f)

        assert result["spec"]["product_name"] == "Test Product"
        assert result["spec"]["topic"] == "Test topic"
        assert result["spec"]["synopsis"] == "Test synopsis"
        assert result["spec"]["solution"] == "Test solution"
        assert len(result["spec"]["issues"]["fixed"]) == 1
        assert result["spec"]["issues"]["fixed"][0]["id"] == "TEST-1"
        assert result["spec"]["skip_customer_notifications"] is False

    finally:
        os.remove(data_filename)
        os.remove(output_filename)


@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_advisory_template_in_full(mock_argparser: MagicMock):
    """Render full advisory template with long strings and partial templates."""
    _, filename = tempfile.mkstemp()

    # Confirm that long strings with spaces aren't broken up in a weird way
    solution = (
        "a really long string with spaces that goes on and on and "
        "on and on and on and on and on, it's so long that you would "
        "expect that something is going to linewrap it at some point. so long."
    )
    # Confirm that long strings with no spaces aren't broken up in a weird way
    topic = (
        "a-really-long-string-with-dashes-that-goes-on-and-on-and-"
        "on-and-on-and-on-and-on-and-on,-it's-so-long-that-you-would-"
        "expect-that-something-is-going-to-linewrap-it-at-some-point.-so-long."
    )
    # Confirm contributed partial templates are rendered
    synopsis = "{% if advisory.spec.type == 'RHEA' %} Enhancement{%- endif %} synopsis"
    try:
        args = MagicMock()
        args.template = "templates/advisory.yaml.jinja"
        args.data = json.dumps(
            {
                "advisory_name": "advisory",
                "advisory_ship_date": "today",
                "advisory": {
                    "spec": {
                        "product_id": 1,
                        "product_name": "name",
                        "product_version": "4.20",
                        "product_stream": "stream",
                        "cpe": "cpe:/id",
                        "type": "RHEA",
                        "skip_customer_notifications": True,
                        "topic": topic,
                        "description": "description",
                        "solution": solution,
                        "synopsis": synopsis,
                        "references": ["testing"],
                        "content": {},
                        "issues": {
                            "fixed": [
                                {
                                    "id": "KONFLUX-1",
                                    "source": "issues.redhat.com",
                                    "summary": "issue 1",
                                    "public": "true",
                                },
                                {
                                    "id": "KONFLUX-2",
                                    "source": "issues.redhat.com",
                                    "summary": "issue 2",
                                    "public": "false",
                                },
                            ]
                        },
                    }
                },
            }
        )

        args.output = filename
        mock_argparser.return_value = args

        # Act
        main()

        with open(filename, "r") as f:
            result = json.load(f)

        assert result["spec"]["product_id"] == 1
        assert result["spec"]["product_version"] == "4.20"
        assert result["spec"]["product_stream"] == "stream"
        assert result["spec"]["solution"] == solution
        assert result["spec"]["topic"] == topic
        assert result["spec"]["synopsis"] == "Enhancement synopsis"
        assert result["spec"]["skip_customer_notifications"] is True
        for issue in result["spec"]["issues"]["fixed"]:
            assert len(issue.keys()) == 3
    finally:
        os.remove(filename)


@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_advisory_template_fail_syntax_error(mock_argparser: MagicMock):
    """Raise TemplateSyntaxError when template has syntax errors."""
    _, filename = tempfile.mkstemp()

    # error in this partial template
    synopsis = "{% if advisory.spec.type == ' %}FAILURE{%- endif %}"
    #                    error is here     ^^^
    try:
        args = MagicMock()
        args.template = "templates/advisory.yaml.jinja"
        args.data = json.dumps(
            {
                "advisory_name": "advisory",
                "advisory_ship_date": "today",
                "advisory": {
                    "spec": {
                        "product_id": 1,
                        "product_name": "name",
                        "product_version": "version",
                        "product_stream": "stream",
                        "cpe": "cpe:/id",
                        "type": "RHEA",
                        "topic": "topic",
                        "description": "description",
                        "solution": "solution",
                        "synopsis": synopsis,
                        "references": ["testing"],
                        "content": {},
                    }
                },
            }
        )

        args.output = filename
        mock_argparser.return_value = args

        # Act
        with pytest.raises(TemplateSyntaxError):
            main()

    finally:
        os.remove(filename)


@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_restricts_private_attribute_access(
    mock_argparser: MagicMock,
) -> None:
    """Reject template expressions that reach Python "dunder"/private attrs.

    The environment is configured to only allow plain data lookups, filters,
    and comparisons inside rendered fields; direct attribute traversal
    (names starting with `_`) is not part of the supported template syntax.
    """
    _, filename = tempfile.mkstemp()

    # Attribute names starting with `_` are outside the supported template
    # syntax for advisory text fields.
    restricted_expression = "{{ ''.__class__.__mro__[1].__subclasses__() }}"
    try:
        args = MagicMock()
        args.template = "templates/advisory.yaml.jinja"
        args.data = json.dumps(
            {
                "advisory_name": "advisory",
                "advisory_ship_date": "today",
                "advisory": {
                    "spec": {
                        "product_id": 1,
                        "product_name": "name",
                        "product_version": "version",
                        "product_stream": "stream",
                        "cpe": "cpe:/id",
                        "type": "RHEA",
                        "topic": "topic",
                        "description": "description",
                        "solution": "solution",
                        "synopsis": restricted_expression,
                        "references": ["testing"],
                        "content": {},
                    }
                },
            }
        )

        args.output = filename
        mock_argparser.return_value = args

        # Act / Assert: raised during the 2nd pass, when the 1st pass output
        # is re-parsed as a template.
        with pytest.raises(SecurityError):
            main()

    finally:
        os.remove(filename)


@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_second_pass_renders_missing_vars_empty(
    mock_argparser: MagicMock,
) -> None:
    """Render missing names in 2nd-pass advisory fields as empty strings."""
    _, filename = tempfile.mkstemp()

    # Pass 2 used to be a bare Template(), which substitutes missing names
    # with empty strings. Keep that so leftover ``{{ }}`` is not written
    # into the advisory JSON.
    synopsis = "{{ missing_value }}done"
    try:
        args = MagicMock()
        args.template = "templates/advisory.yaml.jinja"
        args.data = json.dumps(
            {
                "advisory_name": "advisory",
                "advisory_ship_date": "today",
                "advisory": {
                    "spec": {
                        "product_id": 1,
                        "product_name": "name",
                        "product_version": "version",
                        "product_stream": "stream",
                        "cpe": "cpe:/id",
                        "type": "RHEA",
                        "topic": "topic",
                        "description": "description",
                        "solution": "solution",
                        "synopsis": synopsis,
                        "references": ["testing"],
                        "content": {},
                    }
                },
            }
        )

        args.output = filename
        mock_argparser.return_value = args

        main()

        with open(filename, "r") as f:
            result = json.load(f)

        assert result["spec"]["synopsis"] == "done"
        assert "{{" not in result["spec"]["synopsis"]
        assert "}}" not in result["spec"]["synopsis"]

    finally:
        os.remove(filename)


@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_second_pass_allows_list_append(
    mock_argparser: MagicMock,
) -> None:
    """Keep RPM-style list.append working in 2nd-pass advisory fields.

    ImmutableSandboxedEnvironment treats list.append as unsafe. Existing
    advisory templates mutate nested lists that way, e.g.
    ``src['rpms'][rpm_key]['arches'].append(artifact.architecture)``.
    """
    _, filename = tempfile.mkstemp()

    synopsis = (
        "arches "
        "{% set rpm_key = 'pkg' %}"
        "{% set _ = src['rpms'][rpm_key]['arches'].append("
        "artifact.architecture) %}"
        "{{ src['rpms'][rpm_key]['arches'] | join(',') }}"
    )
    try:
        args = MagicMock()
        args.template = "templates/advisory.yaml.jinja"
        args.data = json.dumps(
            {
                "advisory_name": "advisory",
                "advisory_ship_date": "today",
                "src": {"rpms": {"pkg": {"arches": ["x86_64"]}}},
                "artifact": {"architecture": "s390x"},
                "advisory": {
                    "spec": {
                        "product_id": 1,
                        "product_name": "name",
                        "product_version": "version",
                        "product_stream": "stream",
                        "cpe": "cpe:/id",
                        "type": "RHEA",
                        "topic": "topic",
                        "description": "description",
                        "solution": "solution",
                        "synopsis": synopsis,
                        "references": ["testing"],
                        "content": {},
                    }
                },
            }
        )

        args.output = filename
        mock_argparser.return_value = args

        main()

        with open(filename, "r") as f:
            result = json.load(f)

        assert result["spec"]["synopsis"] == "arches x86_64,s390x"

    finally:
        os.remove(filename)


@patch("release_service_utils.helpers.apply_template.apply_template.setup_argparser")
def test_apply_template_second_pass_allows_dict_update(
    mock_argparser: MagicMock,
) -> None:
    """Keep RPM-style dict.update working in 2nd-pass advisory fields.

    ImmutableSandboxedEnvironment also treats dict.update as unsafe.
    Existing RPM advisory templates mutate nested dicts that way.
    """
    _, filename = tempfile.mkstemp()

    synopsis = (
        "nvr "
        "{% set rpm_key = 'pkg' %}"
        "{% set _ = src['rpms'][rpm_key].update({'nvr': artifact.nvr}) %}"
        "{{ src['rpms'][rpm_key]['nvr'] }}"
    )
    try:
        args = MagicMock()
        args.template = "templates/advisory.yaml.jinja"
        args.data = json.dumps(
            {
                "advisory_name": "advisory",
                "advisory_ship_date": "today",
                "src": {"rpms": {"pkg": {"arches": ["x86_64"]}}},
                "artifact": {"nvr": "foo-1.0-1"},
                "advisory": {
                    "spec": {
                        "product_id": 1,
                        "product_name": "name",
                        "product_version": "version",
                        "product_stream": "stream",
                        "cpe": "cpe:/id",
                        "type": "RHEA",
                        "topic": "topic",
                        "description": "description",
                        "solution": "solution",
                        "synopsis": synopsis,
                        "references": ["testing"],
                        "content": {},
                    }
                },
            }
        )

        args.output = filename
        mock_argparser.return_value = args

        main()

        with open(filename, "r") as f:
            result = json.load(f)

        assert result["spec"]["synopsis"] == "nvr foo-1.0-1"

    finally:
        os.remove(filename)


def test_numeric_strings_preserved_through_json_parsing() -> None:
    """Numeric-looking strings are preserved when using json.loads (not yaml.safe_load).

    Regression test for: https://github.com/konflux-ci/release-service-catalog/issues/2691

    The bug was at line 279 in apply_template.py:
        data = yaml.safe_load(content)  # BUG: can convert strings to numbers

    The fix:
        data = json.loads(content)      # CORRECT: content is always JSON

    This test verifies that numeric strings with leading zeros are preserved.
    The bug may only manifest in certain PyYAML versions or pipeline environments.
    """
    from release_service_utils.helpers.apply_template import render_template_to_json_file

    _, template_path = tempfile.mkstemp()
    _, output_path = tempfile.mkstemp()

    try:
        # Template with numeric-looking strings (as JSON from apply_mapping)
        template_content = json.dumps(
            {
                "accounts": ["041287970199", "0428492", "0001", "007"],
                "version": "9.9",
                "buildId": "1.0.1",
            },
            indent=2,
        )

        with open(template_path, "w") as f:
            f.write(template_content)

        # Render with apply_template
        render_template_to_json_file(
            output_path=output_path,
            template_path=template_path,
            template_data={},
        )

        # Load result
        with open(output_path, "r") as f:
            result = json.load(f)

        # Verify strings are preserved (not converted to numbers)
        assert result["accounts"] == [
            "041287970199",
            "0428492",
            "0001",
            "007",
        ], f"Account IDs corrupted: {result['accounts']}"
        assert result["version"] == "9.9", f"Version corrupted: {result['version']}"
        assert result["buildId"] == "1.0.1"

        # Verify types are strings
        for i, acc in enumerate(result["accounts"]):
            assert isinstance(acc, str), (
                f"accounts[{i}] = {acc!r} is {type(acc).__name__}, should be str. "
                f"This indicates apply_template is using yaml.safe_load instead of json.loads!"
            )

        assert isinstance(
            result["version"], str
        ), f"version is {type(result['version']).__name__}, should be str"

    finally:
        os.remove(template_path)
        os.remove(output_path)


def test_aws_account_ids_preserved() -> None:
    """AWS account IDs with leading zeros must be preserved as strings.

    Real-world regression test for the exact data pattern that triggered the bug.

    User reported: sharing_accounts ["041287970199", "418272754616", ...]
    became [41287970199, "418272754616", ...] after apply_mapping.

    This happens when apply_template.py line 279 uses yaml.safe_load() instead
    of json.loads() to parse JSON content.
    """
    from release_service_utils.helpers.apply_template import render_template_to_json_file

    _, template_path = tempfile.mkstemp()
    _, output_path = tempfile.mkstemp()

    try:
        # Real-world data from the bug report
        component_data = {
            "name": "test-component",
            "starmap": [
                {
                    "cloud": "aws",
                    "meta": {
                        "sharing_accounts": [
                            "041287970199",
                            "418272754616",
                            "599476212575",
                            "855246887846",
                        ]
                    },
                }
            ],
        }

        # This is what apply_mapping does at line 610
        template_content = json.dumps(component_data, indent=2, sort_keys=True)

        with open(template_path, "w") as f:
            f.write(template_content)

        # This calls render_template_to_json_file which hits the bug at line 279
        render_template_to_json_file(
            output_path=output_path,
            template_path=template_path,
            template_data={},
        )

        # Load result
        with open(output_path, "r") as f:
            result = json.load(f)

        accounts = result["starmap"][0]["meta"]["sharing_accounts"]

        # The CRITICAL assertion: first account has leading zero
        assert accounts[0] == "041287970199", (
            f"BUG REPRODUCED: accounts[0] = {accounts[0]!r} "
            f"(type={type(accounts[0]).__name__}), "
            f"expected '041287970199' (type=str). "
            f"Leading zero was lost! This is the exact bug from the user report."
        )

        # Verify it's a string, not a number
        assert isinstance(accounts[0], str), (
            f"BUG: accounts[0] = {accounts[0]!r} is "
            f"{type(accounts[0]).__name__}, should be str. "
            f"yaml.safe_load() at line 279 converted the string to a number!"
        )

        # All accounts should be strings
        for i, acc in enumerate(accounts):
            assert isinstance(
                acc, str
            ), f"accounts[{i}] = {acc!r} ({type(acc).__name__}) should be str"

    finally:
        os.remove(template_path)
        os.remove(output_path)
