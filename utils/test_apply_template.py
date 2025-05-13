import tempfile
import json
import os

from jinja2 import TemplateSyntaxError
import yaml

import pytest

from unittest.mock import patch, MagicMock

from apply_template import setup_argparser, main


@patch(
    "argparse._sys.argv",
    ["apply_template", "--data", "{}", "--template", "somefile", "-o", "newfile"],
)
def test_setup_argparser_proper_args():
    args_out = setup_argparser()
    assert args_out.data == "{}"
    assert args_out.template == "somefile"
    assert args_out.output == "newfile"


def test_setup_argparser_improper_args():
    with pytest.raises(SystemExit) as e:
        setup_argparser()
    assert e.value.code == 2


@patch("builtins.open")
@patch("apply_template.Template.render")
@patch("apply_template.setup_argparser")
def test_apply_template_advisory_template(
    mock_argparser: MagicMock, mock_render: MagicMock, mock_open: MagicMock
):
    args = MagicMock()
    args.template = "templates/advisory.yaml.jinja"
    args.data = "{}"
    args.output = "somefile"
    args.verbose = True
    mock_argparser.return_value = args
    mock_render.return_value = "applied template file"
    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2]
    mock_open1.__enter__.return_value.read.return_value = "foo: bar"
    file = mock_open2.__enter__.return_value

    # Act
    main()

    file.write.assert_called_once_with("applied template file")


@patch("apply_template.setup_argparser")
def test_apply_template_advisory_template_in_full(mock_argparser: MagicMock):
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
                        "product_version": "version",
                        "product_stream": "stream",
                        "cpe": "cpe:/id",
                        "type": "RHEA",
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
            result = yaml.safe_load(f.read())

        assert result["spec"]["solution"] == solution
        assert result["spec"]["topic"] == topic
        assert result["spec"]["synopsis"] == "Enhancement synopsis"
        for issue in result["spec"]["issues"]["fixed"]:
            assert len(issue.keys()) == 3
    finally:
        os.remove(filename)


@patch("apply_template.setup_argparser")
def test_apply_template_advisory_template_fail_syntax_error(mock_argparser: MagicMock):
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
