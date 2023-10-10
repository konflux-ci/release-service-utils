#!/usr/bin/env python3
from jinja2 import Template
from jinja2_ansible_filters import AnsibleCoreFiltersExtension
import argparse
import json


def setup_argparser() -> argparse.Namespace:  # pragma: no cover
    """Setup argument parser

    :return: Initialized argument parser
    """

    parser = argparse.ArgumentParser(description="Applies a template.")

    parser.add_argument(
        "--data",
        help="JSON string containing data to use in the template.",
        required=True,
    )
    parser.add_argument(
        "--template",
        help="Path to the template file to use.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="The desired filename of the result.",
        required=True,
    )
    return parser.parse_args()


def main():  # pragma: no cover
    """Main func"""

    args = setup_argparser()

    with open(args.template) as t:
        template = Template(t.read(), extensions=[AnsibleCoreFiltersExtension])
    content = template.render(json.loads(args.data))

    filename = args.output
    with open(filename, mode="w", encoding="utf-8") as advisory:
        advisory.write(content)
        print(f"Wrote {filename}")


if __name__ == "__main__":  # pragma: no cover
    main()
