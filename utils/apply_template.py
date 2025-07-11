#!/usr/bin/env python3
import traceback

import yaml
from jinja2 import Template, DebugUndefined, exceptions
from jinja2_ansible_filters import AnsibleCoreFiltersExtension
import argparse
import json
import logging
import sys
from typing import Any

LOGGER = logging.getLogger("apply_template")


def setup_argparser() -> argparse.Namespace:  # pragma: no cover
    """Setup argument parser

    :return: Initialized argument parser
    """

    parser = argparse.ArgumentParser(description="Applies a template.")

    # Create mutually exclusive group for data input
    data_group = parser.add_mutually_exclusive_group(required=True)
    data_group.add_argument(
        "--data",
        help="JSON string containing data to use in the template.",
    )
    data_group.add_argument(
        "--data-file",
        help="Path to file containing JSON data to use in the template.",
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
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    return parser.parse_args()


def main():  # pragma: no cover
    """Main func"""

    args = setup_argparser()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logger(level=log_level)

    # Load JSON data from either --data argument or --data-file
    if args.data:
        template_data = json.loads(args.data)
    else:  # args.data_file
        with open(args.data_file, "r") as f:
            template_data = json.loads(f.read())

    with open(args.template) as t:
        template = Template(
            t.read(), extensions=[AnsibleCoreFiltersExtension], undefined=DebugUndefined
        )
    LOGGER.info("Rendering 1st pass")
    try:
        content = template.render(template_data)
        LOGGER.debug(content)
        first_pass = content
    except exceptions.TemplateSyntaxError as jexc:
        LOGGER.exception("Exception with Template Syntax:")
        # we use this traceback to get the line number
        LOGGER.error(traceback.format_exc())
        raise jexc

    # try 2nd pass
    LOGGER.info("Rendering 2nd pass")
    try:
        content = Template(content).render(template_data)
        LOGGER.debug(content)
    except exceptions.TemplateSyntaxError as jexc:
        LOGGER.exception("Exception with Template Syntax:")
        # we use this traceback to get the line number
        LOGGER.error(traceback.format_exc())
        raise jexc

    try:
        # load to check it is valid yaml
        LOGGER.info("Load 2nd pass content")
        yaml.safe_load(content)
    except yaml.YAMLError:
        LOGGER.exception("Invalid yaml...fall back to first pass rendered content")
        # we use this traceback to get the line number
        LOGGER.error(traceback.format_exc())
        # fallback to valid first pass
        content = first_pass
        try:
            # load to check it is valid yaml
            LOGGER.info("Load 1st pass content")
            yaml.safe_load(content)
        except yaml.YAMLError as exc:
            LOGGER.exception("Invalid yaml")
            # we use this traceback to get the line number
            LOGGER.error(traceback.format_exc())
            raise exc

    filename = args.output
    with open(filename, mode="w", encoding="utf-8") as advisory:
        advisory.write(content)
        LOGGER.info(f"Wrote {filename}")


def setup_logger(level: int = logging.INFO, log_format: Any = None):
    """Set up and configure logger.
    Args:
        level (str, optional): Logging level. Defaults to logging.INFO.
        log_format (Any, optional): Logging message format. Defaults to None.
    :return: Logger object
    """
    if log_format is None:
        log_format = "%(asctime)s [%(name)s] %(levelname)s %(message)s"

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)

    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[stream_handler],
    )


if __name__ == "__main__":  # pragma: no cover
    main()
