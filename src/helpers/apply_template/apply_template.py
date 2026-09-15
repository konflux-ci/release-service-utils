"""Jinja2 template rendering with YAML-to-JSON conversion for advisory data.

Two-pass rendering with AnsibleCoreFiltersExtension support. Outputs JSON to
prevent YAML type coercion (e.g., version strings like "33158e1" being
parsed as numbers).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path
from typing import Any

import yaml
from jinja2 import DebugUndefined, Template, exceptions
from jinja2_ansible_filters import AnsibleCoreFiltersExtension

LOGGER = logging.getLogger(__name__)


def setup_argparser() -> argparse.Namespace:  # pragma: no cover
    """Set up argument parser for CLI usage.

    Returns:
        Parsed command-line arguments.

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


def render_template_to_json_file(
    output_path: str | Path,
    template_path: str | Path,
    template_data: dict[str, Any],
    *,
    verbose: bool = False,
) -> None:
    """Render Jinja2 template with two-pass processing and write as JSON.

    Two-pass Jinja render of *template_path* with *template_data*; write JSON to
    *output_path*.

    YAML is used as an intermediate representation; output is JSON so values like
    version strings are not corrupted by YAML type coercion.

    Args:
        output_path: Path where the rendered JSON will be written.
        template_path: Path to the Jinja2 template file.
        template_data: Dictionary of data to use in template rendering.
        verbose: Enable debug logging if True.

    """
    log_level = logging.DEBUG if verbose else logging.INFO
    setup_logger(level=log_level)

    with open(template_path, encoding="utf-8") as template_file:
        # DebugUndefined renders undefined variables as empty strings
        # instead of raising errors.
        template = Template(
            template_file.read(),
            extensions=[AnsibleCoreFiltersExtension],
            undefined=DebugUndefined,
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
        raise jexc from jexc

    # try 2nd pass
    LOGGER.info("Rendering 2nd pass")
    try:
        content = Template(content).render(template_data)
        LOGGER.debug(content)
    except exceptions.TemplateSyntaxError as jexc:
        LOGGER.exception("Exception with Template Syntax:")
        # we use this traceback to get the line number
        LOGGER.error(traceback.format_exc())
        raise jexc from jexc

    try:
        # load to check it is valid yaml
        LOGGER.info("Load 2nd pass content")
        yaml.safe_load(content)
    except yaml.YAMLError as exc:
        LOGGER.exception("Invalid yaml...fall back to first pass rendered content")
        # we use this traceback to get the line number
        LOGGER.error(traceback.format_exc())
        # fallback to valid first pass
        content = first_pass
        try:
            # load to check it is valid yaml
            LOGGER.info("Load 1st pass content")
            yaml.safe_load(content)
        except yaml.YAMLError as first_exc:
            LOGGER.exception("Invalid yaml")
            # we use this traceback to get the line number
            LOGGER.error(traceback.format_exc())
            raise first_exc from exc

    # Convert to JSON for safer parsing. Jinja works cleaner with YAML syntax
    # but YAML type conversion can corrupt data e.g. "33158e1" to 331580 JSON
    # output prevents this.
    data = yaml.safe_load(content)
    out = Path(output_path)
    with open(out, mode="w", encoding="utf-8") as data_file:
        json.dump(data, data_file, indent=2)
        LOGGER.info("Wrote %s", out)


def setup_logger(level: int = logging.INFO, log_format: Any = None) -> None:
    """Set up and configure logger with stdout and stderr handlers.

    Logs at passed level to stdout, ERROR and above to stderr.

    Args:
        level: Minimum logging level for stdout (default: logging.INFO)
        log_format: Logging message format (default: standard format)

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


def main() -> int:  # pragma: no cover
    """CLI entrypoint for applying Jinja2 templates.

    Returns:
        Exit code (0 for success).

    """
    args = setup_argparser()
    if args.data:
        template_data = json.loads(args.data)
    else:
        with open(args.data_file, encoding="utf-8") as data_file:
            template_data = json.loads(data_file.read())
    render_template_to_json_file(
        args.output,
        args.template,
        template_data,
        verbose=args.verbose,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
