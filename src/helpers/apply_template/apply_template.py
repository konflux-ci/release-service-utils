"""Jinja2 template rendering with YAML-to-JSON conversion for advisory data.

Two-pass rendering with AnsibleCoreFiltersExtension support. Outputs JSON to
prevent YAML type coercion (e.g., version strings like "33158e1" being
parsed as numbers).
"""

from __future__ import annotations

import argparse
import json
import logging
import traceback
from pathlib import Path
from typing import Any

from ..logger import logger as LOGGER

import yaml
from jinja2 import DebugUndefined, exceptions
from jinja2.lexer import Lexer, Token, TokenStream
from jinja2.sandbox import SandboxedEnvironment
from jinja2_ansible_filters import AnsibleCoreFiltersExtension


class CustomLexer(Lexer):
    """Custom Lexer that merges hyphenated identifiers into single tokens.

    Allows strings like 'foo-bar' in templates to be treated as identifiers
    rather than being parsed as mathematical expressions (foo minus bar).
    """

    def tokenize(self, source, name=None, filename=None, state=None):
        """Merge hyphenated identifiers into single tokens."""
        token_stream = super().tokenize(source, name, filename, state)
        tokens = list(token_stream)

        result = []
        i = 0
        while i < len(tokens):
            if (
                i + 2 < len(tokens)
                and tokens[i].test("name")
                and tokens[i + 1].test("sub")
                and tokens[i + 2].test("name")
            ):
                # Merge NAME - NAME - NAME ... into a single NAME token
                merged_parts = [tokens[i].value]
                j = i + 1
                while (
                    j + 1 < len(tokens)
                    and tokens[j].test("sub")
                    and tokens[j + 1].test("name")
                ):
                    merged_parts.append(tokens[j + 1].value)
                    j += 2
                merged_value = "-".join(merged_parts)
                merged_token = Token(tokens[i].lineno, "name", merged_value)
                result.append(merged_token)
                i = j
            else:
                result.append(tokens[i])
                i += 1

        return TokenStream(result, name, filename)


class CustomEnvironment(SandboxedEnvironment):
    """Custom Jinja2 Environment that uses CustomLexer for tokenization."""

    def _tokenize(self, source, name, filename=None, state=None):
        """Override _tokenize to use custom lexer."""
        if not hasattr(self, "_custom_lexer"):
            self._custom_lexer = CustomLexer(self)
        return self._custom_lexer.tokenize(source, name, filename, state)


class LabelsProvider:
    """Custom class to provide label access in Jinja2 templates.

    Supports both simple and nested label access:
    - {{labels.mylabel}} - accesses label "mylabel"
    - {{labels.mylabel.with-dash}} - accesses label "mylabel.with-dash"

    Builds label path through attribute access and resolves when converted to string.
    """

    def __init__(self, labels: dict[str, Any], path: str = ""):
        """Initialize the LabelsProvider.

        Args:
            labels: Dictionary containing all available labels.
            path: Current path being built (used internally for nested access).

        """
        self._labels = labels
        self._path = path

    def __getattr__(self, name: str) -> "LabelsProvider":
        """Handle attribute access to build up label paths.

        Args:
            name: The attribute name being accessed.

        Returns:
            A new LabelsProvider instance with extended path.

        """
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        new_path = f"{self._path}.{name}" if self._path else name
        return LabelsProvider(self._labels, path=new_path)

    def __getitem__(self, key: str) -> "LabelsProvider":
        """Handle item access for labels with special characters.

        Args:
            key: The key being accessed.

        Returns:
            A new LabelsProvider instance with extended path.

        """
        new_path = f"{self._path}.{key}" if self._path else key
        return LabelsProvider(self._labels, new_path)

    def __str__(self) -> str:
        """Resolve the label path and return its value.

        Returns:
            The label value as a string, or empty string if not found.

        """
        if not self._path:
            return ""

        value = self._labels.get(self._path, "")
        return str(value) if value is not None else ""


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
    labels_ext: bool = False,
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
        labels_ext: Enable custom label extension for hyphenated label names.

    """
    log_level = logging.DEBUG if verbose else logging.INFO
    LOGGER.setLevel(log_level)

    # When labels_ext is enabled, wrap the labels dict with LabelsProvider
    # and use CustomEnvironment for hyphenated identifier support
    render_data = template_data.copy()
    if labels_ext and "labels" in render_data:
        labels_dict = render_data["labels"]
        render_data["labels"] = LabelsProvider(labels_dict)

    with open(template_path, encoding="utf-8") as template_file:
        template_content = template_file.read()
        # DebugUndefined renders undefined variables as empty strings
        # instead of raising errors.
        if labels_ext:
            env = CustomEnvironment(
                extensions=[AnsibleCoreFiltersExtension],
                undefined=DebugUndefined,
            )
            template = env.from_string(template_content)
        else:
            env = SandboxedEnvironment(
                extensions=[AnsibleCoreFiltersExtension],
                undefined=DebugUndefined,
            )
            template = env.from_string(template_content)

    LOGGER.info("Rendering 1st pass")
    try:
        content = template.render(render_data)
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
        if labels_ext:
            env = CustomEnvironment(undefined=DebugUndefined)
            second_template = env.from_string(content)
        else:
            env = SandboxedEnvironment()
            second_template = env.from_string(content)
        content = second_template.render(render_data)
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
