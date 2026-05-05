#!/usr/bin/env python3
import argparse
import json
import logging
import sys

from typing import Any, Dict, List, Optional

from release_service_utils.bootstrap import setup_logger

from jinja2 import Environment, DebugUndefined, StrictUndefined
from jinja2.lexer import Lexer, Token, TokenStream

LOGGER = logging.getLogger("subst_template")


class CustomLexer(Lexer):
    """
    Custom Lexer that modifies tokenization to not treat '-' as a mathematical operator.

    This allows strings like 'foo-bar' in templates to be treated as identifiers
    rather than being parsed as mathematical expressions (foo minus bar).
    """

    def tokenize(self, source, name=None, filename=None, state=None):
        """Custom tokenize that merges hyphenated identifiers."""
        # Get token stream from original tokenizer
        token_stream = super().tokenize(source, name, filename, state)

        # Convert to list to process
        tokens = list(token_stream)

        # Process tokens to merge hyphenated names
        result = []
        i = 0
        while i < len(tokens):
            if (
                i + 2 < len(tokens)
                and tokens[i].test("name")
                and tokens[i + 1].test("sub")
                and tokens[i + 2].test("name")
            ):
                # Merge NAME - NAME into a single NAME token
                merged_value = f"{tokens[i].value}-{tokens[i + 2].value}"
                merged_token = Token(tokens[i].lineno, "name", merged_value)
                result.append(merged_token)
                i += 3  # Skip the three tokens we just merged
            else:
                result.append(tokens[i])
                i += 1

        # Return TokenStream instead of plain iterator
        return TokenStream(result, name, filename)


class CustomEnvironment(Environment):
    """
    Custom Jinja2 Environment that uses CustomLexer for tokenization.
    """

    def _tokenize(self, source, name, filename=None, state=None):
        """Override _tokenize to use our custom lexer."""
        # Create our custom lexer if not already created
        if not hasattr(self, "_custom_lexer"):
            self._custom_lexer = CustomLexer(self)
        return self._custom_lexer.tokenize(source, name, filename, state)


class LabelsProvider:
    """
    A custom class to provide label access in Jinja2 templates.

    Supports both simple and nested label access:
    - {{labels.mylabel}} - accesses label "mylabel"
    - {{labels.mylabel.with-dash}} - accesses label "mylabel.with-dash"

    The class builds up the label path through attribute access and resolves
    it when the value is needed.
    """

    def __init__(self, labels: Dict[str, Any], path: str = "", strict: bool = False):
        """
        Initialize the LabelsProvider.

        Args:
            labels: Dictionary containing all available labels
            path: Current path being built (used internally for nested access)
        """
        self._labels = labels
        self._path = path
        self._strict = strict

    def __getattr__(self, name: str) -> "LabelsProvider":
        """
        Handle attribute access to build up label paths.

        Args:
            name: The attribute name being accessed

        Returns:
            A new LabelsProvider instance with extended path
        """
        # Avoid infinite recursion for special attributes
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        # Build the new path
        new_path = f"{self._path}.{name}" if self._path else name
        return LabelsProvider(self._labels, path=new_path, strict=self._strict)

    def __getitem__(self, key: str) -> "LabelsProvider":
        """
        Handle item access for labels with special characters.

        Args:
            key: The key being accessed

        Returns:
            A new LabelsProvider instance with extended path
        """
        new_path = f"{self._path}.{key}" if self._path else key
        return LabelsProvider(self._labels, new_path, strict=self._strict)

    def __str__(self) -> str:
        """
        Resolve the label path and return its value.

        Returns:
            The label value as a string, or empty string if not found
        """
        if not self._path:
            raise KeyError(f"No label specified in path '{self._path}'")

        # Look up the value in the labels dictionary
        if self._strict and self._path not in self._labels:
            raise KeyError(f"Label '{self._path}' not found in labels")
        value = self._labels.get(self._path, "")
        return str(value) if value is not None else ""


def setup_argparser(args: List[str]) -> argparse.Namespace:  # pragma: no cover
    """
    Setup argument parser.

    Returns:
        Initialized argument parser
    """
    parser = argparse.ArgumentParser(
        description="Process Jinja2 template with input data and output the result. "
        "Custom --labels-ext extension allows accessing labels with "
        "hyphens in their names."
    )

    parser.add_argument(
        "--template",
        help="Path to the template file to process. If not specified, reads from stdin.",
        required=False,
    )
    parser.add_argument(
        "--data",
        help="Path to JSON file containing input data",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Path to the output file. If not specified, prints to stdout.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--labels-ext",
        action="store_true",
        help=(
            "Use custom Jinja2 extension to handle labels "
            + "in identifiers (e.g. labels.mylabel.with-dash)"
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Use strict undefined behavior (raise error on undefined variables)",
    )
    parser.add_argument(
        "--allow-empty-inputs",
        action="store_true",
        help='Allow empty inputs (e.g. empty string ("") or null) without raising errors',
    )

    return parser.parse_args(args)


def validate_input_data(input_data: Dict[str, Any], allow_empty_inputs: bool = False):
    """Validate input data before processing.
    Args:
        input_data: The input data dictionary to validate
        allow_empty_inputs: Whether to allow empty inputs without raising errors
    Raises:
        ValueError: If required fields are missing or if empty inputs are not allowed
    """
    for key, value in input_data.items():
        if (value is None or value == "") and not allow_empty_inputs:
            raise ValueError(f"Input '{key}' is empty but empty inputs are not allowed")


def setup_jinja(
    input_data: Dict[str, Any], labels_ext: bool = False, strict: bool = False
) -> Environment:

    undefined_class = StrictUndefined if strict else DebugUndefined

    if labels_ext:
        # Create LabelsProvider instance
        labels_dict = input_data.get("labels", {})
        labels_provider = LabelsProvider(labels_dict, strict=strict)

        input_data["labels"] = labels_provider

        # Create Jinja2 environment with custom extension
        env = CustomEnvironment(undefined=undefined_class)
    else:
        env = Environment(undefined=undefined_class)

    return env


def subst_template(
    env: Environment, template_str: str, data: Dict[str, Any], allow_empty_inputs: bool = True
) -> str:
    template = env.from_string(template_str)
    validate_input_data(data, allow_empty_inputs=allow_empty_inputs)
    content = template.render(data)
    return content


def load_input_data(path: str) -> Dict[str, Any]:
    LOGGER.info(f"Loading input data from {path}")
    with open(path, "r") as f:
        return json.load(f)


def load_template(path: Optional[str]) -> str:
    if path:
        LOGGER.info(f"Loading template from {path}")
        with open(path, "r") as f:
            return f.read()
    else:
        LOGGER.info("Loading template from stdin")
        return sys.stdin.read()


def write_output(content: str, path: Optional[str] = None):
    if path:
        LOGGER.info(f"Writing output to {path}")
        with open(path, "w") as f:
            f.write(content)
    else:
        LOGGER.info("Writing output to stdout")
        print(content)


def main(args):
    """Main function."""
    args = setup_argparser(args)
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logger(level=log_level)

    # Load input data
    input_data = load_input_data(args.data)
    env = setup_jinja(input_data, labels_ext=args.labels_ext, strict=args.strict)

    # Load and render template
    template_str = load_template(args.template)

    LOGGER.info("Rendering template")
    content = subst_template(
        env, template_str, input_data, allow_empty_inputs=args.allow_empty_inputs
    )

    # Output result
    write_output(content, args.output)


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
