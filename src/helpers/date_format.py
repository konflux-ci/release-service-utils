"""Timestamp formatting via the system ``date`` command.

Format strings supplied by users (``timestampFormat`` in release mappings)
are arbitrary GNU ``date`` format specifiers (``%Y``, `%s`` for Unix epoch,
etc.). Rather than reimplementing GNU date's parsing/formatting rules in
Python, this helper shells out to ``date`` so behavior matches exactly.
"""

from __future__ import annotations

from release_service_utils.helpers.subprocess_cmd import run_cmd_text


def current_timestamp() -> str:
    """Return the current local date/time formatted as ``%Y%m%d %T``."""
    return run_cmd_text(["date", "+%Y%m%d %T"]).strip()


def format_date(date_string: str, date_format: str) -> str:
    """Reformat ``date_string`` using ``date_format``.

    ``date_string`` can be any value GNU ``date -d`` accepts (e.g. an ISO 8601
    timestamp, or the fixed ``%Y%m%d %T`` format returned by
    :func:`current_timestamp`).

    Raises:
        subprocess.CalledProcessError: if ``date_string`` cannot be parsed.

    """
    return run_cmd_text(["date", "-d", date_string, f"+{date_format}"]).strip()
