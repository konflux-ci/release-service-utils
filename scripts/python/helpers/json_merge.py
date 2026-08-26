"""JSON merge helpers backed by the ``jq`` library, used by release tasks.

These wrap small ``jq`` programs (rather than reimplementing ``jq``'s merge
and ordering semantics in Python) so behavior is guaranteed to match ``jq``
exactly, including edge cases like its total ordering of JSON value types
(``null < false < true < number < string < array < object``) and its
recursive object-multiply (``*``) semantics.
"""

from __future__ import annotations

from typing import Any

import jq

_UNIQUE_PROGRAM = jq.compile(". | unique")
_MULTIPLY_PROGRAM = jq.compile(".[0] * .[1]")

# Mirrors the ``merge-json`` shell utility (``utils/merge-json``): objects are
# merged recursively, arrays are concatenated and deduplicated/sorted (``jq``'s
# ``unique``), and any other type has ``b``'s value win unless ``b``'s value
# is ``null``, in which case ``a``'s value is kept. The explicit ``!= null``
# check (rather than ``//``) is required to preserve literal ``false`` values
# from ``b``, since ``//`` treats ``false`` the same as ``null``.
_MERGE_DEEP_UNION_ARRAYS_PROGRAM = jq.compile("""
    def merge_objects(a; b):
      a as $a | b as $b |
      ($a | keys) + ($b | keys) | unique | map({
        key: .,
        value: (
          if ($a[.] | type) == "object" and ($b[.] | type) == "object" then
            merge_objects($a[.]; $b[.])
          elif ($a[.] | type) == "array" and ($b[.] | type) == "array" then
            ($a[.] + $b[.]) | unique
          else
            if ($b[.] != null) then $b[.] else $a[.] end
          end
        )
      }) | from_entries;
    .[0] as $first | .[1] as $second | merge_objects($first; $second)
    """)


def unique_sorted(values: list[Any]) -> list[Any]:
    """Sort ``values`` and drop duplicates, mirroring ``jq``'s ``unique``."""
    return _UNIQUE_PROGRAM.input_value(values).first()


def jq_multiply(a: Any, b: Any) -> Any:
    """Merge ``a`` and ``b`` like ``jq``'s ``*`` (object multiply) operator.

    When both operands are objects, they are merged recursively: keys present
    in both that are themselves objects are merged recursively, and any other
    key is taken from ``b``. Only intended for object operands, matching this
    module's callers; other ``jq`` ``*`` behaviors (e.g. number multiplication,
    string repetition) apply for non-object inputs, and mismatched types
    ``jq`` can't multiply (e.g. two arrays) raise ``ValueError``.
    """
    return _MULTIPLY_PROGRAM.input_value([a, b]).first()


def merge_deep_union_arrays(a: dict, b: dict) -> dict:
    """Recursively merge two JSON objects, unioning arrays instead of replacing them.

    Mirrors the ``merge-json`` shell utility: object values are merged
    recursively, array values are concatenated and deduplicated (via
    :func:`unique_sorted`), and any other type has ``b``'s value win unless
    ``b``'s value is ``None``, in which case ``a``'s value is kept.
    """
    return _MERGE_DEEP_UNION_ARRAYS_PROGRAM.input_value([a, b]).first()
