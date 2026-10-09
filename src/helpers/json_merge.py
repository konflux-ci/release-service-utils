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

# Objects are merged recursively. Arrays of objects with a "name" field are
# merged by matching on that field; other arrays are joined and deduplicated.
# Scalars: b wins, unless b is null in which case a is kept. The != null check
# (not //) is intentional: jq's // also triggers on false, which would drop an
# explicit false from b.
#
# merge_named_arrays is defined inside merge_objects so it can call back into
# merge_objects; jq does not allow a top-level function to reference one defined later.
_MERGE_DEEP_UNION_ARRAYS_PROGRAM = jq.compile("""
    def is_named:   type == "object" and has("name");
    def is_unnamed: is_named | not;
    def has_named_objects: any(.[]; is_named);

    def merge_objects(base; overlay):
      def merge_named_arrays(base_arr; overlay_arr):
        # build index by merging same-named base entries, not last-wins
        (base_arr | map(select(is_named))
                  | reduce .[] as $item (
                      {};
                      .[$item.name] = merge_objects((.[$item.name] // {}); $item)
                    )) as $base_index |
        # unnamed items are kept as-is from both sides
        (base_arr    | map(select(is_unnamed))) as $base_unnamed |
        (overlay_arr | map(select(is_unnamed))) as $overlay_unnamed |
        # merge each overlay entry with its base counterpart; new names start from {}
        (overlay_arr | map(select(is_named))
                     | reduce .[] as $item (
                         $base_index;
                         .[$item.name] = merge_objects((.[$item.name] // {}); $item)
                       )) as $result |
        [$result[]] + $base_unnamed + $overlay_unnamed;
      base as $base | overlay as $overlay |
      ($base | keys) + ($overlay | keys) | unique | map({
        key: .,
        value: (
          if ($base[.] | type) == "object" and ($overlay[.] | type) == "object" then
            merge_objects($base[.]; $overlay[.])
          elif ($base[.] | type) == "array" and ($overlay[.] | type) == "array" then
            if ($base[.] | has_named_objects) or ($overlay[.] | has_named_objects) then
              merge_named_arrays($base[.]; $overlay[.])
            else
              ($base[.] + $overlay[.]) | unique
            end
          else
            if ($overlay[.] != null) then $overlay[.] else $base[.] end
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
    """Recursively merge two JSON objects.

    Objects are merged recursively.  Arrays whose elements carry a ``"name"``
    key are merged by that key; same-named entries are combined rather than
    duplicated.  All other arrays are concatenated and deduplicated via
    ``jq unique``.  For any other type ``b``'s value wins; if ``b``'s value is
    ``None``, ``a``'s value is kept.
    """
    return _MERGE_DEEP_UNION_ARRAYS_PROGRAM.input_value([a, b]).first()
