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

# Objects are merged recursively, arrays are combined and deduped, and "b"
# wins on anything else unless it's null. On top of that, an array field
# named in $merge_keys is matched up by identity and merged entry by entry
# instead of just being combined, so two "repositories" lists for the same
# thing don't end up with two entries where there should be one.
_MERGE_DEEP_UNION_ARRAYS_PROGRAM = jq.compile("""
    def has_identity($id_fields):   type == "object" and has($id_fields[0]);
    def lacks_identity($id_fields): has_identity($id_fields) | not;
    def any_has_identity($id_fields): any(.[]; has_identity($id_fields));
    # Turns an entry's identity field(s) into one string, so we can match on it.
    def key_of($id_fields): [$id_fields[] as $f | .[$f]] | tostring;

    def merge_objects(a; b; $merge_keys):
      # Merges two arrays entry by entry, matching on $id_fields instead of
      # just appending. Entries without an identity field pass through as is.
      def merge_by_identity(a_arr; b_arr; $id_fields):
        def merge_in(acc; arr):
          reduce arr[] as $item (
            acc;
            ($item | key_of($id_fields)) as $k |
            .[$k] = merge_objects((.[$k] // {}); $item; $merge_keys)
          );
        (a_arr | map(select(lacks_identity($id_fields)))) as $a_rest |
        (b_arr | map(select(lacks_identity($id_fields)))) as $b_rest |
        (merge_in({}; a_arr | map(select(has_identity($id_fields))))) as $merged_a |
        (merge_in($merged_a; b_arr | map(select(has_identity($id_fields))))) as $merged |
        # Sorted and deduplicated, same as any plain array so output order doesn't
        # depend on fold/insertion order.
        ([$merged[]] + $a_rest + $b_rest) | unique;

      a as $a | b as $b |
      ($a | keys) + ($b | keys) | unique | map({
        key: .,
        value: (
          if ($a[.] | type) == "object" and ($b[.] | type) == "object" then
            merge_objects($a[.]; $b[.]; $merge_keys)
          elif ($a[.] | type) == "array" and ($b[.] | type) == "array" then
            . as $field |
            # Only merge by identity if this field is configured for it and
            # either side actually has a matchable entry. Otherwise, just
            # combine and deduplicate.
            if ($merge_keys | has($field)) and
               (($a[$field] | any_has_identity($merge_keys[$field]))
                or ($b[$field] | any_has_identity($merge_keys[$field]))) then
              merge_by_identity($a[$field]; $b[$field]; $merge_keys[$field])
            else
              ($a[.] + $b[.]) | unique
            end
          else
            # != null, not //, because // also triggers on false and would
            # silently drop an explicit false from b.
            if ($b[.] != null) then $b[.] else $a[.] end
          end
        )
      }) | from_entries;

    .[0] as $first | .[1] as $second | .[2] as $merge_keys |
    merge_objects($first; $second; $merge_keys)
    """)


# Objects are merged recursively (jq's ``*``, same as jq_multiply), arrays
# present on both sides are concatenated with no dedup or sort and any other
# type has ``b``'s value win unless it is ``null`` or ``false`` (jq's ``//``),
# in which case ``a``'s value is kept.
_MERGE_CONCAT_ARRAYS_PROGRAM = jq.compile("""
    def merge_result(a; b):
      # Store current values as $base and get all unique keys from both objects
      a as $base | b as $new |
      ($base | keys) + ($new | keys) | unique |
      # Process each key and build the merged result
      reduce .[] as $key ({}; . + {($key): (
        # Case 1: Both values are arrays - concatenate them
        if ($new[$key] | type == "array") and ($base[$key] | type == "array")
        then $base[$key] + $new[$key]
        else
          # Case 2: Both values are objects - merge them recursively
          if ($new[$key] | type == "object") and ($base[$key] | type == "object")
          then $base[$key] * $new[$key]
          # Case 3: Default - use new value or fall back to base value
          else $new[$key] // $base[$key]
          end
        end
      )});
    .[0] as $first | .[1] as $second | merge_result($first; $second)
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


def merge_deep_union_arrays(
    a: dict,
    b: dict,
    merge_keys: dict[str, list[str]] | None = None,
) -> dict:
    """Recursively merge two JSON objects, unioning arrays instead of replacing them.

    Objects are merged recursively. Array of object fields listed in
    ``merge_keys`` are matched up by identity and merged entry by entry
    instead of duplicated. Every other array is concatenated and
    deduplicated. For any other type ``b``'s value wins unless it's ``None``,
    in which case ``a``'s value is kept.
    """
    return _MERGE_DEEP_UNION_ARRAYS_PROGRAM.input_value([a, b, merge_keys or {}]).first()


def merge_concat_arrays(a: dict, b: dict) -> dict:
    """Recursively merge two JSON objects, concatenating arrays instead of unioning them.

    Object values are merged recursively, array values are concatenated with no
    dedup or sort and any other type has ``b``'s value win unless it is ``None``
    or ``False``, in which case ``a``'s value is kept.
    """
    return _MERGE_CONCAT_ARRAYS_PROGRAM.input_value([a, b]).first()
