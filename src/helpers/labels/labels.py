"""Helpers for reading labels from Konflux snapshot component metadata."""

from __future__ import annotations

from typing import Any


def get_label_value(component: dict[str, Any], label_name: str) -> str | None:
    """Extract a label value from a component's metadata labels list.

    Return ``None`` when the label key is absent from the metadata. Return
    an empty string when the label is present but its value is empty or
    whitespace-only, so callers can distinguish an unset label from one
    that was set to an empty value.
    """
    labels = component.get("metadata", {}).get("labels") or []
    for label in labels:
        if label.get("name") == label_name:
            val = label.get("value")
            return str(val).strip() if val is not None else ""
    return None
