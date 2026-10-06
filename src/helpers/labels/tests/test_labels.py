"""Test the shared ``labels`` helper module."""

from __future__ import annotations

from release_service_utils.helpers.labels import get_label_value


def _make_component(
    labels: list[dict] | None = None,
) -> dict:
    """Build a minimal component dict with metadata labels."""
    comp: dict = {"name": "comp1", "metadata": {}}
    if labels is not None:
        comp["metadata"]["labels"] = labels
    return comp


def test_get_label_value_found() -> None:
    """Return the value when the label exists."""
    comp = _make_component(labels=[{"name": "foo", "value": "bar"}])
    assert get_label_value(comp, "foo") == "bar"


def test_get_label_value_missing() -> None:
    """Return None when the label is absent."""
    comp = _make_component(labels=[])
    assert get_label_value(comp, "foo") is None


def test_get_label_value_no_labels_key() -> None:
    """Return None when metadata has no labels at all."""
    comp = _make_component()
    assert get_label_value(comp, "foo") is None


def test_get_label_value_present_but_empty() -> None:
    """Return an empty string when the label value is empty."""
    comp = _make_component(labels=[{"name": "foo", "value": ""}])
    assert get_label_value(comp, "foo") == ""


def test_get_label_value_present_but_whitespace() -> None:
    """Return an empty string when the label value is whitespace-only."""
    comp = _make_component(labels=[{"name": "foo", "value": "   "}])
    assert get_label_value(comp, "foo") == ""


def test_get_label_value_none_value() -> None:
    """Return an empty string when the label value is None."""
    comp = _make_component(labels=[{"name": "foo", "value": None}])
    assert get_label_value(comp, "foo") == ""


def test_get_label_value_strips_whitespace() -> None:
    """Strip leading and trailing whitespace from the label value."""
    comp = _make_component(labels=[{"name": "foo", "value": "  bar  "}])
    assert get_label_value(comp, "foo") == "bar"


def test_get_label_value_null_labels() -> None:
    """Return None when labels is explicitly null."""
    comp = {"name": "comp1", "metadata": {"labels": None}}
    assert get_label_value(comp, "foo") is None


def test_get_label_value_empty_metadata() -> None:
    """Return None when metadata is empty dict."""
    comp = {"name": "comp1", "metadata": {}}
    assert get_label_value(comp, "foo") is None
