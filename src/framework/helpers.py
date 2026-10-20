"""Base classes for helper modules."""

from __future__ import annotations

from abc import ABC


class Helper(ABC):
    """Base class for helper functionality.

    Helpers encapsulate reusable logic (API clients, data processing, etc).
    Utility functions that don't represent core helper functionality should
    remain as standalone module-level functions.

    Subclasses are stateless - all state passed as method arguments.
    Instantiation is primarily for dependency injection in tests.
    """

    pass


class BinaryHelper(Helper):
    """Base class for helpers that wrap external CLI binaries.

    Subclasses must declare the binary they wrap via the `bin` class attribute.
    This enables automatic dependency tracking for test selection.

    Example:
        class Skopeo(BinaryHelper):
            bin = "skopeo"

            def inspect(self, image_ref: str) -> dict:
                # Wraps skopeo inspect command
                ...

    """

    bin: str  # Required class attribute - which binary this helper wraps
