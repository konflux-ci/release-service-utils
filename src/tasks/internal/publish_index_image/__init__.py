"""Publish Index Image - Copies an index image from source to target registry.

This script implements idempotent image publishing with digest-based deduplication.
"""

from . import publish_index_image  # noqa: F401
