"""Redact credentials from text before logging or writing Tekton results."""

from .redact import RedactHelper, redact_secrets  # noqa: F401
