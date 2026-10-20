"""Redact credentials from text before logging or writing Tekton results."""

from __future__ import annotations

import os
import re

from release_service_utils.framework import Helper

_HTTPS_CREDENTIAL_URL = re.compile(
    r"https://([^/@\s:]+):([^@\s]+)@",
    re.IGNORECASE,
)
_ACCESS_TOKEN_ASSIGNMENT = re.compile(
    r"(ACCESS_TOKEN=)\S+",
    re.IGNORECASE,
)


class RedactHelper(Helper):
    """Redact credentials from text before logging or writing Tekton results."""

    def redact_secrets(self, text: str) -> str:
        """Redact credential URLs, ``ACCESS_TOKEN=...``, and the ``ACCESS_TOKEN`` env value."""
        if not text:
            return ""
        out = _HTTPS_CREDENTIAL_URL.sub(r"https://\1:[REDACTED]@", text)
        out = _ACCESS_TOKEN_ASSIGNMENT.sub(r"\1[REDACTED]", out)
        token = os.environ.get("ACCESS_TOKEN")
        # Skip very short values: they match substrings inside normal
        # words (e.g. "t" in "kinit").
        if token and len(token) >= 8:
            out = out.replace(token, "[REDACTED]")
        return out


# Singleton instance for backward compatibility
_helper = RedactHelper()

# Export instance methods as module-level functions
redact_secrets = _helper.redact_secrets
