"""Tests for credential redaction helpers."""

from __future__ import annotations

import pytest

from redact import redact_secrets


def test_redact_secrets_oauth_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """oauth2 credentials in URLs are redacted."""
    monkeypatch.setenv("ACCESS_TOKEN", "sekret")
    raw = "fatal: https://oauth2:sekret@gitlab.com/org/r.git"
    assert redact_secrets(raw) == "fatal: https://oauth2:[REDACTED]@gitlab.com/org/r.git"


def test_redact_secrets_access_token_in_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """``ACCESS_TOKEN`` values are redacted from text."""
    monkeypatch.setenv("ACCESS_TOKEN", "my-token-xyz")
    assert redact_secrets("error: my-token-xyz denied") == "error: [REDACTED] denied"


def test_redact_secrets_skips_short_access_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Very short ``ACCESS_TOKEN`` values are not replaced inside unrelated words."""
    monkeypatch.setenv("ACCESS_TOKEN", "t")
    msg = "logging in with Kerberos (kinit): Command 'kinit' returned non-zero exit status 1."
    assert redact_secrets(msg) == msg


def test_redact_secrets_access_token_assignment() -> None:
    """``ACCESS_TOKEN=...`` assignments are redacted."""
    assert redact_secrets("env ACCESS_TOKEN=glpat-abc") == "env ACCESS_TOKEN=[REDACTED]"


def test_redact_secrets_empty_text() -> None:
    """Empty input is returned unchanged."""
    assert redact_secrets("") == ""
