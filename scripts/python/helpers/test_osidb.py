"""Tests for the ``osidb`` helper module."""

from __future__ import annotations

from unittest import mock

import pytest
import requests_kerberos

import osidb


def test_get_access_token_returns_access_string() -> None:
    """Return a non-empty string ``access`` from a JSON body.

    The token URL is ``{base}/auth/token`` (trailing slash on the base is
    stripped before join).
    """
    with mock.patch("http_client.get_text", return_value='{"access": "tok-abc"}') as m:
        out = osidb.get_access_token("https://osidb.example.com/")
    assert out == "tok-abc"
    m.assert_called_once()
    cargs, ckwargs = m.call_args
    assert cargs[0] == "https://osidb.example.com/auth/token"
    assert "auth" in ckwargs
    assert isinstance(ckwargs["auth"], requests_kerberos.HTTPKerberosAuth)


def test_get_access_token_rejects_empty_body() -> None:
    """A blank HTTP body raises with a message about the token request."""
    with mock.patch("http_client.get_text", return_value=""):
        with pytest.raises(ValueError) as e:
            osidb.get_access_token("https://u")
    assert e.value
    assert "empty" in str(e.value) or "token" in str(e.value)


def test_fetch_flaw_response_builds_url_and_headers() -> None:
    """Flaws GET uses the v2 endpoint with bearer auth and include_fields."""
    with mock.patch("http_client.get_text", return_value="{}") as get_text:
        osidb.fetch_flaw_response(
            "https://osidb.example/",
            "tok-abc",
            "CVE-2024-1",
            include_fields="cve_id,embargoed",
        )
    get_text.assert_called_once()
    url = get_text.call_args.args[0]
    assert url.startswith("https://osidb.example/osidb/api/v2/flaws?")
    assert "cve_id=CVE-2024-1" in url
    assert "include_fields=cve_id%2Cembargoed" in url
    assert get_text.call_args.kwargs["headers"]["Authorization"] == "Bearer tok-abc"


@pytest.mark.parametrize("body", ['{"x": 1}', '{"access": ""}'])
def test_get_access_token_rejects_invalid_access(body: str) -> None:
    """``ValueError`` when JSON has no non-empty string ``access`` (missing or empty)."""
    with mock.patch("http_client.get_text", return_value=body):
        with pytest.raises(ValueError) as e:
            osidb.get_access_token("https://u")
    assert "access" in str(e.value) or e.value
