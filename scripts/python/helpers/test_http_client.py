"""Tests for the ``http_client`` helper module."""

from __future__ import annotations

import unittest.mock as mock

import pytest
import requests
from requests.adapters import HTTPAdapter

import http_client


def test_get_retry_session_custom_methods_and_status_codes() -> None:
    """Callers can configure retried methods and HTTP status codes."""
    session = http_client.get_retry_session(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=5.0,
        status_forcelist=(503,),
        allowed_methods=frozenset({"PATCH"}),
    )
    adapter = session.adapters["https://"]
    assert isinstance(adapter, HTTPAdapter)
    retry = adapter.max_retries
    assert retry.total == 5
    assert retry.connect == 5
    assert retry.read == 5
    assert retry.status == 5
    assert retry.backoff_factor == 5.0
    assert set(retry.status_forcelist) == {503}
    assert set(retry.allowed_methods) == {"PATCH"}


def test_get_retry_session_get_defaults() -> None:
    """GET retry settings used by get_text mount adapters on both schemes."""
    session = http_client.get_retry_session(
        total=3,
        connect=3,
        read=3,
        status=2,
        backoff_factor=0.4,
        allowed_methods=frozenset({"GET"}),
    )
    https_adapter = session.adapters["https://"]
    http_adapter = session.adapters["http://"]
    assert isinstance(https_adapter, HTTPAdapter)
    assert isinstance(http_adapter, HTTPAdapter)
    assert https_adapter.max_retries.total == 3
    assert http_adapter.max_retries.total == 3
    assert set(https_adapter.max_retries.allowed_methods) == {"GET"}


def test_get_text_uses_session_and_headers() -> None:
    """``get_text`` passes *headers* through to the session ``.get`` call."""
    session = mock.MagicMock()
    r = mock.MagicMock()
    r.status_code = 200
    r.text = "body"
    r.raise_for_status = mock.MagicMock()
    session.get.return_value = r
    with mock.patch("http_client.get_retry_session", return_value=session):
        out = http_client.get_text("https://e/x", headers={"A": "b", "C": "d"})

    assert out == "body"
    assert session.get.call_count == 1
    cargs, ckw = session.get.call_args
    assert cargs[0] == "https://e/x"
    assert ckw["headers"] == {"A": "b", "C": "d"}
    assert ckw["auth"] is None
    r.raise_for_status.assert_not_called()


def test_get_text_auth_passed() -> None:
    """*auth* is passed through (e.g. for SPNEGO)."""
    session = mock.MagicMock()
    r = mock.MagicMock()
    r.status_code = 200
    r.text = "t"
    session.get.return_value = r
    ra = object()
    with mock.patch("http_client.get_retry_session", return_value=session):
        assert http_client.get_text("https://u", auth=ra) == "t"
    assert session.get.call_args[1]["auth"] is ra


def test_get_text_http_error() -> None:
    """A failed HTTP status raises ``requests.HTTPError`` (like ``curl --fail``)."""
    session = mock.MagicMock()
    r = mock.MagicMock()
    r.status_code = 500
    r.raise_for_status.side_effect = requests.HTTPError("nope", response=mock.MagicMock())
    session.get.return_value = r
    with mock.patch("http_client.get_retry_session", return_value=session):
        with pytest.raises(requests.HTTPError):
            http_client.get_text("https://u/")


def test_get_text_retries_429_then_succeeds() -> None:
    """HTTP 429 is retried with backoff; a later 2xx response returns body text."""
    session = mock.MagicMock()

    r1 = mock.MagicMock()
    r1.status_code = 429
    r1.text = "rate-limited-1"
    r1.raise_for_status.side_effect = requests.HTTPError("429", response=mock.MagicMock())
    r2 = mock.MagicMock()
    r2.status_code = 429
    r2.text = "rate-limited-2"
    r2.raise_for_status.side_effect = requests.HTTPError("429", response=mock.MagicMock())
    r3 = mock.MagicMock()
    r3.status_code = 200
    r3.text = "ok"
    r3.raise_for_status = mock.MagicMock()
    session.get.side_effect = [r1, r2, r3]

    with (
        mock.patch("http_client.get_retry_session", return_value=session),
        mock.patch("http_client.random.randint", return_value=0),
        mock.patch("http_client.time.sleep") as sleep_mock,
    ):
        assert http_client.get_text("https://u") == "ok"

    assert session.get.call_count == 3
    sleep_mock.assert_has_calls([mock.call(1), mock.call(2)])


def test_get_text_retries_404_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP 404 retries happen only when `CURL_WITH_RETRY_RETRY_404` is set."""
    monkeypatch.setenv("CURL_WITH_RETRY_RETRY_404", "1")
    session = mock.MagicMock()

    r1 = mock.MagicMock()
    r1.status_code = 404
    r1.text = "not-found"
    r1.raise_for_status.side_effect = requests.HTTPError("404", response=mock.MagicMock())
    r2 = mock.MagicMock()
    r2.status_code = 200
    r2.text = "ok"
    r2.raise_for_status = mock.MagicMock()
    session.get.side_effect = [r1, r2]

    with (
        mock.patch("http_client.get_retry_session", return_value=session),
        mock.patch("http_client.random.randint", return_value=0),
        mock.patch("http_client.time.sleep") as sleep_mock,
    ):
        assert http_client.get_text("https://u") == "ok"

    assert session.get.call_count == 2
    sleep_mock.assert_called_once_with(1)


def test_get_text_allow_error_status_returns_body() -> None:
    """Non-2xx responses return body text when `allow_error_status` is true."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.status_code = 404
    response.text = '{"error": true}'
    session.get.return_value = response
    with mock.patch("http_client.get_retry_session", return_value=session):
        out = http_client.get_text("https://u/", allow_error_status=True)
    assert out == '{"error": true}'


def test_get_text_allow_error_status_retries_429() -> None:
    """429 retries still run when `allow_error_status` is true."""
    session = mock.MagicMock()

    rate_limited = mock.MagicMock()
    rate_limited.status_code = 429
    rate_limited.text = "rate-limited"
    ok = mock.MagicMock()
    ok.status_code = 200
    ok.text = "ok"
    session.get.side_effect = [rate_limited, ok]

    with (
        mock.patch("http_client.get_retry_session", return_value=session),
        mock.patch("http_client.random.randint", return_value=0),
        mock.patch("http_client.time.sleep") as sleep_mock,
    ):
        out = http_client.get_text("https://u/", allow_error_status=True)

    assert out == "ok"
    assert session.get.call_count == 2
    sleep_mock.assert_called_once_with(1)


def test_get_text_cert_on_session() -> None:
    """Client certificate paths are applied to the shared session."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.status_code = 200
    response.text = "ok"
    session.get.return_value = response
    cert = ("/tmp/cert", "/tmp/key")
    with mock.patch("http_client.get_retry_session", return_value=session):
        assert http_client.get_text("https://u/", cert=cert) == "ok"
    assert session.cert == cert
