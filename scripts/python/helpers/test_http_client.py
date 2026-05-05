"""Tests for the ``http_client`` helper module."""

from __future__ import annotations

import unittest.mock as mock

import pytest
import requests
from requests.adapters import HTTPAdapter

import http_client


def test_retries_policy_defaults() -> None:
    """The shared retry config matches the module defaults."""
    r = http_client._retries()
    assert r.total == 3
    assert r.connect == 3
    assert r.read == 3
    assert r.status == 2
    assert r.backoff_factor == 0.4
    assert r.raise_on_status is False
    assert set(r.status_forcelist) == {500, 502, 503, 504}
    assert set(r.allowed_methods) == {"GET"}


def test_get_session_mounts_retry_adapter() -> None:
    """`get_session` mounts an `HTTPAdapter` for both HTTP schemes."""
    s = http_client.get_session()
    https_adapter = s.adapters["https://"]
    http_adapter = s.adapters["http://"]
    assert isinstance(https_adapter, HTTPAdapter)
    assert isinstance(http_adapter, HTTPAdapter)
    assert https_adapter.max_retries.total == 3
    assert http_adapter.max_retries.total == 3


def test_get_text_uses_session_and_headers() -> None:
    """``get_text`` passes *headers* through to ``get_session``’s ``.get`` call."""
    session = mock.MagicMock()
    r = mock.MagicMock()
    r.status_code = 200
    r.text = "body"
    r.raise_for_status = mock.MagicMock()
    session.get.return_value = r
    with mock.patch("http_client.get_session", return_value=session):
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
    with mock.patch("http_client.get_session", return_value=session):
        assert http_client.get_text("https://u", auth=ra) == "t"
    assert session.get.call_args[1]["auth"] is ra


def test_get_text_http_error() -> None:
    """A failed HTTP status raises ``requests.HTTPError`` (like ``curl --fail``)."""
    session = mock.MagicMock()
    r = mock.MagicMock()
    r.status_code = 500
    r.raise_for_status.side_effect = requests.HTTPError("nope", response=mock.MagicMock())
    session.get.return_value = r
    with mock.patch("http_client.get_session", return_value=session):
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
        mock.patch("http_client.get_session", return_value=session),
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
        mock.patch("http_client.get_session", return_value=session),
        mock.patch("http_client.random.randint", return_value=0),
        mock.patch("http_client.time.sleep") as sleep_mock,
    ):
        assert http_client.get_text("https://u") == "ok"

    assert session.get.call_count == 2
    sleep_mock.assert_called_once_with(1)
