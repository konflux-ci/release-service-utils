"""Tests for the ``cgw_idempotency.call_cgw_api`` retry behavior."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from utils.cgw_idempotency import call_cgw_api


def _mock_response(
    *,
    ok: bool = True,
    status_code: int = 200,
    reason: str = "OK",
    text: str = "",
    json_value: object | None = None,
) -> Mock:
    response = Mock()
    response.ok = ok
    response.status_code = status_code
    response.reason = reason
    response.text = text
    response.json.return_value = json_value
    return response


def test_call_cgw_api_retries_connection_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ConnectionError on an idempotent GET is retried until it succeeds."""
    monkeypatch.setattr("time.sleep", lambda _: None)

    session = Mock()
    session.request.side_effect = [
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        _mock_response(json_value=[{"id": 123}]),
    ]

    response = call_cgw_api(
        host="https://cgw.example.com",
        method="GET",
        endpoint="/products/1/versions/2/files",
        session=session,
    )

    assert response.json() == [{"id": 123}]
    assert session.request.call_count == 3


def test_call_cgw_api_retries_update_post_with_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ConnectionError on an update POST (carries an ``id``) is safe to retry."""
    monkeypatch.setattr("time.sleep", lambda _: None)

    session = Mock()
    session.request.side_effect = [
        requests.exceptions.ConnectionError("Connection aborted."),
        _mock_response(json_value={"id": 123}),
    ]

    response = call_cgw_api(
        host="https://cgw.example.com",
        method="POST",
        endpoint="/products/1/versions/2/files",
        session=session,
        data={"id": 123, "label": "updated"},
    )

    assert response.json() == {"id": 123}
    assert session.request.call_count == 2


def test_call_cgw_api_does_not_retry_create_post_on_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A create POST (no ``id``) is not retried: retrying could create a duplicate file."""
    monkeypatch.setattr("time.sleep", lambda _: None)

    session = Mock()
    session.request.side_effect = requests.exceptions.ConnectionError("Connection aborted.")

    with pytest.raises(RuntimeError, match="API call failed"):
        call_cgw_api(
            host="https://cgw.example.com",
            method="POST",
            endpoint="/products/1/versions/2/files",
            session=session,
            data={"label": "new file", "shortURL": "/pub/example/file.iso"},
        )

    assert session.request.call_count == 1


def test_call_cgw_api_raises_after_exhausting_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """When every attempt hits a ConnectionError, the last one is wrapped and raised."""
    monkeypatch.setattr("time.sleep", lambda _: None)

    session = Mock()
    session.request.side_effect = requests.exceptions.ConnectionError(
        "('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))"
    )

    with pytest.raises(RuntimeError, match="API call failed.*Connection reset by peer"):
        call_cgw_api(
            host="https://cgw.example.com",
            method="GET",
            endpoint="/products",
            session=session,
        )

    assert session.request.call_count == 3


def test_call_cgw_api_does_not_retry_http_error_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-2xx response is a real error and must not be retried."""
    monkeypatch.setattr("time.sleep", lambda _: None)

    session = Mock()
    session.request.return_value = _mock_response(
        ok=False, status_code=400, reason="Bad Request", text="invalid product code"
    )

    with pytest.raises(RuntimeError, match="API call failed: invalid product code"):
        call_cgw_api(
            host="https://cgw.example.com",
            method="GET",
            endpoint="/products",
            session=session,
        )

    assert session.request.call_count == 1
