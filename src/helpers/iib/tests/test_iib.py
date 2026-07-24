"""Tests for the ``iib`` helper module."""

from __future__ import annotations

import json
import unittest.mock as mock

import pytest
import requests

import iib

# ---------------------------------------------------------------------------
# compress / decompress round-trip
# ---------------------------------------------------------------------------


def test_compress_decompress_roundtrip() -> None:
    """Compressed data decompresses back to the original dict."""
    data: iib.IIBBuild = {
        "id": 42,
        "state": "complete",
        "from_index": "registry.example.com/idx:v4.12",
        "fbc_fragments": ["frag-a", "frag-b"],
    }
    compressed = iib.compress_build_info(data)
    assert isinstance(compressed, str)
    assert iib.decompress_build_info(compressed) == data


def test_compress_returns_ascii_string() -> None:
    """The compressed string contains only base64-safe ASCII characters."""
    data: iib.IIBBuild = {"id": 1, "state": "in_progress"}
    compressed = iib.compress_build_info(data)
    compressed.encode("ascii")


def test_decompress_invalid_base64_raises() -> None:
    """Invalid base64 input raises an error."""
    with pytest.raises(Exception):
        iib.decompress_build_info("not-valid-base64!!!")


# ---------------------------------------------------------------------------
# query_builds
# ---------------------------------------------------------------------------


def test_query_builds_constructs_url_and_parses_response() -> None:
    """``query_builds`` encodes query parameters and parses the JSON body."""
    response_body: iib.IIBQueryResponse = {
        "items": [
            {"id": 10, "state": "complete", "fbc_fragments": ["a"]},
        ],
    }
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text",
        return_value=json.dumps(response_body),
    ) as get_mock:
        result = iib.query_builds(
            "https://iib.example.com",
            user="bot@REALM",
            from_index="registry/idx:v4.12",
            state="complete",
        )

    assert result == response_body
    url = get_mock.call_args[0][0]
    assert "user=bot%40REALM" in url
    assert "from_index=registry%2Fidx%3Av4.12" in url
    assert "state=complete" in url
    assert url.startswith("https://iib.example.com/builds?")


def test_query_builds_propagates_http_error() -> None:
    """HTTP errors from ``get_text`` propagate unchanged."""
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text",
        side_effect=requests.HTTPError("500"),
    ):
        with pytest.raises(requests.HTTPError):
            iib.query_builds(
                "https://iib",
                user="u",
                from_index="i",
                state="complete",
            )


# ---------------------------------------------------------------------------
# get_build
# ---------------------------------------------------------------------------


def test_get_build_fetches_by_id() -> None:
    """``get_build`` hits ``/builds/{id}`` and parses JSON."""
    build: iib.IIBBuild = {"id": 55, "state": "complete"}
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text",
        return_value=json.dumps(build),
    ) as get_mock:
        result = iib.get_build("https://iib.example.com", 55)

    assert result == build
    assert get_mock.call_args[0][0] == "https://iib.example.com/builds/55"


# ---------------------------------------------------------------------------
# submit_fbc_operation
# ---------------------------------------------------------------------------


def test_submit_fbc_operation_posts_with_auth() -> None:
    """``submit_fbc_operation`` POSTs JSON with the supplied auth object."""
    build: iib.IIBBuild = {"id": 99, "state": "in_progress"}
    payload: iib.FBCOperationPayload = {
        "fbc_fragments": ["frag-1"],
        "from_index": "registry/idx:v4.12",
    }
    fake_auth = mock.MagicMock()

    resp = mock.MagicMock()
    resp.json.return_value = build
    resp.raise_for_status = mock.MagicMock()

    with mock.patch("requests.Session") as session_cls:
        session_cls.return_value.post.return_value = resp
        result = iib.submit_fbc_operation(
            "https://iib",
            payload,
            auth=fake_auth,
        )

    assert result == build
    call_kwargs = session_cls.return_value.post.call_args[1]
    assert call_kwargs["json"] == payload
    assert call_kwargs["auth"] is fake_auth
    assert call_kwargs["verify"] is False


def test_submit_fbc_operation_raises_on_iib_error() -> None:
    """An ``error`` field in the response body raises ``ValueError``."""
    resp = mock.MagicMock()
    resp.json.return_value = {"error": "something went wrong"}
    resp.raise_for_status = mock.MagicMock()

    with mock.patch("requests.Session") as session_cls:
        session_cls.return_value.post.return_value = resp
        with pytest.raises(ValueError, match="something went wrong"):
            iib.submit_fbc_operation(
                "https://iib",
                {"fbc_fragments": ["x"], "from_index": "y"},
                auth=mock.MagicMock(),
            )


def test_submit_fbc_operation_raises_on_http_error() -> None:
    """Non-2xx responses raise ``requests.HTTPError``."""
    resp = mock.MagicMock()
    resp.raise_for_status.side_effect = requests.HTTPError("403")

    with mock.patch("requests.Session") as session_cls:
        session_cls.return_value.post.return_value = resp
        with pytest.raises(requests.HTTPError):
            iib.submit_fbc_operation(
                "https://iib",
                {"fbc_fragments": ["x"], "from_index": "y"},
                auth=mock.MagicMock(),
            )


def test_submit_fbc_operation_verify_ssl_passthrough() -> None:
    """``verify_ssl=True`` is forwarded to the session POST call."""
    resp = mock.MagicMock()
    resp.json.return_value = {"id": 1, "state": "in_progress"}
    resp.raise_for_status = mock.MagicMock()

    with mock.patch("requests.Session") as session_cls:
        session_cls.return_value.post.return_value = resp
        iib.submit_fbc_operation(
            "https://iib",
            {"fbc_fragments": ["x"], "from_index": "y"},
            auth=mock.MagicMock(),
            verify_ssl=True,
        )

    assert session_cls.return_value.post.call_args[1]["verify"] is True


# ---------------------------------------------------------------------------
# parse_date_to_epoch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "date_str",
    [
        "2026-05-26T15:32:23.47548548Z",  # 8 fractional digits
        "2026-04-15T20:01:40.139676757Z",  # 9 fractional digits (nanoseconds)
        "2024-01-01T00:00:00.123456Z",  # 6 fractional digits (microseconds)
        "2024-01-01T00:00:00Z",  # no fractional seconds
        "2024-06-15T12:30:00+00:00",  # explicit UTC offset
        "2024-06-15T14:30:00+02:00",  # non-UTC offset
        "2024-01-01T00:00:00",  # no timezone
    ],
)
def test_parse_date_to_epoch_valid(date_str: str) -> None:
    """Valid ISO 8601 dates parse to a positive integer epoch."""
    result = iib.parse_date_to_epoch(date_str)
    assert isinstance(result, int)
    assert result > 0


def test_parse_date_to_epoch_known_value() -> None:
    """A known UTC timestamp produces the expected epoch value."""
    assert iib.parse_date_to_epoch("2024-01-01T00:00:00Z") == 1704067200


def test_parse_date_to_epoch_preserves_timezone() -> None:
    """The same instant in different timezones produces the same epoch."""
    utc = iib.parse_date_to_epoch("2024-06-15T12:00:00+00:00")
    plus2 = iib.parse_date_to_epoch("2024-06-15T14:00:00+02:00")
    assert utc == plus2


def test_parse_date_to_epoch_nanoseconds_match_microseconds() -> None:
    """Nanosecond and microsecond variants of the same date produce the same epoch."""
    micro = iib.parse_date_to_epoch("2026-05-26T15:32:23.475485Z")
    nano = iib.parse_date_to_epoch("2026-05-26T15:32:23.47548548Z")
    assert micro == nano


@pytest.mark.parametrize(
    "bad_date",
    [
        "",
        "not-a-date",
        "2024-13-01T00:00:00Z",  # invalid month
        "2024-01-32T00:00:00Z",  # invalid day
        "15/06/2024",  # wrong format
        "1234567890",  # bare epoch string
    ],
)
def test_parse_date_to_epoch_invalid_raises(bad_date: str) -> None:
    """Invalid date strings raise ``ValueError``."""
    with pytest.raises(ValueError):
        iib.parse_date_to_epoch(bad_date)


# ---------------------------------------------------------------------------
# extract_log_url
# ---------------------------------------------------------------------------


def test_extract_log_url_present() -> None:
    """Return the URL when the ``logs`` dict has a ``url`` key."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "logs": {"url": "https://iib.example.com/logs/42"},
    }
    assert iib.extract_log_url(build) == "https://iib.example.com/logs/42"


def test_extract_log_url_missing_logs_key() -> None:
    """Return empty string when ``logs`` is absent."""
    build: iib.IIBBuild = {"id": 1, "state": "complete"}
    assert iib.extract_log_url(build) == ""


def test_extract_log_url_empty_logs() -> None:
    """Return empty string when ``logs`` is an empty dict."""
    build: iib.IIBBuild = {"id": 1, "state": "complete", "logs": {}}
    assert iib.extract_log_url(build) == ""


def test_extract_log_url_logs_missing_url_key() -> None:
    """Return empty string when ``logs`` dict has no ``url`` key."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "logs": {"other": "value"},  # type: ignore[typeddict-item]
    }
    assert iib.extract_log_url(build) == ""


def test_extract_log_url_logs_none() -> None:
    """Return empty string when ``logs`` is explicitly ``None``."""
    build: iib.IIBBuild = {
        "id": 1,
        "state": "complete",
        "logs": None,  # type: ignore[typeddict-item]
    }
    assert iib.extract_log_url(build) == ""
