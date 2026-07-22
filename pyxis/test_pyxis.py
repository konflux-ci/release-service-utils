"""Unit tests for pyxis API client."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import pyxis
from requests import HTTPError, Response, Session

API_URL = "https://foo.com/v1/bar"
REQUEST_BODY = {
    "a": "1",
    "b": "2",
}
QUERY = "myquery"


@patch("os.path.exists")
def test_get_session_cert(mock_path_exists: MagicMock, monkeypatch: Any) -> None:
    """Return a session with cert and key when valid paths are provided."""
    mock_path_exists.return_value = True
    monkeypatch.setenv("PYXIS_CERT_PATH", "/path/to/cert.pem")
    monkeypatch.setenv("PYXIS_KEY_PATH", "/path/to/key.key")

    session = pyxis._get_session()

    assert session.cert == ("/path/to/cert.pem", "/path/to/key.key")


@patch("os.path.exists")
def test_get_session_cert_not_exist(mock_path_exists: MagicMock, monkeypatch: Any) -> None:
    """Raise an exception when the cert or key path does not exist."""
    mock_path_exists.return_value = False
    monkeypatch.setenv("PYXIS_CERT_PATH", "/path/to/cert.pem")
    monkeypatch.setenv("PYXIS_KEY_PATH", "/path/to/key.key")

    with pytest.raises(Exception):
        pyxis._get_session()


def test_get_session_no_auth(monkeypatch: Any) -> None:
    """Return a session without auth when auth_required is False."""
    session = pyxis._get_session(auth_required=False)

    assert session.cert is None


@patch("pyxis.add_session_retries")
def test_get_session_retry_allowed_methods(mock_add_retries: MagicMock) -> None:
    """Forward retry_allowed_methods to add_session_retries as allowed_methods."""
    pyxis._get_session(auth_required=False, retry_allowed_methods=None)

    mock_add_retries.assert_called_once()
    _, kwargs = mock_add_retries.call_args
    assert kwargs["allowed_methods"] is None


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_post(mock_get_session: MagicMock) -> None:
    """Return the response from the session POST call."""
    resp = pyxis.post(API_URL, {})

    assert resp == mock_get_session.return_value.post.return_value
    mock_get_session.assert_called_once_with()


@patch("pyxis.session")
@patch("pyxis._get_session")
def test_post_existing_session(mock_get_session, mock_session: MagicMock) -> None:
    """Use the existing session without creating a new one."""
    resp = pyxis.post(API_URL, {})

    assert resp == mock_session.post.return_value
    mock_get_session.assert_not_called()


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_post_error(mock_get_session: MagicMock) -> None:
    """Raise HTTPError when the POST response has an error status."""
    response = Response()
    response.status_code = 400
    mock_get_session.return_value.post.return_value.raise_for_status.side_effect = HTTPError(
        response=response
    )

    with pytest.raises(HTTPError):
        pyxis.post(API_URL, {})


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_patch(mock_get_session: MagicMock) -> None:
    """Return the response from the session PATCH call."""
    resp = pyxis.patch(API_URL, {})

    assert resp == mock_get_session.return_value.patch.return_value
    mock_get_session.assert_called_once_with()


@patch("pyxis.session")
@patch("pyxis._get_session")
def test_patch_existing_session(mock_get_session, mock_session: MagicMock) -> None:
    """Use the existing session without creating a new one."""
    resp = pyxis.patch(API_URL, {})

    assert resp == mock_session.patch.return_value
    mock_get_session.assert_not_called()


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_patch_error(mock_get_session: MagicMock) -> None:
    """Raise HTTPError when the PATCH response has an error status."""
    response = Response()
    response.status_code = 400
    mock_get_session.return_value.patch.return_value.raise_for_status.side_effect = HTTPError(
        response=response
    )

    with pytest.raises(HTTPError):
        pyxis.patch(API_URL, {})


@patch("pyxis.post")
def test_graphql_query__success(mock_post: MagicMock):
    """Return query data when the GraphQL response contains no errors."""
    mock_data = {
        "output": "something",
    }
    mock_post.return_value.json.return_value = {
        "data": {
            QUERY: {
                "data": mock_data,
                "error": None,
            }
        }
    }

    data = pyxis.graphql_query(API_URL, REQUEST_BODY)

    assert data[QUERY]["data"] == mock_data
    mock_post.assert_called_once_with(API_URL, REQUEST_BODY)


@patch("pyxis.post")
def test_graphql_query__general_graphql_error(mock_post: MagicMock):
    """Raise RuntimeError when the response has a top-level errors field.

    For example, if there is a syntax error in the query,
    the response won't even include the query property.
    """
    mock_post.return_value.json.return_value = {
        "data": None,
        "errors": [{"message": "Major failure"}],
    }

    with pytest.raises(RuntimeError):
        pyxis.graphql_query(API_URL, REQUEST_BODY)

    mock_post.assert_called_once_with(API_URL, REQUEST_BODY)


@patch("pyxis.post")
def test_graphql_query__pyxis_error(mock_post: MagicMock):
    """Raise RuntimeError when the response contains a Pyxis-level error.

    For example, if the image id does not exist in Pyxis
    there will be an error property under the query property.
    """
    mock_post.return_value.json.return_value = {
        "data": {
            QUERY: {
                "data": None,
                "error": {"detail": "Not found"},
            }
        }
    }

    with pytest.raises(RuntimeError):
        pyxis.graphql_query(API_URL, REQUEST_BODY)

    mock_post.assert_called_once_with(API_URL, REQUEST_BODY)


@patch("pyxis.post")
def test_graphql_query__allow_not_found_with_404(mock_post: MagicMock):
    """Return data without raising when allow_not_found=True and error is 404.

    When allow_not_found=True and the error is a 404,
    the function should return the data without raising.
    """
    mock_post.return_value.json.return_value = {
        "data": {
            QUERY: {
                "data": None,
                "error": {"status": 404, "detail": "Document not found"},
            }
        }
    }

    data = pyxis.graphql_query(API_URL, REQUEST_BODY, allow_not_found=True)

    assert data[QUERY]["data"] is None
    assert data[QUERY]["error"]["status"] == 404
    mock_post.assert_called_once_with(API_URL, REQUEST_BODY)


@patch("pyxis.post")
def test_graphql_query__allow_not_found_with_other_error(mock_post: MagicMock):
    """Raise RuntimeError when allow_not_found=True but the error is not a 404.

    When allow_not_found=True but the error is not a 404,
    the function should still raise.
    """
    mock_post.return_value.json.return_value = {
        "data": {
            QUERY: {
                "data": None,
                "error": {"status": 500, "detail": "Internal server error"},
            }
        }
    }

    with pytest.raises(RuntimeError):
        pyxis.graphql_query(API_URL, REQUEST_BODY, allow_not_found=True)

    mock_post.assert_called_once_with(API_URL, REQUEST_BODY)


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_put(mock_get_session: MagicMock) -> None:
    """Return the JSON body from the session PUT call."""
    mock_get_session.return_value.put.return_value.json.return_value = {"key": "val"}

    resp = pyxis.put(API_URL, {})

    assert resp == {"key": "val"}
    mock_get_session.assert_called_once_with()


@patch("pyxis.session")
@patch("pyxis._get_session")
def test_put_existing_session(mock_get_session, mock_session: MagicMock) -> None:
    """Use the existing session without creating a new one."""
    mock_session.put.return_value.json.return_value = {"key": "val"}

    resp = pyxis.put(API_URL, {})

    assert resp == {"key": "val"}
    mock_get_session.assert_not_called()


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_put_error(mock_get_session: MagicMock) -> None:
    """Raise HTTPError when the PUT response has an error status."""
    response = Response()
    response.status_code = 400
    mock_get_session.return_value.put.return_value.raise_for_status.side_effect = HTTPError(
        response=response
    )

    with pytest.raises(HTTPError):
        pyxis.put(API_URL, {})


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_get(mock_get_session: MagicMock) -> None:
    """Return the response from the session GET call."""
    mock_get_session.return_value.get.return_value = {"key": "val"}

    resp = pyxis.get(API_URL)

    assert resp == {"key": "val"}
    mock_get_session.assert_called_once_with()


@patch("pyxis.session")
@patch("pyxis._get_session")
def test_get_existing_session(mock_get_session, mock_session: MagicMock) -> None:
    """Use the existing session without creating a new one."""
    mock_session.get.return_value = {"key": "val"}

    resp = pyxis.get(API_URL)

    assert resp == {"key": "val"}
    mock_get_session.assert_not_called()


def test_add_session_retries() -> None:
    """Configure retry settings on both http:// and https:// adapters."""
    status_forcelist = (404, 503)
    total = 3
    backoff_factor = 0.5
    session = Session()

    pyxis.add_session_retries(
        session,
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )

    assert session.adapters["http://"].max_retries.total == total
    assert session.adapters["http://"].max_retries.backoff_factor == backoff_factor
    assert session.adapters["http://"].max_retries.status_forcelist == status_forcelist
    assert session.adapters["https://"].max_retries.total == total
    assert session.adapters["https://"].max_retries.backoff_factor == backoff_factor
    assert session.adapters["https://"].max_retries.status_forcelist == status_forcelist


def test_add_session_retries_custom_allowed_methods() -> None:
    """Set allowed_methods to None on adapters when None is passed."""
    session = Session()

    pyxis.add_session_retries(session, allowed_methods=None)

    assert session.adapters["http://"].max_retries.allowed_methods is None
    assert session.adapters["https://"].max_retries.allowed_methods is None
