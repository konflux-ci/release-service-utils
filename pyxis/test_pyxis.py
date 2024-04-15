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
    mock_path_exists.return_value = True
    monkeypatch.setenv("PYXIS_CERT_PATH", "/path/to/cert.pem")
    monkeypatch.setenv("PYXIS_KEY_PATH", "/path/to/key.key")

    session = pyxis._get_session()

    assert session.cert == ("/path/to/cert.pem", "/path/to/key.key")


@patch("os.path.exists")
def test_get_session_cert_not_exist(mock_path_exists: MagicMock, monkeypatch: Any) -> None:
    mock_path_exists.return_value = False
    monkeypatch.setenv("PYXIS_CERT_PATH", "/path/to/cert.pem")
    monkeypatch.setenv("PYXIS_KEY_PATH", "/path/to/key.key")

    with pytest.raises(Exception):
        pyxis._get_session()


def test_get_session_no_auth(monkeypatch: Any) -> None:
    session = pyxis._get_session(auth_required=False)

    assert session.cert is None


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_post(mock_get_session: MagicMock) -> None:
    resp = pyxis.post(API_URL, {})

    assert resp == mock_get_session.return_value.post.return_value
    mock_get_session.assert_called_once_with()


@patch("pyxis.session")
@patch("pyxis._get_session")
def test_post_existing_session(mock_get_session, mock_session: MagicMock) -> None:
    resp = pyxis.post(API_URL, {})

    assert resp == mock_session.post.return_value
    mock_get_session.assert_not_called()


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_post_error(mock_get_session: MagicMock) -> None:
    response = Response()
    response.status_code = 400
    mock_get_session.return_value.post.return_value.raise_for_status.side_effect = HTTPError(
        response=response
    )

    with pytest.raises(HTTPError):
        pyxis.post(API_URL, {})


@patch("pyxis.post")
def test_graphql_query__success(mock_post: MagicMock):
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
    """For example, if there is a syntax error in the query,
    the response won't even include the query property"""
    mock_post.return_value.json.return_value = {
        "data": None,
        "errors": [{"message": "Major failure"}],
    }

    with pytest.raises(RuntimeError):
        pyxis.graphql_query(API_URL, REQUEST_BODY)

    mock_post.assert_called_once_with(API_URL, REQUEST_BODY)


@patch("pyxis.post")
def test_graphql_query__pyxis_error(mock_post: MagicMock):
    """For example, if the image id does not exist in Pyxis
    there will be an error property under the query property"""
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


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_put(mock_get_session: MagicMock) -> None:
    mock_get_session.return_value.put.return_value.json.return_value = {"key": "val"}

    resp = pyxis.put(API_URL, {})

    assert resp == {"key": "val"}
    mock_get_session.assert_called_once_with()


@patch("pyxis.session")
@patch("pyxis._get_session")
def test_put_existing_session(mock_get_session, mock_session: MagicMock) -> None:
    mock_session.put.return_value.json.return_value = {"key": "val"}

    resp = pyxis.put(API_URL, {})

    assert resp == {"key": "val"}
    mock_get_session.assert_not_called()


@patch("pyxis.session", None)
@patch("pyxis._get_session")
def test_put_error(mock_get_session: MagicMock) -> None:
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
    mock_get_session.return_value.get.return_value = {"key": "val"}

    resp = pyxis.get(API_URL)

    assert resp == {"key": "val"}
    mock_get_session.assert_called_once_with()


@patch("pyxis.session")
@patch("pyxis._get_session")
def test_get_existing_session(mock_get_session, mock_session: MagicMock) -> None:
    mock_session.get.return_value = {"key": "val"}

    resp = pyxis.get(API_URL)

    assert resp == {"key": "val"}
    mock_get_session.assert_not_called()


def test_add_session_retries() -> None:
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
