from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import pyxis
from requests import HTTPError, Response, Session


@patch("os.path.exists")
def test_get_session_cert(mock_path_exists: MagicMock, monkeypatch: Any) -> None:
    mock_path_exists.return_value = True
    monkeypatch.setenv("PYXIS_CERT_PATH", "/path/to/cert.pem")
    monkeypatch.setenv("PYXIS_KEY_PATH", "/path/to/key.key")
    session = pyxis._get_session("test")

    assert session.cert == ("/path/to/cert.pem", "/path/to/key.key")


@patch("os.path.exists")
def test_get_session_cert_not_exist(mock_path_exists: MagicMock, monkeypatch: Any) -> None:
    mock_path_exists.return_value = False
    monkeypatch.setenv("PYXIS_CERT_PATH", "/path/to/cert.pem")
    monkeypatch.setenv("PYXIS_KEY_PATH", "/path/to/key.key")

    with pytest.raises(Exception):
        pyxis._get_session("test")


def test_get_session_no_auth(monkeypatch: Any) -> None:
    session = pyxis._get_session("test", auth_required=False)
    assert session.cert is None


@patch("pyxis._get_session")
def test_post(mock_session: MagicMock) -> None:
    mock_session.return_value.post.return_value.json.return_value = {"key": "val"}
    resp = pyxis.post("https://foo.com/v1/bar", {})

    assert resp == {"key": "val"}


@patch("pyxis._get_session")
def test_post_error(mock_session: MagicMock) -> None:
    response = Response()
    response.status_code = 400
    mock_session.return_value.post.return_value.raise_for_status.side_effect = HTTPError(
        response=response
    )
    with pytest.raises(HTTPError):
        pyxis.post("https://foo.com/v1/bar", {})


@patch("pyxis._get_session")
def test_put(mock_session: MagicMock) -> None:
    mock_session.return_value.put.return_value.json.return_value = {"key": "val"}
    resp = pyxis.put("https://foo.com/v1/bar", {})

    assert resp == {"key": "val"}


@patch("pyxis._get_session")
def test_put_error(mock_session: MagicMock) -> None:
    response = Response()
    response.status_code = 400
    mock_session.return_value.put.return_value.raise_for_status.side_effect = HTTPError(
        response=response
    )
    with pytest.raises(HTTPError):
        pyxis.put("https://foo.com/v1/bar", {})


@patch("pyxis._get_session")
def test_get(mock_session: MagicMock) -> None:
    mock_session.return_value.get.return_value = {"key": "val"}
    resp = pyxis.get("https://foo.com/v1/bar")

    assert resp == {"key": "val"}


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
