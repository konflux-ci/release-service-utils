import logging
import os
import sys
from typing import Any, Dict, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests

LOGGER = logging.getLogger("pyxis")


def _get_session(pyxis_url: str, auth_required: bool = True) -> requests.Session:
    """Create a Pyxis http session with auth based on env variables.

    Auth is optional and can be set to use either API key or certificate + key.

    Args:
        url (str): Pyxis API URL
        auth_required (bool): Whether authentication should be required for the session

    Raises:
        Exception: Exception is raised when auth ENV variables are missing.

    :return: Pyxis session
    """
    cert_string = "PYXIS_CERT_PATH"
    key_string = "PYXIS_KEY_PATH"
    cert = os.environ.get(cert_string)
    key = os.environ.get(key_string)

    session = requests.Session()
    add_session_retries(session)

    if not auth_required:
        LOGGER.debug("Pyxis session without authentication is created")
        return session

    if cert and key:
        if os.path.exists(cert) and os.path.exists(key):
            LOGGER.debug("Pyxis session using cert + key is created")
            session.cert = (cert, key)
        else:
            raise Exception(
                f"{cert_string} or {key_string} does not point to a file that exists."
            )
    else:
        # cert + key need to be provided using env variable
        raise Exception(
            f"No auth details provided for Pyxis. Define {cert_string} + {key_string}"
        )

    return session


def post(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """POST pyxis API request to given URL with given payload

    Args:
        url (str): Pyxis API URL
        body (Dict[str, Any]): Request payload

    :return: Pyxis response
    """
    session = _get_session(url)

    LOGGER.debug(f"POST Pyxis request: {url}")
    resp = session.post(url, json=body)

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        LOGGER.exception(
            f"Pyxis POST query failed with {url} - {resp.status_code} - {resp.text}"
        )
        raise
    return resp.json()


def put(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """PUT pyxis API request to given URL with given payload

    Args:
        url (str): Pyxis API URL
        body (Dict[str, Any]): Request payload

    :return: Pyxis response
    """
    session = _get_session(url)

    LOGGER.debug(f"PATCH Pyxis request: {url}")
    resp = session.put(url, json=body)

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        LOGGER.exception(
            f"Pyxis PUT query failed with {url} - {resp.status_code} - {resp.text}"
        )
        raise
    return resp.json()


def get(url: str, params: Optional[Dict[str, str]] = None, auth_required: bool = True) -> Any:
    """Pyxis GET request

    Args:
        url (str): Pyxis URL
        params (dict): Additional query parameters
        auth_required (bool): Whether authentication should be required for the session

    :return: Pyxis GET request response
    """
    session = _get_session(url, auth_required=auth_required)
    LOGGER.debug(f"GET Pyxis request url: {url}")
    LOGGER.debug(f"GET Pyxis request params: {params}")
    resp = session.get(url, params=params)
    # Not raising exception for error statuses, because GET request can be used to check
    # if something exists. We don't want a 404 to cause failures.

    return resp


def add_session_retries(
    session: requests.Session,
    total: int = 10,
    backoff_factor: int = 1,
    status_forcelist: Optional[Tuple[int, ...]] = (408, 500, 502, 503, 504),
) -> None:
    """Adds retries to a requests HTTP/HTTPS session.
    The default values provide exponential backoff for a max wait of ~8.5 mins

    Reference the urllib3 documentation for more details about the kwargs.

    Args:
        session (Session): A requests session
        total (int): See urllib3 docs
        backoff_factor (int): See urllib3 docs
        status_forcelist (tuple[int]|None): See urllib3 docs
    """
    retries = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        # Don't raise a MaxRetryError for codes in status_forcelist.
        # This allows for more graceful exception handling using
        # Response.raise_for_status.
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)


def setup_logger(level: int = logging.INFO, log_format: Any = None) -> Any:
    """Set up and configure 'pyxis' logger.
    Args:
        level (str, optional): Logging level. Defaults to logging.INFO.
        log_format (Any, optional): Logging message format. Defaults to None.
    :return: Logger object
    """

    logger = logging.getLogger("pyxis")
    logger.propagate = False
    logger.setLevel(level)

    if log_format is None:
        log_format = "%(asctime)s [%(name)s] %(levelname)s %(message)s"

    stream_formatter = logging.Formatter(log_format)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

    return logger
