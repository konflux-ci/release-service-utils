"""HTTP helpers for task code using the requests library.

get_text performs GET with retries (similar to curl --retry 3). Kerberos or
bearer auth uses HTTPKerberosAuth from requests_kerberos after kinit, or
headers on a Session with urllib3 retry support.

Use get_retry_session to build a Session with caller-chosen methods and
status codes. get_text uses it with GET-only retry settings.
"""

from __future__ import annotations

import os
import random
import time
from collections.abc import Mapping
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

MAX_429_ATTEMPTS = 5
MAX_404_ATTEMPTS = 3
BASE_SLEEP_TIME_SECONDS = 1


def get_retry_session(
    *,
    total: int = 5,
    connect: int | None = None,
    read: int | None = None,
    status: int | None = None,
    backoff_factor: float = 0.4,
    status_forcelist: tuple[int, ...] = (500, 502, 503, 504),
    allowed_methods: frozenset[str] | set[str],
    raise_on_status: bool = False,
) -> requests.Session:
    """Build a requests.Session with urllib3 retries on http and https.

    Callers choose which HTTP methods and status codes are retried.
    Tests can patch this function to supply a custom session.
    """
    connect_attempts = total if connect is None else connect
    read_attempts = total if read is None else read
    status_attempts = 2 if status is None else status
    retry = Retry(
        total=total,
        connect=connect_attempts,
        read=read_attempts,
        status=status_attempts,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
        raise_on_status=raise_on_status,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_text(
    url: str,
    *,
    auth: Any = None,
    cert: tuple[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 60.0,
    allow_error_status: bool = False,
) -> str:
    """Perform an HTTP(S) GET for the given URL and return the body as a string.

    Non-2xx responses become requests.HTTPError (similar to curl --fail).
    Optional auth (for example HTTPKerberosAuth) and headers are supported.
    timeout is seconds for the call.

    When allow_error_status is true, return the response body for any HTTP
    status without raising (similar to curl without --fail). Retries for
    HTTP 429 and optional 404 still run before returning a non-2xx body.

    HTTP 429 is retried with exponential backoff plus 0-2s jitter up to
    MAX_429_ATTEMPTS total attempts. HTTP 404 is retried similarly only if
    CURL_WITH_RETRY_RETRY_404 is set to a non-empty value.
    """
    session = get_retry_session(
        total=3,
        connect=3,
        read=3,
        status=2,
        backoff_factor=0.4,
        allowed_methods=frozenset({"GET"}),
    )
    if cert is not None:
        session.cert = cert
    retries_429 = 0
    retries_404 = 0
    should_retry_404 = bool(os.environ.get("CURL_WITH_RETRY_RETRY_404"))

    while True:
        r = session.get(
            url,
            auth=auth,
            headers=dict(headers) if headers is not None else None,
            timeout=timeout,
        )
        if 200 <= r.status_code < 300:
            return r.text

        if r.status_code == 429:
            retries_429 += 1
            if retries_429 < MAX_429_ATTEMPTS:
                delay = BASE_SLEEP_TIME_SECONDS * (2 ** (retries_429 - 1))
                delay += random.randint(0, 2)
                time.sleep(delay)
                continue

        if r.status_code == 404 and should_retry_404:
            retries_404 += 1
            if retries_404 < MAX_404_ATTEMPTS:
                delay = BASE_SLEEP_TIME_SECONDS * (2 ** (retries_404 - 1))
                delay += random.randint(0, 2)
                time.sleep(delay)
                continue

        if allow_error_status:
            return r.text

        r.raise_for_status()
        return r.text
