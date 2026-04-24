"""HTTP helpers for task code using the third-party ``requests`` library.

The catalog often shows shell with ``curl --retry 3`` and Kerberos or bearer
auth. The equivalent here is ``get_text`` (GET with retries on the session) plus
either ``HTTPKerberosAuth`` from the ``requests_kerberos`` package (after
``kinit``) or regular headers, using urllib3's retry support on a ``Session``.
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


def _retries() -> Retry:
    """Transients similar to common ``curl --retry 3`` (connection + some HTTP)."""
    return Retry(
        total=3,
        connect=3,
        read=3,
        status=2,
        backoff_factor=0.4,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )


def get_session() -> requests.Session:
    """
    Build a ``requests.Session`` with the shared retry policy on http and https.
    ``get_text`` uses this. Tests can patch this function to supply a custom session.
    """
    s = requests.Session()
    a = HTTPAdapter(max_retries=_retries())
    s.mount("https://", a)
    s.mount("http://", a)
    return s


def get_text(
    url: str,
    *,
    auth: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 60.0,
) -> str:
    """
    Perform an HTTP(S) GET for the given URL and return the body as a string.

    Non-2xx responses become ``requests.HTTPError`` (similar to ``curl
    --fail``). You may pass optional ``auth`` (for example
    ``HTTPKerberosAuth``) and request ``headers`` (bearer token, content type,
    etc.). ``timeout`` is seconds for the call.

    HTTP 429 is retried with exponential backoff plus 0-2s jitter up to
    ``MAX_429_ATTEMPTS`` total attempts. HTTP 404 is retried similarly only if
    ``CURL_WITH_RETRY_RETRY_404`` is set to a non-empty value.
    """
    session = get_session()
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

        r.raise_for_status()
        return r.text
