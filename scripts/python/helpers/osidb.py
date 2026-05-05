"""OSIDB (Open Security Issue Database) client helpers for task scripts.

The usual flow is: load a service-account style mount (files such as
``name``, ``base64_keytab``, and ``osidb_url``) using
``authentication.load_service_account`` (pass ``principal_file`` and
``keytab_b64_file`` for the mount, plus e.g. ``"osidb_url"`` in ``text_files``), then run
``kinit`` using helpers in
``authentication``, then call ``get_access_token`` so later calls can use a
bearer token. ``get_access_token`` fetches a token with the ``requests``
library, Kerberos negotiate auth from ``requests_kerberos``, and an HTTP GET
helper from the ``http_client`` module.
"""

from __future__ import annotations

import json
from typing import Any

import http_client
from requests_kerberos import HTTPKerberosAuth, OPTIONAL


def get_access_token(osidb_url: str) -> str:
    """
    Get a short-lived bearer string from the OSIDB ``/auth/token`` URL using
    GSS/SPNEGO. You need a working Kerberos cache (for example from
    ``kinit`` / ``kinit_with_retry`` in ``authentication``). The response is
    JSON; this function returns the value of the top ``"access"`` string for
    use as ``Authorization: Bearer ...`` on later API calls. Raises
    ``ValueError`` if the body is empty or the token is missing.
    """
    base = osidb_url.rstrip("/")
    body = http_client.get_text(
        f"{base}/auth/token",
        auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL),
    )
    if not body.strip():
        err = f"empty token response from {base}/auth/token"
        raise ValueError(err)
    data: dict[str, Any] = json.loads(body)
    t = data.get("access")
    if t is None or t == "":
        err = f"no .access in token response: {data!r}"
        raise ValueError(err)
    return t if isinstance(t, str) else str(t)
