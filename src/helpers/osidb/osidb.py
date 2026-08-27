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
import urllib.parse
from typing import Any

from release_service_utils.helpers import http_client
from requests_kerberos import HTTPKerberosAuth, OPTIONAL


def get_access_token(osidb_url: str) -> str:
    """Get a short-lived bearer string from the OSIDB ``/auth/token`` URL.

    Uses GSS/SPNEGO. You need a working Kerberos cache (for example from
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


def fetch_flaw_response(
    osidb_url: str,
    token: str,
    cve_id: str,
    *,
    include_fields: str,
) -> str:
    """GET OSIDB v2 flaws for *cve_id* and return the response body.

    *include_fields* is passed through to the API as a comma-separated field
    list (for example ``cve_id,embargoed``).
    """
    query = urllib.parse.urlencode(
        [("cve_id", cve_id), ("include_fields", include_fields)],
    )
    url = f"{osidb_url.rstrip('/')}/osidb/api/v2/flaws?{query}"
    return http_client.get_text(
        url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
