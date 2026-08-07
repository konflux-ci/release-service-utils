"""IIB (Index Image Build) REST API client helpers.

Provide functions for querying, submitting, and monitoring IIB builds,
plus gzip/base64 compression for Tekton result files (4 KB limit).
"""

from __future__ import annotations

import base64
import gzip
import json
from datetime import datetime
from typing import TypedDict
from urllib.parse import urlencode

import requests
from requests.auth import AuthBase

from release_service_utils.helpers import http_client
from release_service_utils.helpers.logger import logger


class IIBBuildLogs(TypedDict, total=False):
    """Log URLs attached to an IIB build."""

    url: str


class IIBBuild(TypedDict, total=False):
    """Subset of fields returned by the IIB builds endpoint."""

    id: int
    state: str
    state_reason: str | None
    from_index: str | None
    from_index_resolved: str | None
    index_image: str | None
    index_image_resolved: str | None
    internal_index_image_copy: str | None
    fbc_fragments: list[str] | None
    build_tags: list[str] | None
    distribution_scope: str | None
    updated: str | None
    created: str | None
    logs: IIBBuildLogs
    state_history: list[dict[str, str]]


class IIBQueryResponse(TypedDict):
    """Paginated response from ``GET /builds``."""

    items: list[IIBBuild]


class FBCOperationPayload(TypedDict, total=False):
    """JSON body for ``POST /builds/fbc-operations``."""

    fbc_fragments: list[str]
    from_index: str
    build_tags: list[str]
    add_arches: list[str]
    overwrite_from_index: bool
    overwrite_from_index_token: str


def query_builds(
    iib_url: str,
    *,
    user: str,
    from_index: str,
    state: str,
) -> IIBQueryResponse:
    """Query IIB for builds matching *user*, *from_index*, and *state*.

    Return the parsed JSON response body (contains an ``items`` list).
    The IIB read endpoint is unauthenticated.
    """
    params = urlencode({"user": user, "from_index": from_index, "state": state})
    url = f"{iib_url}/builds?{params}"
    body = http_client.get_text(url)
    return json.loads(body)


def get_build(
    iib_url: str,
    build_id: int,
) -> IIBBuild:
    """Fetch a single IIB build by *build_id*.

    Return the parsed JSON response body.  The endpoint is unauthenticated.
    """
    url = f"{iib_url}/builds/{build_id}"
    body = http_client.get_text(url)
    return json.loads(body)


def submit_fbc_operation(
    iib_url: str,
    payload: FBCOperationPayload,
    *,
    auth: AuthBase,
    verify_ssl: bool = False,
) -> IIBBuild:
    """POST an FBC operation to IIB with Kerberos negotiate auth.

    *verify_ssl* defaults to ``False`` to match the original bash behaviour
    (``curl --insecure``).  Raise ``requests.HTTPError`` on non-2xx or
    ``ValueError`` when the IIB response body contains an ``error`` field.
    """
    url = f"{iib_url}/builds/fbc-operations"
    logger.info("Submitting FBC operation to %s", url)

    session = requests.Session()
    resp = session.post(
        url,
        json=payload,
        auth=auth,
        verify=verify_ssl,
        timeout=120,
    )
    resp.raise_for_status()

    data: IIBBuild = resp.json()
    error = data.get("error")  # type: ignore[call-overload]
    if error is not None:
        raise ValueError(f"IIB service error: {error}")
    return data


def parse_date_to_epoch(date_str: str) -> int:
    """Parse an ISO 8601 date string to a Unix epoch integer."""
    return int(datetime.fromisoformat(date_str).timestamp())


def extract_log_url(build_info: IIBBuild) -> str:
    """Extract the IIB log URL from build info, or return empty string."""
    logs = build_info.get("logs")
    if logs and isinstance(logs, dict):
        return logs.get("url", "")
    return ""


def compress_build_info(data: IIBBuild) -> str:
    """Serialize, gzip-compress, and base64-encode build info.

    Return an ASCII string suitable for writing to a Tekton result file.
    """
    raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(gzip.compress(raw)).decode("ascii")


def decompress_build_info(compressed: str) -> IIBBuild:
    """Reverse of ``compress_build_info``.

    Decode base64, gunzip, and parse JSON.
    """
    raw = gzip.decompress(base64.b64decode(compressed))
    return json.loads(raw)
