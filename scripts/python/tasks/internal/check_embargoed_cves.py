#!/usr/bin/env python3
"""Check whether CVEs are embargoed using the OSIDB API.

* Reads service account data from ``/mnt/osidb-service-account/`` (or
  ``OSIDB_SERVICE_ACCOUNT_MOUNT``): ``name``, ``base64_keytab``, ``osidb_url``.
* Authenticate with kinit (retried), then for each CVE obtain a token and GET
  ``/osidb/api/v2/flaws`` with the requested fields.
* Writes result paths from ``RESULT_RESULT`` and ``RESULT_EMBARGOED_CVES`` (set by
  the runner, e.g. a Tekton task).
* After a valid invocation with those env vars, always exits with status ``0``;
  success or failure is in the result files, including a short message that names
  the step that failed (e.g. ``kinit_with_retry``, ``get_access_token``) or that
  listed CVEs are not clearly public in OSIDB. Bad or missing/empty
  ``--cves`` exits before result handling (our checks use 1; argparse uses 2 for
  malformed argv).

``OSIDB_SERVICE_ACCOUNT_MOUNT`` can be overridden in tests to use a temp
directory with the same file layout.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence

import requests
import urllib.parse

import authentication
import file
import http_client
import osidb
import tekton

PROG = "check_embargoed_cves.py"


def parse_cve_list(value: str) -> list[str]:
    """Split a string into non-empty CVE tokens on whitespace (strip first)."""
    return [c for c in re.split(r"\s+", value.strip()) if c]


def is_embargoed_flaw_response(data: dict[str, Any]) -> bool:
    """
    Return True if the first flaw in the list response is not clearly not embargoed.

    Only ``results[0].embargoed`` with JSON value ``false`` is treated as
    not embargoed. Empty ``results``, a missing first row, a non-dict first row,
    a missing key, or ``null``/``true`` is treated as embargoed or not visible.
    """
    results = data.get("results")
    if not isinstance(results, list) or not results:
        return True
    first = results[0]
    if not isinstance(first, dict):
        return True
    em = first.get("embargoed", None)
    if em is False:
        return False
    return True


def _embargo_finding_result_text(program_name: str) -> str:
    """
    Text to write to ``RESULT_RESULT`` when the run finished without a Python
    exception but the OSIDB API indicates at least one listed CVE is embargoed
    or not clearly public.

    CVE ids are written to ``RESULT_EMBARGOED_CVES``; this string in
    ``RESULT_RESULT`` points readers there.
    """
    return (
        f"{program_name}: check failed: at least one CVE is embargoed or not "
        f"clearly public in OSIDB; see the embargoed_cves result for ids"
    )


def fetch_flaw_state(osidb_url: str, token: str, cve_id: str) -> dict[str, Any]:
    """
    GET the v2 flaw list for one CVE, asking only for ``cve_id`` and
    ``embargoed`` fields, using the bearer token.

    Returns a parsed JSON object, or an empty dict if the response body is
    empty (treated as no visible flaw for embargo decisions).
    """
    q = urllib.parse.urlencode([("cve_id", cve_id), ("include_fields", "cve_id,embargoed")])
    u = f"{osidb_url.rstrip('/')}/osidb/api/v2/flaws?{q}"
    body = http_client.get_text(
        u,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    if not body.strip():
        return {}
    return json.loads(body)


def _usage_text() -> str:
    """Return the short usage summary printed to stderr on bad CLI usage."""
    return (
        f"usage: {PROG} [--cves 'CVE-1 CVE-2']\n"
        f"  --cves  The CVEs to check (space-separated, quote if you pass several)\n"
    )


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """
    Parse CLI arguments: ``--cves`` (required) and ``-h`` / ``--help``.

    The Tekton task uses a fixed argv shape; this uses strict parsing (extra
    arguments are rejected by ``argparse`` with exit 2). Help, or missing/blank
    ``--cves``, print our usage to stderr and exit 1. Returns a namespace
    with a ``cves`` string.
    """
    p = argparse.ArgumentParser(prog=PROG, add_help=False, usage=argparse.SUPPRESS)
    p.add_argument("-h", "--help", action="store_true")
    p.add_argument("--cves", metavar="CVES")
    ns = p.parse_args(argv or [])
    if ns.help:
        print(_usage_text(), file=sys.stderr, end="")
        raise SystemExit(1)
    if not ns.cves or not str(ns.cves).strip():
        print(_usage_text(), file=sys.stderr, end="")
        raise SystemExit(1)
    return ns


def run_check(
    cve_ids: Sequence[str],
    mount: Path,
    *,
    kinit: Any = authentication.kinit_with_retry,
    get_token: Any = osidb.get_access_token,
    get_flaw: Any = fetch_flaw_state,
    krb5_template: Path = Path("/etc/krb5.conf"),
) -> tuple[list[str], int]:
    """
    Core check: kinit, then for each CVE fetch token + flaw JSON and test embargo.

    Writes the keytab and a patched KRB5_CONFIG to temp files, runs ``kinit``,
    then for each id obtains a token and queries flaws. Injected callables
    (``kinit``, ``get_token``, ``get_flaw``) default to the real implementation
    and are only replaced in tests.

    Return value: ``(affected_cve_ids, 0 or 1)`` — the second is ``1`` if
    at least one CVE is embargoed or not clearly public, and ``0`` if all
    are clear. Raises ``ValueError`` if ``cve_ids`` is empty. On operational
    failures, raises ``CheckStepError`` from the ``tekton`` helper, which
    carries a short English *action* and the original exception.
    """
    if not cve_ids:
        raise ValueError("no CVEs")
    # Read principal, keytab, and base URL from the same layout as the Tekton secret mount.
    try:
        princ, keytab_bytes, text = authentication.load_service_account(
            mount,
            ("osidb_url",),
            principal_file="name",
            keytab_b64_file="base64_keytab",
        )
    except (OSError, ValueError) as e:
        raise tekton.CheckStepError("reading the mounted OSIDB service account", e) from e
    osidb_url = text["osidb_url"]

    # Ephemeral keytab and credential cache: never leave secret material in fixed paths.
    kpath = file.make_tempfile_path("keytab-", keytab_bytes)
    cca_fd, cca_name = tempfile.mkstemp()
    os.close(cca_fd)
    ccache_path = Path(cca_name)

    # krb5.conf: patch a temp copy (dns_canonicalize_hostname) so kinit works in the container.
    try:
        ksrc = krb5_template.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        raise tekton.CheckStepError("reading the Kerberos configuration", e) from e
    kcfg = file.make_tempfile_path(
        "krb5-", authentication.patch_krb5_config(ksrc).encode("utf-8")
    )

    try:
        kenv = {
            "KRB5CCNAME": str(ccache_path),
            "KRB5_CONFIG": str(kcfg),
        }
        try:
            kinit(princ, kpath, kenv, max_attempts=5)
        except subprocess.CalledProcessError as e:
            raise tekton.CheckStepError("logging in with Kerberos (kinit)", e) from e
        # kinit’s env is only for the child; GSS/HTTP in this process needs
        # the same ccache in ``os.environ``.
        os.environ.update(kenv)

        # Short-lived token per CVE, matching the shell task (avoids an expired
        # token mid-loop).
        found: list[str] = []
        for cve in cve_ids:
            print(f"Checking CVE {cve}", flush=True)
            try:
                tok = get_token(osidb_url)
            except (OSError, requests.RequestException, ValueError) as e:
                raise tekton.CheckStepError(
                    "getting an OSIDB access token (HTTP request)", e
                ) from e
            try:
                data = get_flaw(osidb_url, tok, cve)
            except (
                OSError,
                requests.RequestException,
                TypeError,
                json.JSONDecodeError,
                ValueError,
            ) as e:
                raise tekton.CheckStepError(
                    "querying the OSIDB flaws API (HTTP request)", e
                ) from e
            if is_embargoed_flaw_response(data):
                print(f"CVE {cve} is embargoed", flush=True)
                found.append(cve)

        # Return code 0/1 is logical outcome only; main() maps it to the Tekton result files.
        return (found, 0 if not found else 1)
    finally:
        # kpath, ccache, kcfg: remove all temp files even when kinit or a later step raises.
        for p in (kpath, ccache_path, kcfg):
            p.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    """
    CLI entry: bad args return 1; a normal run writes result files and returns 0.

    Normal runs (with ``RESULT_RESULT`` and ``RESULT_EMBARGOED_CVES``) always
    exit 0 at the process level; logical success is ``Success`` in the first
    result, logical failure is an error-line style message and optional CVE
    list in the second. If required result env vars are missing, the process
    raises ``SystemExit`` with code 1.
    """
    a = sys.argv[1:] if argv is None else argv[1:]
    try:
        args = parse_args(a)
    except SystemExit as e:
        # argparse / parse_args use exit(1) for help or bad input; that path
        # cannot write results.
        code = e.code
        if isinstance(code, int):
            return code
        return 1
    cve_list = parse_cve_list(args.cves)
    if not cve_list:
        return 1

    rpath, epath = tekton.result_paths("RESULT_RESULT", "RESULT_EMBARGOED_CVES")
    # Clear both files up front: if run_check throws, the embargoed list
    # stays empty and we still fill result.
    epath.write_text("", encoding="utf-8")
    # Use argv[0] base name in messages (how users see the process in task
    # logs, not __file__).
    name = str(Path((argv or sys.argv)[0]).name)
    mount = file.path_from_env_variable(
        "OSIDB_SERVICE_ACCOUNT_MOUNT", "/mnt/osidb-service-account"
    )
    problem: list[str] = []
    out_rc: int = 0
    step_err: tekton.CheckStepError | None = None
    try:
        problem, out_rc = run_check(cve_list, mount=mount)
    except tekton.CheckStepError as e:
        # CheckStepError carries a plain-English action plus the cause.
        out_rc, problem = 1, []
        step_err = e
    except Exception as e:
        # Any bug or unexpected error still produces a result line; never drop
        # Tekton with no "result" body.
        out_rc, problem = 1, []
        step_err = tekton.CheckStepError("running the check", e)

    epath.write_text("".join(f"{c} " for c in problem), encoding="utf-8")
    if not out_rc:
        rpath.write_text("Success", encoding="utf-8")
    elif problem:
        # The API ran, but at least one CVE is not clearly public; not a step exception.
        rpath.write_text(_embargo_finding_result_text(name), encoding="utf-8")
    elif step_err is not None:
        # Exception on the way: which step and the original error; process
        # still exits 0 below.
        rpath.write_text(
            tekton.result_text_for_check_step_error(name, step_err), encoding="utf-8"
        )
    # Task step must exit 0 or Tekton may not publish results; the catalog documents this.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
