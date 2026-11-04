#!/usr/bin/env python3
"""Get the number of in-progress IIB builds for each given from_index.

* Reads the IIB API url from ``/mnt/iib-services-config/url`` (or
  ``IIB_SERVICES_CONFIG_MOUNT``).
* For each from_index in ``--from-indexes`` (a JSON array), queries IIB for the
  in-progress builds submitted by ``--user`` and records ``meta.total``, the
  match count across all pages. A from_index whose count can't be fetched is
  left out of the loads result, so callers can rank it as unknown.
* Writes result paths from ``RESULT_RESULT`` and ``RESULT_LOADS`` (set by the
  Tekton task). ``RESULT_LOADS`` is a compact JSON object mapping each
  from_index to its count, e.g. ``{"registry.io/index:v4.18":3}``.
* Always exits with status ``0`` once ``RESULT_RESULT`` is set; success or
  failure, including missing or malformed arguments and a missing
  ``RESULT_LOADS``, is in the result files. Without ``RESULT_RESULT`` there is
  nowhere to report, so it exits with ``1``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import requests

from release_service_utils.helpers import authentication
from release_service_utils.helpers import file
from release_service_utils.helpers import iib
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

PROG = "get_iib_builds_load.py"
USAGE = (
    f"usage: {PROG} --from-indexes '<json array>' --user <iib user>\n"
    f"  --from-indexes  JSON array of from_index pullspecs to count in-progress builds for\n"
    f"  --user          IIB user whose in-progress builds are counted\n"
)


def parse_args(argv: list[str]) -> Any:
    """Parse ``--from-indexes`` and ``--user``; print usage and exit 1 if missing."""
    p = tekton.tekton_argument_parser(PROG)
    p.add_argument("-h", "--help", action="store_true")
    p.add_argument("--from-indexes", metavar="JSON")
    p.add_argument("--user", metavar="USER")
    ns = p.parse_args(argv)
    if ns.help or tekton.missing_blank_option_values(
        ("--from-indexes", ns.from_indexes), ("--user", ns.user)
    ):
        tekton.exit_with_usage(USAGE)
    return ns


def parse_from_indexes(value: str) -> list[str]:
    """Return the from_indexes from a JSON array of strings.

    Raise ``ValueError`` if *value* is not a JSON array of strings.
    """
    from_indexes = json.loads(value)
    if not isinstance(from_indexes, list) or not all(isinstance(i, str) for i in from_indexes):
        raise ValueError(f"expected a JSON array of strings, got: {value}")
    return from_indexes


def get_loads(iib_url: str, from_indexes: list[str], user: str) -> dict[str, int]:
    """Return the in-progress build count of *user* for each from_index.

    A from_index whose count can't be fetched or parsed is logged and left out.
    """
    loads: dict[str, int] = {}
    for from_index in from_indexes:
        try:
            response = iib.query_builds(
                iib_url, user=user, from_index=from_index, state="in_progress"
            )
            # meta.total counts every page; items only holds the first one.
            total = dict(response)["meta"]["total"]
            if not isinstance(total, int) or isinstance(total, bool):
                raise ValueError(f"meta.total is not an integer: {total!r}")
        except (
            requests.RequestException,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as e:
            logger.warning(
                "Could not get the in-progress IIB builds for %s: %s", from_index, e
            )
            continue
        logger.info("%s: %d in-progress builds", from_index, total)
        loads[from_index] = total
    return loads


def main(argv: list[str] | None = None) -> int:
    """Run the task: write the loads and result files, and return 0.

    Every failure once ``RESULT_RESULT`` is known, including a missing
    ``RESULT_LOADS``, bad arguments and result-file writes, is written to
    ``RESULT_RESULT`` instead of failing the step, so Tekton still publishes the
    results. If ``RESULT_RESULT`` is not set, raise ``SystemExit(1)``.
    """
    (rpath,) = tekton.result_paths_from_env("RESULT_RESULT")
    name = str(Path((argv or sys.argv)[0]).name)
    a = sys.argv[1:] if argv is None else argv[1:]
    mount = file.path_from_env_variable(
        "IIB_SERVICES_CONFIG_MOUNT", "/mnt/iib-services-config"
    )

    try:
        try:
            (lpath,) = tekton.result_paths_from_env("RESULT_LOADS")
        except SystemExit as e:
            raise tekton.CheckStepError(
                "reading the loads result path", ValueError("RESULT_LOADS must be set")
            ) from e
        # Written up front so the loads result is valid JSON even if a step raises.
        lpath.write_text("{}", encoding="utf-8")
        try:
            args = parse_args(a)
        except SystemExit as e:
            # parse_args already printed the usage to stderr.
            raise tekton.CheckStepError(
                "parsing the arguments", ValueError("missing or invalid arguments")
            ) from e
        try:
            from_indexes = parse_from_indexes(args.from_indexes)
        except ValueError as e:
            raise tekton.CheckStepError("parsing --from-indexes", e) from e
        try:
            iib_url = authentication.read_mounted_text(mount, "url")
        except OSError as e:
            raise tekton.CheckStepError(
                "reading the IIB url from iib-services-config", e
            ) from e
        loads = get_loads(iib_url, from_indexes, args.user)
        lpath.write_text(json.dumps(loads, separators=(",", ":")), encoding="utf-8")
        rpath.write_text("Success", encoding="utf-8")
    except Exception as e:
        # Internal task: never fail the step, or Tekton drops the results.
        try:
            tekton.write_failure_result(
                rpath, name, e, workflow_action="getting the IIB builds load"
            )
        except OSError as write_error:
            logger.error("Could not write the failure result: %s", write_error)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
