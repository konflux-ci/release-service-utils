#!/usr/bin/env python3
"""Filter the ``cve`` collector output down to a Conforma-facing RHSA summary.

The ``cve`` collector writes the full ACS scan of every component image into the
collectors results directory. This managed task reads that output, keeps only the
Critical/Important CVEs that carry an RHSA advisory, groups them per RHSA, attaches
each RHSA's ``released_on`` date from the Red Hat Security Data API, and overwrites
the raw collector file in place with a compact summary.

Overwriting in place matters: ``save-collectors-results`` persists every file in the
results directory verbatim into ``Release.status.collectors``; the raw scan can hit
etcd's object-size limit and block the release. The summary keeps only what Conforma
needs.

Filtering is per-CVE (ACS ``cveSeverity``): an errata's own severity can be higher
because of CVEs the image does not contain. This task performs no date math (Conforma
computes ``fix_by``/block) and emits no container grading verdict. Only
``released_on`` comes from the Security Data API (``/csaf.json?rhsa_ids=...``),
which supports batch lookups without auth.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file
from release_service_utils.helpers import http_client
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

_SEVERITY_RANK = {"CRITICAL": 2, "IMPORTANT": 1}
"""Gating ACS severities; membership is the gate, the rank picks the bucket."""

CSAF_INDEX_URL = "https://access.redhat.com/hydra/rest/securitydata/csaf.json"
"""Security Data API CSAF index; supports a comma-separated ``rhsa_ids`` batch."""

_RHSA_RE = re.compile(r"^RHSA-\d{4}:\d+$")
"""Exact RHSA id form; RHBA/RHEA/empty advisories are not gating."""


def find_cve_outputs(results_dir: Path) -> list[tuple[Path, dict[str, Any]]]:
    """Return ``(path, payload)`` for each ``cve`` collector output under *results_dir*.

    The ``cve`` collector is identified by content (a top-level ``cves`` list), not by
    filename: ``run-collectors`` names files ``<prefix>-<name>.json`` with a
    tenant-chosen name. Non-JSON files and collector outputs with a different shape
    (such as ``fixed-cve`` which emits ``{"releaseNotes": ...}``) are ignored.
    """
    outputs: list[tuple[Path, dict[str, Any]]] = []
    for path in file.contained_regular_files(results_dir, "*.json"):
        try:
            payload = file.load_json_dict(path)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("skipping unparseable file %s: %s", path, e)
            continue
        if isinstance(payload.get("cves"), list):
            outputs.append((path, payload))
    return outputs


def gating_records(payload: dict[str, Any], *, source: Path) -> list[dict[str, Any]]:
    """Return the Critical/Important records of *payload* that reference an RHSA.

    The returned records carry a normalized (whitespace-stripped) ``advisoryId``.
    """
    cves = payload.get("cves")
    if not isinstance(cves, list):
        raise ValueError(f"{source}: expected an object with a 'cves' list")

    records: list[dict[str, Any]] = []
    for record in cves:
        if not isinstance(record, dict):
            raise ValueError(f"{source}: expected every 'cves' entry to be an object")
        if record.get("cveSeverity") not in _SEVERITY_RANK:
            continue
        advisory = record.get("advisoryId")
        if not isinstance(advisory, str):
            continue
        advisory = advisory.strip()
        if not _RHSA_RE.match(advisory):
            continue
        records.append({**record, "advisoryId": advisory})
    return records


def fetch_errata(rhsas: list[str]) -> dict[str, dict[str, Any]]:
    """Return the Security Data API index entries keyed by RHSA id.

    Fail closed: a missing RHSA (the API answers ``200 []``) raises ``LookupError``
    naming the absent ids, because an absent RHSA means lag or a bug, not embargo.
    """
    if not rhsas:
        return {}
    unique = sorted(set(rhsas))
    body = http_client.get_text(f"{CSAF_INDEX_URL}?rhsa_ids={','.join(unique)}")
    entries = json.loads(body)
    if not isinstance(entries, list):
        raise ValueError(f"unexpected Security Data API response: {body[:200]}")
    by_rhsa = {entry["RHSA"]: entry for entry in entries if isinstance(entry, dict)}
    missing = [rhsa for rhsa in unique if rhsa not in by_rhsa]
    if missing:
        raise LookupError(f"no Security Data API entry for: {', '.join(missing)}")
    return by_rhsa


def _released_on(entry: dict[str, Any]) -> str:
    """Return the ``released_on`` string from a Security Data API entry, verbatim."""
    value = entry.get("released_on")
    if not isinstance(value, str) or not value:
        raise ValueError(f"{entry.get('RHSA')}: missing released_on")
    return value


def build_rhsa_summary(
    records: list[dict[str, Any]], errata: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Group *records* by RHSA into the Conforma-facing ``{"RHSA": ...}`` summary.

    Each RHSA appears once, in the bucket of the most severe ``cveSeverity`` present
    for it (CRITICAL if any gating CRITICAL CVE references it, else IMPORTANT). CVE
    ids and Konflux components are deduped and sorted; ``released_on`` is copied
    verbatim from the errata entry.
    """
    grouped: dict[str, dict[str, Any]] = {}
    for record in records:
        advisory = record["advisoryId"]
        rank = _SEVERITY_RANK[record["cveSeverity"]]
        entry = grouped.setdefault(
            advisory, {"rank": rank, "cves": set(), "components": set()}
        )
        entry["rank"] = max(entry["rank"], rank)
        cve = record.get("cveId")
        if cve:
            entry["cves"].add(cve)
        component = record.get("konfluxComponent")
        if component:
            entry["components"].add(component)

    buckets: dict[str, list[dict[str, Any]]] = {"CRITICAL": [], "IMPORTANT": []}
    for advisory in sorted(grouped):
        data = grouped[advisory]
        bucket = "CRITICAL" if data["rank"] == _SEVERITY_RANK["CRITICAL"] else "IMPORTANT"
        buckets[bucket].append(
            {
                "advisory": advisory,
                "released_on": _released_on(errata[advisory]),
                "cves": sorted(data["cves"]),
                "components": sorted(data["components"]),
            }
        )
    return {"RHSA": buckets}


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path* atomically (temp file in the same dir + replace).

    A partial write can never truncate *path*: on any failure before the rename the
    original file is untouched, so a retry still sees the raw collector output.
    """
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def filter_cve(results_dir: Path) -> list[Path]:
    """Overwrite each ``cve`` collector output in place with the RHSA summary."""
    outputs = find_cve_outputs(results_dir)
    if not outputs:
        raise FileNotFoundError(f"no cve collector output found in {results_dir}")

    records = [
        record for path, payload in outputs for record in gating_records(payload, source=path)
    ]
    errata = fetch_errata([record["advisoryId"] for record in records])
    summary = build_rhsa_summary(records, errata)

    logger.info(
        "filtered cve output: %d CRITICAL, %d IMPORTANT RHSAs",
        len(summary["RHSA"]["CRITICAL"]),
        len(summary["RHSA"]["IMPORTANT"]),
    )
    written: list[Path] = []
    for path, _ in outputs:
        _atomic_write_json(path, summary)
        written.append(path)
    return written


def main() -> int:
    """Read the Tekton environment and overwrite the cve collector output in place."""
    results_dir = Path(tekton.require_env("RESULTS_DIR"))
    for path in filter_cve(results_dir):
        logger.info("overwrote %s", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
