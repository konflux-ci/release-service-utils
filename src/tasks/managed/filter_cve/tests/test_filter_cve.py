"""Unit tests for the filter_cve managed task."""

from __future__ import annotations

import importlib
import json
import logging
from pathlib import Path

import pytest

filter_cve = importlib.import_module(
    "release_service_utils.tasks.managed.filter_cve.filter_cve"
)


def _record(
    cve: str = "CVE-2026-1",
    severity: str = "CRITICAL",
    advisory: str = "RHSA-2026:1",
    component: str = "my-component",
) -> dict[str, object]:
    return {
        "cveId": cve,
        "cveSeverity": severity,
        "advisoryId": advisory,
        "componentName": "expat",
        "konfluxComponent": component,
    }


def _errata(
    rhsa: str = "RHSA-2026:1",
    released_on: str = "2026-01-01T00:00:00Z",
) -> dict[str, object]:
    return {"RHSA": rhsa, "released_on": released_on, "CVEs": []}


def _write(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# --- cve-output detection by content ---------------------------------------


def test_find_cve_outputs_detects_by_content(tmp_path: Path) -> None:
    """A top-level ``cves`` list identifies the cve collector regardless of name."""
    _write(tmp_path / "tenant-anything.json", {"cves": []})
    _write(tmp_path / "managed-other.json", {"releaseNotes": {}})
    (tmp_path / "not-json.json").write_text("not json at all", encoding="utf-8")

    outputs = filter_cve.find_cve_outputs(tmp_path)

    assert [path.name for path, _ in outputs] == ["tenant-anything.json"]


def test_find_cve_outputs_ignores_non_dict_json(tmp_path: Path) -> None:
    """A JSON file whose root is a list is skipped, not an error."""
    _write(tmp_path / "list.json", [1, 2, 3])
    _write(tmp_path / "tenant-cve.json", {"cves": []})

    outputs = filter_cve.find_cve_outputs(tmp_path)

    assert [path.name for path, _ in outputs] == ["tenant-cve.json"]


def test_find_cve_outputs_warns_on_unparseable_file(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A corrupt file is skipped with a warning so it is not silently lost."""
    (tmp_path / "tenant-corrupt.json").write_text("{bad json", encoding="utf-8")
    _write(tmp_path / "tenant-cve.json", {"cves": []})

    with caplog.at_level(logging.WARNING, logger="release"):
        outputs = filter_cve.find_cve_outputs(tmp_path)

    assert [path.name for path, _ in outputs] == ["tenant-cve.json"]
    assert any("tenant-corrupt.json" in m for m in caplog.messages)


def test_filter_cve_fails_without_cve_output(tmp_path: Path) -> None:
    """No cve collector output is an error, not an empty result."""
    _write(tmp_path / "managed-other.json", {"releaseNotes": {}})
    with pytest.raises(FileNotFoundError, match="no cve collector output"):
        filter_cve.filter_cve(tmp_path)


# --- gating filter ----------------------------------------------------------


@pytest.mark.parametrize(
    ("severity", "advisory", "kept"),
    [
        ("CRITICAL", "RHSA-2026:1", True),
        ("IMPORTANT", "RHSA-2026:1", True),
        ("MODERATE", "RHSA-2026:1", False),
        ("LOW", "RHSA-2026:1", False),
        ("CRITICAL", "", False),
        ("CRITICAL", "RHBA-2026:1", False),
        ("CRITICAL", "RHEA-2026:1", False),
    ],
)
def test_gating_records_filters(
    tmp_path: Path, severity: str, advisory: str, kept: bool
) -> None:
    """Only Critical/Important CVEs that reference an RHSA advisory are gating."""
    payload = {"cves": [_record(severity=severity, advisory=advisory)]}

    records = filter_cve.gating_records(payload, source=tmp_path / "x.json")

    assert bool(records) is kept


def test_gating_records_normalizes_rhsa_whitespace(tmp_path: Path) -> None:
    """Surrounding whitespace on the advisory id is stripped."""
    payload = {"cves": [_record(advisory="  RHSA-2026:1\n")]}

    records = filter_cve.gating_records(payload, source=tmp_path / "x.json")

    assert records[0]["advisoryId"] == "RHSA-2026:1"


@pytest.mark.parametrize("advisory", ["RHSA-2026-1", "RHSA2026:1", "rhsa-2026:bad"])
def test_gating_records_raises_on_malformed_rhsa(tmp_path: Path, advisory: str) -> None:
    """A gating record with an RHSA-prefixed but malformed id fails closed."""
    payload = {"cves": [_record(advisory=advisory)]}

    with pytest.raises(ValueError, match="malformed RHSA advisory id"):
        filter_cve.gating_records(payload, source=tmp_path / "x.json")


def test_gating_records_ignores_malformed_rhsa_below_threshold(tmp_path: Path) -> None:
    """A non-gating-severity record with a malformed RHSA is ignored, not an error."""
    payload = {"cves": [_record(severity="MODERATE", advisory="RHSA-2026-1")]}

    assert filter_cve.gating_records(payload, source=tmp_path / "x.json") == []


@pytest.mark.parametrize("advisory", [None, 123, ["RHSA-2026:1"]])
def test_gating_records_ignores_non_string_advisory(tmp_path: Path, advisory: object) -> None:
    """A gating record whose ``advisoryId`` is not a string is ignored, not an error."""
    payload = {"cves": [{**_record(), "advisoryId": advisory}]}

    assert filter_cve.gating_records(payload, source=tmp_path / "x.json") == []


def test_gating_records_rejects_missing_cves_list(tmp_path: Path) -> None:
    """A collector output without a ``cves`` list fails closed."""
    with pytest.raises(ValueError, match="'cves' list"):
        filter_cve.gating_records({}, source=tmp_path / "x.json")


@pytest.mark.parametrize("entry", [None, "not-an-object", 42])
def test_gating_records_rejects_non_object_entry(tmp_path: Path, entry: object) -> None:
    """A non-object entry in ``cves`` fails closed rather than being dropped."""
    payload = {"cves": [entry, _record()]}
    with pytest.raises(ValueError, match="every 'cves' entry to be an object"):
        filter_cve.gating_records(payload, source=tmp_path / "x.json")


# --- errata fetch (fail closed) --------------------------------------------


def test_fetch_errata_returns_empty_without_ids() -> None:
    """No RHSA ids means no API call and an empty index."""
    assert filter_cve.fetch_errata([]) == {}


def test_fetch_errata_batches_unique_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """One request carries every unique RHSA id, and entries are keyed by RHSA."""
    urls: list[str] = []

    def fake_get(url: str) -> str:
        urls.append(url)
        return json.dumps([_errata(), _errata(rhsa="RHSA-2026:2")])

    monkeypatch.setattr(filter_cve.http_client, "get_text", fake_get)

    errata = filter_cve.fetch_errata(["RHSA-2026:2", "RHSA-2026:1", "RHSA-2026:2"])

    assert urls == [
        "https://access.redhat.com/hydra/rest/securitydata/csaf.json"
        "?rhsa_ids=RHSA-2026:1,RHSA-2026:2"
    ]
    assert sorted(errata) == ["RHSA-2026:1", "RHSA-2026:2"]


def test_fetch_errata_fails_closed_when_an_rhsa_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing errata (API answers ``[]``) names the absent id and raises."""
    monkeypatch.setattr(
        filter_cve.http_client, "get_text", lambda url: json.dumps([_errata()])
    )
    with pytest.raises(LookupError, match="RHSA-2026:2"):
        filter_cve.fetch_errata(["RHSA-2026:1", "RHSA-2026:2"])


def test_fetch_errata_rejects_unexpected_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-list body is an error, not an empty index."""
    monkeypatch.setattr(filter_cve.http_client, "get_text", lambda url: "{}")
    with pytest.raises(ValueError, match="unexpected Security Data API"):
        filter_cve.fetch_errata(["RHSA-2026:1"])


# --- grouping / summary shape ----------------------------------------------


def test_build_rhsa_summary_groups_and_dedupes() -> None:
    """One RHSA across packages/components yields a single deduped, sorted entry."""
    records = [
        _record(cve="CVE-2026-2", component="comp-b"),
        _record(cve="CVE-2026-1", component="comp-a"),
        _record(cve="CVE-2026-1", component="comp-a"),
    ]
    errata = {"RHSA-2026:1": _errata()}

    summary = filter_cve.build_rhsa_summary(records, errata)

    assert summary == {
        "RHSA": {
            "CRITICAL": [
                {
                    "advisory": "RHSA-2026:1",
                    "released_on": "2026-01-01T00:00:00Z",
                    "cves": ["CVE-2026-1", "CVE-2026-2"],
                    "components": ["comp-a", "comp-b"],
                }
            ],
            "IMPORTANT": [],
        }
    }


def test_build_rhsa_summary_uses_most_severe_cve_present() -> None:
    """An RHSA with both a CRITICAL and an IMPORTANT CVE lands in CRITICAL."""
    records = [
        _record(cve="CVE-2026-1", severity="IMPORTANT"),
        _record(cve="CVE-2026-2", severity="CRITICAL"),
    ]
    errata = {"RHSA-2026:1": _errata()}

    summary = filter_cve.build_rhsa_summary(records, errata)

    assert [e["advisory"] for e in summary["RHSA"]["CRITICAL"]] == ["RHSA-2026:1"]
    assert summary["RHSA"]["IMPORTANT"] == []


def test_build_rhsa_summary_separates_buckets_sorted() -> None:
    """Each bucket lists its RHSAs sorted by advisory id."""
    records = [
        _record(cve="CVE-2026-1", severity="IMPORTANT", advisory="RHSA-2026:9"),
        _record(cve="CVE-2026-2", severity="CRITICAL", advisory="RHSA-2026:2"),
        _record(cve="CVE-2026-3", severity="IMPORTANT", advisory="RHSA-2026:1"),
    ]
    errata = {
        "RHSA-2026:9": _errata("RHSA-2026:9"),
        "RHSA-2026:2": _errata("RHSA-2026:2"),
        "RHSA-2026:1": _errata("RHSA-2026:1"),
    }

    summary = filter_cve.build_rhsa_summary(records, errata)

    assert [e["advisory"] for e in summary["RHSA"]["CRITICAL"]] == ["RHSA-2026:2"]
    assert [e["advisory"] for e in summary["RHSA"]["IMPORTANT"]] == [
        "RHSA-2026:1",
        "RHSA-2026:9",
    ]


def test_build_rhsa_summary_passes_released_on_verbatim() -> None:
    """The ``released_on`` string is copied verbatim, not reformatted."""
    errata = {"RHSA-2026:1": _errata(released_on="2026-08-03T14:37:22Z")}

    summary = filter_cve.build_rhsa_summary([_record()], errata)

    assert summary["RHSA"]["CRITICAL"][0]["released_on"] == "2026-08-03T14:37:22Z"


@pytest.mark.parametrize("released_on", [None, "", 20260101])
def test_build_rhsa_summary_fails_closed_on_bad_released_on(
    released_on: object,
) -> None:
    """A missing, blank, or non-string ``released_on`` fails closed, naming the RHSA."""
    errata = {"RHSA-2026:1": {"RHSA": "RHSA-2026:1", "released_on": released_on}}
    with pytest.raises(ValueError, match="RHSA-2026:1"):
        filter_cve.build_rhsa_summary([_record()], errata)


# --- in-place overwrite / orchestration ------------------------------------


def test_filter_cve_overwrites_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The source file is overwritten with the summary; no ``cves`` key remains."""
    source = _write(
        tmp_path / "tenant-main.json",
        {"cves": [_record(), _record(advisory="")]},
    )
    monkeypatch.setattr(
        filter_cve, "fetch_errata", lambda rhsas, **kwargs: {"RHSA-2026:1": _errata()}
    )

    written = filter_cve.filter_cve(tmp_path)

    assert written == [source]
    payload = json.loads(source.read_text(encoding="utf-8"))
    assert "cves" not in payload
    assert set(payload["RHSA"]) == {"CRITICAL", "IMPORTANT"}
    assert [e["advisory"] for e in payload["RHSA"]["CRITICAL"]] == ["RHSA-2026:1"]


def test_filter_cve_empty_buckets_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both buckets are present even when no gating CVEs exist."""
    _write(tmp_path / "tenant-main.json", {"cves": [_record(severity="LOW")]})
    monkeypatch.setattr(filter_cve, "fetch_errata", lambda rhsas, **kwargs: {})

    written = filter_cve.filter_cve(tmp_path)

    payload = json.loads(written[0].read_text(encoding="utf-8"))
    assert payload == {"RHSA": {"CRITICAL": [], "IMPORTANT": []}}


def test_main_reads_results_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``main`` reads ``RESULTS_DIR``, overwrites in place, and returns 0."""
    source = _write(tmp_path / "managed-main.json", {"cves": [_record()]})
    monkeypatch.setenv("RESULTS_DIR", str(tmp_path))
    monkeypatch.setattr(
        filter_cve, "fetch_errata", lambda rhsas, **kwargs: {"RHSA-2026:1": _errata()}
    )

    assert filter_cve.main() == 0
    payload = json.loads(source.read_text(encoding="utf-8"))
    assert payload["RHSA"]["CRITICAL"][0]["advisory"] == "RHSA-2026:1"


def test_filter_cve_leaves_source_unchanged_on_lookup_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed errata lookup aborts before any write, so a retry still sees the scan."""
    original = {"cves": [_record()]}
    source = _write(tmp_path / "tenant-main.json", original)
    before = source.read_bytes()

    def boom(rhsas: list[str]) -> dict[str, object]:
        raise LookupError("no Security Data API entry for: RHSA-2026:1")

    monkeypatch.setattr(filter_cve, "fetch_errata", boom)

    with pytest.raises(LookupError, match="RHSA-2026:1"):
        filter_cve.filter_cve(tmp_path)

    assert source.read_bytes() == before


def test_atomic_write_json_removes_tmp_on_failure(tmp_path: Path) -> None:
    """A serialization failure leaves the target untouched and removes the temp file."""
    target = _write(tmp_path / "tenant-main.json", {"cves": []})
    before = target.read_bytes()

    # A set is not JSON-serializable, so json.dump fails mid-write.
    with pytest.raises(TypeError):
        filter_cve._atomic_write_json(target, {"bad": {1, 2}})

    assert target.read_bytes() == before
    assert list(tmp_path.glob(".*.tmp")) == []


def test_atomic_write_json_tolerates_missing_tmp_on_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the temp file is already gone during cleanup, the original error still raises."""

    def gone(_path: object) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(filter_cve.os, "unlink", gone)

    with pytest.raises(TypeError):
        filter_cve._atomic_write_json(tmp_path / "out.json", {"bad": {1, 2}})


def test_main_module_imports_entry_point() -> None:
    """The package ``__main__`` module wires up the task's ``main`` entry point."""
    module = importlib.import_module("release_service_utils.tasks.managed.filter_cve.__main__")

    assert module.main is filter_cve.main


def test_filter_cve_overwrites_every_output_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Records from all cve outputs merge into one summary written over each file."""
    file_a = _write(
        tmp_path / "managed-a.json",
        {"cves": [_record(cve="CVE-2026-1", advisory="RHSA-2026:1")]},
    )
    file_b = _write(
        tmp_path / "tenant-b.json",
        {"cves": [_record(cve="CVE-2026-2", advisory="RHSA-2026:2")]},
    )
    monkeypatch.setattr(
        filter_cve,
        "fetch_errata",
        lambda rhsas: {
            "RHSA-2026:1": _errata("RHSA-2026:1"),
            "RHSA-2026:2": _errata("RHSA-2026:2"),
        },
    )

    written = filter_cve.filter_cve(tmp_path)

    assert set(written) == {file_a, file_b}
    for path in (file_a, file_b):
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert "cves" not in payload
        advisories = [e["advisory"] for e in payload["RHSA"]["CRITICAL"]]
        assert advisories == ["RHSA-2026:1", "RHSA-2026:2"]
