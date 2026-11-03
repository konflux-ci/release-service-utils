"""Test the ``get_iib_builds_load`` task script."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import requests

from release_service_utils.tasks.internal import get_iib_builds_load

IDX_18 = "registry-proxy.engineering.redhat.com/rh-osbs/iib-preview-rhtap:v4.18"
IDX_19 = "registry-proxy.engineering.redhat.com/rh-osbs/iib-preview-rhtap:v4.19"
IDX_20 = "registry-proxy.engineering.redhat.com/rh-osbs/iib-preview-rhtap:v4.20"
QUERY = "release_service_utils.helpers.iib.query_builds"


def _fake_query(totals: dict[str, Any]) -> Any:
    """Return a ``query_builds`` stand-in answering ``meta.total`` per from_index.

    A from_index mapped to an exception raises it instead.
    """

    def query(iib_url: str, *, user: str, from_index: str, state: str) -> dict[str, Any]:
        assert iib_url == "https://iib.example.com/api/v1"
        assert user == "iib-bot@REDHAT.COM"
        assert state == "in_progress"
        total = totals[from_index]
        if isinstance(total, Exception):
            raise total
        return {"items": [], "meta": {"total": total}}

    return query


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    from_indexes: str,
    *,
    url: str | None = "https://iib.example.com/api/v1",
    args: list[str] | None = None,
) -> tuple[int, str, str]:
    """Run ``main`` with Tekton env vars set; return (rc, result, loads).

    *args* replaces the default ``--from-indexes``/``--user`` arguments.
    """
    mount = tmp_path / "iib-services-config"
    mount.mkdir()
    if url is not None:
        (mount / "url").write_text(f"{url}\n", encoding="utf-8")
    rpath, lpath = tmp_path / "result", tmp_path / "loads"
    monkeypatch.setenv("RESULT_RESULT", str(rpath))
    monkeypatch.setenv("RESULT_LOADS", str(lpath))
    monkeypatch.setenv("IIB_SERVICES_CONFIG_MOUNT", str(mount))
    if args is None:
        args = ["--from-indexes", from_indexes, "--user", "iib-bot@REDHAT.COM"]
    rc = get_iib_builds_load.main(["get_iib_builds_load.py", *args])
    return rc, rpath.read_text(encoding="utf-8"), lpath.read_text(encoding="utf-8")


def test_main_writes_loads_and_skips_failed_lookups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed lookup is left out; the others are written and result is Success."""
    query = _fake_query({IDX_18: 3, IDX_19: 0, IDX_20: requests.HTTPError("503")})
    with mock.patch(QUERY, side_effect=query):
        rc, result, loads = _run(tmp_path, monkeypatch, json.dumps([IDX_18, IDX_19, IDX_20]))

    assert rc == 0
    assert result == "Success"
    # Compact JSON, same as the `jq -c` output the catalog task used to write.
    assert loads == f'{{"{IDX_18}":3,"{IDX_19}":0}}'


def test_main_missing_url_writes_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without the iib-services-config url, exit 0 with a failure result."""
    rc, result, loads = _run(tmp_path, monkeypatch, json.dumps([IDX_18]), url=None)

    assert rc == 0
    assert "Failed while reading the IIB url from iib-services-config" in result
    assert loads == "{}"


@pytest.mark.parametrize("value", ["not json", '{"a": 1}', "[1, 2]"])
def test_main_bad_from_indexes_writes_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """A --from-indexes value that isn't a JSON array of strings is a failure result."""
    rc, result, loads = _run(tmp_path, monkeypatch, value)

    assert rc == 0
    assert "Failed while parsing --from-indexes" in result
    assert loads == "{}"


def test_main_unexpected_error_writes_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unexpected exception still exits 0 with a failure result."""
    with mock.patch(QUERY, side_effect=RuntimeError("boom")):
        rc, result, loads = _run(tmp_path, monkeypatch, json.dumps([IDX_18]))

    assert rc == 0
    assert "Failed while getting the IIB builds load: boom" in result
    assert loads == "{}"


@pytest.mark.parametrize(
    "response",
    [
        {"items": []},
        {"items": [], "meta": {}},
        {"items": [], "meta": {"total": "3"}},
        {"items": [], "meta": {"total": True}},
    ],
)
def test_get_loads_skips_responses_without_integer_total(response: dict[str, Any]) -> None:
    """A response without an integer meta.total is left out."""
    with mock.patch(QUERY, return_value=response):
        loads = get_iib_builds_load.get_loads("https://iib", [IDX_18], "u")

    assert loads == {}


def test_get_loads_uses_meta_total_not_page_size() -> None:
    """The count is meta.total (all pages), not the number of items on the page."""
    response = {"items": [{"id": i} for i in range(20)], "meta": {"total": 364}}
    with mock.patch(QUERY, return_value=response):
        loads = get_iib_builds_load.get_loads("https://iib", [IDX_18], "u")

    assert loads == {IDX_18: 364}


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--from-indexes", "[]"],
        ["--user", "u"],
        ["--from-indexes", " ", "--user", "u"],
        ["--unknown"],
        ["-h"],
    ],
)
def test_main_bad_args_writes_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, argv: list[str]
) -> None:
    """Missing, blank, unknown, or help arguments exit 0 with a failure result."""
    rc, result, loads = _run(tmp_path, monkeypatch, "", args=argv)

    assert rc == 0
    assert "Failed while parsing the arguments: missing or invalid arguments" in result
    assert loads == "{}"


def test_main_loads_write_failure_writes_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A loads result that can't be written still exits 0 with a failure result."""
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path / "result"))
    # A directory can't be written as a file.
    monkeypatch.setenv("RESULT_LOADS", str(tmp_path))

    rc = get_iib_builds_load.main(["get_iib_builds_load.py", "--from-indexes", "[]"])

    assert rc == 0
    assert "Failed while getting the IIB builds load" in (tmp_path / "result").read_text(
        encoding="utf-8"
    )


def test_main_unwritable_results_still_exit_0(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When no result file can be written, log the error and still exit 0."""
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path))
    monkeypatch.setenv("RESULT_LOADS", str(tmp_path))

    assert get_iib_builds_load.main(["get_iib_builds_load.py"]) == 0


def test_main_missing_loads_env_writes_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing RESULT_LOADS is reported in RESULT_RESULT, still exiting 0."""
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path / "result"))
    monkeypatch.delenv("RESULT_LOADS", raising=False)

    rc = get_iib_builds_load.main(["get_iib_builds_load.py", "--from-indexes", "[]"])

    assert rc == 0
    assert "Failed while reading the loads result path: RESULT_LOADS must be set" in (
        tmp_path / "result"
    ).read_text(encoding="utf-8")


def test_main_missing_result_env_exits_1(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without RESULT_RESULT there is nowhere to report, so exit 1."""
    monkeypatch.delenv("RESULT_RESULT", raising=False)
    monkeypatch.setenv("RESULT_LOADS", "/tmp/loads")

    with pytest.raises(SystemExit) as e:
        get_iib_builds_load.main(["get_iib_builds_load.py"])
    assert e.value.code == 1
