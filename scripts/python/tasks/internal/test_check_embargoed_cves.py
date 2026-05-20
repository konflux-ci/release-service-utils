"""Tests for ``check_embargoed_cves``."""

from __future__ import annotations

import base64
import runpy
import subprocess
from pathlib import Path
from unittest import mock

import check_embargoed_cves
import file
import pytest
import tekton


def _write_service_account(
    d: Path, *, name: str = "myname", keytab: bytes = b"kb", url: str = "myurl"
) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "name").write_text(name, encoding="utf-8")
    (d / "base64_keytab").write_text(
        base64.b64encode(keytab).decode("ascii"), encoding="utf-8"
    )
    (d / "osidb_url").write_text(url, encoding="utf-8")


def _minimal_krb5(path: Path) -> Path:
    path.write_text(
        "# t\n[libdefaults]\n    default_realm = FOO\n",
        encoding="utf-8",
    )
    return path


def _no_kinit(
    *args: object,
    **kwargs: object,
) -> None:
    del args, kwargs


def _setup_tekton_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mount: Path
) -> tuple[Path, Path]:
    rpath = tmp_path / "r"
    epath = tmp_path / "e"
    monkeypatch.setenv("RESULT_RESULT", str(rpath))
    monkeypatch.setenv("RESULT_EMBARGOED_CVES", str(epath))
    monkeypatch.setenv("OSIDB_SERVICE_ACCOUNT_MOUNT", str(mount))
    return rpath, epath


def _is_expected_embargo_outcome(s: str) -> bool:
    return "embargoed" in s or "not clearly public" in s


def test_parse_cve_list() -> None:
    """Whitespace around tokens is stripped; multiple CVEs split on whitespace."""
    assert check_embargoed_cves.parse_cve_list("  CVE-1  CVE-2  ") == ["CVE-1", "CVE-2"]


@pytest.mark.parametrize(
    ("payload", "exp"),
    [
        ({}, True),
        ({"results": []}, True),
        ({"results": None}, True),
        ({"results": ["not-a-dict"]}, True),
        ({"results": [{}]}, True),
        ({"results": [{"embargoed": None}]}, True),
        ({"results": [{"embargoed": True}]}, True),
        ({"results": [{"embargoed": False}]}, False),
    ],
)
def test_is_embargoed_flaw_response(payload: dict, exp: bool) -> None:
    """Only ``results[0].embargoed == false`` means not embargoed; all other shapes do."""
    assert check_embargoed_cves.is_embargoed_flaw_response(payload) is exp


def test_fetch_flaw_state_empty_bodies() -> None:
    """An empty body from the flaws request is an empty result dict."""
    with mock.patch("http_client.get_text", return_value="") as m:
        d = check_embargoed_cves.fetch_flaw_state("https://u", "t", "CVE-1")
        m.assert_called()
    assert d == {}


def test_fetch_flaw_state_parses_json_body() -> None:
    """A non-empty flaws body is parsed with `json.loads` and returned as a dict."""
    body = '{"results": [{"cve_id": "CVE-1", "embargoed": false}]}'
    with mock.patch("http_client.get_text", return_value=body):
        out = check_embargoed_cves.fetch_flaw_state("https://u", "tok", "CVE-1")
    assert out == {"results": [{"cve_id": "CVE-1", "embargoed": False}]}


def test_parse_args_help() -> None:
    """``-h`` / ``--help`` prints usage to stderr and exits with code 1."""
    with pytest.raises(SystemExit) as e:
        check_embargoed_cves.parse_args(["-h"])
    assert e.value.code == 1


def test_parse_args_missing_cves() -> None:
    """Missing or blank ``--cves`` prints usage and exits with code 1."""
    with pytest.raises(SystemExit) as e:
        check_embargoed_cves.parse_args(["--cves", "  "])
    assert e.value.code == 1


def test_parse_args_rejects_trailing_junk() -> None:
    """argparse fails extra argv tokens (e.g. stray positionals) with exit 2."""
    with pytest.raises(SystemExit) as e:
        check_embargoed_cves.parse_args(["--cves", "CVE-1", "extra"])
    assert e.value.code == 2


def test_parse_args_ok() -> None:
    """Valid ``--cves`` yields a namespace with the raw string (including for multi-CVE)."""
    a = check_embargoed_cves.parse_args(["--cves", "CVE-1"])
    assert a.cves == "CVE-1"


def test_main_passes_service_account_mount_resolved_by_environ(
    sa_mount: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The mount directory passed to ``run_check`` is
    ``path_from_env_variable("OSIDB_SERVICE_ACCOUNT_MOUNT", ...)`` (set vs default).
    """
    _setup_tekton_env(tmp_path, monkeypatch, sa_mount)
    seen: list[Path] = []

    def _fake_run_check(_cve: list[str], mount: Path, **_k: object) -> tuple[list[str], int]:
        seen.append(mount)
        return ([], 0)

    with mock.patch.object(check_embargoed_cves, "run_check", side_effect=_fake_run_check):
        check_embargoed_cves.main(["check_embargoed_cves.py", "--cves", "CVE-1"])
    assert seen[0] == sa_mount
    assert seen[0] == file.path_from_env_variable(
        "OSIDB_SERVICE_ACCOUNT_MOUNT", "/mnt/osidb-service-account"
    )
    seen.clear()
    monkeypatch.delenv("OSIDB_SERVICE_ACCOUNT_MOUNT", raising=False)
    with mock.patch.object(check_embargoed_cves, "run_check", side_effect=_fake_run_check):
        check_embargoed_cves.main(["check_embargoed_cves.py", "--cves", "CVE-1"])
    assert seen[0] == Path("/mnt/osidb-service-account")
    assert seen[0] == file.path_from_env_variable(
        "OSIDB_SERVICE_ACCOUNT_MOUNT", "/mnt/osidb-service-account"
    )


@pytest.fixture
def sa_mount(tmp_path: Path) -> Path:
    p = tmp_path / "m"
    _write_service_account(p)
    return p


def test_run_check_all_flaws_not_embargoed(sa_mount: Path, tmp_path: Path) -> None:
    """When every flaw response shows not embargoed, the affected list is empty and rc is 0."""
    krb5 = _minimal_krb5(tmp_path / "k5.conf")

    def gtok(_: str) -> str:
        return "dummy-token"

    def gflav(_u: str, _t: str, _c: str) -> dict:  # noqa: ARG001
        return {"results": [{"embargoed": False}]}

    p, r = check_embargoed_cves.run_check(
        ["CVE-123", "CVE-456"],
        sa_mount,
        kinit=_no_kinit,
        get_token=gtok,
        get_flaw=gflav,
        krb5_template=krb5,
    )
    assert p == [] and r == 0


def test_run_check_rejects_empty_cve_list(sa_mount: Path) -> None:
    """`run_check` with no CVE ids raises `ValueError` before any API or kinit work."""
    with pytest.raises(ValueError, match="no CVEs"):
        check_embargoed_cves.run_check([], sa_mount)


def test_run_check_wraps_service_account_read_errors(tmp_path: Path) -> None:
    """Errors while reading the mounted service account are wrapped as `CheckStepError`."""
    with pytest.raises(tekton.CheckStepError) as e:
        check_embargoed_cves.run_check(["CVE-1"], tmp_path / "missing-mount")
    assert "reading the mounted OSIDB service account" in str(e.value)


def test_run_check_wraps_krb5_read_errors(sa_mount: Path, tmp_path: Path) -> None:
    """If the Kerberos template cannot be read, `run_check` raises `CheckStepError`."""
    with pytest.raises(tekton.CheckStepError) as e:
        check_embargoed_cves.run_check(
            ["CVE-1"],
            sa_mount,
            kinit=_no_kinit,
            get_token=lambda _: "dummy-token",
            get_flaw=lambda _u, _t, _c: {"results": [{"embargoed": False}]},
            krb5_template=tmp_path / "missing-krb5.conf",
        )
    assert "reading the Kerberos configuration" in str(e.value)


def test_run_check_wraps_kinit_called_process_error(sa_mount: Path, tmp_path: Path) -> None:
    """`CalledProcessError` from the injected `kinit` is wrapped with a kinit action."""
    krb5 = _minimal_krb5(tmp_path / "k5.conf")

    def _kfail(*_a: object, **_k: object) -> None:
        raise subprocess.CalledProcessError(1, "kinit")

    with pytest.raises(tekton.CheckStepError) as e:
        check_embargoed_cves.run_check(
            ["CVE-1"],
            sa_mount,
            kinit=_kfail,
            get_token=lambda _: "dummy-token",
            get_flaw=lambda _u, _t, _c: {"results": [{"embargoed": False}]},
            krb5_template=krb5,
        )
    assert "logging in with Kerberos (kinit)" in str(e.value)


def test_run_check_wraps_get_token_errors(sa_mount: Path, tmp_path: Path) -> None:
    """Token acquisition failures are wrapped as a `CheckStepError` with token context."""
    krb5 = _minimal_krb5(tmp_path / "k5.conf")

    def _gtok_fail(_u: str) -> str:
        raise ValueError("bad token response")

    with pytest.raises(tekton.CheckStepError) as e:
        check_embargoed_cves.run_check(
            ["CVE-1"],
            sa_mount,
            kinit=_no_kinit,
            get_token=_gtok_fail,
            get_flaw=lambda _u, _t, _c: {"results": [{"embargoed": False}]},
            krb5_template=krb5,
        )
    assert "getting an OSIDB access token" in str(e.value)


def test_run_check_wraps_get_flaw_errors(sa_mount: Path, tmp_path: Path) -> None:
    """Flaw API parse/request failures are wrapped as a `CheckStepError`."""
    krb5 = _minimal_krb5(tmp_path / "k5.conf")

    def _gflaw_fail(_u: str, _t: str, _c: str) -> dict:
        raise ValueError("malformed flaws json")

    with pytest.raises(tekton.CheckStepError) as e:
        check_embargoed_cves.run_check(
            ["CVE-1"],
            sa_mount,
            kinit=_no_kinit,
            get_token=lambda _: "dummy-token",
            get_flaw=_gflaw_fail,
            krb5_template=krb5,
        )
    assert "querying the OSIDB flaws API" in str(e.value)


def test_run_check_empty_body_treated_as_embargoed(sa_mount: Path, tmp_path: Path) -> None:
    """An empty flaws dict has no clear ``embargoed: false``; CVE is reported."""
    krb5 = _minimal_krb5(tmp_path / "k5.conf")

    def gtok(_: str) -> str:
        return "dummy-token"

    def gflav(_u: str, _t: str, cve: str) -> dict:  # noqa: ARG001
        if cve == "CVE-noaccess":
            return {}
        return {"results": [{"embargoed": False}]}

    p, r = check_embargoed_cves.run_check(
        ["CVE-noaccess"],
        sa_mount,
        kinit=_no_kinit,
        get_token=gtok,
        get_flaw=gflav,
        krb5_template=krb5,
    )
    assert p == ["CVE-noaccess"] and r == 1


def test_run_check_mixed_list_reports_embargoed_cve(sa_mount: Path, tmp_path: Path) -> None:
    """Only CVEs with embargoed (or not clearly public) flaw data are returned."""
    krb5 = _minimal_krb5(tmp_path / "k5.conf")

    def gtok(_: str) -> str:
        return "dummy-token"

    def gflav(_u: str, _t: str, cve: str) -> dict:  # noqa: ARG001
        if cve == "CVE-embargo":
            return {"results": [{"embargoed": True}]}
        return {"results": [{"embargoed": False}]}

    p, r = check_embargoed_cves.run_check(
        ["CVE-123", "CVE-embargo"],
        sa_mount,
        kinit=_no_kinit,
        get_token=gtok,
        get_flaw=gflav,
        krb5_template=krb5,
    )
    assert p == ["CVE-embargo"] and r == 1


def test_main_all_clear(
    sa_mount: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    ``Success`` in ``RESULT_RESULT`` and an empty embargo list when
    ``run_check`` finds no issues.
    """
    rpath, epath = _setup_tekton_env(tmp_path, monkeypatch, sa_mount)
    with mock.patch.object(
        check_embargoed_cves,
        "run_check",
        return_value=([], 0),
    ) as run_mock:
        out = check_embargoed_cves.main(
            ["check_embargoed_cves.py", "--cves", "CVE-123 CVE-456"]
        )
    run_mock.assert_called()
    assert out == 0
    assert rpath.read_text() == "Success"
    assert epath.read_text() == ""


def test_main_cve_treated_inaccessible(
    sa_mount: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With affected CVEs from ``run_check``, the main result file gets the embargo summary."""
    rpath, epath = _setup_tekton_env(tmp_path, monkeypatch, sa_mount)
    with mock.patch.object(
        check_embargoed_cves,
        "run_check",
        return_value=(["CVE-noaccess"], 1),
    ):
        out = check_embargoed_cves.main(["check_embargoed_cves.py", "--cves", "CVE-noaccess"])
    assert out == 0
    assert _is_expected_embargo_outcome(rpath.read_text())
    assert epath.read_text() == "CVE-noaccess "


def test_main_mixed_cves_one_reported_embargoed(
    sa_mount: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Single affected id is written to the embargo result, summary in the
    other.
    """
    rpath, epath = _setup_tekton_env(tmp_path, monkeypatch, sa_mount)
    with mock.patch.object(
        check_embargoed_cves,
        "run_check",
        return_value=(["CVE-embargo"], 1),
    ):
        out = check_embargoed_cves.main(
            ["check_embargoed_cves.py", "--cves", "CVE-123 CVE-embargo"]
        )
    assert out == 0
    assert _is_expected_embargo_outcome(rpath.read_text())
    assert epath.read_text() == "CVE-embargo "


def test_main_kinit_failure_writes_subprocess_error(
    sa_mount: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``CheckStepError`` from ``run_check`` becomes a formatted one-line result message."""
    rpath, epath = _setup_tekton_env(tmp_path, monkeypatch, sa_mount)
    e = subprocess.CalledProcessError(1, "kinit")
    with mock.patch.object(
        check_embargoed_cves,
        "run_check",
        side_effect=tekton.CheckStepError("logging in with Kerberos (kinit)", e),
    ):
        out = check_embargoed_cves.main(["c", "--cves", "CVE-1"])
    assert out == 0
    t = rpath.read_text()
    assert "logging in with Kerberos" in t
    assert "kinit" in t.lower()
    assert "Failed while" in t
    assert t.startswith("c:")  # program basename from argv[0]
    assert epath.read_text() == ""


def test_main_arg_parse_returns_one() -> None:
    """
    ``main`` returns the same exit code as ``parse_args``/argparse (1 for
    help, missing ``--cves``).
    """
    assert check_embargoed_cves.main(["p"]) == 1
    assert check_embargoed_cves.main(["p", "-h"]) == 1


def test_main_parse_args_non_int_exit_code_returns_one() -> None:
    """A non-int `SystemExit.code` from parsing maps to return code 1 in `main`."""
    with mock.patch.object(check_embargoed_cves, "parse_args", side_effect=SystemExit("x")):
        assert check_embargoed_cves.main(["p", "--cves", "CVE-1"]) == 1


def test_main_returns_one_when_cve_list_is_empty_after_parsing() -> None:
    """An empty parsed CVE list returns 1 before touching Tekton result paths."""
    ns = mock.MagicMock(cves="   ")
    with mock.patch.object(check_embargoed_cves, "parse_args", return_value=ns):
        assert check_embargoed_cves.main(["p", "--cves", "ignored"]) == 1


def test_main_missing_tekton_env() -> None:
    """Without ``RESULT_*`` env vars, ``result_paths`` raises ``SystemExit(1)``."""
    with mock.patch.object(
        check_embargoed_cves, "parse_args", return_value=mock.MagicMock(cves="CVE-1")
    ):
        with pytest.raises(SystemExit) as e:
            check_embargoed_cves.main(["p", "--cves", "CVE-1"])
    assert e.value.code == 1


def test_main_wraps_unexpected_exceptions_in_running_check_action(
    sa_mount: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unexpected exceptions from `run_check` are wrapped as `running the check`."""
    rpath, epath = _setup_tekton_env(tmp_path, monkeypatch, sa_mount)
    with mock.patch.object(
        check_embargoed_cves, "run_check", side_effect=RuntimeError("boom")
    ):
        out = check_embargoed_cves.main(["check_embargoed_cves.py", "--cves", "CVE-1"])
    assert out == 0
    text = rpath.read_text()
    assert "running the check" in text
    assert "boom" in text
    assert epath.read_text() == ""


def test_module_main_guard_raises_system_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Executing the file as `__main__` triggers the bottom `raise SystemExit(main())`."""
    monkeypatch.setattr("sys.argv", ["check_embargoed_cves.py"])
    with pytest.raises(SystemExit) as e:
        runpy.run_path(str(Path(check_embargoed_cves.__file__)), run_name="__main__")
    assert e.value.code == 1
