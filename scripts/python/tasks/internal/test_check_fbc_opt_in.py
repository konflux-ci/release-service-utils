"""Tests for `check_fbc_opt_in`."""

from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path
from unittest import mock

import check_fbc_opt_in
import pytest
import requests
import tekton


def _write_service_account(
    d: Path, principal: str = "user@REALM", keytab: bytes = b"kt"
) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "principal").write_text(principal, encoding="utf-8")
    (d / "keytab").write_text(base64.b64encode(keytab).decode("ascii"), encoding="utf-8")


def _write_krb5(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "krb5.conf").write_text("[libdefaults]\n default_realm = FOO\n", encoding="utf-8")


def _no_kinit(*_a: object, **_k: object) -> None:
    return None


def test_parse_container_images_ok() -> None:
    """A JSON list of non-empty strings is parsed in order."""
    assert check_fbc_opt_in.parse_container_images('["a:b", "c:d"]') == ["a:b", "c:d"]


@pytest.mark.parametrize("raw", ['{"x": 1}', '["ok", ""]', "[1]"])
def test_parse_container_images_invalid(raw: str) -> None:
    """Non-array or non-string/blank items raise `ValueError`."""
    with pytest.raises(ValueError):
        check_fbc_opt_in.parse_container_images(raw)


def test_get_fbc_opt_in_true_false_and_missing() -> None:
    """Only explicit `fbc_opt_in: true` maps to `True`."""
    with mock.patch("http_client.get_text", return_value='{"fbc_opt_in": true}'):
        assert check_fbc_opt_in.get_fbc_opt_in("https://p", "r.io/repo/i:1", None) is True
    with mock.patch("http_client.get_text", return_value='{"fbc_opt_in": false}'):
        assert check_fbc_opt_in.get_fbc_opt_in("https://p", "r.io/repo/i:1", None) is False
    with mock.patch("http_client.get_text", return_value="{}"):
        assert check_fbc_opt_in.get_fbc_opt_in("https://p", "r.io/repo/i:1", None) is False


def test_get_fbc_opt_in_http_error_returns_false() -> None:
    """HTTP exceptions are treated as opt-out."""
    with mock.patch(
        "http_client.get_text",
        side_effect=requests.HTTPError("boom", response=mock.MagicMock()),
    ):
        assert check_fbc_opt_in.get_fbc_opt_in("https://p", "r.io/repo/i:1", None) is False


def test_run_check_returns_results_for_each_input(tmp_path: Path) -> None:
    """Each input image produces one output object with computed `fbcOptIn`."""
    sa = tmp_path / "sa"
    cfg = tmp_path / "cfg"
    _write_service_account(sa)
    _write_krb5(cfg)

    def _opt(_u: str, image: str, _a: object) -> bool:
        return image.endswith(":yes")

    out = check_fbc_opt_in.run_check(
        ["r/repo/i:yes", "r/repo/i:no"],
        "https://pyxis/v1",
        sa,
        cfg,
        kinit=_no_kinit,
        get_opt_in=_opt,
    )
    assert out == [
        {"containerImage": "r/repo/i:yes", "fbcOptIn": True},
        {"containerImage": "r/repo/i:no", "fbcOptIn": False},
    ]


def test_run_check_wraps_service_account_errors(tmp_path: Path) -> None:
    """Missing principal/keytab files become `CheckStepError` with mount context."""
    cfg = tmp_path / "cfg"
    _write_krb5(cfg)
    with pytest.raises(tekton.CheckStepError, match="mounted IIB service account"):
        check_fbc_opt_in.run_check(["r/repo/i:1"], "https://pyxis/v1", tmp_path / "sa", cfg)


def test_run_check_wraps_krb5_errors(tmp_path: Path) -> None:
    """Missing `krb5.conf` becomes `CheckStepError` with Kerberos context."""
    sa = tmp_path / "sa"
    _write_service_account(sa)
    with pytest.raises(tekton.CheckStepError, match="Kerberos configuration"):
        check_fbc_opt_in.run_check(["r/repo/i:1"], "https://pyxis/v1", sa, tmp_path / "cfg")


def test_run_check_wraps_kinit_error(tmp_path: Path) -> None:
    """A failed `kinit` command is wrapped as `CheckStepError`."""
    sa = tmp_path / "sa"
    cfg = tmp_path / "cfg"
    _write_service_account(sa)
    _write_krb5(cfg)

    def _fail_kinit(*_a: object, **_k: object) -> None:
        raise subprocess.CalledProcessError(1, "kinit")

    with pytest.raises(tekton.CheckStepError, match="logging in with Kerberos"):
        check_fbc_opt_in.run_check(
            ["r/repo/i:1"],
            "https://pyxis/v1",
            sa,
            cfg,
            kinit=_fail_kinit,
            get_opt_in=lambda _u, _i, _a: False,
        )


def test_main_writes_result_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`main` writes JSON to `RESULT_OPT_IN_RESULTS` and returns 0."""
    rpath = tmp_path / "result"
    sa = tmp_path / "sa"
    cfg = tmp_path / "cfg"
    _write_service_account(sa)
    _write_krb5(cfg)
    monkeypatch.setenv("RESULT_OPT_IN_RESULTS", str(rpath))
    monkeypatch.setenv("CONTAINER_IMAGES", '["r/repo/i:1"]')
    monkeypatch.setenv("PYXIS_URL", "https://pyxis/v1")
    monkeypatch.setenv("IIB_SERVICE_ACCOUNT_MOUNT", str(sa))
    monkeypatch.setenv("IIB_SERVICES_CONFIG_MOUNT", str(cfg))

    with mock.patch.object(check_fbc_opt_in, "run_check", return_value=[{"x": 1}]):
        out = check_fbc_opt_in.main()

    assert out == 0
    assert json.loads(rpath.read_text(encoding="utf-8")) == [{"x": 1}]


def test_main_requires_pyxis_url_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`PYXIS_URL` must be set."""
    rpath = tmp_path / "result"
    monkeypatch.setenv("RESULT_OPT_IN_RESULTS", str(rpath))
    monkeypatch.setenv("CONTAINER_IMAGES", '["r/repo/i:1"]')
    monkeypatch.delenv("PYXIS_URL", raising=False)
    with pytest.raises(SystemExit, match=r"check_fbc_opt_in\.py: PYXIS_URL must be set"):
        check_fbc_opt_in.main()


def test_main_missing_result_env_raises_system_exit() -> None:
    """Missing `RESULT_OPT_IN_RESULTS` is rejected by `tekton.result_paths`."""
    with pytest.raises(SystemExit):
        check_fbc_opt_in.main()


def test_main_invalid_container_images_exits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Invalid `CONTAINER_IMAGES` raises `SystemExit` with program prefix."""
    rpath = tmp_path / "result"
    monkeypatch.setenv("RESULT_OPT_IN_RESULTS", str(rpath))
    monkeypatch.setenv("CONTAINER_IMAGES", '{"bad": 1}')
    monkeypatch.setenv("PYXIS_URL", "https://pyxis/v1")
    with pytest.raises(SystemExit, match="check_fbc_opt_in.py"):
        check_fbc_opt_in.main()
