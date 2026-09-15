"""Tests for `check_fbc_opt_in`."""

from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path
from unittest import mock

import pytest
import requests
from release_service_utils.helpers import tekton

import release_service_utils.tasks.internal.check_fbc_opt_in as check_fbc_opt_in

TASK = "release_service_utils.tasks.internal.check_fbc_opt_in"


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
    assert check_fbc_opt_in.check_fbc_opt_in.parse_container_images('["a:b", "c:d"]') == [
        "a:b",
        "c:d",
    ]


@pytest.mark.parametrize("raw", ['{"x": 1}', '["ok", ""]', "[1]"])
def test_parse_container_images_invalid(raw: str) -> None:
    """Non-array or non-string/blank items raise `TypeError` or `ValueError`."""
    with pytest.raises((TypeError, ValueError)):
        check_fbc_opt_in.check_fbc_opt_in.parse_container_images(raw)


def test_get_fbc_opt_in_true_false_and_missing() -> None:
    """Only explicit `fbc_opt_in: true` maps to `True`."""
    with mock.patch(
        f"{TASK}.check_fbc_opt_in.http_client.get_text",
        return_value='{"fbc_opt_in": true}',
    ) as m:
        assert (
            check_fbc_opt_in.check_fbc_opt_in.get_fbc_opt_in(
                "https://p", "r.io/repo/i:1", None
            )
            is True
        )
        m.assert_called_once()
        assert "/tag/" not in m.call_args[0][0]
    with mock.patch(
        f"{TASK}.check_fbc_opt_in.http_client.get_text",
        return_value='{"fbc_opt_in": false}',
    ) as m:
        assert (
            check_fbc_opt_in.check_fbc_opt_in.get_fbc_opt_in(
                "https://p", "r.io/repo/i:1", None
            )
            is False
        )
        assert "/tag/" not in m.call_args[0][0]
    with mock.patch(
        f"{TASK}.check_fbc_opt_in.http_client.get_text",
        return_value="{}",
    ) as m:
        assert (
            check_fbc_opt_in.check_fbc_opt_in.get_fbc_opt_in(
                "https://p", "r.io/repo/i:1", None
            )
            is False
        )
        assert "/tag/" not in m.call_args[0][0]


def test_get_fbc_opt_in_strips_digest() -> None:
    """A pull spec with a digest is stripped to the repository level."""
    spec = "r.io/repo/i@sha256:abc123"
    with mock.patch(
        f"{TASK}.check_fbc_opt_in.http_client.get_text",
        return_value='{"fbc_opt_in": true}',
    ) as m:
        assert (
            check_fbc_opt_in.check_fbc_opt_in.get_fbc_opt_in("https://p", spec, None) is True
        )
        url = m.call_args[0][0]
        assert "/tag/" not in url
        assert "sha256" not in url


def test_get_fbc_opt_in_http_error_raises_check_step_error() -> None:
    """HTTP/connection failures must not be treated as a legitimate opt-out."""
    with mock.patch(
        f"{TASK}.check_fbc_opt_in.http_client.get_text",
        side_effect=requests.HTTPError("boom", response=mock.MagicMock()),
    ):
        with pytest.raises(tekton.CheckStepError, match="querying Pyxis"):
            check_fbc_opt_in.check_fbc_opt_in.get_fbc_opt_in(
                "https://p", "r.io/repo/i:1", None
            )


def test_get_fbc_opt_in_connection_error_raises_check_step_error() -> None:
    """A network-level failure (Pyxis unreachable) raises, not a silent opt-out."""
    with mock.patch(
        f"{TASK}.check_fbc_opt_in.http_client.get_text",
        side_effect=requests.ConnectionError("connection refused"),
    ):
        with pytest.raises(tekton.CheckStepError, match="querying Pyxis"):
            check_fbc_opt_in.check_fbc_opt_in.get_fbc_opt_in(
                "https://p", "r.io/repo/i:1", None
            )


def test_get_fbc_opt_in_malformed_json_raises_check_step_error() -> None:
    """A non-JSON Pyxis response raises rather than being treated as opt-out."""
    with mock.patch(
        f"{TASK}.check_fbc_opt_in.http_client.get_text",
        return_value="not json",
    ):
        with pytest.raises(tekton.CheckStepError, match="parsing the Pyxis response"):
            check_fbc_opt_in.check_fbc_opt_in.get_fbc_opt_in(
                "https://p", "r.io/repo/i:1", None
            )


def test_run_check_returns_results_for_each_input(tmp_path: Path) -> None:
    """Each input image produces one output object with computed `fbcOptIn`."""
    sa = tmp_path / "sa"
    cfg = tmp_path / "cfg"
    _write_service_account(sa)
    _write_krb5(cfg)

    def _opt(_u: str, image: str, _a: object) -> bool:
        return image.endswith(":yes")

    out = check_fbc_opt_in.check_fbc_opt_in.run_check(
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
        check_fbc_opt_in.check_fbc_opt_in.run_check(
            ["r/repo/i:1"], "https://pyxis/v1", tmp_path / "sa", cfg
        )


def test_run_check_wraps_krb5_errors(tmp_path: Path) -> None:
    """Missing `krb5.conf` becomes `CheckStepError` with Kerberos context."""
    sa = tmp_path / "sa"
    _write_service_account(sa)
    with pytest.raises(tekton.CheckStepError, match="Kerberos configuration"):
        check_fbc_opt_in.check_fbc_opt_in.run_check(
            ["r/repo/i:1"], "https://pyxis/v1", sa, tmp_path / "cfg"
        )


def test_run_check_wraps_kinit_error(tmp_path: Path) -> None:
    """A failed `kinit` command is wrapped as `CheckStepError`."""
    sa = tmp_path / "sa"
    cfg = tmp_path / "cfg"
    _write_service_account(sa)
    _write_krb5(cfg)

    def _fail_kinit(*_a: object, **_k: object) -> None:
        raise subprocess.CalledProcessError(1, "kinit")

    with pytest.raises(tekton.CheckStepError, match="logging in with Kerberos"):
        check_fbc_opt_in.check_fbc_opt_in.run_check(
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

    with mock.patch.object(
        check_fbc_opt_in.check_fbc_opt_in, "run_check", return_value=[{"x": 1}]
    ):
        out = check_fbc_opt_in.check_fbc_opt_in.main()

    assert out == 0
    assert json.loads(rpath.read_text(encoding="utf-8")) == [{"x": 1}]


def test_main_requires_pyxis_url_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Missing `PYXIS_URL` fails the step via `SystemExit`.

    This task runs as its own InternalRequest/PipelineRun; the managed caller
    checks the InternalRequest's Succeeded condition before trusting the
    result content, so validation failures must fail the step, not exit 0.
    """
    rpath = tmp_path / "result"
    monkeypatch.setenv("RESULT_OPT_IN_RESULTS", str(rpath))
    monkeypatch.setenv("CONTAINER_IMAGES", '["r/repo/i:1"]')
    monkeypatch.delenv("PYXIS_URL", raising=False)
    assert check_fbc_opt_in.check_fbc_opt_in.main() == 0
    assert json.loads(rpath.read_text(encoding="utf-8")) == []


def test_main_missing_result_env_raises_system_exit() -> None:
    """Missing `RESULT_OPT_IN_RESULTS` is rejected by `tekton.result_paths`."""
    with pytest.raises(SystemExit):
        check_fbc_opt_in.check_fbc_opt_in.main()


def test_main_invalid_container_images_raises_system_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Invalid `CONTAINER_IMAGES` writes empty result and returns 0."""
    rpath = tmp_path / "result"
    monkeypatch.setenv("RESULT_OPT_IN_RESULTS", str(rpath))
    monkeypatch.setenv("CONTAINER_IMAGES", '{"bad": 1}')
    monkeypatch.setenv("PYXIS_URL", "https://pyxis/v1")
    assert check_fbc_opt_in.check_fbc_opt_in.main() == 0
    assert json.loads(rpath.read_text(encoding="utf-8")) == []


def test_main_pyxis_outage_does_not_fake_opt_in(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A Pyxis outage during the check must not produce a fake opt-in result.

    The managed caller (prepare-fbc-parameters) would trust whatever is in
    the result file as if it were a real opt-in decision, so the result must
    be an empty list — no image should appear opted in.
    """
    rpath = tmp_path / "result"
    monkeypatch.setenv("RESULT_OPT_IN_RESULTS", str(rpath))
    monkeypatch.setenv("CONTAINER_IMAGES", '["r/repo/i:1"]')
    monkeypatch.setenv("PYXIS_URL", "https://pyxis/v1")

    outage = tekton.CheckStepError(
        "querying Pyxis for FBC opt-in status of r/repo/i:1",
        requests.ConnectionError("connection refused"),
    )
    with mock.patch.object(check_fbc_opt_in.check_fbc_opt_in, "run_check", side_effect=outage):
        assert check_fbc_opt_in.check_fbc_opt_in.main() == 0

    # The result file must exist but contain an empty list — no image
    # should be reported as opted in when Pyxis was unreachable.
    assert json.loads(rpath.read_text(encoding="utf-8")) == []
