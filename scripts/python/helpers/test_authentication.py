"""Tests for the ``authentication`` helper module."""

from __future__ import annotations

import base64
import subprocess
from pathlib import Path
from unittest import mock

import pytest

import authentication


def test_read_mounted_text_strips_whitespace(tmp_path: Path) -> None:
    """Stripped file contents are returned without surrounding whitespace."""
    f = tmp_path / "f"
    f.write_text("  a\nb  ", encoding="utf-8")
    assert authentication.read_mounted_text(tmp_path, "f") == "a\nb"


def test_load_keytab_from_mount_resolves_princ_and_keytab(tmp_path: Path) -> None:
    """Principal and decoded keytab bytes come from the paths given by the keyword args."""
    d = tmp_path
    (d / "name").write_text("p@REALM", encoding="utf-8")
    (d / "base64_keytab").write_text(
        base64.b64encode(b"ktab").decode("ascii"), encoding="utf-8"
    )
    princ, raw = authentication.load_keytab_from_mount(
        d, principal_file="name", keytab_b64_file="base64_keytab"
    )
    assert princ == "p@REALM" and raw == b"ktab"


def test_load_keytab_from_mount_renamed_files(tmp_path: Path) -> None:
    """
    Any filenames passed as *principal_file* and *keytab_b64_file* are read
    under the mount.
    """
    d = tmp_path
    (d / "x").write_text("other", encoding="utf-8")
    (d / "y").write_text(base64.b64encode(b"A").decode("ascii"), encoding="utf-8")
    princ, raw = authentication.load_keytab_from_mount(
        d, principal_file="x", keytab_b64_file="y"
    )
    assert princ == "other" and raw == b"A"


def test_load_service_account_includes_text_files_in_dict(tmp_path: Path) -> None:
    """*text_files* entries are the third return value, keyed by filename."""
    d = tmp_path
    (d / "name").write_text("u1", encoding="utf-8")
    (d / "base64_keytab").write_text(base64.b64encode(b"ab").decode("ascii"), encoding="utf-8")
    (d / "api_url").write_text("https://ex/a", encoding="utf-8")
    n, raw, files = authentication.load_service_account(
        d, ("api_url",), principal_file="name", keytab_b64_file="base64_keytab"
    )
    assert n == "u1" and raw == b"ab" and files == {"api_url": "https://ex/a"}


def test_load_service_account_multiple_text_files(tmp_path: Path) -> None:
    """*text_files* can name several strip-read files; values map by filename."""
    d = tmp_path
    (d / "name").write_text("p", encoding="utf-8")
    (d / "base64_keytab").write_text(base64.b64encode(b"x").decode("ascii"), encoding="utf-8")
    (d / "errata_api").write_text("https://errata/", encoding="utf-8")
    (d / "other").write_text("o", encoding="utf-8")
    _, _, files = authentication.load_service_account(
        d, ("errata_api", "other"), principal_file="name", keytab_b64_file="base64_keytab"
    )
    assert files == {"errata_api": "https://errata/", "other": "o"}


def test_patch_krb5_inserts_after_libdefaults() -> None:
    """A line is inserted right after the ``[libdefaults]`` section header."""
    src = "[libdefaults]\n# c\n[realms]\n"
    out = authentication.patch_krb5_config(src)
    assert "dns_canonicalize_hostname = false" in out
    assert out.index("[libdefaults]") < out.index("dns_canonicalize_hostname")


def test_kinit_succeeds_first_try(tmp_path: Path) -> None:
    """A successful kinit is not retried; environment keys are passed through."""
    kt = tmp_path / "t.kt"
    kt.write_bytes(b"x")
    cc = tmp_path / "cc"
    cfg = tmp_path / "c.conf"
    cc.write_text("x", encoding="utf-8")
    cfg.write_text("x", encoding="utf-8")
    calls: list = []

    def _fake_run(
        cmd: list[str] | str,
        check: object,
        env: dict,  # noqa: ARG001
    ) -> object:  # type: ignore[no-untyped-def]
        del check
        calls.append((list(cmd) if isinstance(cmd, list) else [cmd], env.get("KRB5CCNAME")))
        r = mock.MagicMock()
        r.returncode = 0
        return r

    with mock.patch("authentication.subprocess.run", side_effect=_fake_run):
        authentication.kinit_with_retry(
            "p", kt, {"KRB5CCNAME": str(cc), "KRB5_CONFIG": str(cfg)}
        )
    assert len(calls) == 1
    assert calls[0][0][0:4] == ["kinit", "p", "-k", "-t"]


def test_kinit_fails_five_times(tmp_path: Path) -> None:
    """After ``max_attempts`` failures, the last ``CalledProcessError`` is raised."""
    kt = tmp_path / "t.kt"
    kt.write_bytes(b"x")
    cc = tmp_path / "cc"
    cfg = tmp_path / "c.conf"
    cc.write_text("x", encoding="utf-8")
    cfg.write_text("x", encoding="utf-8")

    def _fail(*args, **kwargs) -> object:  # type: ignore[no-untyped-def]
        del args, kwargs
        r = mock.MagicMock()
        r.returncode = 1
        return r

    with (
        mock.patch("authentication.subprocess.run", side_effect=_fail),
        mock.patch("authentication.retry.time.sleep") as sleep_mock,
    ):
        with pytest.raises(subprocess.CalledProcessError):
            authentication.kinit_with_retry(
                "p", kt, {"KRB5CCNAME": str(cc), "KRB5_CONFIG": str(cfg)}
            )
    sleep_mock.assert_has_calls([mock.call(5), mock.call(10), mock.call(20), mock.call(40)])
