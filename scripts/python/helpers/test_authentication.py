"""Tests for authentication helpers."""

from __future__ import annotations

import base64
import os
import subprocess
from pathlib import Path
from unittest import mock

import pytest

import authentication

# ---------------------------------------------------------------------------
# read_mounted_text
# ---------------------------------------------------------------------------


def test_read_mounted_text(tmp_path: Path) -> None:
    """File content is read as UTF-8 and returned with surrounding whitespace stripped."""
    f = tmp_path / "secret"
    f.write_text("  value\n", encoding="utf-8")
    assert authentication.read_mounted_text(tmp_path, "secret") == "value"


# ---------------------------------------------------------------------------
# load_keytab_from_mount
# ---------------------------------------------------------------------------


def test_load_keytab_from_mount(tmp_path: Path) -> None:
    """Principal string and base64-decoded keytab bytes are returned from mounted files."""
    raw_keytab = b"\x05\x02keytab-bytes"
    encoded = base64.b64encode(raw_keytab).decode("ascii")
    (tmp_path / "principal").write_text("user@REALM\n", encoding="utf-8")
    (tmp_path / "keytab.b64").write_text(encoded + "\n", encoding="utf-8")

    princ, keytab = authentication.load_keytab_from_mount(
        tmp_path, principal_file="principal", keytab_b64_file="keytab.b64"
    )
    assert princ == "user@REALM"
    assert keytab == raw_keytab


# ---------------------------------------------------------------------------
# load_service_account
# ---------------------------------------------------------------------------


def test_load_service_account(tmp_path: Path) -> None:
    """Principal, keytab, and extra text files are all loaded and returned correctly."""
    raw_keytab = b"\x05\x02data"
    encoded = base64.b64encode(raw_keytab).decode("ascii")
    (tmp_path / "principal").write_text("sa@REALM", encoding="utf-8")
    (tmp_path / "keytab.b64").write_text(encoded, encoding="utf-8")
    (tmp_path / "api_url").write_text("https://api.example.com  ", encoding="utf-8")

    princ, keytab, extra = authentication.load_service_account(
        tmp_path,
        ("api_url",),
        principal_file="principal",
        keytab_b64_file="keytab.b64",
    )
    assert princ == "sa@REALM"
    assert keytab == raw_keytab
    assert extra == {"api_url": "https://api.example.com"}


# ---------------------------------------------------------------------------
# patch_krb5_config
# ---------------------------------------------------------------------------


def test_patch_krb5_config_inserts_after_libdefaults() -> None:
    """dns_canonicalize_hostname setting is inserted on the line after [libdefaults]."""
    source = "[libdefaults]\n default_realm = EXAMPLE.COM\n"
    result = authentication.patch_krb5_config(source)
    lines = result.splitlines()
    idx = lines.index("[libdefaults]")
    assert "dns_canonicalize_hostname = false" in lines[idx + 1]


def test_patch_krb5_config_no_libdefaults_unchanged() -> None:
    """Source text without a [libdefaults] section is returned unchanged."""
    source = "[realms]\n EXAMPLE.COM = {}\n"
    assert authentication.patch_krb5_config(source) == source


# ---------------------------------------------------------------------------
# kinit_with_retry
# ---------------------------------------------------------------------------


def test_kinit_with_retry_success(tmp_path: Path) -> None:
    """The kinit command is called once with the correct principal and keytab on success."""
    keytab = tmp_path / "test.keytab"
    keytab.write_bytes(b"fake")

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.Mock(returncode=0)
        authentication.kinit_with_retry("user@REALM", keytab, {})

    mock_run.assert_called_once()
    args = mock_run.call_args[0][0]
    assert args[0] == "kinit"
    assert "user@REALM" in args


def test_kinit_with_retry_fails_after_max_attempts(tmp_path: Path) -> None:
    """CalledProcessError is raised after exhausting max_attempts retries."""
    keytab = tmp_path / "test.keytab"
    keytab.write_bytes(b"fake")

    with mock.patch("subprocess.run") as mock_run, mock.patch("time.sleep"):
        mock_run.return_value = mock.Mock(returncode=1)
        with pytest.raises(subprocess.CalledProcessError):
            authentication.kinit_with_retry("user@REALM", keytab, {}, max_attempts=2)

    assert mock_run.call_count == 2


# ---------------------------------------------------------------------------
# write_docker_config
# ---------------------------------------------------------------------------


def test_write_docker_config(tmp_path: Path) -> None:
    """config.json is written with mode 0600 inside a 0700 .docker directory."""
    with mock.patch("pathlib.Path.home", return_value=tmp_path):
        authentication.write_docker_config('{"auths":{}}')
    config = tmp_path / ".docker" / "config.json"
    assert config.read_text() == '{"auths":{}}'
    assert oct(config.stat().st_mode & 0o777) == oct(0o600)
    assert oct((tmp_path / ".docker").stat().st_mode & 0o777) == oct(0o700)


# ---------------------------------------------------------------------------
# setup_docker_config
# ---------------------------------------------------------------------------


def test_setup_docker_config_writes_file(tmp_path: Path) -> None:
    """Docker config JSON from a mounted secret file is written to ~/.docker/config.json."""
    secret = tmp_path / "secret" / ".dockerconfigjson"
    secret.parent.mkdir()
    secret.write_text('{"auths":{}}', encoding="utf-8")

    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        authentication.setup_docker_config(secret)

    assert (home / ".docker" / "config.json").read_text() == '{"auths":{}}'


def test_setup_docker_config_optional_missing_file(tmp_path: Path) -> None:
    """A missing optional secret file is silently skipped without writing anything."""
    missing = tmp_path / ".dockerconfigjson"
    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        authentication.setup_docker_config(missing, optional=True)

    assert not (home / ".docker" / "config.json").exists()


def test_setup_docker_config_optional_empty_file(tmp_path: Path) -> None:
    """An empty optional secret file is silently skipped without writing anything."""
    empty = tmp_path / ".dockerconfigjson"
    empty.write_bytes(b"")
    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        authentication.setup_docker_config(empty, optional=True)

    assert not (home / ".docker" / "config.json").exists()


def test_setup_docker_config_strip_noise(tmp_path: Path) -> None:
    """Leading/trailing non-JSON characters are stripped before writing config.json."""
    secret = tmp_path / ".dockerconfigjson"
    # k8s sometimes wraps the JSON in outer single-quotes or other noise characters
    secret.write_text("'{\"auths\":{}}'", encoding="utf-8")

    home = tmp_path / "home"
    home.mkdir()
    with mock.patch("pathlib.Path.home", return_value=home):
        authentication.setup_docker_config(secret, strip_noise=True)

    written = (home / ".docker" / "config.json").read_text()
    assert written.startswith("{")
    assert written.endswith("}")


def test_setup_docker_config_required_missing_raises(tmp_path: Path) -> None:
    """A missing non-optional secret file raises FileNotFoundError."""
    missing = tmp_path / ".dockerconfigjson"
    with pytest.raises(FileNotFoundError):
        authentication.setup_docker_config(missing)


# ---------------------------------------------------------------------------
# kerberos_login
# ---------------------------------------------------------------------------


def _no_kinit(*_a: object, **_k: object) -> None:
    return None


def test_kerberos_login_creates_and_cleans_temp_files(
    tmp_path: Path,
) -> None:
    """Temp files exist inside the context and are removed after."""
    created_files: list[Path] = []

    def _track_kinit(
        _princ: str,
        keytab: Path,
        env: dict[str, str],
        **_kw: object,
    ) -> None:
        created_files.append(keytab)
        created_files.append(Path(env["KRB5_CONFIG"]))
        created_files.append(Path(env["KRB5CCNAME"]))
        for f in created_files:
            assert f.exists()

    with authentication.kerberos_login(
        "user@REALM",
        b"keytab-data",
        "[libdefaults]\n",
        kinit_fn=_track_kinit,
    ):
        for f in created_files:
            assert f.exists()

    for f in created_files:
        assert not f.exists()


def test_kerberos_login_updates_and_cleans_environ() -> None:
    """KRB5 env vars are set inside the context and removed after."""
    with authentication.kerberos_login(
        "user@REALM",
        b"kt",
        "[libdefaults]\n",
        kinit_fn=_no_kinit,
    ):
        assert "KRB5_CONFIG" in os.environ
        assert "KRB5CCNAME" in os.environ
        assert os.environ["KRB5_TRACE"] == "/dev/stderr"

    assert "KRB5_CONFIG" not in os.environ
    assert "KRB5CCNAME" not in os.environ
    assert "KRB5_TRACE" not in os.environ


def test_kerberos_login_calls_kinit_with_correct_args() -> None:
    """``kinit_fn`` is called with the principal, keytab path, and env."""
    calls: list[tuple[str, bytes, dict[str, str]]] = []

    def _spy(
        princ: str,
        keytab: Path,
        env: dict[str, str],
        **_kw: object,
    ) -> None:
        calls.append((princ, keytab.read_bytes(), dict(env)))

    with authentication.kerberos_login(
        "bot@REALM",
        b"kt-bytes",
        "[libdefaults]\n",
        kinit_fn=_spy,
    ):
        pass

    assert len(calls) == 1
    princ, keytab_data, env = calls[0]
    assert princ == "bot@REALM"
    assert keytab_data == b"kt-bytes"
    assert "KRB5_CONFIG" in env
    assert "KRB5CCNAME" in env


def test_kerberos_login_cleans_up_on_kinit_failure() -> None:
    """Temp files are removed even when kinit raises."""
    created_files: list[Path] = []

    def _fail_kinit(
        _princ: str,
        keytab: Path,
        env: dict[str, str],
        **_kw: object,
    ) -> None:
        created_files.append(keytab)
        created_files.append(Path(env["KRB5_CONFIG"]))
        created_files.append(Path(env["KRB5CCNAME"]))
        raise subprocess.CalledProcessError(1, "kinit")

    with pytest.raises(subprocess.CalledProcessError):
        with authentication.kerberos_login(
            "user@REALM",
            b"kt",
            "[libdefaults]\n",
            kinit_fn=_fail_kinit,
        ):
            pass

    for f in created_files:
        assert not f.exists()


# ---------------------------------------------------------------------------
# setup_ca_cert
# ---------------------------------------------------------------------------


def test_setup_ca_cert_sets_ssl_cert_file_when_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SSL_CERT_FILE is set when CA cert file exists."""
    ca_file = tmp_path / "ca-bundle.crt"
    ca_file.write_text("CERT", encoding="utf-8")
    monkeypatch.setenv("CA_CERT_PATH", str(ca_file))
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    authentication.setup_ca_cert()
    assert os.environ["SSL_CERT_FILE"] == str(ca_file)


def test_setup_ca_cert_noop_when_file_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No action when CA cert file does not exist."""
    monkeypatch.setenv("CA_CERT_PATH", str(tmp_path / "missing.crt"))
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    authentication.setup_ca_cert()
    assert "SSL_CERT_FILE" not in os.environ


def test_setup_ca_cert_noop_when_env_not_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No action when CA_CERT_PATH is not set."""
    monkeypatch.delenv("CA_CERT_PATH", raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    authentication.setup_ca_cert()
    assert "SSL_CERT_FILE" not in os.environ


def test_setup_ca_cert_noop_when_env_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No action when CA_CERT_PATH is empty string."""
    monkeypatch.setenv("CA_CERT_PATH", "")
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    authentication.setup_ca_cert()
    assert "SSL_CERT_FILE" not in os.environ
