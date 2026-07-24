"""Tests for the push_artifacts_to_cdn.py wrapper."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.helpers import build_checksum_map
from release_service_utils.helpers import compress_artifacts
from release_service_utils.helpers import extract_artifacts
from release_service_utils.helpers import generate_checksums
from release_service_utils.helpers import push_artifacts as push_artifacts_mod
from release_service_utils.helpers import push_unsigned
from release_service_utils.helpers import sign_mac
from release_service_utils.helpers import sign_windows

_WRAPPER_PATH = (
    Path(__file__).parent.parent.parent / "push_artifacts_to_cdn" / "push_artifacts_to_cdn.py"
)
_spec = importlib.util.spec_from_file_location("push_artifacts_to_cdn_wrapper", _WRAPPER_PATH)
wrapper = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wrapper)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

REQUIRED_ARGS = [
    "push_artifacts_to_cdn.py",
    "--quay-url",
    "quay.io/org",
    "--pipeline-run-uid",
    "uid-123",
    "--exodus-gw-env",
    "pre",
    "--cgw-hostname",
    "https://cgw.example.com",
]


def _setup_result_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, Path]:
    """Create result file paths and set the required env vars."""
    rpath = tmp_path / "result"
    cmap_path = tmp_path / "checksum_map"
    published_path = tmp_path / "published_files"
    monkeypatch.setenv("RESULT_RESULT", str(rpath))
    monkeypatch.setenv("RESULT_CHECKSUM_MAP", str(cmap_path))
    monkeypatch.setenv("RESULT_PUBLISHED_FILES", str(published_path))
    return rpath, cmap_path, published_path


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


def test_parse_args_defaults() -> None:
    """Default values for concurrent_limit, kerberos_realm, and cert_expiration_warn_days."""
    args = wrapper.parse_args(
        [
            "--quay-url",
            "quay.io/org",
            "--pipeline-run-uid",
            "uid",
            "--exodus-gw-env",
            "pre",
            "--cgw-hostname",
            "cgw.example.com",
        ]
    )
    assert args.concurrent_limit == 3
    assert args.kerberos_realm == "IPA.REDHAT.COM"
    assert args.cert_expiration_warn_days == 7


def test_parse_args_explicit_values() -> None:
    """All arguments are correctly parsed when all are supplied explicitly."""
    args = wrapper.parse_args(
        [
            "--quay-url",
            "quay.io/myorg",
            "--pipeline-run-uid",
            "my-uid",
            "--exodus-gw-env",
            "live",
            "--cgw-hostname",
            "cgw.mycompany.com",
            "--concurrent-limit",
            "5",
            "--kerberos-realm",
            "MY.REALM",
            "--cert-expiration-warn-days",
            "14",
        ]
    )
    assert args.quay_url == "quay.io/myorg"
    assert args.pipeline_run_uid == "my-uid"
    assert args.exodus_gw_env == "live"
    assert args.cgw_hostname == "cgw.mycompany.com"
    assert args.concurrent_limit == 5
    assert args.kerberos_realm == "MY.REALM"
    assert args.cert_expiration_warn_days == 14


# ---------------------------------------------------------------------------
# main – success path
# ---------------------------------------------------------------------------


def test_main_success_calls_all_steps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """All pipeline steps are called with the correct arguments on a successful run."""
    rpath, cmap_path, _ = _setup_result_env(tmp_path, monkeypatch)
    ref = "quay.io/org/checksum-map@sha256:abc123"

    with (
        mock.patch.object(extract_artifacts, "run") as m_extract,
        mock.patch.object(push_unsigned, "run") as m_push_unsigned,
        mock.patch.object(sign_mac, "run") as m_sign_mac,
        mock.patch.object(sign_windows, "run") as m_sign_windows,
        mock.patch.object(compress_artifacts, "run") as m_compress,
        mock.patch.object(generate_checksums, "run") as m_checksums,
        mock.patch.object(push_artifacts_mod, "run") as m_push,
        mock.patch.object(build_checksum_map, "run", return_value=ref) as m_cmap,
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    assert rpath.read_text(encoding="utf-8") == "Success"
    assert cmap_path.read_text(encoding="utf-8") == ref

    m_extract.assert_called_once_with(3)
    m_push_unsigned.assert_called_once_with("quay.io/org", "uid-123")
    m_sign_mac.assert_called_once_with("quay.io/org", "uid-123")
    m_sign_windows.assert_called_once_with("quay.io/org", "uid-123")
    m_compress.assert_called_once_with("quay.io/org")
    m_checksums.assert_called_once_with("IPA.REDHAT.COM", "uid-123")
    m_push.assert_called_once_with("pre", "https://cgw.example.com", 7)
    m_cmap.assert_called_once()


def test_main_sets_result_published_files_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RESULT_PUBLISHED_FILES env var points to the published files result path during push."""
    rpath, _, published_path = _setup_result_env(tmp_path, monkeypatch)

    captured: dict[str, str] = {}

    def capture_push_artifacts(exodus_gw_env: str, cgw_hostname: str, days: int) -> None:
        import os

        captured["RESULT_PUBLISHED_FILES"] = os.environ.get("RESULT_PUBLISHED_FILES", "")

    with (
        mock.patch.object(extract_artifacts, "run"),
        mock.patch.object(push_unsigned, "run"),
        mock.patch.object(sign_mac, "run"),
        mock.patch.object(sign_windows, "run"),
        mock.patch.object(compress_artifacts, "run"),
        mock.patch.object(generate_checksums, "run"),
        mock.patch.object(push_artifacts_mod, "run", side_effect=capture_push_artifacts),
        mock.patch.object(build_checksum_map, "run", return_value=""),
    ):
        wrapper.main(REQUIRED_ARGS)

    assert captured["RESULT_PUBLISHED_FILES"] == str(published_path)


# ---------------------------------------------------------------------------
# main – failure paths
# ---------------------------------------------------------------------------


def test_main_extract_fails_stops_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure in extract_artifacts stops all subsequent steps and records the error."""
    rpath, cmap_path, _ = _setup_result_env(tmp_path, monkeypatch)

    with (
        mock.patch.object(extract_artifacts, "run", side_effect=RuntimeError("oras down")),
        mock.patch.object(push_unsigned, "run") as m_push_unsigned,
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    assert "oras down" in rpath.read_text(encoding="utf-8")
    assert cmap_path.read_text(encoding="utf-8") == ""
    m_push_unsigned.assert_not_called()


def test_main_mid_step_failure_writes_partial_cmap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """checksum_map_ref stays empty when a step before build_checksum_map fails."""
    rpath, cmap_path, _ = _setup_result_env(tmp_path, monkeypatch)

    with (
        mock.patch.object(extract_artifacts, "run"),
        mock.patch.object(push_unsigned, "run"),
        mock.patch.object(sign_mac, "run"),
        mock.patch.object(sign_windows, "run"),
        mock.patch.object(compress_artifacts, "run"),
        mock.patch.object(generate_checksums, "run", side_effect=RuntimeError("kinit fail")),
        mock.patch.object(push_artifacts_mod, "run") as m_push,
        mock.patch.object(build_checksum_map, "run") as m_cmap,
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    assert "kinit fail" in rpath.read_text(encoding="utf-8")
    assert cmap_path.read_text(encoding="utf-8") == ""
    m_push.assert_not_called()
    m_cmap.assert_not_called()


def test_main_missing_result_env_raises_system_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SystemExit with code 1 is raised when the RESULT_RESULT env var is missing."""
    monkeypatch.delenv("RESULT_RESULT", raising=False)
    with pytest.raises(SystemExit) as exc:
        wrapper.main(REQUIRED_ARGS)
    assert exc.value.code == 1


# ---------------------------------------------------------------------------
# main – custom argument values forwarded correctly
# ---------------------------------------------------------------------------


def test_main_custom_args_forwarded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Custom CLI argument values are forwarded correctly to each pipeline step."""
    _setup_result_env(tmp_path, monkeypatch)

    calls: dict[str, tuple] = {}

    def fake_extract(limit: int) -> None:
        calls["extract"] = (limit,)

    def fake_checksums(realm: str, uid: str) -> None:
        calls["checksums"] = (realm, uid)

    def fake_push(env: str, host: str, days: int) -> None:
        calls["push"] = (env, host, days)

    with (
        mock.patch.object(extract_artifacts, "run", side_effect=fake_extract),
        mock.patch.object(push_unsigned, "run"),
        mock.patch.object(sign_mac, "run"),
        mock.patch.object(sign_windows, "run"),
        mock.patch.object(compress_artifacts, "run"),
        mock.patch.object(generate_checksums, "run", side_effect=fake_checksums),
        mock.patch.object(push_artifacts_mod, "run", side_effect=fake_push),
        mock.patch.object(build_checksum_map, "run", return_value=""),
    ):
        rc = wrapper.main(
            [
                "push_artifacts_to_cdn.py",
                "--quay-url",
                "quay.io/myorg",
                "--pipeline-run-uid",
                "custom-uid",
                "--exodus-gw-env",
                "live",
                "--cgw-hostname",
                "cgw.custom.com",
                "--concurrent-limit",
                "5",
                "--kerberos-realm",
                "CUSTOM.REALM",
                "--cert-expiration-warn-days",
                "14",
            ]
        )

    assert rc == 0
    assert calls["extract"] == (5,)
    assert calls["checksums"] == ("CUSTOM.REALM", "custom-uid")
    assert calls["push"] == ("live", "cgw.custom.com", 14)
