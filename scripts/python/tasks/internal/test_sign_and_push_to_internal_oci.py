"""Tests for the sign-and-push-to-internal-oci.py wrapper."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest import mock

import extract_oci_artifacts
import push_oci_unsigned
import pytest
import sign_mac
import sign_windows

_WRAPPER_PATH = Path(__file__).parent / "sign-and-push-to-internal-oci.py"
_spec = importlib.util.spec_from_file_location(
    "sign_and_push_to_internal_oci_wrapper", _WRAPPER_PATH
)
wrapper = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wrapper)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

REQUIRED_ARGS = [
    "sign-and-push-to-internal-oci.py",
    "--quay-url",
    "quay.io/org",
    "--pipeline-run-uid",
    "uid-123",
    "--origin",
    "red-hat-desktop-tenant",
]


def _setup_result_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """Create result file paths and set the required env vars."""
    rpath = tmp_path / "result"
    cmap_path = tmp_path / "checksum_map"
    monkeypatch.setenv("RESULT_RESULT", str(rpath))
    monkeypatch.setenv("RESULT_CHECKSUM_MAP", str(cmap_path))
    return rpath, cmap_path


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


def test_parse_args_defaults() -> None:
    """Default values for concurrent_limit and signing script args."""
    args = wrapper.parse_args(
        ["--quay-url", "quay.io/org", "--pipeline-run-uid", "uid-123", "--origin", "my-tenant"]
    )
    assert args.concurrent_limit == 3
    assert args.mac_signing_script is None
    assert args.mac_signing_args == []
    assert args.windows_signing_script is None
    assert args.windows_signing_args == []
    assert args.dest_quay_url is None


def test_parse_args_with_signing_scripts() -> None:
    """Signing script paths and args are parsed correctly."""
    args = wrapper.parse_args(
        [
            "--quay-url",
            "quay.io/org",
            "--pipeline-run-uid",
            "uid-123",
            "--origin",
            "my-tenant",
            "--mac-signing-script",
            "/opt/sign_mac.sh",
            "--mac-signing-args",
            "profile=internal",
            "verbose",
            "--windows-signing-script",
            "C:/Scripts/sign.bat",
            "--windows-signing-args",
            "env=staging",
        ]
    )
    assert args.mac_signing_script == "/opt/sign_mac.sh"
    assert args.mac_signing_args == ["profile=internal", "verbose"]
    assert args.windows_signing_script == "C:/Scripts/sign.bat"
    assert args.windows_signing_args == ["env=staging"]


def test_parse_args_requires_quay_url() -> None:
    """SystemExit is raised when --quay-url is omitted."""
    with pytest.raises(SystemExit):
        wrapper.parse_args(["--pipeline-run-uid", "uid-123"])


def test_parse_args_requires_pipeline_run_uid() -> None:
    """SystemExit is raised when --pipeline-run-uid is omitted."""
    with pytest.raises(SystemExit):
        wrapper.parse_args(["--quay-url", "quay.io/org"])


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_passes_signing_args(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() forwards signing_script and signing_args to sign_mac.run and sign_windows.run."""
    rpath, cmap_path = _setup_result_env(tmp_path, monkeypatch)

    with (
        mock.patch.object(extract_oci_artifacts, "run") as mock_extract,
        mock.patch.object(push_oci_unsigned, "run") as mock_push,
        mock.patch.object(sign_mac, "run") as mock_mac,
        mock.patch.object(sign_windows, "run") as mock_win,
    ):
        rc = wrapper.main(
            REQUIRED_ARGS
            + [
                "--mac-signing-script",
                "/opt/sign.sh",
                "--mac-signing-args",
                "profile=prod",
                "verbose",
                "--windows-signing-script",
                "C:/sign.bat",
                "--windows-signing-args",
                "env=staging",
                "--dest-quay-url",
                "quay.io/internal",
            ]
        )

    assert rc == 0
    mock_extract.assert_called_once_with(3)
    mock_push.assert_called_once_with("quay.io/org", "uid-123")
    mock_mac.assert_called_once_with(
        "quay.io/org",
        "uid-123",
        signing_script="/opt/sign.sh",
        signing_args=["profile=prod", "verbose"],
        dest_quay_url="quay.io/internal",
        origin="red-hat-desktop-tenant",
    )
    mock_win.assert_called_once_with(
        "quay.io/org",
        "uid-123",
        signing_script="C:/sign.bat",
        signing_args=["env=staging"],
        dest_quay_url="quay.io/internal",
        origin="red-hat-desktop-tenant",
    )
    assert rpath.read_text() == "Success"


def test_main_without_signing_scripts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() passes None/[] when no signing script args are given."""
    _setup_result_env(tmp_path, monkeypatch)

    with (
        mock.patch.object(extract_oci_artifacts, "run"),
        mock.patch.object(push_oci_unsigned, "run"),
        mock.patch.object(sign_mac, "run") as mock_mac,
        mock.patch.object(sign_windows, "run") as mock_win,
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    mock_mac.assert_called_once_with(
        "quay.io/org",
        "uid-123",
        signing_script=None,
        signing_args=[],
        dest_quay_url=None,
        origin="red-hat-desktop-tenant",
    )
    mock_win.assert_called_once_with(
        "quay.io/org",
        "uid-123",
        signing_script=None,
        signing_args=[],
        dest_quay_url=None,
        origin="red-hat-desktop-tenant",
    )


def test_main_writes_error_on_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tekton result file receives error text when a stage raises."""
    rpath, cmap_path = _setup_result_env(tmp_path, monkeypatch)

    with (
        mock.patch.object(
            extract_oci_artifacts, "run", side_effect=RuntimeError("extract boom")
        ),
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    assert "ERROR" in rpath.read_text()
    assert "extract boom" in rpath.read_text()
