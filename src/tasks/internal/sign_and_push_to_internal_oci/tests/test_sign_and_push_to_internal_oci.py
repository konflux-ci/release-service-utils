"""Tests for the sign-and-push-to-internal-oci wrapper."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.helpers import (
    extract_oci_artifacts,
    push_oci_unsigned,
    sign_mac,
    sign_windows,
    tekton,
)

_WRAPPER_PATH = Path(__file__).parent.parent / "sign_and_push_to_internal_oci.py"
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


def _setup_result_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create result file path and set the required env var."""
    rpath = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(rpath))
    return rpath


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


def test_parse_args_defaults() -> None:
    """Default values for concurrent_limit and signing script paths."""
    args = wrapper.parse_args(
        [
            "--quay-url",
            "quay.io/org",
            "--pipeline-run-uid",
            "uid-123",
            "--origin",
            "my-tenant",
        ]
    )
    assert args.concurrent_limit == 3
    assert args.mac_signing_script is None
    assert args.windows_signing_script is None
    assert args.dest_quay_url is None


def test_parse_args_with_signing_scripts() -> None:
    """Signing script paths are parsed correctly."""
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
            "--windows-signing-script",
            "C:/Scripts/sign.bat",
        ]
    )
    assert args.mac_signing_script == "/opt/sign_mac.sh"
    assert args.windows_signing_script == "C:/Scripts/sign.bat"


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


def test_main_passes_signing_scripts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() forwards signing_script to sign_mac/sign_windows.run_custom_signing."""
    rpath = _setup_result_env(tmp_path, monkeypatch)

    with (
        mock.patch.object(extract_oci_artifacts, "run") as mock_extract,
        mock.patch.object(push_oci_unsigned, "run") as mock_push,
        mock.patch.object(sign_mac, "run_custom_signing") as mock_mac,
        mock.patch.object(sign_windows, "run_custom_signing") as mock_win,
    ):
        rc = wrapper.main(
            REQUIRED_ARGS
            + [
                "--mac-signing-script",
                "/opt/sign.sh",
                "--windows-signing-script",
                "C:/sign.bat",
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
        dest_quay_url="quay.io/internal",
        origin="red-hat-desktop-tenant",
    )
    mock_win.assert_called_once_with(
        "quay.io/org",
        "uid-123",
        signing_script="C:/sign.bat",
        dest_quay_url="quay.io/internal",
        origin="red-hat-desktop-tenant",
    )
    assert rpath.read_text() == "Success"


def test_main_without_signing_scripts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() passes None when no signing script paths are given."""
    _setup_result_env(tmp_path, monkeypatch)

    with (
        mock.patch.object(extract_oci_artifacts, "run"),
        mock.patch.object(push_oci_unsigned, "run"),
        mock.patch.object(sign_mac, "run_custom_signing") as mock_mac,
        mock.patch.object(sign_windows, "run_custom_signing") as mock_win,
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    mock_mac.assert_called_once_with(
        "quay.io/org",
        "uid-123",
        signing_script=None,
        dest_quay_url=None,
        origin="red-hat-desktop-tenant",
    )
    mock_win.assert_called_once_with(
        "quay.io/org",
        "uid-123",
        signing_script=None,
        dest_quay_url=None,
        origin="red-hat-desktop-tenant",
    )


def test_main_preserves_check_step_error_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CheckStepError from a phase is re-wrapped with the outer action, preserving cause."""
    rpath = _setup_result_env(tmp_path, monkeypatch)

    inner = RuntimeError("ssh connection refused")
    with mock.patch.object(
        extract_oci_artifacts,
        "run",
        side_effect=tekton.CheckStepError("connecting to registry", inner),
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    result_text = rpath.read_text()
    assert "Failed while" in result_text
    assert "ssh connection refused" in result_text


def test_main_writes_error_on_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tekton result file receives error text when a stage raises."""
    rpath = _setup_result_env(tmp_path, monkeypatch)

    with mock.patch.object(
        extract_oci_artifacts,
        "run",
        side_effect=RuntimeError("extract boom"),
    ):
        rc = wrapper.main(REQUIRED_ARGS)

    assert rc == 0
    assert "Failed while" in rpath.read_text()
    assert "extract boom" in rpath.read_text()
