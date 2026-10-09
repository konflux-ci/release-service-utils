"""Tests for prepare_and_finalize_artifacts."""

from __future__ import annotations

import runpy
from pathlib import Path
from unittest.mock import patch

import pytest
from release_service_utils.tasks.managed.prepare_and_finalize_artifacts import (
    main,
    select_artifact,
)

TASK = (
    "release_service_utils.tasks.managed"
    ".prepare_and_finalize_artifacts.prepare_and_finalize_artifacts"
)
_RPM = "oci:registry/rpm-artifact@sha256:aaa"
_BASE = "oci:registry/base-artifact@sha256:bbb"
_BOTH_EMPTY = (
    "Both rpmDataArtifact and baseDataArtifact are empty. At least one must be provided."
)


def test_select_artifact_rpm_preferred() -> None:
    """Prefer the rpm artifact when both values are set."""
    assert select_artifact(_RPM, _BASE) == _RPM


def test_select_artifact_rpm_only() -> None:
    """Return the rpm artifact when the base artifact is empty."""
    assert select_artifact(_RPM, "") == _RPM


def test_select_artifact_base_only() -> None:
    """Return the base artifact when the rpm artifact is empty."""
    assert select_artifact("", _BASE) == _BASE


def test_select_artifact_both_empty() -> None:
    """Raise ValueError when both artifacts are empty."""
    with pytest.raises(ValueError, match=_BOTH_EMPTY):
        select_artifact("", "")


def test_main_whitespace_only_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Treat whitespace-only env values as empty and raise ValueError."""
    result_file = tmp_path / "selected_artifact"
    monkeypatch.setenv("RESULT_SELECTED_ARTIFACT", str(result_file))
    monkeypatch.setenv("PARAM_RPM_DATA_ARTIFACT", "   ")
    monkeypatch.setenv("PARAM_BASE_DATA_ARTIFACT", "   ")

    with pytest.raises(ValueError, match=_BOTH_EMPTY):
        main()

    assert not result_file.exists()


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write the selected artifact to the result file and return 0."""
    result_file = tmp_path / "selected_artifact"
    monkeypatch.setenv("RESULT_SELECTED_ARTIFACT", str(result_file))
    monkeypatch.setenv("PARAM_RPM_DATA_ARTIFACT", _RPM)
    monkeypatch.setenv("PARAM_BASE_DATA_ARTIFACT", _BASE)

    assert main() == 0
    assert result_file.read_text(encoding="utf-8") == _RPM


def test_main_base_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write the base artifact when the rpm env var is empty."""
    result_file = tmp_path / "selected_artifact"
    monkeypatch.setenv("RESULT_SELECTED_ARTIFACT", str(result_file))
    monkeypatch.setenv("PARAM_RPM_DATA_ARTIFACT", "")
    monkeypatch.setenv("PARAM_BASE_DATA_ARTIFACT", _BASE)

    assert main() == 0
    assert result_file.read_text(encoding="utf-8") == _BASE


def test_main_unset_optional_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Treat missing optional env vars as empty and select the base artifact."""
    result_file = tmp_path / "selected_artifact"
    monkeypatch.setenv("RESULT_SELECTED_ARTIFACT", str(result_file))
    monkeypatch.delenv("PARAM_RPM_DATA_ARTIFACT", raising=False)
    monkeypatch.setenv("PARAM_BASE_DATA_ARTIFACT", _BASE)

    assert main() == 0
    assert result_file.read_text(encoding="utf-8") == _BASE


def test_main_both_empty_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Let ValueError propagate when both artifacts are empty."""
    result_file = tmp_path / "selected_artifact"
    monkeypatch.setenv("RESULT_SELECTED_ARTIFACT", str(result_file))
    monkeypatch.setenv("PARAM_RPM_DATA_ARTIFACT", "")
    monkeypatch.setenv("PARAM_BASE_DATA_ARTIFACT", "")

    with pytest.raises(ValueError, match=_BOTH_EMPTY):
        main()

    assert not result_file.exists()


def test_main_missing_result_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when RESULT_SELECTED_ARTIFACT is not set."""
    monkeypatch.delenv("RESULT_SELECTED_ARTIFACT", raising=False)
    monkeypatch.setenv("PARAM_RPM_DATA_ARTIFACT", _RPM)
    monkeypatch.setenv("PARAM_BASE_DATA_ARTIFACT", "")

    with pytest.raises(SystemExit):
        main()


def test_dunder_main_invokes_main() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.managed.prepare_and_finalize_artifacts"
    with (
        patch(f"{TASK}.main", return_value=0) as mock_main,
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()
