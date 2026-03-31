"""Unit tests for ``catalog_e2e_helpers``."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import catalog_e2e_helpers as ceh

HELPER_PY = Path(__file__).resolve().parent.parent / "catalog_e2e_helpers.py"


def test_require_env_returns_stripped_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the env value when it is set and non-empty after strip."""
    monkeypatch.setenv("FOO_VAR", "  bar  ")
    assert ceh.require_env("FOO_VAR") == "bar"


def test_require_env_missing_exits(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit with code 1 when the variable is unset or blank."""
    monkeypatch.delenv("MISSING_XYZ", raising=False)
    with pytest.raises(SystemExit) as ei:
        ceh.require_env("MISSING_XYZ")
    assert ei.value.code == 1
    assert "MISSING_XYZ" in capsys.readouterr().err


def test_patch_catalog_utils_image_refs_replaces_inline_image(tmp_path: Path) -> None:
    """Single-line image refs ending in release-service-utils are rewritten."""
    y = tmp_path / "task.yaml"
    y.write_text(
        "image: quay.io/konflux-ci/release-service-utils@sha256:abc\n",
        encoding="utf-8",
    )
    assert ceh.patch_catalog_utils_image_refs(tmp_path, "registry.example/ns/img:v9") == 1
    assert "registry.example/ns/img:v9" in y.read_text(encoding="utf-8")
    assert "release-service-utils" not in y.read_text(encoding="utf-8")


def test_patch_catalog_utils_image_refs_skips_task_tests_fixtures(tmp_path: Path) -> None:
    """Do not patch YAML under tasks/**/tests/."""
    tasks = tmp_path / "tasks" / "managed" / "t"
    fixture = tasks / "tests" / "data.yaml"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    fixture.write_text(
        "image: quay.io/konflux-ci/release-service-utils@sha256:abc\n",
        encoding="utf-8",
    )
    assert ceh.patch_catalog_utils_image_refs(tmp_path, "other:img") == 0
    assert "release-service-utils" in fixture.read_text(encoding="utf-8")


def test_patch_catalog_utils_image_refs_multiline_image(tmp_path: Path) -> None:
    """Match image: newline continuation style used in some Task YAML."""
    y = tmp_path / "t.yaml"
    y.write_text(
        "steps:\n"
        "- name: s\n"
        "  image:\n"
        "    quay.io/konflux-ci/release-service-utils@sha256:beef\n",
        encoding="utf-8",
    )
    assert (
        ceh.patch_catalog_utils_image_refs(tmp_path, "x/y/release-service-utils-custom:v1")
        == 1
    )
    text = y.read_text(encoding="utf-8")
    assert "release-service-utils-custom:v1" in text
    assert "quay.io/konflux-ci/release-service-utils@" not in text


def test_patch_catalog_utils_image_refs_returns_zero_when_no_matching_refs(
    tmp_path: Path,
) -> None:
    """Return 0 when YAML exists but has no release-service-utils image refs."""
    (tmp_path / "other.yaml").write_text("image: other:img\n", encoding="utf-8")
    assert ceh.patch_catalog_utils_image_refs(tmp_path, "replacement:img") == 0


def test_run_as_script_patches_from_cwd(tmp_path: Path) -> None:
    """``python3 catalog_e2e_helpers.py`` patches when run from catalog root."""
    (tmp_path / "a.yaml").write_text(
        "image: quay.io/konflux-ci/release-service-utils:v1\n",
        encoding="utf-8",
    )
    proc = subprocess.run(
        [sys.executable, str(HELPER_PY)],
        cwd=tmp_path,
        env={**os.environ, "UTILS_IMAGE": "replacement:img"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "replacement:img" in (tmp_path / "a.yaml").read_text(encoding="utf-8")


def test_run_as_script_exits_1_when_nothing_patched(tmp_path: Path) -> None:
    """``python3 catalog_e2e_helpers.py`` exits 1 if no refs match."""
    (tmp_path / "b.yaml").write_text("image: other:img\n", encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(HELPER_PY)],
        cwd=tmp_path,
        env={**os.environ, "UTILS_IMAGE": "replacement:img"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 1
    assert "No YAML changes" in proc.stderr
