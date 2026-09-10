"""Shared pytest fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_global_git_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep `git config --global` away from the real `~/.gitconfig` during tests."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / ".gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", os.devnull)
