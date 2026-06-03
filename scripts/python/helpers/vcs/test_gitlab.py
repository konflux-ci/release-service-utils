"""Tests for `vcs.gitlab`."""

from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

import pytest

from . import git
from . import gitlab


def test_read_credentials_from_mount(tmp_path: Path) -> None:
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "gitlab_host").write_text("gitlab.example.com", encoding="utf-8")
    (secret / "gitlab_access_token").write_text("tok", encoding="utf-8")
    (secret / "git_author_name").write_text("Author", encoding="utf-8")
    (secret / "git_author_email").write_text("a@example.com", encoding="utf-8")
    (secret / "git_repo").write_text("https://gitlab.example.com/g/r.git", encoding="utf-8")
    creds = gitlab.read_credentials_from_mount(secret)
    assert creds.gitlab_host == "gitlab.example.com"
    assert creds.access_token == "tok"
    assert creds.git_repo.endswith("r.git")


def test_export_env_for_image_helpers(tmp_path: Path) -> None:
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "gitlab_host").write_text("h", encoding="utf-8")
    (secret / "gitlab_access_token").write_text("t", encoding="utf-8")
    (secret / "git_author_name").write_text("n", encoding="utf-8")
    (secret / "git_author_email").write_text("e", encoding="utf-8")
    (secret / "git_repo").write_text("https://gitlab.example.com/g/r.git", encoding="utf-8")
    creds = gitlab.read_credentials_from_mount(secret)
    gitlab.export_env_for_image_helpers(creds)
    assert os.environ["GITLAB_HOST"] == "h"
    assert os.environ["ACCESS_TOKEN"] == "t"


def test_raw_file_url() -> None:
    url = gitlab.raw_file_url(
        "https://gitlab.example.com/g/r.git",
        "path/to/file.yaml",
    )
    assert url == "https://gitlab.example.com/g/r/-/raw/main/path/to/file.yaml"


def test_configure_git_oauth2_auth_sets_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GIT_ASKPASS", raising=False)
    gitlab.configure_git_oauth2_auth("my-token")
    assert os.environ["GITLAB_OAUTH2_TOKEN"] == "my-token"
    assert os.environ["GIT_TERMINAL_PROMPT"] == "0"
    assert Path(os.environ["GIT_ASKPASS"]).is_file()


def test_clone_project_sparse_delegates_to_git(tmp_path: Path) -> None:
    with mock.patch.object(git, "clone", return_value=tmp_path / "repo") as m:
        out = gitlab.clone_project_sparse(
            "https://gitlab.example.com/g/r.git",
            "main",
            ["schema"],
            parent_dir=tmp_path,
            stderr_path=None,
        )
    assert out == tmp_path / "repo"
    m.assert_called_once()
    assert m.call_args.args[1] == "https://gitlab.example.com/g/r.git"
    assert "oauth2:" not in m.call_args.args[1]
