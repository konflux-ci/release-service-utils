"""Tests for `vcs.gitlab`."""

from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

import pytest

from . import git
from . import gitlab


def test_read_credentials_from_mount(tmp_path: Path) -> None:
    """Load GitLab host, token, author, and repo URL from a secret mount."""
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


def test_export_env_for_image_helpers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Export GitLab credentials to env vars for image helper scripts."""
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
    for var in ("GITLAB_HOST", "ACCESS_TOKEN", "GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL"):
        monkeypatch.delenv(var, raising=False)


def test_raw_file_url() -> None:
    """Build a GitLab raw file URL for a path on the default branch."""
    url = gitlab.raw_file_url(
        "https://gitlab.example.com/g/r.git",
        "path/to/file.yaml",
    )
    assert url == "https://gitlab.example.com/g/r/-/raw/main/path/to/file.yaml"


def test_configure_git_oauth2_auth_sets_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure OAuth2 token and GIT_ASKPASS for non-interactive git."""
    monkeypatch.delenv("GIT_ASKPASS", raising=False)
    gitlab.configure_git_oauth2_auth("my-token")
    assert os.environ["GITLAB_OAUTH2_TOKEN"] == "my-token"
    assert os.environ["GIT_TERMINAL_PROMPT"] == "0"
    assert Path(os.environ["GIT_ASKPASS"]).is_file()


def test_clone_project_sparse_delegates_to_git(tmp_path: Path) -> None:
    """Sparse-clone via `git.clone` using an OAuth2-authenticated URL."""
    repo_root = tmp_path / "repo"
    with mock.patch.object(git, "clone", return_value=repo_root) as m:
        out = gitlab.clone_project_sparse(
            "https://gitlab.example.com/g/r.git",
            "main",
            ["schema"],
            parent_dir=tmp_path,
            stderr_path=None,
        )
    assert out is repo_root
    m.assert_called_once()
    assert m.call_args.args[1] == "https://gitlab.example.com/g/r.git"
    assert m.call_args.kwargs["shallow"] is True
    assert "oauth2:" not in m.call_args.args[1]


@pytest.mark.parametrize(
    ("repository", "expected"),
    [
        ("https://gitlab.com/org/up.git", "org/up"),
        ("org/up", "org/up"),
    ],
)
def test_gitlab_project_path(repository: str, expected: str) -> None:
    """Normalize repository URLs to ``group/project`` paths."""
    assert gitlab.gitlab_project_path(repository) == expected
