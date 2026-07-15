"""GitHub.com hosting helpers (App auth, REST API, PR workflow)."""

from __future__ import annotations

import atexit
import base64
import json
import logging
import os
import re
import stat
import subprocess
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import http_client
import requests

from . import git

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GitHubAppSession:
    """Installation access token and API base URL."""

    api_url: str
    token: str


def _jwt_json_segment(data: dict[str, Any]) -> bytes:
    """Encode a dict as compact JSON and base64url without padding (JWT segment)."""
    raw = json.dumps(data, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=")


def app_jwt(
    private_key_path: Path,
    app_id: str,
    *,
    expire_seconds: int = 600,
) -> str:
    """Build a GitHub App JWT signed with the app private key."""
    now = int(time.time())
    header = _jwt_json_segment({"typ": "JWT", "alg": "RS256"})
    payload = _jwt_json_segment({"iat": now, "exp": now + expire_seconds, "iss": app_id})
    header_payload = header + b"." + payload
    proc = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", str(private_key_path)],
        input=header_payload,
        check=True,
        stdout=subprocess.PIPE,
    )
    signature = base64.urlsafe_b64encode(proc.stdout).rstrip(b"=")
    return (header_payload + b"." + signature).decode()


def open_session(
    *,
    api_url: str,
    private_key_path: Path,
    app_id: str,
    installation_id: str,
) -> GitHubAppSession:
    """Exchange an app JWT for an installation access token."""
    jwt_token = app_jwt(private_key_path, app_id)
    url = f"{api_url.rstrip('/')}/app/installations/{installation_id}/access_tokens"
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github.machine-man-preview+json",
        },
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    token = body.get("token")
    if not token:
        msg = f"GitHub App authentication failed: {body!r}"
        raise RuntimeError(msg)
    return GitHubAppSession(api_url=api_url.rstrip("/"), token=str(token))


def _api_url(session: GitHubAppSession, path: str) -> str:
    """Return an absolute GitHub REST URL for *path*."""
    return path if path.startswith("http") else f"{session.api_url}{path}"


def _auth_headers(
    session: GitHubAppSession,
    extra_headers: dict[str, str] | None,
) -> dict[str, str]:
    """Build Authorization headers for *session*, merging *extra_headers*."""
    headers = {"Authorization": f"Bearer {session.token}"}
    if extra_headers:
        headers.update(extra_headers)
    return headers


def _get_json(
    session: GitHubAppSession,
    path: str,
    *,
    extra_headers: dict[str, str] | None = None,
) -> Any:
    """Perform a GitHub REST GET via `http_client.get_text` and parse JSON."""
    text = http_client.get_text(
        _api_url(session, path),
        headers=_auth_headers(session, extra_headers),
        timeout=60,
    )
    return json.loads(text)


def api_request(
    session: GitHubAppSession,
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> requests.Response:
    """Call the GitHub REST API for POST/PATCH relative to *session.api_url*."""
    return requests.request(
        method,
        _api_url(session, path),
        headers=_auth_headers(session, extra_headers),
        json=json_body,
        timeout=60,
    )


def configure_git_askpass_auth(access_token: str) -> None:
    """Set process env so git HTTPS uses a token via GIT_ASKPASS, not embedded URLs.

    Keeps credentials out of remote URLs so git stderr and config dumps cannot
    leak the installation token.
    """
    fd, path = tempfile.mkstemp(prefix="git-askpass-", suffix=".sh")
    askpass = Path(path)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write('#!/bin/sh\nexec echo "$GITHUB_ACCESS_TOKEN"\n')
    askpass.chmod(askpass.stat().st_mode | stat.S_IXUSR)
    atexit.register(lambda: askpass.unlink(missing_ok=True))
    os.environ["GIT_TERMINAL_PROMPT"] = "0"
    os.environ["GIT_ASKPASS"] = str(askpass)
    os.environ["GITHUB_ACCESS_TOKEN"] = access_token


def owner_repo_from_url(repo_url: str) -> str:
    """Return `owner/repo` from a GitHub HTTPS URL."""
    return "/".join(repo_url.rstrip("/").split("/")[-2:])


def run_gh_command(
    cmd: list[str],
    *,
    gh_token: str,
    check: bool = True,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a gh CLI command with the token in the environment."""
    env = os.environ.copy()
    env["GH_TOKEN"] = gh_token
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=check,
        env=env,
        cwd=cwd,
    )


def branch_name_from_origin_repo(origin_repo: str) -> str:
    """Derive a branch name from the last path segment of a repository URL."""
    return origin_repo.rstrip("/").split("/")[-1]


def force_push_updated_files(
    session: GitHubAppSession,
    *,
    clone_dir: Path,
    target_repo: str,
    branch: str,
    relative_paths: Sequence[str],
    commit_email: str = "release-service@redhat.com",
    commit_name: str = "release-service",
) -> None:
    """Commit listed paths in *clone_dir* and force-push *branch* to *target_repo*.

    Recreates *branch* at the current HEAD (the base already synced by the
    caller) before committing, so each run produces one commit on the PR
    without re-fetching `origin/main` or resetting the remote ref first
    (which can close the open pull request).
    """
    configure_git_askpass_auth(session.token)
    git.configure_git_global_user(commit_name, commit_email)
    repo_url = f"https://github.com/{target_repo}.git"
    git.set_remote_url(clone_dir, "origin", repo_url)
    git.checkout(clone_dir, branch, reset=True, start_point="HEAD")
    git.index_add_commit(
        clone_dir,
        relative_paths,
        "Update from release-service",
        stderr_path=None,
    )
    git.push(clone_dir, remote="origin", branch=branch, force=True)


def create_pull_request(
    session: GitHubAppSession,
    target_repo: str,
    *,
    head_branch: str,
    base_branch: str = "main",
    title: str,
) -> dict[str, Any]:
    """Open a pull request and return the JSON body.

    On 422 (for example pull request already exists), return the error JSON
    without raising so callers can look up the existing PR.
    """
    response = api_request(
        session,
        "POST",
        f"/repos/{target_repo}/pulls",
        json_body={
            "head": head_branch,
            "base": base_branch,
            "title": title,
            "maintainer_can_modify": False,
        },
        extra_headers={"Accept": "application/vnd.github.v3+json"},
    )
    if response.status_code in (201, 422):
        return response.json()
    response.raise_for_status()
    return response.json()


def find_open_pull_request_by_branch(
    session: GitHubAppSession,
    target_repo: str,
    branch: str,
) -> dict[str, Any] | None:
    """Return the open pull request for *branch*, if any."""
    owner = target_repo.split("/", 1)[0]
    items = _get_json(
        session,
        f"/repos/{target_repo}/pulls?head={owner}:{branch}&state=open",
        extra_headers={"Accept": "application/vnd.github.v3+json"},
    )
    if not items:
        return None
    return items[0]


def pull_request_url_for_commit_sha(session: GitHubAppSession, sha: str) -> str | None:
    """Search for a PR link associated with *sha*.

    Returns ``None`` when no matching pull request is found so that
    callers can degrade gracefully instead of aborting the entire task.
    """
    data = _get_json(
        session,
        f"/search/issues?q={sha}",
        extra_headers={"Accept": "application/vnd.github.v3+json"},
    )
    items = data.get("items") or []
    if not items:
        logger.warning("no pull request found for commit %s", sha)
        return None
    pull_request = items[0].get("pull_request") or {}
    url = pull_request.get("html_url")
    if not url:
        logger.warning("search result missing pull_request html_url for %s", sha)
        return None
    return str(url)


def compare_changelog(
    session: GitHubAppSession,
    source_repo_url: str,
    old_rev: str,
    new_rev: str,
) -> str:
    """Return a markdown changelog section for `old_rev...new_rev`."""
    owner_repo = owner_repo_from_url(source_repo_url)
    path = f"/repos/{owner_repo}/compare/{old_rev}...{new_rev}"
    headers = {"Accept": "application/vnd.github.v3+json"}
    try:
        data = _get_json(session, path, extra_headers=headers)
        commits = data.get("commits") or []
        if not commits:
            return ""
        lines = ["## Changelog"]
        for commit_info in commits:
            sha = commit_info["sha"][:7]
            url = commit_info["html_url"]
            message = commit_info["commit"]["message"].split("\n", 1)[0]
            message = re.sub(
                r"#(\d+)",
                rf"[#\1](https://github.com/{owner_repo}/pull/\1)",
                message,
            )
            author = commit_info.get("author")
            login = author.get("login") if isinstance(author, dict) else None
            if login:
                lines.append(f"- [`{sha}`]({url}) {message} - @{login}")
            else:
                author_name = commit_info["commit"]["author"]["name"]
                lines.append(f"- [`{sha}`]({url}) {message} - {author_name}")
        return "\n".join(lines)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 0
        print(f"Compare API returned {status}, skipping changelog")
        return ""
    except Exception as exc:
        print(f"Compare changelog failed, skipping: {exc}")
        return ""


def update_pull_request_body(
    session: GitHubAppSession,
    pr_api_url: str,
    body: str,
) -> dict[str, Any]:
    """PATCH the pull request description at *pr_api_url*."""
    response = api_request(
        session,
        "PATCH",
        pr_api_url,
        json_body={"body": body},
        extra_headers={"Accept": "application/vnd.github.v3+json"},
    )
    response.raise_for_status()
    return response.json()
