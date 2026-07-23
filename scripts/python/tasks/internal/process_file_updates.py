#!/usr/bin/env python3
"""Apply file edits to a GitLab repository and open a merge request with the changes.

Intended to be invoked by a release pipeline to automatically propagate release
artifacts (e.g. updated image digests, version strings) into downstream
configuration repositories, creating an MR for human review.

* Reads credentials from ``/mnt/file-updates-secret/`` (or
  ``FILE_UPDATES_SECRET_MOUNT``): ``gitlab_host``, ``gitlab_access_token``,
  ``git_author_name``, ``git_author_email``.
* Clones ``--repo`` at ``--ref``, rebases on ``--upstream-repo``, applies path
  seeds and YAML replacements (``yq`` for key lookup), then commits or reuses an MR.
* Writes ``RESULT_FILE_UPDATES_*`` and internal-request name results.
* After a valid run with result env vars, almost always exits ``0``; failures use
  ``RESULT_FILE_UPDATES_STATE``. Invalid YAML on a replacement target exits ``1``.
  Bad CLI flags exit before result handling (``1``; argparse uses ``2`` for
  malformed argv).

Pass ``gitlab_client`` to ``run_file_updates`` to inject a client in unit tests.
Catalog Tekton tests mock ``git`` via ``tests/mocks/git`` on ``PATH``.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import gitlab
from gitlab.exceptions import GitlabError

import file
import redact
import tekton
from logger import logger
from vcs import git as vcs_git
from vcs import gitlab as vcs_gitlab

PROG = "process_file_updates.py"
FILE_UPDATES_SECRET_MOUNT_ENV = "FILE_UPDATES_SECRET_MOUNT"
_REPLACEMENT_EXPRESSION = re.compile(r"^\|([^|\n]*)\|([^|\n]*)\|$")


def _usage_text() -> str:
    """Return the short usage summary printed to stderr on bad CLI usage."""
    return (
        f"usage: {PROG} --upstream-repo REPO --repo REPO --ref REF --paths JSON \\\n"
        f"  --component-group GROUP --internal-request-pipeline-run-name NAME \\\n"
        f"  --internal-request-task-run-name NAME [--temp-dir DIR]\n"
    )


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse required CLI flags; help or missing values exit ``1`` (extras exit ``2``)."""
    p = argparse.ArgumentParser(prog=PROG, add_help=False, usage=argparse.SUPPRESS)
    p.add_argument("-h", "--help", action="store_true")
    p.add_argument("--upstream-repo", metavar="REPO")
    p.add_argument("--repo", metavar="REPO")
    p.add_argument("--ref", metavar="REF")
    p.add_argument("--paths", metavar="JSON")
    p.add_argument("--component-group", metavar="GROUP")
    p.add_argument("--internal-request-pipeline-run-name", metavar="NAME")
    p.add_argument("--internal-request-task-run-name", metavar="NAME")
    p.add_argument("--temp-dir", metavar="DIR")
    ns = p.parse_args(argv or [])
    if ns.help:
        print(_usage_text(), file=sys.stderr, end="")
        raise SystemExit(1)
    required = (
        "upstream_repo",
        "repo",
        "ref",
        "paths",
        "component_group",
        "internal_request_pipeline_run_name",
        "internal_request_task_run_name",
    )
    if any(not getattr(ns, f) or not str(getattr(ns, f)).strip() for f in required):
        print(_usage_text(), file=sys.stderr, end="")
        raise SystemExit(1)
    return ns


def load_file_updates_secrets(mount: Path) -> dict[str, str]:
    """Read git/GitLab secret files under *mount*."""
    keys = ("gitlab_host", "gitlab_access_token", "git_author_name", "git_author_email")
    out: dict[str, str] = {}
    for key in keys:
        path = mount / key
        try:
            out[key] = path.read_text(encoding="utf-8").strip()
        except OSError as e:
            raise tekton.CheckStepError(f"reading secret file {key}", e) from e
    return out


def git_functions_init(author_name: str, author_email: str, token: str) -> None:
    """Validate identity fields and set global git user.name and user.email."""
    if not author_name or not author_email or not token:
        raise tekton.CheckStepError(
            "initializing git",
            ValueError("GIT_AUTHOR_NAME, GIT_AUTHOR_EMAIL, and ACCESS_TOKEN are required"),
        )
    vcs_git.configure_git_global_user(author_name, author_email)


def gitlab_create_mr(
    *,
    head: str,
    title: str,
    target_branch: str,
    description: str,
    upstream_repo: str,
    gitlab_client: gitlab.Gitlab,
) -> str:
    """Create a merge request; return JSON ``{"merge_request": url}`` for Tekton results."""
    project_path = vcs_gitlab.gitlab_project_path(upstream_repo)
    try:
        project = gitlab_client.projects.get(project_path)
        mr = project.mergerequests.create(
            {
                "source_branch": head,
                "target_branch": target_branch,
                "title": title,
                "description": description,
            }
        )
    except GitlabError as e:
        raise tekton.CheckStepError("creating GitLab merge request", e) from e

    url = getattr(mr, "web_url", None) or ""
    if url:
        return json.dumps({"merge_request": url})

    raise tekton.CheckStepError(
        "creating GitLab merge request",
        ValueError("merge request created but web_url was empty"),
    )


def list_konflux_open_mrs(upstream_repo: str, gitlab_client: gitlab.Gitlab) -> list[Any]:
    """List open MRs in *upstream_repo* matching ``Konflux release`` (paginated)."""
    project_path = vcs_gitlab.gitlab_project_path(upstream_repo)
    try:
        project = gitlab_client.projects.get(project_path)
    except GitlabError as e:
        raise tekton.CheckStepError("getting GitLab project", e) from e

    page = 1
    found: list[Any] = []
    while True:
        try:
            batch = project.mergerequests.list(
                state="opened",
                search="Konflux release",
                per_page=100,
                page=page,
            )
        except GitlabError as e:
            raise tekton.CheckStepError("listing GitLab merge requests", e) from e
        if not batch:
            break
        found.extend(batch)
        page += 1
    return found


def blank_lines_before_yaml(target_file: Path) -> int:
    """Lines before first non-comment YAML line (matches legacy bash/sed numbering)."""
    try:
        with target_file.open(encoding="utf-8") as f:
            for i, line in enumerate(f):
                if re.search(r"[a-zA-Z]", line) and not line.lstrip().startswith("#"):
                    return i
    except OSError:
        return 0
    return 0


def parse_replacement_expression(replacement: str) -> tuple[str, str] | None:
    """Parse ``|search|replace|``; return ``None`` if the format is invalid."""
    match = _REPLACEMENT_EXPRESSION.fullmatch(replacement)
    if match is None:
        return None
    return match.group(1), match.group(2)


def apply_replacement_block(
    target_file: Path,
    start_block: int,
    value_size: int,
    replacement: str,
    temp_dir: Path,
) -> tuple[str | None, Path | None]:
    """Apply one ``|search|replace|`` to a YAML block; write diff to *temp_dir*.

    Returns ``(None, diff_path)`` on success or ``(error, diff_path)`` on failure.
    """
    parsed = parse_replacement_expression(replacement)
    if parsed is None:
        return "Replace expression should be in '|search|replace|' format", None

    search, replace_str = parsed
    search_re = re.compile(search)

    lines = target_file.read_text(encoding="utf-8").splitlines(keepends=True)
    start_idx = start_block - 1
    end_idx = start_idx + value_size + 1

    for i in range(start_idx, min(end_idx, len(lines))):
        lines[i] = search_re.sub(replace_str, lines[i])

    target_file.write_text("".join(lines), encoding="utf-8")

    result_lines = lines[start_idx:end_idx]
    result_text = "".join(result_lines)
    result_path = temp_dir / "result.txt"
    result_path.write_text(result_text, encoding="utf-8")

    found_path = temp_dir / "found.txt"
    found_text = found_path.read_text(encoding="utf-8") if found_path.exists() else ""
    diff_path = temp_dir / "diff.txt"
    diff_output = "".join(
        difflib.unified_diff(
            found_text.splitlines(keepends=True),
            result_text.splitlines(keepends=True),
            fromfile=str(found_path),
            tofile=str(result_path),
        )
    )
    diff_path.write_text(diff_output, encoding="utf-8")

    replaced_block_lines = len(result_text.splitlines())
    if replaced_block_lines != value_size + 1:
        return "Text block size differs from the original", diff_path

    replaced_count = sum(1 for line in result_text.splitlines() if replace_str in line)
    if replaced_count != 1:
        return (
            "Too many lines replaced. Check if the replace expression isn't too greedy",
            diff_path,
        )
    return None, diff_path


def write_error_result(info_path: Path, state_path: Path, diff_text: str, error: str) -> None:
    """Write replacement failure JSON to result files (diff trim matches legacy bash)."""
    trimmed = diff_text[1:3701] if diff_text else ""
    payload = json.dumps({"str": trimmed, "error": error})
    info_path.write_text(payload, encoding="utf-8")
    state_path.write_text("Failed", encoding="utf-8")


def write_json_error_result(info_path: Path, state_path: Path, error: str) -> None:
    """Write a logical failure (e.g. no keys replaced) to both result files as JSON."""
    payload = json.dumps({"str": error, "error": error})
    info_path.write_text(payload, encoding="utf-8")
    state_path.write_text("Failed", encoding="utf-8")


@dataclass
class PathProcessingState:
    """Mutable outcome of seed/replacement work across all paths."""

    replacements_update_error: str | None = None
    diff_path: Path | None = None
    replacements_performed: int = 0
    key_not_found: bool = False


def configure_git_environment(secrets: dict[str, str]) -> str:
    """Export git/GitLab env vars, configure OAuth2 auth; return the access token."""
    token = secrets["gitlab_access_token"]
    os.environ["GITLAB_HOST"] = secrets["gitlab_host"]
    os.environ["ACCESS_TOKEN"] = token
    os.environ["GIT_AUTHOR_NAME"] = secrets["git_author_name"]
    os.environ["GIT_AUTHOR_EMAIL"] = secrets["git_author_email"]
    vcs_gitlab.configure_git_oauth2_auth(token)
    return token


def _fetch_merge_request_head(repo_cwd: Path, mr_iid: int) -> str:
    """Fetch MR *mr_iid* from ``origin`` into ``mr_<iid>``; return the local ref name."""
    local_ref = f"mr_{mr_iid}"
    vcs_git.fetch(repo_cwd, "origin", f"merge-requests/{mr_iid}/head:{local_ref}")
    return local_ref


def write_paths_manifest(paths_json: str, temp_dir: Path) -> tuple[Path, list[dict[str, Any]]]:
    """Persist ``--paths`` JSON to *temp_dir* and return the file path and parsed data."""
    update_paths_file = temp_dir / "updatePaths.json"
    update_paths_file.write_text(paths_json + "\n", encoding="utf-8")
    return update_paths_file, json.loads(paths_json)


def prepare_repository(
    repo: str,
    revision: str,
    upstream_repo: str,
    temp_dir: Path,
    paths_data: list[dict[str, Any]],
) -> Path:
    """Clone *repo* at *revision*, rebase on *upstream_repo*, and return the repo cwd."""
    logger.info("=== UPDATING %s ON BRANCH %s ===", repo, revision)
    if not any(entry.get("path") for entry in paths_data):
        raise tekton.CheckStepError(
            "cloning repository",
            ValueError("paths JSON must include at least one path entry"),
        )
    repo_cwd = vcs_git.clone(
        temp_dir,
        repo,
        revision=revision,
        shallow=True,
    )
    vcs_git.rebase_onto_remote(
        repo_cwd,
        remote_name="glab-base",
        remote_repository=upstream_repo,
        revision=revision,
    )
    return repo_cwd


def resolve_target_file(repo_cwd: Path, entry_path: str) -> Path:
    """Return a path under *repo_cwd* for *entry_path*, rejecting traversal and absolutes."""
    if not entry_path or not str(entry_path).strip():
        raise ValueError("path entry must be a non-empty relative path")
    path_obj = Path(entry_path)
    if path_obj.is_absolute():
        raise ValueError(f"path entry must be relative, not absolute: {entry_path!r}")
    if len(entry_path) >= 2 and entry_path[1] == ":":
        raise ValueError(f"path entry must be relative, not a drive path: {entry_path!r}")
    if ".." in path_obj.parts:
        raise ValueError(f"path entry must not contain '..': {entry_path!r}")
    repo_root = repo_cwd.resolve()
    target_file = (repo_cwd / entry_path).resolve()
    if not target_file.is_relative_to(repo_root):
        raise ValueError(f"path entry escapes repository root: {entry_path!r}")
    return target_file


def seed_target_file(entry: dict[str, Any], target_file: Path, repo_cwd: Path) -> None:
    """Create or overwrite *target_file* when the path entry includes a seed value."""
    seed = entry.get("seed") or ""
    if isinstance(seed, str):
        seed = seed.strip('"')
    logger.info("%s", seed)
    if not seed:
        return
    logger.info("seed operation to perform")
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text(
        seed + ("\n" if not seed.endswith("\n") else ""),
        encoding="utf-8",
    )
    logger.info("-- start targetFile --")
    logger.info("%s", target_file.read_text(encoding="utf-8"))
    logger.info("-- end targetFile --")
    rel = str(target_file.relative_to(repo_cwd))
    vcs_git.index_add_commit(repo_cwd, [rel], "", commit=False)
    logger.info("%s", vcs_git.working_tree_status(repo_cwd))


def apply_replacements_for_entry(
    entry: dict[str, Any],
    target_file: Path,
    repo_cwd: Path,
    temp_dir: Path,
    state: PathProcessingState,
) -> tuple[str, str, int] | None:
    """Apply replacements for one path; return early tuple if YAML is invalid."""
    replacements = entry.get("replacements") or []
    replacements_length = len(replacements)
    logger.info("Replacements to perform: %s", replacements_length)
    if replacements_length == 0:
        return None

    performed_this_entry = 0
    blank_offset = blank_lines_before_yaml(target_file)

    yaml_check = subprocess.run(
        ["yq", str(target_file)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if yaml_check.returncode != 0:
        msg = f"fileUpdates: the targetFile {entry['path']} is not a yaml file"
        return msg, "", 1

    for replacement_index, repl in enumerate(replacements):
        logger.info("REPLACEMENT: #%s", replacement_index)
        key = repl["key"]
        replacement = repl["replacement"]

        print(f"Searching for key `{key}`: ", end="", flush=True)
        proc = subprocess.run(
            ["yq", f"{key} | (line, .)", str(target_file)],
            text=True,
            capture_output=True,
            check=True,
        )
        found_path = temp_dir / "found.txt"
        found_path.write_text(proc.stdout, encoding="utf-8")
        print(proc.stdout, end="", flush=True)

        lines = proc.stdout.splitlines()
        found_at = int(lines[0].strip()) if lines else 0
        if found_at == 0:
            logger.info("NOT FOUND")
            state.key_not_found = True
            continue
        logger.info("FOUND")

        value_proc = subprocess.run(
            ["yq", key, str(target_file)],
            text=True,
            capture_output=True,
            check=True,
        )
        value_size = len(value_proc.stdout.splitlines())
        start_block = found_at + blank_offset

        found_content = found_path.read_text(encoding="utf-8")
        found_lines = found_content.splitlines()
        if found_lines:
            found_path.write_text(
                "\n".join(found_lines[1:]) + ("\n" if len(found_lines) > 1 else ""),
                encoding="utf-8",
            )

        logger.info("--start file--")
        logger.info("%s", target_file.read_text(encoding="utf-8"))
        logger.info("--end file--")

        err, diff_path = apply_replacement_block(
            target_file, start_block, value_size, replacement, temp_dir
        )
        if err:
            state.replacements_performed += performed_this_entry
            state.replacements_update_error = err
            state.diff_path = diff_path
            return None
        performed_this_entry += 1

    state.replacements_performed += performed_this_entry
    return None


def process_all_paths(
    paths_data: list[dict[str, Any]],
    update_paths_file: Path,
    repo_cwd: Path,
    temp_dir: Path,
) -> tuple[PathProcessingState, tuple[str, str, int] | None]:
    """Apply seeds and replacements for all paths in *paths_data*."""
    state = PathProcessingState()
    logger.info("%s", update_paths_file.read_text(encoding="utf-8"))

    for entry in paths_data:
        logger.info("-- start updatePathsTmpfile --")
        logger.info("%s", update_paths_file.read_text(encoding="utf-8"))
        logger.info("-- end updatePathsTmpfile --")

        try:
            target_file = resolve_target_file(repo_cwd, entry["path"])
        except ValueError as e:
            raise tekton.CheckStepError("validating path entry", e) from e
        logger.info("targetFile: %s", target_file.relative_to(repo_cwd.resolve()))

        seed_target_file(entry, target_file, repo_cwd)

        early = apply_replacements_for_entry(entry, target_file, repo_cwd, temp_dir, state)
        if early is not None:
            return state, early

        vcs_git.index_add_commit(
            repo_cwd,
            [str(target_file.relative_to(repo_cwd))],
            "",
            commit=False,
        )
        if state.replacements_update_error:
            break

    return state, None


def outcome_after_path_processing(
    state: PathProcessingState,
) -> tuple[str, str, int] | None:
    """Map *state* to a result tuple, or ``None`` if the workflow should continue."""
    if state.replacements_update_error:
        diff_text = (
            state.diff_path.read_text(encoding="utf-8")
            if state.diff_path and state.diff_path.exists()
            else ""
        )
        return diff_text, state.replacements_update_error, 0

    if state.replacements_performed == 0:
        if state.key_not_found:
            err = '"no replacements were performed"'
            return err, err, 0
        return "nothing needs change\n", "Success", 0

    return None


def get_cached_diff(repo_cwd: Path, temp_dir: Path) -> str:
    """Return ``git diff --cached`` output and log the staged changes banner."""
    logger.info("*** START LOCAL CHANGES ***")
    logger.info("*** Result from git diff --cached ***")
    cached_diff = vcs_git.working_tree_diff(repo_cwd, cached=True)
    (temp_dir / "tempMRFile-cached.diff").write_text(cached_diff, encoding="utf-8")
    logger.info("%s", cached_diff)
    logger.info("*** END LOCAL CHANGES ***")
    return cached_diff


def find_existing_mr_with_same_diff(
    upstream_repo: str,
    repo_cwd: Path,
    temp_dir: Path,
    gitlab_client: gitlab.Gitlab,
) -> str | None:
    """Return result info when an open MR already has the same staged diff."""
    for mr in list_konflux_open_mrs(upstream_repo, gitlab_client):
        mr_num = mr.iid
        local_ref = _fetch_merge_request_head(repo_cwd, mr_num)
        final_diff = vcs_git.working_tree_diff(repo_cwd, cached=True, other_ref=local_ref)
        (temp_dir / "final.diff").write_text(final_diff, encoding="utf-8")
        if not final_diff.strip():
            mr_url = (
                getattr(mr, "web_url", None) or f"{upstream_repo}/-/merge_requests/{mr_num}"
            )
            return (
                "There is an existing MR with the same updates in the repo\n"
                + json.dumps({"merge_request": mr_url})
                + "\n"
            )
    return None


def commit_and_create_mr(
    *,
    component_group: str,
    revision: str,
    upstream_repo: str,
    repo_cwd: Path,
    gitlab_client: gitlab.Gitlab,
) -> tuple[str, str, int]:
    """Commit staged changes, push a branch, and open a new GitLab merge request."""
    working_branch = uuid.uuid4().hex[:8]
    vcs_git.checkout(repo_cwd, working_branch)
    vcs_git.commit_staged(repo_cwd, "fileUpdates changes")
    vcs_git.push(repo_cwd, working_branch)

    logger.info("Creating Pull Request...")
    mr_msg = f"[Konflux release] {component_group}: fileUpdates changes {working_branch}"
    mr_json = gitlab_create_mr(
        head=working_branch,
        title=mr_msg,
        target_branch=revision,
        description=mr_msg,
        upstream_repo=upstream_repo,
        gitlab_client=gitlab_client,
    )
    return mr_json + "\n", "Success", 0


def run_file_updates(
    *,
    upstream_repo: str,
    repo: str,
    revision: str,
    paths_json: str,
    component_group: str,
    temp_dir: Path,
    secrets: dict[str, str],
    gitlab_client: gitlab.Gitlab | None = None,
) -> tuple[str, str, int]:
    """Run clone, path updates, and MR dedup or create.

    Returns ``(info_body, state_or_error, exit_code)`` for ``main`` to map to
    Tekton results. Builds a GitLab client when *gitlab_client* is omitted.
    """
    logger.info("Temp Dir: %s", temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    token = configure_git_environment(secrets)
    if gitlab_client is None:
        gitlab_client = gitlab.Gitlab(secrets["gitlab_host"], private_token=token)
    git_functions_init(secrets["git_author_name"], secrets["git_author_email"], token)

    update_paths_file, paths_data = write_paths_manifest(paths_json, temp_dir)
    repo_cwd = prepare_repository(repo, revision, upstream_repo, temp_dir, paths_data)

    path_state, early = process_all_paths(paths_data, update_paths_file, repo_cwd, temp_dir)
    if early is not None:
        return early

    after_paths = outcome_after_path_processing(path_state)
    if after_paths is not None:
        return after_paths

    if not get_cached_diff(repo_cwd, temp_dir).strip():
        return "nothing needs change\n", "Success", 0

    existing_mr = find_existing_mr_with_same_diff(
        upstream_repo, repo_cwd, temp_dir, gitlab_client
    )
    if existing_mr is not None:
        return existing_mr, "Success", 0

    return commit_and_create_mr(
        component_group=component_group,
        revision=revision,
        upstream_repo=upstream_repo,
        repo_cwd=repo_cwd,
        gitlab_client=gitlab_client,
    )


def main(argv: list[str] | None = None) -> int:
    """Parse args, run file updates, and write Tekton result files."""
    raw_argv = sys.argv if argv is None else argv
    try:
        args = parse_args(raw_argv[1:])
    except SystemExit as e:
        code = e.code
        return code if isinstance(code, int) else 1

    temp_dir = (
        Path(args.temp_dir)
        if args.temp_dir
        else Path(os.environ.get("TEMP", "/tmp/file-updates"))
    )

    (
        info_path,
        state_path,
        ir_pr_path,
        ir_tr_path,
    ) = tekton.result_paths_from_env(
        "RESULT_FILE_UPDATES_INFO",
        "RESULT_FILE_UPDATES_STATE",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME",
    )

    ir_pr_path.write_text(args.internal_request_pipeline_run_name, encoding="utf-8")
    ir_tr_path.write_text(args.internal_request_task_run_name, encoding="utf-8")

    mount = file.path_from_env_variable(
        FILE_UPDATES_SECRET_MOUNT_ENV, "/mnt/file-updates-secret"
    )
    program = Path(raw_argv[0]).name

    try:
        secrets = load_file_updates_secrets(mount)
        info_body, state_or_error, exit_code = run_file_updates(
            upstream_repo=args.upstream_repo,
            repo=args.repo,
            revision=args.ref,
            paths_json=args.paths,
            component_group=args.component_group,
            temp_dir=temp_dir,
            secrets=secrets,
        )
    except tekton.CheckStepError as e:
        why = redact.redact_secrets(tekton.result_text_from_exception(e.cause))
        msg = f"{program}: Failed while {e.action}: {why}."
        info_path.write_text(msg, encoding="utf-8")
        state_path.write_text("Failed", encoding="utf-8")
        return 0
    except subprocess.CalledProcessError as e:
        cmd_preview = redact.redact_secrets(
            tekton.subprocess_cmd_preview_for_tekton_result(e.cmd)
        )
        detail = redact.redact_secrets(e.stderr or e.stdout or str(e))
        msg = f"{program}: Failed while running a command: {cmd_preview}: {detail}"
        info_path.write_text(msg[:500], encoding="utf-8")
        state_path.write_text("Failed", encoding="utf-8")
        return 0

    if exit_code == 1:
        info_path.write_text(info_body, encoding="utf-8")
        return 1

    if info_body == state_or_error and info_body.startswith('"'):
        write_json_error_result(info_path, state_path, state_or_error)
        return 0

    if state_or_error != "Success":
        write_error_result(info_path, state_path, info_body, state_or_error)
        return 0

    with info_path.open("a", encoding="utf-8") as f:
        f.write(info_body)
    state_path.write_text("Success", encoding="utf-8")
    logger.info("=== FINISHED ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
