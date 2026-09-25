#!/usr/bin/env python3
"""Clone infra-deployments, run the update script, and open or refresh the GitHub PR."""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import warnings
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from RestrictedPython import compile_restricted, safe_globals
from RestrictedPython.Eval import default_guarded_getitem
from RestrictedPython.Guards import safer_getattr

from release_service_utils.helpers import file
from release_service_utils.helpers import image_ref
from release_service_utils.helpers import snapshot
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.vcs import git
from release_service_utils.helpers.vcs import github

_REVISION_PLACEHOLDER = "{{ revision }}"
_OLD_REVISION_LINE = re.compile(r"^-\s+(newTag|digest):\s+(\S+)", re.MULTILINE)
_CHANGELOG_MARKER = "\n\n## Changelog"


def _normalize_pr_body_newlines(body: str) -> str:
    """Use LF newlines so changelog detection works on CRLF or LF bodies."""
    return body.replace("\r\n", "\n")


@dataclass(frozen=True)
class TaskParams:
    """Tekton paths, data file locations, and GitHub App defaults for one task run."""

    work_dir: Path
    data_dir: Path
    data_json_path: Path
    snapshot_path: Path
    default_target_repo: str
    default_app_id: str
    default_installation_id: str
    github_api_url: str
    github_app_key_path: Path


@dataclass(frozen=True)
class SnapshotContext:
    """Git revision and image metadata from the release snapshot."""

    revision: str
    origin_repo: str
    container_image: str


@dataclass(frozen=True)
class ApplyResult:
    """Snapshot metadata and repo changes after running the update script."""

    snap: SnapshotContext
    old_revision: str
    changed_paths: list[str]


def _github_app_ids(
    data: dict[str, Any],
    *,
    default_app_id: str,
    default_installation_id: str,
) -> tuple[str, str]:
    """Return GitHub App ID and installation ID from *data* or task defaults."""
    app_id = str(data.get("githubAppID", default_app_id))
    installation_id = str(data.get("githubAppInstallationID", default_installation_id))
    return app_id, installation_id


def _sandboxed_script_from_data(data: dict[str, Any]) -> str | None:
    """Return the sandboxed Python update script from *data*, or ``None``."""
    raw = data.get("infraDeploymentUpdates")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _bash_script_from_data(data: dict[str, Any]) -> str | None:
    """Return the legacy bash update script from *data*, or ``None``."""
    raw = data.get("infra-deployment-update-script")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _run_update_script(script: str, repo_dir: Path) -> None:
    """Execute *script* as bash in *repo_dir* and print its stdout/stderr."""
    proc = subprocess.run(
        ["bash", "-euo", "pipefail"],
        cwd=repo_dir,
        input=script,
        text=True,
        check=True,
        capture_output=True,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)


class InfraReplacementError(Exception):
    """Raised when a sandboxed replacement operation fails."""


def _validate_path(relative: str, clone_dir: Path) -> Path:
    """Resolve *relative* inside *clone_dir* and reject path traversal."""
    if not relative or relative.startswith("/"):
        raise InfraReplacementError(f"absolute paths are not allowed: {relative!r}")
    resolved = (clone_dir / relative).resolve()
    clone_resolved = clone_dir.resolve()
    if not str(resolved).startswith(str(clone_resolved) + "/"):
        raise InfraReplacementError(f"path traversal detected: {relative!r}")
    return resolved


def _extract_image_tag(container_image: str) -> str:
    """Return the OCI tag from a container image reference.

    Handles ``registry/repo:tag@sha256:digest`` and ``registry/repo:tag``.
    Returns an empty string when no tag is present (digest-only reference).
    """
    without_digest = container_image.split("@")[0]
    parts = without_digest.rsplit(":", 1)
    if len(parts) < 2:
        return ""
    candidate = parts[1]
    if "/" in candidate:
        return ""
    return candidate


def _normalize_oci_tag(tag_value: str) -> str:
    """Apply OCI semver normalization: ``_`` becomes ``+``."""
    return tag_value.replace("_", "+")


def _replace_on_line_after(content: str, regex: str, value: str, after_pattern: str) -> str:
    """Replace *regex* with *value* only on lines after an *after_pattern* match."""
    compiled = re.compile(regex)
    after_re = re.compile(after_pattern)
    lines = content.splitlines(keepends=True)
    result: list[str] = []
    matched_after = False
    for line in lines:
        if matched_after:
            line = compiled.sub(value, line)
            matched_after = False
        if after_re.search(line):
            matched_after = True
        result.append(line)
    return "".join(result)


def _replace_nth_match(content: str, regex: str, value: str, n: int) -> str:
    """Replace only the *n*-th match (1-based) of *regex* with *value*."""
    compiled = re.compile(regex)
    count = 0

    def _replacer(m: re.Match[str]) -> str:
        nonlocal count
        count += 1
        if count == n:
            return compiled.sub(value, m.group(0))
        return m.group(0)

    return compiled.sub(_replacer, content)


def _read(file_path: str, *, clone_dir: Path) -> str:
    """Read and return the contents of a file in the cloned repo."""
    target = _validate_path(file_path, clone_dir)
    if not target.exists():
        raise InfraReplacementError(f"file not found: {file_path!r}")
    return target.read_text(encoding="utf-8")


def _write(file_path: str, content: str, *, clone_dir: Path) -> None:
    """Write *content* to a file in the cloned repo."""
    target = _validate_path(file_path, clone_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    logger.info("Wrote %s", file_path)


def _replace(
    file_path: str,
    regex: str,
    value: str,
    *,
    clone_dir: Path,
    revision: str,
    after: str | None = None,
    occurrence: int | None = None,
) -> None:
    """Apply a regex substitution on a file in the cloned repo."""
    if after is not None and occurrence is not None:
        raise InfraReplacementError("after and occurrence are mutually exclusive")

    resolved_value = value.replace(_REVISION_PLACEHOLDER, revision)
    target = _validate_path(file_path, clone_dir)
    if not target.exists():
        raise InfraReplacementError(f"file not found: {file_path!r}")

    try:
        re.compile(regex)
    except re.error as exc:
        raise InfraReplacementError(f"invalid regex {regex!r}: {exc}") from exc

    content = target.read_text(encoding="utf-8")

    if after is not None:
        try:
            re.compile(after)
        except re.error as exc:
            raise InfraReplacementError(f"invalid after regex {after!r}: {exc}") from exc
        new_content = _replace_on_line_after(content, regex, resolved_value, after)
    elif occurrence is not None:
        if occurrence < 1:
            raise InfraReplacementError(f"occurrence must be >= 1, got {occurrence}")
        new_content = _replace_nth_match(content, regex, resolved_value, occurrence)
    else:
        new_content = re.sub(regex, resolved_value, content)

    target.write_text(new_content, encoding="utf-8")
    logger.info("Updated %s", file_path)


def _tag(
    component_name: str,
    filter_regex: str,
    *,
    snapshot_data: dict[str, Any],
) -> str:
    """Look up a tag from a snapshot component's container image."""
    components = snapshot_data.get("components")
    if not isinstance(components, list):
        components = []
    for comp in components:
        if not isinstance(comp, dict):
            continue
        if comp.get("name") != component_name:
            continue
        image = str(comp.get("containerImage", ""))
        raw_tag = _extract_image_tag(image)
        if not raw_tag:
            raise InfraReplacementError(
                f"component {component_name!r} has no image tag" f" in {image!r}"
            )
        normalized = _normalize_oci_tag(raw_tag)
        try:
            if re.fullmatch(filter_regex, normalized):
                return normalized
        except re.error as exc:
            raise InfraReplacementError(
                f"invalid filter regex {filter_regex!r}: {exc}"
            ) from exc
        raise InfraReplacementError(
            f"tag {normalized!r} does not match filter {filter_regex!r}"
        )
    raise InfraReplacementError(f"component {component_name!r} not found in snapshot")


def execute_infra_updates(
    script_text: str,
    clone_dir: Path,
    snapshot_data: dict[str, Any],
    revision: str,
) -> None:
    """Compile and execute *script_text* in a RestrictedPython sandbox.

    Available functions: ``read()``, ``write()``, ``replace()``, ``tag()``.
    """
    logger.info("Compiling infra update script with RestrictedPython")
    try:
        byte_code = compile_restricted(script_text, "<infra-update>", "exec")
    except SyntaxError as exc:
        raise InfraReplacementError(f"script compilation failed: {exc}") from exc

    restricted = safe_globals.copy()
    restricted["read"] = partial(_read, clone_dir=clone_dir)
    restricted["write"] = partial(_write, clone_dir=clone_dir)
    restricted["replace"] = partial(_replace, clone_dir=clone_dir, revision=revision)
    restricted["tag"] = partial(_tag, snapshot_data=snapshot_data)
    restricted["_getattr_"] = safer_getattr
    restricted["_getitem_"] = default_guarded_getitem
    restricted["_getiter_"] = iter
    restricted["_write_"] = lambda x: x
    restricted["__builtins__"] = {
        k: v
        for k, v in restricted.get("__builtins__", {}).items()
        if k not in {"__import__", "open", "exec", "eval", "compile"}
    }

    logger.info("Executing sandboxed infra update script")
    try:
        exec(byte_code, restricted, {})  # noqa: S102
    except InfraReplacementError:
        raise
    except Exception as exc:
        raise InfraReplacementError(f"script execution failed: {exc}") from exc
    logger.info("Infra update script completed successfully")


def _extract_old_revision_from_diff(diff_text: str) -> str:
    """Parse the removed `newTag` or `digest` value from a unified diff."""
    match = _OLD_REVISION_LINE.search(diff_text)
    if match is None:
        return ""
    return match.group(2)


def _split_pr_body(body: str | None) -> tuple[str, str]:
    """Split PR body into the included-PR links block and any changelog section."""
    links_part = body if body else "Included PRs:"
    normalized = _normalize_pr_body_newlines(links_part)
    idx = normalized.find(_CHANGELOG_MARKER)
    if idx == -1:
        return normalized, ""
    return normalized[:idx], normalized[idx:]


def _changelog_commit_lines(changelog_text: str) -> list[str]:
    """Return markdown list lines from a changelog section."""
    return [
        line
        for line in _normalize_pr_body_newlines(changelog_text).splitlines()
        if line.startswith("- ")
    ]


def _merge_changelog_section(body: str, new_changelog: str) -> str:
    """Append new changelog commit lines, keeping any existing changelog entries."""
    body = _normalize_pr_body_newlines(body)
    new_changelog = _normalize_pr_body_newlines(new_changelog)
    new_lines = _changelog_commit_lines(new_changelog)
    if not new_lines:
        return body
    idx = body.find(_CHANGELOG_MARKER)
    if idx == -1:
        return f"{body}{_CHANGELOG_MARKER}\n" + "\n".join(new_lines)
    prefix = body[: idx + len(_CHANGELOG_MARKER)]
    existing_lines = _changelog_commit_lines(body[idx + len(_CHANGELOG_MARKER) :])
    merged_lines = list(existing_lines)
    for line in new_lines:
        if line not in merged_lines:
            merged_lines.append(line)
    return prefix + "\n" + "\n".join(merged_lines)


def _build_pr_description(
    session: github.GitHubAppSession,
    *,
    existing_body: str | None,
    origin_repo: str,
    revision: str,
    old_revision: str,
    container_image: str,
) -> str:
    """Assemble PR body text with source PR link and optional changelog section."""
    logger.info("Building pull request description for revision %s", revision)
    links_part, changelog_part = _split_pr_body(existing_body)
    logger.info("Searching GitHub for pull request linked to commit %s", revision)
    new_pr_link = github.pull_request_url_for_commit_sha(session, revision)
    if new_pr_link:
        pr_line = f"- {new_pr_link}"
        if pr_line not in links_part:
            links_part = f"{links_part}\n{pr_line}"
    body = links_part + changelog_part

    changelog_rev = ""
    if old_revision:
        if old_revision.startswith("sha256:"):
            logger.info("Resolving Quay digest to git SHA for changelog")
            resolved = image_ref.resolve_quay_digest_to_git_sha(
                old_revision,
                container_image,
            )
            changelog_rev = resolved or ""
        else:
            changelog_rev = old_revision
    if changelog_rev:
        logger.info(
            "Fetching changelog for %s (%s...%s)",
            origin_repo,
            changelog_rev,
            revision,
        )
        changelog = github.compare_changelog(
            session,
            origin_repo,
            changelog_rev,
            revision,
        )
        body = _merge_changelog_section(body, changelog)
    return body


def _snapshot_from_params(params: TaskParams) -> SnapshotContext:
    """Load snapshot fields from the task snapshot file."""
    snapshot_file = params.data_dir / params.snapshot_path
    return SnapshotContext(**snapshot.first_component(snapshot_file))


def _run_patched_script(script: str, revision: str, clone_dir: Path) -> None:
    """Patch *script* with *revision* and execute it in *clone_dir*."""
    patched = script.replace(_REVISION_PLACEHOLDER, revision)
    logger.info("Running update script in %s", clone_dir)
    _run_update_script(patched, clone_dir)


def _collect_apply_result(snap: SnapshotContext, clone_dir: Path) -> ApplyResult:
    """Read pre-update revision and changed paths from the clone after the script."""
    logger.info("Reading working tree diff in %s", clone_dir)
    diff_text = git.working_tree_diff(clone_dir)
    old_revision = _extract_old_revision_from_diff(diff_text)
    logger.info("Reading git status in %s", clone_dir)
    changed_paths = git.changed_paths_from_status(clone_dir)
    if old_revision:
        logger.info("old revision: %s", old_revision)
    logger.info("changed files: %d", len(changed_paths))
    return ApplyResult(
        snap=snap,
        old_revision=old_revision,
        changed_paths=changed_paths,
    )


def _create_or_update_pr(
    params: TaskParams,
    data: dict[str, Any],
    *,
    target_repo: str,
    clone_dir: Path,
    apply_result: ApplyResult,
) -> None:
    """Push changes and open or refresh the infra-deployments pull request."""
    changed_paths = apply_result.changed_paths
    if not changed_paths:
        logger.info("No files to add to a PR, exiting")
        return

    snap = apply_result.snap
    branch = github.branch_name_from_origin_repo(snap.origin_repo)
    logger.info(
        "Updating pull request on %s (branch %s, %d files)",
        target_repo,
        branch,
        len(changed_paths),
    )

    app_id, installation_id = _github_app_ids(
        data,
        default_app_id=params.default_app_id,
        default_installation_id=params.default_installation_id,
    )
    logger.info("Authenticating GitHub App (application_id=%s)", app_id)
    session = github.open_session(
        api_url=params.github_api_url,
        private_key_path=params.github_app_key_path,
        app_id=app_id,
        installation_id=installation_id,
    )

    logger.info("Committing and force-pushing branch %s to %s", branch, target_repo)
    github.force_push_updated_files(
        session,
        clone_dir=clone_dir,
        target_repo=target_repo,
        branch=branch,
        relative_paths=changed_paths,
    )

    logger.info("Creating pull request for branch %s", branch)
    infra_pr = github.create_pull_request(
        session,
        target_repo,
        head_branch=branch,
        title=f"{branch} update",
    )
    if "url" not in infra_pr:
        logger.info("Create returned no URL; searching for existing open pull request")
        existing = github.find_open_pull_request_by_branch(session, target_repo, branch)
        if existing is None:
            message = infra_pr.get("message", infra_pr)
            raise RuntimeError(f"PR not created or did not already exist: {message}")
        infra_pr = existing
        logger.info("Using existing pull request: %s", infra_pr["url"])

    if "body" not in infra_pr:
        message = infra_pr.get("message", infra_pr)
        raise RuntimeError(f"PR not created or did not already exist: {message}")

    new_body = _build_pr_description(
        session,
        existing_body=infra_pr.get("body"),
        origin_repo=snap.origin_repo,
        revision=snap.revision,
        old_revision=apply_result.old_revision,
        container_image=snap.container_image,
    )
    logger.info("Updating pull request body at %s", infra_pr["url"])
    github.update_pull_request_body(session, infra_pr["url"], new_body)
    logger.info("Pull request updated: %s", infra_pr["url"])


def run_update_infra_deployments(params: TaskParams) -> None:
    """Clone, run the update script when configured, and create or update the PR."""
    data_path = params.data_dir / params.data_json_path
    logger.info("Loading data from %s", data_path)
    data = file.load_json_dict(data_path)

    sandboxed_script = _sandboxed_script_from_data(data)
    bash_script = _bash_script_from_data(data)

    if sandboxed_script is None and bash_script is None:
        logger.info("No update script configured in data")
        return
    if bash_script is not None and sandboxed_script is None:
        warnings.warn(
            "'infra-deployment-update-script' is deprecated, use"
            " 'infraDeploymentUpdates' instead",
            DeprecationWarning,
            stacklevel=2,
        )

    target_repo = str(data.get("targetGHRepo", params.default_target_repo)).strip()
    params.work_dir.mkdir(parents=True, exist_ok=True)
    clone_path = params.work_dir / "cloned"
    if clone_path.exists():
        shutil.rmtree(clone_path)
    logger.info("Cloning %s into %s/cloned", target_repo, params.work_dir)
    clone_dir = git.clone(
        params.work_dir,
        f"https://github.com/{target_repo}.git",
        directory_name="cloned",
    )
    logger.info("Clone complete at %s", clone_dir)

    snapshot_file = params.data_dir / params.snapshot_path
    logger.info("Loading snapshot from %s", snapshot_file)
    snap = _snapshot_from_params(params)
    logger.info("Syncing clone to origin/main before applying updates")
    git.sync_to_origin_main(clone_dir)
    logger.info(
        "Applying revision %s from %s",
        snap.revision,
        snap.origin_repo,
    )

    if sandboxed_script is not None:
        snapshot_data = file.load_json_dict(snapshot_file)
        execute_infra_updates(sandboxed_script, clone_dir, snapshot_data, snap.revision)
    else:
        _run_patched_script(bash_script, snap.revision, clone_dir)

    apply_result = _collect_apply_result(snap, clone_dir)
    _create_or_update_pr(
        params,
        data,
        target_repo=target_repo,
        clone_dir=clone_dir,
        apply_result=apply_result,
    )


def _params_from_env() -> TaskParams:
    """Build `TaskParams` from Tekton `PARAM_*` and GitHub App environment variables."""
    return TaskParams(
        work_dir=Path(tekton.require_env("PARAM_WORK_DIR")),
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        data_json_path=Path(tekton.require_env("PARAM_DATA_JSON_PATH")),
        snapshot_path=Path(tekton.require_env("PARAM_SNAPSHOT_PATH")),
        default_target_repo=tekton.require_env("PARAM_DEFAULT_TARGET_GH_REPO"),
        default_app_id=tekton.require_env("PARAM_DEFAULT_GITHUB_APP_ID"),
        default_installation_id=tekton.require_env("PARAM_DEFAULT_GITHUB_APP_INSTALLATION_ID"),
        github_api_url=tekton.require_env("GITHUB_API_URL"),
        github_app_key_path=Path(tekton.require_env("GITHUBAPP_KEY_PATH")),
    )


def main() -> int:
    """Entry point: load params from the environment and run the workflow."""
    run_update_infra_deployments(_params_from_env())
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
