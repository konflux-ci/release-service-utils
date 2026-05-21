#!/usr/bin/env python3
"""Update files in Git repositories and open GitLab merge requests when needed.

Reads credentials from ``/mnt/file-updates-secret/`` (or
``FILE_UPDATES_SECRET_MOUNT``). Tekton passes result file paths via
``RESULT_FILE_UPDATES_INFO``, ``RESULT_FILE_UPDATES_STATE``, and the internal
request name results. Most logical failures exit 0 with state ``Failed`` so
results are published; invalid YAML on a replacement target exits 1 after
writing ``fileUpdatesInfo``.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Sequence

import file
import tekton

PROG = "process_file_updates.py"
FILE_UPDATES_SECRET_MOUNT_ENV = "FILE_UPDATES_SECRET_MOUNT"
# When set (catalog unit tests), git/glab run under bash after sourcing this script.
SHELL_INIT_ENV = "PROCESS_FILE_UPDATES_SHELL_INIT"
USE_SHELL_ENV = "PROCESS_FILE_UPDATES_USE_SHELL"


def _usage_text() -> str:
    return (
        f"usage: {PROG} --upstream-repo REPO --repo REPO --ref REF --paths JSON \\\n"
        f"  --component-group GROUP --internal-request-pipeline-run-name NAME \\\n"
        f"  --internal-request-task-run-name NAME [--temp-dir DIR]\n"
    )


def parse_args(argv: list[str] | None) -> argparse.Namespace:
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
    keys = ("gitlab_host", "gitlab_access_token", "git_author_name", "git_author_email")
    out: dict[str, str] = {}
    for key in keys:
        path = mount / key
        try:
            out[key] = path.read_text(encoding="utf-8").strip()
        except OSError as e:
            raise tekton.CheckStepError(f"reading secret file {key}", e) from e
    return out


def _shell_wrap(command: str, cwd: Path | None) -> subprocess.CompletedProcess[str]:
    init = os.environ.get(SHELL_INIT_ENV, "").strip()
    if init:
        command = f"{init}; {command}"
    return subprocess.run(
        ["bash", "-c", command],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )


def run_git(args: Sequence[str], *, cwd: Path | None = None, check: bool = True) -> str:
    if os.environ.get(USE_SHELL_ENV):
        cmd = "git " + " ".join(shlex.quote(a) for a in args)
        proc = _shell_wrap(cmd, cwd)
        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode, ["bash", "-c", cmd], proc.stdout, proc.stderr
            )
        return proc.stdout or ""
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, ["git", *args], proc.stdout, proc.stderr
        )
    return proc.stdout or ""


def run_glab(
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    check: bool = True,
    input_text: str | None = None,
) -> str:
    if os.environ.get(USE_SHELL_ENV):
        cmd = "glab " + " ".join(shlex.quote(a) for a in args)
        if input_text is not None:
            cmd += " <<< " + shlex.quote(input_text)
        proc = _shell_wrap(cmd, cwd)
        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode, ["bash", "-c", cmd], proc.stdout, proc.stderr
            )
        return proc.stdout or ""
    proc = subprocess.run(
        ["glab", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        input=input_text,
        check=False,
    )
    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, ["glab", *args], proc.stdout, proc.stderr
        )
    return proc.stdout or ""


def auth_repo_url(repository: str, token: str) -> str:
    if "://" in repository:
        return f"https://oauth2:{token}@{repository.split('://', 1)[1]}"
    return repository


def gitlab_init(host: str, token: str) -> None:
    run_glab(["auth", "login", "-h", host, "--stdin"], input_text=token, check=True)


def git_functions_init(author_name: str, author_email: str, token: str) -> None:
    if not author_name or not author_email or not token:
        raise tekton.CheckStepError(
            "initializing git",
            ValueError("GIT_AUTHOR_NAME, GIT_AUTHOR_EMAIL, and ACCESS_TOKEN are required"),
        )
    run_git(["config", "--global", "user.name", author_name])
    run_git(["config", "--global", "user.email", author_email])


def git_clone_and_checkout(repository: str, revision: str, token: str, work_dir: Path) -> Path:
    repo_dir = Path(repository.rstrip("/")).name
    if repo_dir.endswith(".git"):
        repo_dir = repo_dir[:-4]
    clone_url = auth_repo_url(repository, token)
    run_git(
        ["clone", "--depth", "1", "--branch", revision, clone_url],
        cwd=work_dir,
    )
    repo_path = work_dir / repo_dir
    return repo_path


def git_rebase_upstream(
    name: str, remote: str, revision: str, token: str, repo_cwd: Path
) -> None:
    remote_url = auth_repo_url(remote, token)
    run_git(["remote", "add", name, remote_url], cwd=repo_cwd, check=False)
    run_git(["fetch", name, revision], cwd=repo_cwd)
    run_git(["rebase", f"{name}/{revision}"], cwd=repo_cwd)


def git_commit_and_push(branch: str, message: str, repo_cwd: Path) -> None:
    run_git(["checkout", "-b", branch], cwd=repo_cwd)
    run_git(["commit", "-a", "-m", message], cwd=repo_cwd)
    run_git(["push", "origin", branch], cwd=repo_cwd)


def gitlab_create_mr(
    *,
    head: str,
    title: str,
    target_branch: str,
    description: str,
    upstream_repo: str,
    repo_cwd: Path,
) -> str:
    run_git(["remote", "add", "glab-base", upstream_repo], cwd=repo_cwd, check=False)
    out = run_glab(
        [
            "mr",
            "create",
            "--title",
            title,
            "--source-branch",
            head,
            "--target-branch",
            target_branch,
            "--description",
            description,
            "-R",
            upstream_repo,
        ],
        cwd=repo_cwd,
    )
    for line in out.splitlines():
        if "merge_request" not in line:
            continue
        text = line.strip()
        if text.startswith("merge_request:"):
            url = text.split(":", 1)[1].strip()
        else:
            url = text.split()[-1]
        proc = subprocess.run(
            ["yq", "-o", "json", f"merge_request: {url}"],
            text=True,
            capture_output=True,
            check=True,
        )
        return proc.stdout.strip()
    return "{}"


def blank_lines_before_yaml(target_file: Path) -> int:
    proc = subprocess.run(
        [
            "awk",
            r"/[[:alpha:]]+/{ if(! match($0, \"^#\")) { print NR-1; exit } }",
            str(target_file),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return 0
    return int(proc.stdout.strip().splitlines()[0])


def count_pipes(replacement: str) -> int:
    return replacement.count("|")


def apply_replacement_block(
    target_file: Path,
    start_block: int,
    value_size: int,
    replacement: str,
    temp_dir: Path,
) -> tuple[str | None, Path | None]:
    """Return (error_message, diff_path) — error_message set on validation failure."""
    if count_pipes(replacement) != 3:
        return "Replace expression should be in '|search|replace|' format", None

    sed_expr = f"{start_block},+{value_size}s{replacement}"
    subprocess.run(["sed", "-i", sed_expr, str(target_file)], check=True)

    replace_str = replacement.split("|")[2] if replacement.count("|") >= 2 else ""
    result_path = temp_dir / "result.txt"
    proc = subprocess.run(
        ["sed", "-ne", f"{start_block},+{value_size}p", str(target_file)],
        text=True,
        capture_output=True,
        check=True,
    )
    result_path.write_text(proc.stdout, encoding="utf-8")

    found_path = temp_dir / "found.txt"
    diff_path = temp_dir / "diff.txt"
    diff_proc = subprocess.run(
        ["diff", "-u", str(found_path), str(result_path)],
        text=True,
        capture_output=True,
        check=False,
    )
    diff_path.write_text(diff_proc.stdout, encoding="utf-8")

    replaced_block_lines = len(result_path.read_text(encoding="utf-8").splitlines())
    if replaced_block_lines != value_size + 1:
        return "Text block size differs from the original", diff_path

    grep_proc = subprocess.run(
        ["sed", "-ne", f"{start_block},+{value_size}p", str(target_file)],
        text=True,
        capture_output=True,
        check=True,
    )
    replaced_count = sum(1 for line in grep_proc.stdout.splitlines() if replace_str in line)
    if replaced_count != 1:
        return (
            "Too many lines replaced. Check if the replace expression isn't too greedy",
            diff_path,
        )
    return None, diff_path


def write_error_result(info_path: Path, state_path: Path, diff_text: str, error: str) -> None:
    trimmed = diff_text[1:3701] if diff_text else ""
    payload = json.dumps({"str": trimmed, "error": error})
    info_path.write_text(payload, encoding="utf-8")
    state_path.write_text("Failed", encoding="utf-8")


def write_json_error_result(info_path: Path, state_path: Path, error: str) -> None:
    payload = json.dumps({"str": error, "error": error})
    info_path.write_text(payload, encoding="utf-8")
    state_path.write_text("Failed", encoding="utf-8")


def list_open_mrs(upstream_repo: str) -> list[str]:
    page = 1
    items: list[str] = []
    while True:
        out = run_glab(
            [
                "mr",
                "list",
                "-R",
                upstream_repo,
                "--search",
                "Konflux release",
                "--per-page",
                "100",
                "--page",
                str(page),
            ],
            check=False,
        )
        mr_page = [ln for ln in out.splitlines() if ln.startswith("!")]
        if not mr_page:
            break
        items.extend(mr_page)
        page += 1
    return items


def run_file_updates(
    *,
    upstream_repo: str,
    repo: str,
    revision: str,
    paths_json: str,
    component_group: str,
    temp_dir: Path,
    secrets: dict[str, str],
) -> tuple[str, str, int]:
    """
    Run the update workflow.

    Returns (info_append_or_replace, state, exit_code) where exit_code is 0 or 1.
    """
    token = secrets["gitlab_access_token"]
    os.environ["GITLAB_HOST"] = secrets["gitlab_host"]
    os.environ["ACCESS_TOKEN"] = token
    os.environ["GIT_AUTHOR_NAME"] = secrets["git_author_name"]
    os.environ["GIT_AUTHOR_EMAIL"] = secrets["git_author_email"]

    print(f"Temp Dir: {temp_dir}", flush=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    gitlab_init(secrets["gitlab_host"], token)
    git_functions_init(secrets["git_author_name"], secrets["git_author_email"], token)

    update_paths_file = temp_dir / "updatePaths.json"
    update_paths_file.write_text(paths_json + "\n", encoding="utf-8")
    paths_data: list[dict[str, Any]] = json.loads(paths_json)

    print(f"=== UPDATING {repo} ON BRANCH {revision} ===\n", flush=True)

    repo_cwd = git_clone_and_checkout(repo, revision, token, temp_dir)
    git_rebase_upstream("glab-base", upstream_repo, revision, token, repo_cwd)

    replacements_update_error: str | None = None
    diff_path: Path | None = None
    replacements_performed: int | None = None
    key_not_found = False

    print(update_paths_file.read_text(encoding="utf-8"), flush=True)

    for path_index, entry in enumerate(paths_data):
        print("-- start updatePathsTmpfile --", flush=True)
        print(update_paths_file.read_text(encoding="utf-8"), flush=True)
        print("-- end updatePathsTmpfile --", flush=True)

        target_file = repo_cwd / entry["path"]
        print(f"targetFile: {target_file.relative_to(repo_cwd)}", flush=True)

        seed = entry.get("seed") or ""
        if isinstance(seed, str):
            seed = seed.strip('"')
        print(seed, flush=True)

        if seed:
            print("seed operation to perform", flush=True)
            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_text(
                seed + ("\n" if not seed.endswith("\n") else ""), encoding="utf-8"
            )
            print("-- start targetFile --", flush=True)
            print(target_file.read_text(encoding="utf-8"), flush=True)
            print("-- end targetFile --", flush=True)
            run_git(["add", str(target_file.relative_to(repo_cwd))], cwd=repo_cwd)
            run_git(["status"], cwd=repo_cwd)

        replacements = entry.get("replacements") or []
        replacements_length = len(replacements)
        print(f"Replacements to perform: {replacements_length}", flush=True)

        if replacements_length > 0:
            replacements_performed = 0
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
                print(f"REPLACEMENT: #{replacement_index}", flush=True)
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
                    print("NOT FOUND", flush=True)
                    key_not_found = True
                    continue
                print("FOUND", flush=True)

                value_proc = subprocess.run(
                    ["yq", key, str(target_file)],
                    text=True,
                    capture_output=True,
                    check=True,
                )
                value_size = len(value_proc.stdout.splitlines())
                start_block = found_at + blank_offset

                # Keep first line in found.txt, drop for diff baseline (sed -i '1d')
                found_content = found_path.read_text(encoding="utf-8")
                found_lines = found_content.splitlines()
                if found_lines:
                    found_path.write_text(
                        "\n".join(found_lines[1:]) + ("\n" if len(found_lines) > 1 else ""),
                        encoding="utf-8",
                    )

                print("--start file--", flush=True)
                print(target_file.read_text(encoding="utf-8"), flush=True)
                print("--end file--", flush=True)

                err, diff_path = apply_replacement_block(
                    target_file, start_block, value_size, replacement, temp_dir
                )
                if err:
                    replacements_update_error = err
                    break
                replacements_performed += 1

        run_git(["add", str(target_file.relative_to(repo_cwd))], cwd=repo_cwd)

    if replacements_update_error:
        diff_text = (
            diff_path.read_text(encoding="utf-8") if diff_path and diff_path.exists() else ""
        )
        return diff_text, replacements_update_error, 0

    if replacements_performed == 0:
        if key_not_found:
            err = '"no replacements were performed"'
            return err, err, 0
        return "nothing needs change\n", "Success", 0

    print("\n*** START LOCAL CHANGES ***\n", flush=True)
    print("\n*** Result from git diff --cached ***\n", flush=True)
    cached_diff = run_git(["diff", "--cached"], cwd=repo_cwd, check=False)
    cached_diff_path = temp_dir / "tempMRFile-cached.diff"
    cached_diff_path.write_text(cached_diff, encoding="utf-8")
    print(cached_diff, flush=True)
    print("\n*** END LOCAL CHANGES ***\n", flush=True)

    if not cached_diff.strip():
        return "nothing needs change\n", "Success", 0

    for one_item in list_open_mrs(upstream_repo):
        mr_num = one_item.split()[0].lstrip("!")
        run_git(
            ["fetch", "origin", f"merge-requests/{mr_num}/head:mr_{mr_num}"],
            cwd=repo_cwd,
        )
        final_diff = run_git(["diff", "--cached", f"mr_{mr_num}"], cwd=repo_cwd, check=False)
        final_path = temp_dir / "final.diff"
        final_path.write_text(final_diff, encoding="utf-8")
        if not final_diff.strip():
            info = (
                "There is an existing MR with the same updates in the repo\n"
                + json.dumps({"merge_request": f"{upstream_repo}/-/merge_requests/{mr_num}"})
                + "\n"
            )
            return info, "Success", 0

    working_branch = uuid.uuid4().hex[:8]
    git_commit_and_push(working_branch, "fileUpdates changes", repo_cwd)

    print("Creating Pull Request...", flush=True)
    mr_msg = f"[Konflux release] {component_group}: fileUpdates changes {working_branch}"
    mr_json = gitlab_create_mr(
        head=working_branch,
        title=mr_msg,
        target_branch=revision,
        description=mr_msg,
        upstream_repo=upstream_repo,
        repo_cwd=repo_cwd,
    )
    return mr_json + "\n", "Success", 0


def main(argv: list[str] | None = None) -> int:
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
    ) = tekton.result_paths(
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
        msg = tekton.result_text_for_check_step_error(program, e)
        info_path.write_text(msg, encoding="utf-8")
        state_path.write_text("Failed", encoding="utf-8")
        return 0
    except subprocess.CalledProcessError as e:
        cmd_preview = tekton.subprocess_cmd_preview_for_tekton_result(e.cmd)
        detail = e.stderr or e.stdout or e
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
    print("=== FINISHED ===\n", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
