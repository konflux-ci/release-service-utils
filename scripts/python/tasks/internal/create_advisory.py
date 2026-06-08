#!/usr/bin/env python3
"""Create advisory YAML under `data/advisories` in a Git repo (Tekton task).

* Reads advisory credentials from `/mnt/advisory_secret` (or
  `ADVISORY_SECRET_MOUNT`) and Errata credentials from `/mnt/errata_secret`
  (or `ERRATA_SECRET_MOUNT`).
* Reserves an Errata `live_id` when the decoded advisory JSON has no
  `live_id`.
* Writes task results from `RESULT_RESULT`, `RESULT_ADVISORY_URL`,
  `RESULT_ADVISORY_INTERNAL_URL`, `RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME`,
  and `RESULT_INTERNAL_REQUEST_TASK_RUN_NAME`.
* After a valid invocation with those env vars, always exits with status `0`;
  success or failure is in the result files.
* Missing env before result handling exits `1`.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import advisory_data
import apply_template
import authentication
import file
import http_client
import internal_request
import requests
import subprocess_cmd
import tekton
import yaml
from jsonschema import ValidationError
from jsonschema.validators import validator_for
from requests_kerberos import OPTIONAL, HTTPKerberosAuth
from vcs import git
from vcs import gitlab

PROG = "create_advisory.py"
ADVISORY_TEMPLATE_PATH = Path("/home/templates/advisory.yaml.jinja")


def _clone_advisory_repo(
    credentials: gitlab.GitLabCredentials,
    origin: str,
    work_dir: Path,
    *,
    stderr_path: Path,
) -> tuple[Path, Path]:
    git.configure_git_global_user(
        credentials.git_author_name,
        credentials.git_author_email,
    )
    # Sparse checkout: only this tenant's advisories plus repo schema (for validation).
    sparse_dirs = [f"data/advisories/{origin}", "schema"]
    repo_root = gitlab.clone_project_sparse(
        credentials.git_repo,
        gitlab.DEFAULT_BRANCH,
        sparse_dirs,
        parent_dir=work_dir,
        stderr_path=stderr_path,
    )
    advisory_base = repo_root / "data" / "advisories" / origin
    return repo_root, advisory_base


def _reserve_errata_live_id(
    errata_api: str,
    errata_mount: Path,
    *,
    stderr_path: Path | None,
    krb5_template: Path = Path("/etc/krb5.conf"),
    kinit_fn: Callable[..., None] = authentication.kinit_with_retry,
) -> int:
    """POST `reserve_live_id` with Negotiate auth after *kinit_fn*."""
    # Errata secret mount uses `name` + `base64_keytab` keys (not the default filenames).
    principal, keytab_bytes, _unused = authentication.load_service_account(
        errata_mount,
        (),
        principal_file="name",
        keytab_b64_file="base64_keytab",
    )
    # Keytab and credential cache live on disk only for the kinit + HTTP call window.
    keytab_path = file.make_tempfile_path("keytab-", keytab_bytes)
    ccache_fd, ccache_temp_path = tempfile.mkstemp()
    os.close(ccache_fd)
    ccache_path = Path(ccache_temp_path)
    try:
        krb5_template_source = krb5_template.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        keytab_path.unlink(missing_ok=True)
        ccache_path.unlink(missing_ok=True)
        raise tekton.CheckStepError("reading the Kerberos configuration", exc) from exc
    # Pod krb5.conf may point at wrong KDC for Errata; patch before kinit.
    krb5_config_path = file.make_tempfile_path(
        "krb5-",
        authentication.patch_krb5_config(krb5_template_source).encode("utf-8"),
    )
    kenv = {
        "KRB5CCNAME": str(ccache_path),
        "KRB5_CONFIG": str(krb5_config_path),
        "KRB5_TRACE": "/dev/stderr",
    }
    try:
        kinit_fn(principal, keytab_path, kenv, max_attempts=5)
        # Propagate ccache + krb5 into the process; omit KRB5_TRACE (very noisy).
        krb5_env_for_process = {
            env_key: env_val for env_key, env_val in kenv.items() if env_key != "KRB5_TRACE"
        }
        os.environ.update(krb5_env_for_process)
        reserve_live_id_url = f"{errata_api.rstrip('/')}/advisory/reserve_live_id"
        session = http_client.post_session()
        auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        try:
            resp = session.post(reserve_live_id_url, auth=auth, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as exc:
            if stderr_path is not None:
                with open(
                    stderr_path,
                    "a",
                    encoding="utf-8",
                    errors="replace",
                ) as errf:
                    errf.write(f"\nreserve_live_id request failed: {exc!r}\n")
            raise
        data: dict[str, Any] = resp.json()
        live_id_raw = data.get("live_id")
        if live_id_raw is None:
            msg = f"no live_id in response: {data!r}"
            raise ValueError(msg)
        return int(live_id_raw)
    finally:
        keytab_path.unlink(missing_ok=True)
        ccache_path.unlink(missing_ok=True)
        krb5_config_path.unlink(missing_ok=True)


def _write_initial_content_file(
    work_dir: Path,
    decoded: dict[str, Any],
    content_list_path: str,
) -> Path:
    # Mutable copy of spec content rows; idempotency filtering rewrites this file.
    content_file = work_dir / "content.json"
    decoded_content_rows = advisory_data.content_array_from_decoded(decoded, content_list_path)
    content_file.write_text(
        json.dumps(decoded_content_rows, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return content_file


def _customer_portal_url(url_prefix: str, errata_type: str, errata_name: str) -> str:
    # Errata name is metadata.name or portal id "YYYY:NNNN" depending on caller path.
    return f"{url_prefix}/{errata_type}-{errata_name}"


def _write_success_results(
    result_paths: dict[str, Path],
    *,
    customer_portal_url: str,
    gitlab_raw_url: str,
) -> None:
    result_paths["result"].write_text("Success", encoding="utf-8")
    result_paths["advisory_url"].write_text(customer_portal_url, encoding="utf-8")
    result_paths["advisory_internal_url"].write_text(gitlab_raw_url, encoding="utf-8")


def _finish_if_all_content_already_published(
    *,
    repo_root: Path,
    advisory_base: Path,
    content_file: Path,
    content_list_path: str,
    content_type: str,
    git_repo: str,
    url_prefix: str,
    stderr_path: Path,
    result_paths: dict[str, Path],
) -> bool:
    """Walk existing advisories (newest first) and filter *content_file*.

    Return True when every row was already published and success results were
    written; False when a new advisory must be created.
    """
    # Side file holding the current advisory's content rows during each loop step.
    existing_content = content_file.parent / "existing_content.json"
    # Repo-relative path to the advisory that last removed rows from content_file.
    latest_advisory_file: str | None = None

    # Newest advisories first (by directory mtime). For each, drop rows already
    # published there; stop early if nothing remains to release.
    for year_num_subdir in advisory_data.list_existing_advisory_subdirs(advisory_base):
        candidate_yaml = advisory_base / year_num_subdir / "advisory.yaml"
        yaml_doc = advisory_data.load_advisory_yaml(candidate_yaml)
        existing_rows = advisory_data.spec_content_array_from_advisory_yaml(
            yaml_doc, content_list_path
        )
        existing_content.write_text(
            json.dumps(existing_rows, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

        rows_before = len(json.loads(content_file.read_text(encoding="utf-8")))

        # Compare row-by-row (image purl/tags, rpm nevra, etc.) via advisory_data helpers.
        filtered = advisory_data.filter_content_by_existing(
            content_type,
            content_file,
            existing_content,
            stderr_path=stderr_path,
        )
        content_file.write_text(filtered + "\n", encoding="utf-8")

        rows_after = len(json.loads(content_file.read_text(encoding="utf-8")))

        if rows_before > rows_after and latest_advisory_file is None:
            # Newest advisory that absorbed at least one of our rows — used for URLs
            # when the release is a no-op (everything already shipped).
            latest_advisory_file = str(candidate_yaml.relative_to(repo_root))

        if rows_after == 0:
            # All requested content already exists in a prior advisory; success without
            # reserving a live_id or pushing a new directory.
            if not latest_advisory_file:
                msg = "all content matched but latest advisory path was not set"
                raise RuntimeError(msg)
            published_path = repo_root / latest_advisory_file
            published_doc = advisory_data.load_advisory_yaml(published_path)
            errata_type = advisory_data.get_advisory_spec_type(published_doc)
            errata_name = advisory_data.get_advisory_metadata_name(published_doc)
            _write_success_results(
                result_paths,
                customer_portal_url=_customer_portal_url(url_prefix, errata_type, errata_name),
                gitlab_raw_url=gitlab.raw_file_url(git_repo, latest_advisory_file),
            )
            return True

    return False


def _read_signing_key_from_config_map(
    config_map_name: str,
    *,
    stderr_path: Path,
) -> str:
    """Return `SIG_KEY_NAMES` from the configmap, or `SIG_KEY_NAME` if absent."""
    raw = subprocess_cmd.run_cmd(
        [
            "kubectl",
            "get",
            "configmap",
            config_map_name,
            "-o",
            "json",
        ],
        stderr_path=stderr_path,
    ).stdout
    data = json.loads(raw).get("data") or {}
    signing_key = (data.get("SIG_KEY_NAMES") or data.get("SIG_KEY_NAME") or "").strip()
    if not signing_key:
        msg = (
            f"configmap {config_map_name!r} has neither SIG_KEY_NAMES nor "
            f"SIG_KEY_NAME data"
        )
        raise ValueError(msg)
    return signing_key


def _build_merged_advisory_with_signing_key(
    decoded: dict[str, Any],
    content_file: Path,
    content_list_path: str,
    config_map_name: str,
    *,
    stderr_path: Path,
) -> dict[str, Any]:
    # Deep copy so we never mutate the `decoded` dict held by the caller.
    merged = json.loads(json.dumps(decoded))
    merged_content_rows = json.loads(content_file.read_text(encoding="utf-8"))
    advisory_data.set_decoded_content_array(merged, content_list_path, merged_content_rows)
    signing_key = _read_signing_key_from_config_map(
        config_map_name,
        stderr_path=stderr_path,
    )
    # Only fill signingKey when a row does not already have one (see advisory_data).
    advisory_data.append_signing_key_to_content(merged, content_list_path, signing_key)
    return merged


def _resolve_live_id_number(
    decoded: dict[str, Any],
    errata_mount: Path,
    *,
    stderr_path: Path,
    krb5_template: Path,
) -> int:
    # Caller may pre-assign live_id; otherwise reserve the next number from Errata Tool.
    live_id_param = decoded.get("live_id")
    if live_id_param is None:
        errata_api = authentication.read_mounted_text(errata_mount, "errata_api")
        return _reserve_errata_live_id(
            errata_api,
            errata_mount,
            stderr_path=stderr_path,
            krb5_template=krb5_template,
        )
    return int(live_id_param)


def _ensure_advisory_number_unused(
    repo_root: Path,
    year: str,
    advisory_number_segment: str,
    listing_path: Path,
    *,
    stderr_path: Path,
) -> None:
    # Another pipeline may have pushed the same year/number.
    # Sparse clone may not list every tenant path.
    pattern = rf"data/advisories/.*/{year}/{advisory_number_segment}/"
    if git.origin_main_has_path_matching(
        repo_root,
        pattern,
        listing_path,
        stderr_path=stderr_path,
    ):
        msg = f"An advisory with number {advisory_number_segment} already exists"
        raise ValueError(msg)


def _render_and_validate_advisory_yaml(
    *,
    repo_root: Path,
    new_advisory_dir: Path,
    merged: dict[str, Any],
    portal_advisory_id: str,
    ship_date: str,
    work_dir: Path,
    stderr_path: Path,
) -> str:
    """Apply the Jinja template, validate schema, return repo-relative YAML path."""
    rendered_json_path = new_advisory_dir / "advisory.json"
    new_advisory_yaml_path = new_advisory_dir / "advisory.yaml"

    # Template expects a wrapped shape and portal id like "2025:1602" (year + live id).
    wrapped_advisory = advisory_data.template_data_for_apply(merged)
    template_variables = advisory_data.template_context_merge(
        wrapped_advisory, portal_advisory_id, ship_date
    )
    apply_template.render_template_to_json_file(
        rendered_json_path,
        ADVISORY_TEMPLATE_PATH,
        template_variables,
        verbose=True,
    )

    # Repo stores advisory.yaml; render via JSON first so types match the template output.
    templated_dict = json.loads(rendered_json_path.read_text(encoding="utf-8"))
    new_advisory_yaml_path.write_text(
        advisory_data.json_dict_to_yaml_text(templated_dict),
        encoding="utf-8",
    )

    # Validate against schema/advisory.json (same as check-jsonschema --schemafile).
    schema = json.loads((repo_root / "schema" / "advisory.json").read_text(encoding="utf-8"))
    instance_doc = yaml.safe_load(new_advisory_yaml_path.read_text(encoding="utf-8"))
    validator_cls = validator_for(schema)
    validator_cls.check_schema(schema)
    validator = validator_cls(schema)
    try:
        validator.validate(instance_doc)
    except ValidationError as err:
        message = f"{new_advisory_yaml_path.name}::{err.json_path}: {err.message}\n"
        with open(stderr_path, "a", encoding="utf-8", errors="replace") as fh:
            fh.write(message)
        raise ValueError(f"schema validation failed for {new_advisory_yaml_path}") from err

    return new_advisory_yaml_path.relative_to(repo_root).as_posix()


def _commit_and_push_new_advisory(
    repo_root: Path,
    yaml_repo_path: str,
    component_group: str,
    *,
    stderr_path: Path,
) -> None:
    git.commit_and_push(
        repo_root,
        [yaml_repo_path],
        f"[Konflux Release] new advisory for {component_group}",
        gitlab.DEFAULT_BRANCH,
        retries=5,
        stderr_path=stderr_path,
    )


def _create_new_advisory(
    *,
    credentials: gitlab.GitLabCredentials,
    repo_root: Path,
    advisory_base: Path,
    merged: dict[str, Any],
    decoded: dict[str, Any],
    year: str,
    advisory_number_segment: str,
    portal_advisory_id: str,
    ship_date: str,
    url_prefix: str,
    work_dir: Path,
    stderr_path: Path,
    result_paths: dict[str, Path],
    params: dict[str, str],
) -> None:
    new_advisory_dir = advisory_base / year / advisory_number_segment
    new_advisory_dir.mkdir(parents=True, exist_ok=True)

    yaml_repo_path = _render_and_validate_advisory_yaml(
        repo_root=repo_root,
        new_advisory_dir=new_advisory_dir,
        merged=merged,
        portal_advisory_id=portal_advisory_id,
        ship_date=ship_date,
        work_dir=work_dir,
        stderr_path=stderr_path,
    )
    _commit_and_push_new_advisory(
        repo_root,
        yaml_repo_path,
        params["component_group"],
        stderr_path=stderr_path,
    )

    advisory_type = str(decoded.get("type", ""))
    _write_success_results(
        result_paths,
        customer_portal_url=_customer_portal_url(
            url_prefix, advisory_type, portal_advisory_id
        ),
        gitlab_raw_url=gitlab.raw_file_url(credentials.git_repo, yaml_repo_path),
    )


def run_create_advisory(
    *,
    advisory_secret: Path,
    errata_mount: Path,
    stderr_path: Path,
    result_paths: dict[str, Path],
    params: dict[str, str],
    decoded: dict[str, Any],
    krb5_template: Path = Path("/etc/krb5.conf"),
) -> None:
    """Run the full workflow. Raises on failure; `main` maps exceptions to result files."""
    credentials = gitlab.read_credentials_from_mount(advisory_secret)
    # Internal-request child results are written before work begins so partial runs
    # still expose pipeline/task run names to the parent.
    internal_request.write_result_paths(
        result_paths,
        pipeline_run_name=params["internal_request_pr_name"],
        task_run_name=params["task_run_name"],
    )
    gitlab.export_env_for_image_helpers(credentials)
    gitlab.configure_git_oauth2_auth(credentials.access_token)

    work_dir = Path(tempfile.mkdtemp(prefix="create-advisory-"))
    try:
        # Dotted path into decoded JSON / advisory YAML spec (e.g. `.content.images`).
        content_list_path = advisory_data.spec_content_json_pointer(params["content_type"])
        repo_root, advisory_base = _clone_advisory_repo(
            credentials,
            params["origin"],
            work_dir,
            stderr_path=stderr_path,
        )
        content_file = _write_initial_content_file(work_dir, decoded, content_list_path)

        ship_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        year = ship_date.split("-", 1)[0]
        url_prefix = advisory_data.advisory_url_prefix(credentials.git_repo)

        # Idempotency: skip create when every content row is already on an advisory.
        if _finish_if_all_content_already_published(
            repo_root=repo_root,
            advisory_base=advisory_base,
            content_file=content_file,
            content_list_path=content_list_path,
            content_type=params["content_type"],
            git_repo=credentials.git_repo,
            url_prefix=url_prefix,
            stderr_path=stderr_path,
            result_paths=result_paths,
        ):
            return

        merged = _build_merged_advisory_with_signing_key(
            decoded,
            content_file,
            content_list_path,
            params["config_map_name"],
            stderr_path=stderr_path,
        )
        # Reserve only after idempotency check — avoids consuming Errata ids on no-ops.
        live_num = _resolve_live_id_number(
            decoded,
            errata_mount,
            stderr_path=stderr_path,
            krb5_template=krb5_template,
        )
        # Directory name under data/advisories/<origin>/<year>/ (four-digit live id).
        advisory_number_segment = f"{live_num:04d}"
        origin_ls_tree_listing = work_dir / "origin_ls_tree.txt"
        _ensure_advisory_number_unused(
            repo_root,
            year,
            advisory_number_segment,
            origin_ls_tree_listing,
            stderr_path=stderr_path,
        )
        portal_advisory_id = f"{year}:{advisory_number_segment}"

        _create_new_advisory(
            credentials=credentials,
            repo_root=repo_root,
            advisory_base=advisory_base,
            merged=merged,
            decoded=decoded,
            year=year,
            advisory_number_segment=advisory_number_segment,
            portal_advisory_id=portal_advisory_id,
            ship_date=ship_date,
            url_prefix=url_prefix,
            work_dir=work_dir,
            stderr_path=stderr_path,
            result_paths=result_paths,
            params=params,
        )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    """CLI entry: decode advisory JSON, run the workflow, and write Tekton results.

    Always returns 0; logical success is ``Success`` in ``RESULT_RESULT``, and
    failures are recorded there via ``write_failure_result``.
    """
    (
        path_step_result,
        path_advisory_url,
        path_advisory_internal_url,
        path_internal_pr,
        path_internal_task_run,
    ) = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_ADVISORY_URL",
        "RESULT_ADVISORY_INTERNAL_URL",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME",
    )
    result_paths = {
        "result": path_step_result,
        "advisory_url": path_advisory_url,
        "advisory_internal_url": path_advisory_internal_url,
        "internal_pr_name": path_internal_pr,
        "internal_task_run_name": path_internal_task_run,
    }

    advisory_json = tekton.require_env("ADVISORY_JSON")
    params = {
        "component_group": tekton.require_env("PARAM_COMPONENT_GROUP"),
        "origin": tekton.require_env("PARAM_ORIGIN"),
        "config_map_name": tekton.require_env("PARAM_CONFIG_MAP_NAME"),
        "content_type": os.environ.get("PARAM_CONTENT_TYPE", "image").strip(),
        "internal_request_pr_name": tekton.require_env(
            "PARAM_INTERNAL_REQUEST_PIPELINE_RUN_NAME"
        ),
        "task_run_name": tekton.require_env("PARAM_TASK_RUN_NAME"),
    }

    program_basename = str(Path((argv or sys.argv)[0]).name)
    # Subprocess/git failures append here; on error the tail is copied into RESULT_RESULT.
    command_log_path = Path("/tmp/create_advisory_command_log.txt")
    command_log_path.write_text("", encoding="utf-8")

    # Placeholders so error handlers can always overwrite URL result files.
    path_advisory_url.write_text("", encoding="utf-8")
    path_advisory_internal_url.write_text("", encoding="utf-8")

    try:
        # ADVISORY_JSON may be gzip+base64 from the parent pipeline (see advisory_data).
        decoded = advisory_data.decode_advisory_param(advisory_json)
        run_create_advisory(
            advisory_secret=file.path_from_env_variable(
                "ADVISORY_SECRET_MOUNT", "/mnt/advisory_secret"
            ),
            errata_mount=file.path_from_env_variable(
                "ERRATA_SECRET_MOUNT", "/mnt/errata_secret"
            ),
            stderr_path=command_log_path,
            result_paths=result_paths,
            params=params,
            decoded=decoded,
        )
    except Exception as e:
        tekton.write_failure_result(
            path_step_result,
            program_basename,
            e,
            command_log_path=command_log_path,
            workflow_action="running the advisory workflow",
        )
    # Tekton step succeeds; operators read failure detail from RESULT_RESULT.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
