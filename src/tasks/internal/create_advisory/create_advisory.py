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

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gitlab import Gitlab
from gitlab.exceptions import GitlabError

from jsonschema.validators import validator_for
from jsonschema import ValidationError
import requests
from requests_kerberos import OPTIONAL, HTTPKerberosAuth
import yaml

from release_service_utils.helpers import advisory_data
import apply_template
from release_service_utils.helpers import authentication
from release_service_utils.helpers import file
from release_service_utils.helpers import http_client
from release_service_utils.helpers.internal_request import internal_request_results
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers import subprocess_cmd
from release_service_utils.helpers import tekton

from release_service_utils.helpers.vcs import git
from release_service_utils.helpers.vcs import gitlab

PROG = "create_advisory.py"
ADVISORY_TEMPLATE_PATH = Path("/home/templates/advisory.yaml.jinja")
# Stay below the create-advisory InternalRequest task timeout (01h00m00s).
_ADVISORY_MR_MERGE_TIMEOUT_SECONDS = 3300
_GITLAB_API_SCOPE_HINT = (
    "gitlab_access_token lacks GitLab API scope; grant api and read_api on the "
    "PAT stored in advisory_secret (read_repository/write_repository are not "
    "enough for merge request operations)"
)


def _reraise_gitlab_api_error(exc: GitlabError) -> None:
    """Raise a clear advisory error when the GitLab token lacks API scopes."""
    if gitlab.is_insufficient_scope_error(exc):
        raise tekton.CheckStepError(
            "accessing the GitLab API",
            RuntimeError(_GITLAB_API_SCOPE_HINT),
        ) from exc
    raise exc


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
    logger.info(
        "cloning advisory repository %s at %s (sparse: %s)",
        credentials.git_repo,
        gitlab.DEFAULT_BRANCH,
        ", ".join(sparse_dirs),
    )
    repo_root = gitlab.clone_project_sparse(
        credentials.git_repo,
        gitlab.DEFAULT_BRANCH,
        sparse_dirs,
        parent_dir=work_dir,
        stderr_path=stderr_path,
    )
    advisory_base = repo_root / "data" / "advisories" / origin
    logger.info("cloned advisory repository to %s", repo_root)
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
        session = http_client.get_retry_session(
            total=3,
            connect=3,
            read=3,
            status=2,
            backoff_factor=0.4,
            allowed_methods=frozenset({"POST"}),
        )
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
    logger.info("checking whether advisory content is already published")
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
            logger.info(
                "all advisory content already published in %s",
                latest_advisory_file,
            )
            return True

    logger.info("advisory content is not fully published yet; continuing")
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
    logger.info("building advisory payload and reading signing key from %s", config_map_name)
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
        logger.info("reserving a new Errata live_id")
        errata_api = authentication.read_mounted_text(errata_mount, "errata_api")
        return _reserve_errata_live_id(
            errata_api,
            errata_mount,
            stderr_path=stderr_path,
            krb5_template=krb5_template,
        )
    logger.info("using pre-assigned live_id %s", live_id_param)
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
    logger.info(
        "rendering advisory.yaml from %s for portal id %s into %s",
        ADVISORY_TEMPLATE_PATH,
        portal_advisory_id,
        new_advisory_yaml_path,
    )

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

    yaml_repo_path = new_advisory_yaml_path.relative_to(repo_root).as_posix()
    logger.info("validated advisory yaml at %s", yaml_repo_path)
    return yaml_repo_path


def _advisory_source_branch(origin: str, content_file: Path) -> str:
    """Return a stable branch name from *origin* and remaining advisory content."""
    digest = hashlib.sha256(
        content_file.read_text(encoding="utf-8").strip().encode("utf-8")
    ).hexdigest()[:12]
    safe_origin = re.sub(r"[^a-zA-Z0-9-]", "-", origin).strip("-")
    branch = f"konflux-advisory-{safe_origin}-{digest}"
    return branch[:255]


def _advisory_merge_request_title(
    component_group: str,
    internal_request_pr_name: str,
) -> str:
    """Build the MR title for a new advisory."""
    return (
        f"[Konflux Release] new advisory for {component_group} "
        f"({internal_request_pr_name})"
    )


def _advisory_merge_request_description(
    component_group: str,
    internal_request_pr_name: str,
    task_run_name: str,
) -> str:
    """Build the MR description for a new advisory."""
    return (
        f"Konflux advisory creation for {component_group}.\n\n"
        f"PipelineRun: {internal_request_pr_name}\n"
        f"TaskRun: {task_run_name}\n"
    )


def _advisory_yaml_path_from_merge_request(merge_request: Any) -> str | None:
    """Return the repo-relative advisory YAML path from MR changes, if any."""
    changes = merge_request.changes()
    for change in changes.get("changes", []):
        new_path = change.get("new_path") or ""
        if new_path.endswith("/advisory.yaml") and "data/advisories/" in new_path:
            return new_path
    return None


def _sync_main_and_finish_if_published(
    *,
    repo_root: Path,
    work_dir: Path,
    decoded: dict[str, Any],
    advisory_base: Path,
    content_list_path: str,
    content_type: str,
    git_repo: str,
    url_prefix: str,
    stderr_path: Path,
    result_paths: dict[str, Path],
) -> bool:
    """Refresh *main* and re-run idempotency after a concurrent merge."""
    git.sync_to_origin_main(repo_root, stderr_path=stderr_path)
    content_file = _write_initial_content_file(work_dir, decoded, content_list_path)
    return _finish_if_all_content_already_published(
        repo_root=repo_root,
        advisory_base=advisory_base,
        content_file=content_file,
        content_list_path=content_list_path,
        content_type=content_type,
        git_repo=git_repo,
        url_prefix=url_prefix,
        stderr_path=stderr_path,
        result_paths=result_paths,
    )


def _write_success_from_published_yaml(
    repo_root: Path,
    yaml_repo_path: str,
    git_repo: str,
    url_prefix: str,
    result_paths: dict[str, Path],
    *,
    stderr_path: Path,
) -> None:
    """Load a merged advisory YAML from *main* and write success results."""
    git.sync_to_origin_main(repo_root, stderr_path=stderr_path)
    published_doc = advisory_data.load_advisory_yaml(repo_root / yaml_repo_path)
    errata_type = advisory_data.get_advisory_spec_type(published_doc)
    errata_name = advisory_data.get_advisory_metadata_name(published_doc)
    _write_success_results(
        result_paths,
        customer_portal_url=_customer_portal_url(url_prefix, errata_type, errata_name),
        gitlab_raw_url=gitlab.raw_file_url(git_repo, yaml_repo_path),
    )


def _release_claimed_work(
    gitlab_client: Gitlab,
    credentials: gitlab.GitLabCredentials,
    source_branch: str | None,
    merge_request: Any | None = None,
) -> None:
    """Best-effort cleanup of a claimed branch and optional merge request."""
    if source_branch is None:
        return

    if merge_request is None:
        try:
            merge_request = gitlab.find_open_merge_request_by_source_branch(
                gitlab_client,
                credentials.git_repo,
                source_branch,
            )
        except Exception as exc:
            logger.warning(
                "failed to look up merge request for branch %s during cleanup: %s",
                source_branch,
                exc,
                exc_info=True,
            )

    if merge_request is not None:
        try:
            gitlab.cleanup_merge_request_branch(
                gitlab_client,
                credentials.git_repo,
                merge_request,
                source_branch,
            )
        except Exception as exc:
            logger.warning(
                "failed to clean up merge request for branch %s: %s",
                source_branch,
                exc,
                exc_info=True,
            )
        return

    try:
        gitlab.delete_remote_branch(
            gitlab_client,
            credentials.git_repo,
            source_branch,
        )
    except Exception as exc:
        logger.warning(
            "failed to delete claimed branch %s: %s",
            source_branch,
            exc,
            exc_info=True,
        )


def _is_claim_only_branch(
    repo_root: Path,
    source_branch: str,
    *,
    stderr_path: Path,
) -> bool:
    """Return True when *source_branch* still points at the same commit as main."""
    branch_sha = git.remote_branch_sha(
        repo_root,
        source_branch,
        stderr_path=stderr_path,
    )
    if branch_sha is None:
        return False
    main_sha = git.remote_branch_sha(
        repo_root,
        gitlab.DEFAULT_BRANCH,
        stderr_path=stderr_path,
    )
    if main_sha is None:
        return False
    return main_sha == branch_sha


def _recover_after_merge_request_wait_timeout(
    *,
    gitlab_client: Gitlab,
    credentials: gitlab.GitLabCredentials,
    repo_root: Path,
    work_dir: Path,
    decoded: dict[str, Any],
    advisory_base: Path,
    content_list_path: str,
    content_type: str,
    source_branch: str,
    component_group: str,
    internal_request_pr_name: str,
    task_run_name: str,
    url_prefix: str,
    stderr_path: Path,
    result_paths: dict[str, Path],
) -> bool:
    """Sync main, rerun idempotency, and join advisory work on *source_branch*.

    Used after an MR wait times out or when the branch could not be claimed.
    Claim-only branches are deleted; branches with advisory commits get an MR.
    """
    if git.remote_branch_exists(
        repo_root,
        source_branch,
        stderr_path=stderr_path,
    ) and _is_claim_only_branch(
        repo_root,
        source_branch,
        stderr_path=stderr_path,
    ):
        _release_claimed_work(
            gitlab_client,
            credentials,
            source_branch,
        )
    if _sync_main_and_finish_if_published(
        repo_root=repo_root,
        work_dir=work_dir,
        decoded=decoded,
        advisory_base=advisory_base,
        content_list_path=content_list_path,
        content_type=content_type,
        git_repo=credentials.git_repo,
        url_prefix=url_prefix,
        stderr_path=stderr_path,
        result_paths=result_paths,
    ):
        return True
    if not git.remote_branch_exists(
        repo_root,
        source_branch,
        stderr_path=stderr_path,
    ):
        return False
    if _is_claim_only_branch(
        repo_root,
        source_branch,
        stderr_path=stderr_path,
    ):
        return False
    merge_request = gitlab.get_or_create_merge_request(
        gitlab_client,
        credentials.git_repo,
        source_branch=source_branch,
        target_branch=gitlab.DEFAULT_BRANCH,
        title=_advisory_merge_request_title(
            component_group,
            internal_request_pr_name,
        ),
        description=_advisory_merge_request_description(
            component_group,
            internal_request_pr_name,
            task_run_name,
        ),
    )
    _finish_from_merged_merge_request(
        gitlab_client=gitlab_client,
        credentials=credentials,
        repo_root=repo_root,
        merge_request=merge_request,
        source_branch=source_branch,
        url_prefix=url_prefix,
        stderr_path=stderr_path,
        result_paths=result_paths,
    )
    return True


def _finish_from_merged_merge_request(
    *,
    gitlab_client: Gitlab,
    credentials: gitlab.GitLabCredentials,
    repo_root: Path,
    merge_request: Any,
    source_branch: str,
    url_prefix: str,
    stderr_path: Path,
    result_paths: dict[str, Path],
) -> str:
    """Merge *merge_request* and write success results from its advisory YAML."""
    mr_url = getattr(merge_request, "web_url", None) or str(
        getattr(merge_request, "iid", "?")
    )
    logger.info(
        "merging advisory merge request %s from branch %s",
        mr_url,
        source_branch,
    )
    gitlab.push_merge_request_to_main(
        gitlab_client,
        credentials.git_repo,
        merge_request,
        source_branch,
        timeout_seconds=_ADVISORY_MR_MERGE_TIMEOUT_SECONDS,
    )
    yaml_repo_path = _advisory_yaml_path_from_merge_request(merge_request)
    if yaml_repo_path is None:
        mr_url = getattr(merge_request, "web_url", None) or str(
            getattr(merge_request, "iid", "?")
        )
        msg = f"merge request {mr_url} does not contain an advisory.yaml path"
        err = RuntimeError(msg)
        raise tekton.CheckStepError("merging the advisory merge request", err) from err
    _write_success_from_published_yaml(
        repo_root,
        yaml_repo_path,
        credentials.git_repo,
        url_prefix,
        result_paths,
        stderr_path=stderr_path,
    )
    return yaml_repo_path


def _try_finish_via_existing_merge_request(
    *,
    gitlab_client: Gitlab,
    credentials: gitlab.GitLabCredentials,
    repo_root: Path,
    work_dir: Path,
    decoded: dict[str, Any],
    advisory_base: Path,
    content_list_path: str,
    content_type: str,
    source_branch: str,
    component_group: str,
    internal_request_pr_name: str,
    task_run_name: str,
    url_prefix: str,
    stderr_path: Path,
    result_paths: dict[str, Path],
) -> bool:
    """Wait for an in-flight MR when *source_branch* already exists.

    Return True when success results were written from the merged advisory.
    """
    logger.info(
        "checking for an in-flight merge request on branch %s",
        source_branch,
    )
    try:
        merge_request = gitlab.find_open_merge_request_by_source_branch(
            gitlab_client,
            credentials.git_repo,
            source_branch,
        )
    except GitlabError as exc:
        if gitlab.is_transient_gitlab_error(exc):
            logger.warning("transient GitLab error while looking up merge request: %s", exc)
            merge_request = None
        else:
            _reraise_gitlab_api_error(exc)
    branch_exists = git.remote_branch_exists(
        repo_root,
        source_branch,
        stderr_path=stderr_path,
    )
    if merge_request is None and not branch_exists:
        logger.info(
            "no open merge request or remote branch %s; syncing main for idempotency",
            source_branch,
        )
        return _sync_main_and_finish_if_published(
            repo_root=repo_root,
            work_dir=work_dir,
            decoded=decoded,
            advisory_base=advisory_base,
            content_list_path=content_list_path,
            content_type=content_type,
            git_repo=credentials.git_repo,
            url_prefix=url_prefix,
            stderr_path=stderr_path,
            result_paths=result_paths,
        )

    if merge_request is None:
        logger.info("waiting for an open merge request on branch %s", source_branch)
        try:
            merge_request = gitlab.wait_for_open_merge_request_by_source_branch(
                gitlab_client,
                credentials.git_repo,
                source_branch,
                timeout_seconds=_ADVISORY_MR_MERGE_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return _recover_after_merge_request_wait_timeout(
                gitlab_client=gitlab_client,
                credentials=credentials,
                repo_root=repo_root,
                work_dir=work_dir,
                decoded=decoded,
                advisory_base=advisory_base,
                content_list_path=content_list_path,
                content_type=content_type,
                source_branch=source_branch,
                component_group=component_group,
                internal_request_pr_name=internal_request_pr_name,
                task_run_name=task_run_name,
                url_prefix=url_prefix,
                stderr_path=stderr_path,
                result_paths=result_paths,
            )

    _finish_from_merged_merge_request(
        gitlab_client=gitlab_client,
        credentials=credentials,
        repo_root=repo_root,
        merge_request=merge_request,
        source_branch=source_branch,
        url_prefix=url_prefix,
        stderr_path=stderr_path,
        result_paths=result_paths,
    )
    return True


def _claim_source_branch(
    repo_root: Path,
    source_branch: str,
    *,
    stderr_path: Path,
) -> tuple[bool, str | None]:
    """Create *source_branch* on the remote when no other run owns it yet.

    Return ``(True, claim_commit_sha)`` when this run created the branch.
    """
    logger.info("claiming source branch %s", source_branch)
    logger.info("syncing local clone to origin/%s", gitlab.DEFAULT_BRANCH)
    git.sync_to_origin_main(repo_root, stderr_path=stderr_path)
    main_ref = f"origin/{gitlab.DEFAULT_BRANCH}"
    logger.info("checking out branch %s from %s", source_branch, main_ref)
    git.checkout(
        repo_root,
        source_branch,
        start_point=main_ref,
        reset=True,
        stderr_path=stderr_path,
    )
    claim_commit_sha = git.rev_parse(repo_root, "HEAD", stderr_path=stderr_path)
    logger.info("pushing new branch %s at %s", source_branch, claim_commit_sha)
    try:
        git.push_new_branch(repo_root, source_branch, stderr_path=stderr_path)
    except subprocess.CalledProcessError:
        if git.remote_branch_exists(
            repo_root,
            source_branch,
            stderr_path=stderr_path,
        ):
            logger.info("branch %s already exists on the remote", source_branch)
            return False, None
        raise
    logger.info("claimed branch %s at %s", source_branch, claim_commit_sha)
    return True, claim_commit_sha


def _commit_and_merge_new_advisory(
    gitlab_client: Gitlab,
    credentials: gitlab.GitLabCredentials,
    repo_root: Path,
    yaml_repo_path: str,
    source_branch: str,
    component_group: str,
    internal_request_pr_name: str,
    task_run_name: str,
    url_prefix: str,
    result_paths: dict[str, Path],
    *,
    claim_commit_sha: str | None = None,
    claim_holder: dict[str, Any] | None = None,
    stderr_path: Path,
) -> str:
    """Commit on *source_branch*, open an MR, and merge it to *main*.

    Return the repo-relative advisory YAML path from the merged MR.
    """
    commit_message = f"[Konflux Release] new advisory for {component_group}"
    logger.info(
        "committing %s on branch %s",
        yaml_repo_path,
        source_branch,
    )
    git.checkout(
        repo_root,
        source_branch,
        stderr_path=stderr_path,
    )
    git.index_add_commit(
        repo_root,
        [yaml_repo_path],
        commit_message,
        stderr_path=stderr_path,
    )
    logger.info("pushing branch %s", source_branch)
    try:
        git.push(repo_root, source_branch, stderr_path=stderr_path)
    except subprocess.CalledProcessError as push_error:
        if not git.remote_branch_exists(
            repo_root,
            source_branch,
            stderr_path=stderr_path,
        ):
            raise
        local_sha = git.rev_parse(repo_root, "HEAD", stderr_path=stderr_path)
        remote_sha = git.remote_branch_sha(
            repo_root,
            source_branch,
            stderr_path=stderr_path,
        )
        if claim_commit_sha is not None and remote_sha == claim_commit_sha:
            raise push_error
        if remote_sha != local_sha:
            merge_request = gitlab.wait_for_open_merge_request_by_source_branch(
                gitlab_client,
                credentials.git_repo,
                source_branch,
                timeout_seconds=_ADVISORY_MR_MERGE_TIMEOUT_SECONDS,
            )
            remote_yaml_path = _advisory_yaml_path_from_merge_request(merge_request)
            if remote_yaml_path is not None and remote_yaml_path != yaml_repo_path:
                return _finish_from_merged_merge_request(
                    gitlab_client=gitlab_client,
                    credentials=credentials,
                    repo_root=repo_root,
                    merge_request=merge_request,
                    source_branch=source_branch,
                    url_prefix=url_prefix,
                    stderr_path=stderr_path,
                    result_paths=result_paths,
                )
            if remote_yaml_path != yaml_repo_path:
                msg = (
                    f"branch {source_branch} exists but its merge request does not "
                    f"contain {yaml_repo_path}"
                )
                err = RuntimeError(msg)
                raise tekton.CheckStepError(
                    "joining the competing advisory merge request",
                    err,
                ) from err

    logger.info("creating or reusing merge request for branch %s", source_branch)
    merge_request = gitlab.get_or_create_merge_request(
        gitlab_client,
        credentials.git_repo,
        source_branch=source_branch,
        target_branch=gitlab.DEFAULT_BRANCH,
        title=_advisory_merge_request_title(
            component_group,
            internal_request_pr_name,
        ),
        description=_advisory_merge_request_description(
            component_group,
            internal_request_pr_name,
            task_run_name,
        ),
    )
    if claim_holder is not None:
        claim_holder["merge_request"] = merge_request
    return _finish_from_merged_merge_request(
        gitlab_client=gitlab_client,
        credentials=credentials,
        repo_root=repo_root,
        merge_request=merge_request,
        source_branch=source_branch,
        url_prefix=url_prefix,
        stderr_path=stderr_path,
        result_paths=result_paths,
    )


def _create_new_advisory(
    *,
    gitlab_client: Gitlab,
    credentials: gitlab.GitLabCredentials,
    repo_root: Path,
    advisory_base: Path,
    merged: dict[str, Any],
    decoded: dict[str, Any],
    year: str,
    advisory_number_segment: str,
    portal_advisory_id: str,
    ship_date: str,
    source_branch: str,
    url_prefix: str,
    work_dir: Path,
    stderr_path: Path,
    result_paths: dict[str, Path],
    params: dict[str, str],
    claim_commit_sha: str | None = None,
    claim_holder: dict[str, Any] | None = None,
) -> None:
    new_advisory_dir = advisory_base / year / advisory_number_segment
    logger.info(
        "creating new advisory %s in %s on branch %s",
        portal_advisory_id,
        new_advisory_dir,
        source_branch,
    )
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
    _commit_and_merge_new_advisory(
        gitlab_client,
        credentials,
        repo_root,
        yaml_repo_path,
        source_branch,
        params["component_group"],
        params["internal_request_pr_name"],
        params["task_run_name"],
        url_prefix,
        result_paths,
        claim_commit_sha=claim_commit_sha,
        claim_holder=claim_holder,
        stderr_path=stderr_path,
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
    logger.info(
        "create advisory workflow starting for origin=%s content_type=%s",
        params["origin"],
        params["content_type"],
    )
    credentials = gitlab.read_credentials_from_mount(advisory_secret)
    # Internal-request child results are written before work begins so partial runs
    # still expose pipeline/task run names to the parent.
    internal_request_results.write_result_paths(
        result_paths,
        pipeline_run_name=params["internal_request_pr_name"],
        task_run_name=params["task_run_name"],
    )
    gitlab.export_env_for_image_helpers(credentials)
    gitlab.configure_git_oauth2_auth(credentials.access_token)

    work_dir = Path(tempfile.mkdtemp(prefix="create-advisory-"))
    claim_holder: dict[str, Any] = {"branch": None, "merge_request": None}
    gitlab_client: Gitlab | None = None
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
        logger.info("wrote advisory content file %s", content_file)

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

        logger.info(
            "connecting to GitLab API at %s",
            gitlab.normalize_gitlab_url(credentials.gitlab_host),
        )
        gitlab_client = gitlab.client_from_credentials(credentials)
        source_branch = _advisory_source_branch(params["origin"], content_file)
        logger.info("using advisory source branch %s", source_branch)
        if _try_finish_via_existing_merge_request(
            gitlab_client=gitlab_client,
            credentials=credentials,
            repo_root=repo_root,
            work_dir=work_dir,
            decoded=decoded,
            advisory_base=advisory_base,
            content_list_path=content_list_path,
            content_type=params["content_type"],
            source_branch=source_branch,
            component_group=params["component_group"],
            internal_request_pr_name=params["internal_request_pr_name"],
            task_run_name=params["task_run_name"],
            url_prefix=url_prefix,
            stderr_path=stderr_path,
            result_paths=result_paths,
        ):
            logger.info("finished from an existing advisory merge request")
            return
        merged = _build_merged_advisory_with_signing_key(
            decoded,
            content_file,
            content_list_path,
            params["config_map_name"],
            stderr_path=stderr_path,
        )
        claimed, claim_commit_sha = _claim_source_branch(
            repo_root,
            source_branch,
            stderr_path=stderr_path,
        )
        if not claimed:
            if _recover_after_merge_request_wait_timeout(
                gitlab_client=gitlab_client,
                credentials=credentials,
                repo_root=repo_root,
                work_dir=work_dir,
                decoded=decoded,
                advisory_base=advisory_base,
                content_list_path=content_list_path,
                content_type=params["content_type"],
                source_branch=source_branch,
                component_group=params["component_group"],
                internal_request_pr_name=params["internal_request_pr_name"],
                task_run_name=params["task_run_name"],
                url_prefix=url_prefix,
                stderr_path=stderr_path,
                result_paths=result_paths,
            ):
                return
            msg = (
                f"could not claim branch {source_branch} and no merge request "
                "is in progress"
            )
            err = RuntimeError(msg)
            raise tekton.CheckStepError("claiming the advisory source branch", err) from err
        claim_holder["branch"] = source_branch
        if _sync_main_and_finish_if_published(
            repo_root=repo_root,
            work_dir=work_dir,
            decoded=decoded,
            advisory_base=advisory_base,
            content_list_path=content_list_path,
            content_type=params["content_type"],
            git_repo=credentials.git_repo,
            url_prefix=url_prefix,
            stderr_path=stderr_path,
            result_paths=result_paths,
        ):
            _release_claimed_work(
                gitlab_client,
                credentials,
                claim_holder["branch"],
            )
            claim_holder["branch"] = None
            return
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
        logger.info("reserved advisory number %s", portal_advisory_id)

        _create_new_advisory(
            gitlab_client=gitlab_client,
            credentials=credentials,
            repo_root=repo_root,
            advisory_base=advisory_base,
            merged=merged,
            decoded=decoded,
            year=year,
            advisory_number_segment=advisory_number_segment,
            portal_advisory_id=portal_advisory_id,
            ship_date=ship_date,
            source_branch=source_branch,
            url_prefix=url_prefix,
            work_dir=work_dir,
            stderr_path=stderr_path,
            result_paths=result_paths,
            params=params,
            claim_commit_sha=claim_commit_sha,
            claim_holder=claim_holder,
        )
        claim_holder["branch"] = None
        claim_holder["merge_request"] = None
        logger.info("create advisory workflow completed successfully")
    finally:
        if gitlab_client is not None and (
            claim_holder.get("branch") or claim_holder.get("merge_request")
        ):
            _release_claimed_work(
                gitlab_client,
                credentials,
                claim_holder.get("branch"),
                claim_holder.get("merge_request"),
            )
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

    logger.info("create advisory task starting")

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
        logger.error(
            "create advisory workflow failed: %s",
            e,
            exc_info=True,
        )
        if command_log_path.is_file():
            log_lines = command_log_path.read_text(
                encoding="utf-8",
                errors="replace",
            ).splitlines()
            if log_lines:
                logger.error(
                    "recent command output:\n%s",
                    "\n".join(log_lines[-20:]),
                )
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
