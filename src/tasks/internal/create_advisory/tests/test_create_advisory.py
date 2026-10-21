"""Tests for `create_advisory`."""

from __future__ import annotations

import time

import base64
import gzip
import json
import os
import subprocess
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import requests
import yaml
from gitlab.exceptions import GitlabError
from release_service_utils.helpers import advisory_data, internal_request, tekton
from release_service_utils.tasks.internal import create_advisory
from release_service_utils.helpers.vcs import gitlab
from release_service_utils.tasks.managed import request_advisory_creation
from release_service_utils.tasks.managed.populate_release_notes import populate_release_notes

TASK = "release_service_utils.tasks.internal.create_advisory"


def _gzip_b64(obj: dict) -> str:
    raw = json.dumps(obj).encode("utf-8")
    return base64.standard_b64encode(gzip.compress(raw)).decode("ascii")


def _configmap_signing_key_stdout(signing_key: str = "key1") -> str:
    return json.dumps({"data": {"SIG_KEY_NAMES": signing_key}})


def _write_errata_mount(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "name").write_text("svc/test", encoding="utf-8")
    (d / "base64_keytab").write_text(
        base64.b64encode(b"x").decode("ascii"),
        encoding="utf-8",
    )
    (d / "errata_api").write_text("https://errata/api/v1", encoding="utf-8")


def _write_gitlab_secret(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "gitlab_host").write_text("gitlab.example.com", encoding="utf-8")
    (d / "gitlab_access_token").write_text("tok", encoding="utf-8")
    (d / "git_author_name").write_text("Author", encoding="utf-8")
    (d / "git_author_email").write_text("a@example.com", encoding="utf-8")
    (d / "git_repo").write_text("https://gitlab.example.com/g/r.git", encoding="utf-8")


def _minimal_schema() -> dict:
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["spec"],
        "properties": {
            "spec": {
                "type": "object",
                "required": ["type"],
                "properties": {"type": {"type": "string", "enum": ["RHSA"]}},
            }
        },
    }


def _valid_advisory_yaml_dict() -> dict:
    return {
        "spec": {"type": "RHSA", "content": {"images": []}},
        "metadata": {"name": "2025:0001"},
    }


def _owned_train_timeout(*_args: Any, **kwargs: Any) -> None:
    kwargs["train_attempt"].queued_by_this_run = True
    raise TimeoutError("merge wait expired")


@pytest.fixture
def creds(tmp_path: Path) -> gitlab.GitLabCredentials:
    """GitLab credentials loaded from a temporary secret mount."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    return gitlab.read_credentials_from_mount(secret)


def test_reserve_errata_live_id_posts_json(tmp_path: Path) -> None:
    """Return `live_id` from a successful Errata API reserve POST."""
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n    default_realm = FOO\n", encoding="utf-8")

    class _Resp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"live_id": 42}

    with mock.patch(
        f"{TASK}.create_advisory.http_client.get_retry_session"
    ) as session_factory:
        sess = mock.MagicMock()
        session_factory.return_value = sess
        sess.post.return_value = _Resp()
        out = create_advisory.create_advisory._reserve_errata_live_id(
            "https://errata/api/v1",
            mount,
            stderr_path=None,
            krb5_template=krb5,
            kinit_fn=lambda *_a, **_k: None,
        )
    assert out == 42


def test_reserve_errata_live_id_krb5_read_error(tmp_path: Path) -> None:
    """Wrap Kerberos setup failures in `CheckStepError`."""
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    missing = tmp_path / "missing.conf"
    with pytest.raises(tekton.CheckStepError, match="Kerberos"):
        create_advisory.create_advisory._reserve_errata_live_id(
            "https://errata/api/v1",
            mount,
            stderr_path=None,
            krb5_template=missing,
            kinit_fn=lambda *_a, **_k: None,
        )


def test_reserve_errata_live_id_request_failure_logs_stderr(tmp_path: Path) -> None:
    """Append reserve POST failures to the optional stderr log."""
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n", encoding="utf-8")
    log = tmp_path / "log.txt"

    with mock.patch(
        f"{TASK}.create_advisory.http_client.get_retry_session"
    ) as session_factory:
        sess = mock.MagicMock()
        session_factory.return_value = sess
        sess.post.side_effect = requests.RequestException("net")
        with pytest.raises(requests.RequestException):
            create_advisory.create_advisory._reserve_errata_live_id(
                "https://errata/api/v1",
                mount,
                stderr_path=log,
                krb5_template=krb5,
                kinit_fn=lambda *_a, **_k: None,
            )
    assert "reserve_live_id" in log.read_text(encoding="utf-8")


def test_reserve_errata_live_id_missing_live_id(tmp_path: Path) -> None:
    """Fail when the Errata API response omits `live_id`."""
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n", encoding="utf-8")

    class _Resp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {}

    with mock.patch(
        f"{TASK}.create_advisory.http_client.get_retry_session"
    ) as session_factory:
        sess = mock.MagicMock()
        session_factory.return_value = sess
        sess.post.return_value = _Resp()
        with pytest.raises(ValueError, match="no live_id"):
            create_advisory.create_advisory._reserve_errata_live_id(
                "https://errata/api/v1",
                mount,
                stderr_path=None,
                krb5_template=krb5,
                kinit_fn=lambda *_a, **_k: None,
            )


def test_clone_advisory_repo(tmp_path: Path, creds: gitlab.GitLabCredentials) -> None:
    """Sparse-clone the advisory repo and return its root and tenant base path."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "data" / "advisories" / "tenant").mkdir(parents=True)
    with mock.patch(
        f"{TASK}.create_advisory.gitlab.clone_project_sparse",
        return_value=repo,
    ):
        out_repo_root, base = create_advisory.create_advisory._clone_advisory_repo(
            creds, "tenant", tmp_path, stderr_path=tmp_path / "e.log"
        )
    assert out_repo_root == repo
    assert base == repo / "data" / "advisories" / "tenant"


def test_write_initial_content_file(tmp_path: Path) -> None:
    """Write the content list slice from decoded advisory JSON to a temp file."""
    decoded = {"content": {"images": [{"a": 1}]}}
    path = create_advisory.create_advisory._write_initial_content_file(
        tmp_path, decoded, ".content.images"
    )
    assert json.loads(path.read_text(encoding="utf-8")) == [{"a": 1}]


def test_customer_portal_url() -> None:
    """Build the customer portal errata URL from type and advisory id."""
    assert (
        create_advisory.create_advisory._customer_portal_url(
            "https://access.redhat.com/errata", "RHSA", "2025:1"
        )
        == "https://access.redhat.com/errata/RHSA-2025:1"
    )


def test_write_success_results(tmp_path: Path) -> None:
    """Write Success and portal URLs to Tekton result files."""
    paths = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    create_advisory.create_advisory._write_success_results(
        paths,
        customer_portal_url="https://portal/x",
        gitlab_raw_url="https://gitlab/raw",
    )
    assert paths["result"].read_text(encoding="utf-8") == "Success"
    assert paths["advisory_url"].read_text(encoding="utf-8") == "https://portal/x"


def test_finish_if_all_content_already_published_true(tmp_path: Path) -> None:
    """Return True and write results when all content is already published."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t" / "2025" / "0001"
    base.mkdir(parents=True)
    (base / "advisory.yaml").write_text(
        "metadata:\n  name: '2025:0001'\nspec:\n  type: RHSA\n  content:\n"
        "    images:\n      - containerImage: q.io/i\n        tags: ['t']\n"
        "        repository: r\n",
        encoding="utf-8",
    )
    work = tmp_path / "work"
    work.mkdir()
    content = work / "content.json"
    content.write_text(
        json.dumps([{"containerImage": "q.io/i", "tags": ["t"], "repository": "r"}]),
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    assert create_advisory.create_advisory._finish_if_all_content_already_published(
        repo_root=repo,
        advisory_base=repo / "data" / "advisories" / "t",
        content_file=content,
        content_list_path=".content.images",
        content_type="image",
        git_repo="https://gitlab.example.com/g/r.git",
        url_prefix="https://access.redhat.com/errata",
        stderr_path=tmp_path / "e.log",
        result_paths=results,
    )
    assert results["result"].read_text(encoding="utf-8") == "Success"


def test_finish_if_all_content_already_published_false(tmp_path: Path) -> None:
    """Return False when no existing advisory covers the requested content."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t"
    base.mkdir(parents=True)
    work = tmp_path / "work"
    work.mkdir()
    content = work / "content.json"
    content.write_text(json.dumps([{"containerImage": "new"}]), encoding="utf-8")
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    assert not create_advisory.create_advisory._finish_if_all_content_already_published(
        repo_root=repo,
        advisory_base=base,
        content_file=content,
        content_list_path=".content.images",
        content_type="image",
        git_repo="https://gitlab.example.com/g/r.git",
        url_prefix="https://access.redhat.com/errata",
        stderr_path=tmp_path / "e.log",
        result_paths=results,
    )


def test_finish_raises_when_no_latest_path(tmp_path: Path) -> None:
    """Fail when idempotency cannot resolve a latest advisory path."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t" / "2025" / "0001"
    base.mkdir(parents=True)
    (base / "advisory.yaml").write_text(
        "metadata:\n  name: n\nspec:\n  type: RHSA\n  content:\n    images: []\n",
        encoding="utf-8",
    )
    work = tmp_path / "work"
    work.mkdir()
    content = work / "content.json"
    content.write_text("[]", encoding="utf-8")
    with pytest.raises(RuntimeError, match="latest advisory path"):
        create_advisory.create_advisory._finish_if_all_content_already_published(
            repo_root=repo,
            advisory_base=repo / "data" / "advisories" / "t",
            content_file=content,
            content_list_path=".content.images",
            content_type="image",
            git_repo="https://gitlab.example.com/g/r.git",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
        )


def test_build_merged_advisory_with_signing_key(tmp_path: Path) -> None:
    """Merge content and attach the signing key from the config map."""
    content = tmp_path / "c.json"
    content.write_text(json.dumps([{"a": 1}]), encoding="utf-8")
    decoded = {"type": "RHSA", "content": {"images": []}}
    with mock.patch(f"{TASK}.create_advisory.subprocess_cmd.run_cmd") as run:
        run.return_value = mock.MagicMock(
            stdout=json.dumps({"data": {"SIG_KEY_NAMES": "key-name"}}),
        )
        merged = create_advisory.create_advisory._build_merged_advisory_with_signing_key(
            decoded, content, ".content.images", "cm", stderr_path=tmp_path / "e.log"
        )
    assert merged["content"]["images"][0]["signingKey"] == "key-name"


def test_read_signing_key_falls_back_to_sig_key_name(tmp_path: Path) -> None:
    """Use SIG_KEY_NAME when SIG_KEY_NAMES is absent."""
    with mock.patch(f"{TASK}.create_advisory.subprocess_cmd.run_cmd") as run:
        run.return_value = mock.MagicMock(
            stdout=json.dumps({"data": {"SIG_KEY_NAME": "legacy-key"}}),
        )
        key = create_advisory.create_advisory._read_signing_key_from_config_map(
            "cm", stderr_path=tmp_path / "e.log"
        )
    assert key == "legacy-key"


def test_read_signing_key_prefers_sig_key_names(tmp_path: Path) -> None:
    """Prefer SIG_KEY_NAMES when both configmap keys are set."""
    with mock.patch(f"{TASK}.create_advisory.subprocess_cmd.run_cmd") as run:
        run.return_value = mock.MagicMock(
            stdout=json.dumps(
                {
                    "data": {
                        "SIG_KEY_NAMES": "names-key",
                        "SIG_KEY_NAME": "name-key",
                    }
                }
            ),
        )
        key = create_advisory.create_advisory._read_signing_key_from_config_map(
            "cm", stderr_path=tmp_path / "e.log"
        )
    assert key == "names-key"


def test_build_merged_advisory_with_signing_key_empty_fails(tmp_path: Path) -> None:
    """Fail when the configmap has neither signing key field."""
    content = tmp_path / "c.json"
    content.write_text(json.dumps([{"a": 1}]), encoding="utf-8")
    decoded = {"type": "RHSA", "content": {"images": []}}
    with mock.patch(f"{TASK}.create_advisory.subprocess_cmd.run_cmd") as run:
        run.return_value = mock.MagicMock(stdout=json.dumps({"data": {}}))
        with pytest.raises(ValueError, match="SIG_KEY_NAMES nor SIG_KEY_NAME"):
            create_advisory.create_advisory._build_merged_advisory_with_signing_key(
                decoded, content, ".content.images", "cm", stderr_path=tmp_path / "e.log"
            )


def test_resolve_live_id_from_decoded() -> None:
    """Use `live_id` from decoded advisory JSON when present."""
    assert (
        create_advisory.create_advisory._resolve_live_id_number(
            {"live_id": 7},
            Path("/unused"),
            stderr_path=Path("/dev/null"),
            krb5_template=Path("/etc/krb5.conf"),
        )
        == 7
    )


def test_resolve_live_id_reserves(tmp_path: Path) -> None:
    """Reserve a new live id via Errata when decoded JSON has none."""
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    with mock.patch(
        f"{TASK}.create_advisory._reserve_errata_live_id",
        return_value=99,
    ) as reserve:
        out = create_advisory.create_advisory._resolve_live_id_number(
            {},
            mount,
            stderr_path=tmp_path / "e.log",
            krb5_template=tmp_path / "k5.conf",
        )
    assert out == 99
    reserve.assert_called_once()


def test_ensure_advisory_number_unused_raises(tmp_path: Path) -> None:
    """Fail when the advisory number already exists on `origin/main`."""
    listing = tmp_path / "origin_ls_tree.txt"
    with mock.patch(
        f"{TASK}.create_advisory.git.origin_main_has_path_matching",
        return_value=True,
    ):
        with pytest.raises(ValueError, match="already exists"):
            create_advisory.create_advisory._ensure_advisory_number_unused(
                tmp_path,
                "2025",
                "0042",
                listing,
                stderr_path=tmp_path / "e.log",
            )


def test_render_and_validate_advisory_yaml(tmp_path: Path) -> None:
    """Render advisory YAML from the template and pass schema validation."""
    repo = tmp_path / "repo"
    schema_dir = repo / "schema"
    schema_dir.mkdir(parents=True)
    schema_path = schema_dir / "advisory.json"
    schema_doc = _minimal_schema()
    schema_doc["$id"] = schema_path.resolve().as_uri()
    schema_path.write_text(json.dumps(schema_doc), encoding="utf-8")
    new_dir = repo / "data" / "2025" / "0042"
    new_dir.mkdir(parents=True)

    def _fake_render(out_path: Path, _tpl: Path, _vars: dict, **_) -> None:
        out_path.write_text(json.dumps(_valid_advisory_yaml_dict()), encoding="utf-8")

    with mock.patch(
        f"{TASK}.create_advisory.render_template_to_json_file",
        side_effect=_fake_render,
    ):
        rel = create_advisory.create_advisory._render_and_validate_advisory_yaml(
            repo_root=repo,
            new_advisory_dir=new_dir,
            merged={"type": "RHSA", "content": {"images": []}},
            portal_advisory_id="2025:0042",
            ship_date="2025-01-01T00:00:00Z",
            work_dir=tmp_path,
            stderr_path=tmp_path / "e.log",
        )
    assert rel == "data/2025/0042/advisory.yaml"
    assert (new_dir / "advisory.yaml").is_file()


def test_render_and_validate_schema_failure(tmp_path: Path) -> None:
    """Fail schema validation and log the invalid advisory YAML."""
    repo = tmp_path / "repo"
    (repo / "schema").mkdir(parents=True)
    (repo / "schema" / "advisory.json").write_text(
        json.dumps(_minimal_schema()), encoding="utf-8"
    )
    new_dir = repo / "data" / "2025" / "0042"
    new_dir.mkdir(parents=True)
    log = tmp_path / "e.log"

    def _bad_render(out_path: Path, _tpl: Path, _vars: dict, **_) -> None:
        out_path.write_text(json.dumps({"spec": {"type": "WRONG"}}), encoding="utf-8")

    with mock.patch(
        f"{TASK}.create_advisory.render_template_to_json_file",
        side_effect=_bad_render,
    ):
        with pytest.raises(ValueError, match="schema validation failed"):
            create_advisory.create_advisory._render_and_validate_advisory_yaml(
                repo_root=repo,
                new_advisory_dir=new_dir,
                merged={"type": "RHSA", "content": {"images": []}},
                portal_advisory_id="2025:0042",
                ship_date="2025-01-01T00:00:00Z",
                work_dir=tmp_path,
                stderr_path=log,
            )
    assert "advisory.yaml" in log.read_text(encoding="utf-8")


def test_advisory_source_branch_is_stable(tmp_path: Path) -> None:
    """Branch names are derived from origin and canonical remaining content."""
    content = tmp_path / "content.json"
    content.write_text('[{"a":1}]\n', encoding="utf-8")
    branch = create_advisory.create_advisory._advisory_source_branch("tenant-a", content)
    assert branch.startswith("konflux-advisory-tenant-a-")
    assert len(branch.split("-")[-1]) == 12


def test_advisory_yaml_path_from_merge_request() -> None:
    """Extract the advisory YAML path from MR file changes."""
    merge_request = mock.Mock()
    merge_request.changes.return_value = {
        "changes": [
            {"new_path": "data/advisories/t/2025/0001/advisory.yaml"},
        ]
    }
    path = create_advisory.create_advisory._advisory_yaml_path_from_merge_request(
        merge_request
    )
    assert path == "data/advisories/t/2025/0001/advisory.yaml"


def test_commit_and_merge_new_advisory(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Commit on a feature branch, open an MR, and merge it to main."""
    client = mock.Mock()
    claim_holder: dict[str, Any] = {"branch": "konflux-advisory-t-deadbeef0001"}
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.git.checkout") as checkout,
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit") as index_commit,
        mock.patch(f"{TASK}.create_advisory.git.push") as push,
        mock.patch(f"{TASK}.create_advisory.gitlab.get_or_create_merge_request") as get_mr,
        mock.patch(
            f"{TASK}.create_advisory._finish_from_merged_merge_request",
            return_value="data/2025/0001/advisory.yaml",
        ) as finish,
    ):
        merge_request = mock.Mock()
        get_mr.return_value = merge_request
        out = create_advisory.create_advisory._commit_and_merge_new_advisory(
            client,
            creds,
            tmp_path,
            "data/2025/0001/advisory.yaml",
            "konflux-advisory-t-deadbeef0001",
            "grp",
            "parent-pr",
            "task-run-1",
            "https://access.redhat.com/errata",
            results,
            advisory_type="RHSA",
            advisory_name="2025:0001",
            claim_holder=claim_holder,
            stderr_path=tmp_path / "e.log",
            deadline=time.monotonic() + 100,
        )
    checkout.assert_called_once()
    index_commit.assert_called_once_with(
        tmp_path,
        ["data/2025/0001/advisory.yaml"],
        "[Konflux Release] new advisory for grp",
        stderr_path=tmp_path / "e.log",
    )
    push.assert_called_once()
    get_mr.assert_called_once()
    finish.assert_called_once()
    assert claim_holder["branch"] is None
    assert finish.call_args.kwargs["yaml_repo_path"] == "data/2025/0001/advisory.yaml"
    assert finish.call_args.kwargs["advisory_type"] == "RHSA"
    assert finish.call_args.kwargs["advisory_name"] == "2025:0001"
    assert out == "data/2025/0001/advisory.yaml"


def test_create_new_advisory(tmp_path: Path, creds: gitlab.GitLabCredentials) -> None:
    """Render, commit, and write success results for a new advisory."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t"
    base.mkdir(parents=True)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with mock.patch(
        f"{TASK}.create_advisory._render_and_validate_advisory_yaml",
        return_value="data/2025/0042/advisory.yaml",
    ):
        with mock.patch(
            f"{TASK}.create_advisory._commit_and_merge_new_advisory"
        ) as commit_merge:
            create_advisory.create_advisory._create_new_advisory(
                gitlab_client=mock.Mock(),
                credentials=creds,
                repo_root=repo,
                advisory_base=base,
                merged={"type": "RHSA", "content": {"images": []}},
                decoded={"type": "RHSA"},
                year="2025",
                advisory_number_segment="0042",
                portal_advisory_id="2025:0042",
                ship_date="2025-01-01T00:00:00Z",
                source_branch="konflux-advisory-t-abc",
                url_prefix="https://access.redhat.com/errata",
                work_dir=tmp_path,
                stderr_path=tmp_path / "e.log",
                result_paths=results,
                params={
                    "component_group": "g",
                    "internal_request_pr_name": "pr",
                    "task_run_name": "tr",
                },
                deadline=time.monotonic() + 100,
            )
    assert commit_merge.call_args.kwargs["advisory_type"] == "RHSA"
    assert commit_merge.call_args.kwargs["advisory_name"] == "2025:0042"


def test_create_new_advisory_stage_portal_url(
    tmp_path: Path,
) -> None:
    """Preserve the staging portal URL prefix through advisory creation."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    secret.joinpath("git_repo").write_text(
        "https://gitlab.com/rhtap-release/repo.git",
        encoding="utf-8",
    )
    creds = gitlab.read_credentials_from_mount(secret)
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t"
    base.mkdir(parents=True)
    yaml_repo_path = "data/2025/0042/advisory.yaml"
    published = repo / yaml_repo_path
    published.parent.mkdir(parents=True)
    # The merge-finishing path requires the advisory file to exist after syncing main.
    published.write_text(
        "metadata:\n  name: '2025:0042'\nspec:\n  type: RHSA\n",
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory._render_and_validate_advisory_yaml",
            return_value=yaml_repo_path,
        ),
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit"),
        mock.patch(f"{TASK}.create_advisory.git.push"),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.get_or_create_merge_request",
            return_value=mock.Mock(state="merged", merge_commit_sha="abc123"),
        ),
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
    ):
        create_advisory.create_advisory._create_new_advisory(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=repo,
            advisory_base=base,
            merged={"type": "RHSA", "content": {"images": []}},
            decoded={"type": "RHSA"},
            year="2025",
            advisory_number_segment="0042",
            portal_advisory_id="2025:0042",
            ship_date="2025-01-01T00:00:00Z",
            source_branch="konflux-advisory-t-abc",
            url_prefix="https://access.stage.redhat.com/errata",
            work_dir=tmp_path,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            deadline=time.monotonic() + 100,
        )
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.stage.redhat.com/errata/RHSA-2025:0042"
    )


def test_create_new_advisory_custom_live_id_portal_url(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Preserve a custom advisory ID in the portal URL through advisory creation."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t"
    base.mkdir(parents=True)
    yaml_repo_path = "data/2025/0999/advisory.yaml"
    published = repo / yaml_repo_path
    published.parent.mkdir(parents=True)
    # The merge-finishing path requires the advisory file to exist after syncing main.
    published.write_text(
        "metadata:\n  name: '2025:0999'\nspec:\n  type: RHSA\n",
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory._render_and_validate_advisory_yaml",
            return_value=yaml_repo_path,
        ),
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit"),
        mock.patch(f"{TASK}.create_advisory.git.push"),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.get_or_create_merge_request",
            return_value=mock.Mock(state="merged", merge_commit_sha="abc123"),
        ),
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
    ):
        create_advisory.create_advisory._create_new_advisory(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=repo,
            advisory_base=base,
            merged={"type": "RHSA", "content": {"images": []}},
            decoded={"type": "RHSA", "live_id": 999},
            year="2025",
            advisory_number_segment="0999",
            portal_advisory_id="2025:0999",
            ship_date="2025-01-01T00:00:00Z",
            source_branch="konflux-advisory-t-abc",
            url_prefix="https://access.redhat.com/errata",
            work_dir=tmp_path,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            deadline=time.monotonic() + 100,
        )
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.redhat.com/errata/RHSA-2025:0999"
    )


def test_try_finish_via_existing_merge_request_false(tmp_path: Path) -> None:
    """Continue without re-walking when no MR or source branch exists."""
    repo = tmp_path / "repo"
    repo.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            return_value=None,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
        ) as sync_finish,
    ):
        finished = create_advisory.create_advisory._try_finish_via_existing_merge_request(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": [{"x": 1}]}},
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
            deadline=time.monotonic() + 100,
        )
    assert finished is False
    sync_finish.assert_not_called()


def test_run_create_advisory_existing_mr_early_return(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Stop after joining an in-flight MR before reserving a live id."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(
                tmp_path / "repo",
                tmp_path / "repo" / "data" / "advisories" / "t",
            ),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="mainsha",
        ),
        mock.patch(f"{TASK}.create_advisory.gitlab.client_from_credentials"),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=True,
        ) as finish_mr,
        mock.patch(f"{TASK}.create_advisory._resolve_live_id_number") as reserve,
        mock.patch(f"{TASK}.create_advisory._create_new_advisory") as create,
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded={"content": {"images": [{"x": 1}]}},
        )
    finish_mr.assert_called_once()
    reserve.assert_not_called()
    create.assert_not_called()


def test_run_create_advisory_idempotent_early_return(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Stop after idempotency when all content is already published."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    with mock.patch(
        f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
        return_value=creds,
    ):
        with mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(
                tmp_path / "repo",
                tmp_path / "repo" / "data" / "advisories" / "t",
            ),
        ):
            with (
                mock.patch(
                    f"{TASK}.create_advisory.git.rev_parse",
                    return_value="mainsha",
                ),
                mock.patch(
                    f"{TASK}.create_advisory._finish_if_all_content_already_published",
                    return_value=True,
                ),
            ):
                create_advisory.create_advisory.run_create_advisory(
                    advisory_secret=secret,
                    errata_mount=errata,
                    stderr_path=tmp_path / "e.log",
                    result_paths=results,
                    params={
                        "component_group": "g",
                        "origin": "t",
                        "config_map_name": "cm",
                        "content_type": "image",
                        "internal_request_pr_name": "pr",
                        "task_run_name": "tr",
                    },
                    decoded={"content": {"images": [{"x": 1}]}},
                )


def test_run_create_advisory_captures_main_sha_before_initial_walk(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Pass the pre-walk origin/main SHA to the post-claim idempotency check."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    call_order: list[str] = []

    def _rev_parse(*_args: object, **_kwargs: object) -> str:
        call_order.append("rev_parse")
        return "pre-walk-sha"

    def _finish_published(*_args: object, **_kwargs: object) -> bool:
        call_order.append("finish_published")
        return False

    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(
                tmp_path / "repo",
                tmp_path / "repo" / "data" / "advisories" / "t",
            ),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            side_effect=_rev_parse,
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            side_effect=_finish_published,
        ),
        mock.patch(f"{TASK}.create_advisory.gitlab.client_from_credentials"),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value={"type": "RHSA", "content": {"images": []}},
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(True, "claimsha"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=True,
        ) as sync_finish,
        mock.patch(f"{TASK}.create_advisory._release_claimed_work"),
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded={"content": {"images": [{"x": 1}]}},
        )
    assert call_order[:2] == ["rev_parse", "finish_published"]
    assert sync_finish.call_args.kwargs["skip_if_main_sha"] == "pre-walk-sha"


def test_run_create_advisory_full_create_path(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Run the full create path when idempotency does not apply."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    repo = tmp_path / "repo"
    (repo / "data" / "advisories" / "t").mkdir(parents=True)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(repo, repo / "data" / "advisories" / "t"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="mainsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value={"type": "RHSA", "content": {"images": []}},
        ),
        mock.patch(f"{TASK}.create_advisory.gitlab.client_from_credentials"),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(True, "claimsha"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._resolve_live_id_number",
            return_value=42,
        ),
        mock.patch(f"{TASK}.create_advisory._ensure_advisory_number_unused"),
        mock.patch(f"{TASK}.create_advisory._create_new_advisory") as create,
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded={"type": "RHSA", "content": {"images": []}},
        )
    create.assert_called_once()


def _setup_main_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    paths = {
        "RESULT_RESULT": tmp_path / "result.txt",
        "RESULT_ADVISORY_URL": tmp_path / "url.txt",
        "RESULT_ADVISORY_INTERNAL_URL": tmp_path / "internal.txt",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME": tmp_path / "pr.txt",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME": tmp_path / "tr.txt",
    }
    for key, path in paths.items():
        monkeypatch.setenv(key, str(path))
        path.write_text("", encoding="utf-8")
    monkeypatch.setenv("ADVISORY_JSON", _gzip_b64({"type": "RHSA", "content": {"images": []}}))
    monkeypatch.setenv("PARAM_COMPONENT_GROUP", "g")
    monkeypatch.setenv("PARAM_ORIGIN", "t")
    monkeypatch.setenv("PARAM_CONFIG_MAP_NAME", "cm")
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_PIPELINE_RUN_NAME", "parent-pr")
    monkeypatch.setenv("PARAM_TASK_RUN_NAME", "task-run-1")
    return paths


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit zero when the advisory workflow completes successfully."""
    _setup_main_env(tmp_path, monkeypatch)
    with mock.patch(f"{TASK}.create_advisory.run_create_advisory"):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0


def test_main_writes_failure_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write workflow errors to the Tekton result file and exit zero."""
    paths = _setup_main_env(tmp_path, monkeypatch)
    with (
        mock.patch(
            f"{TASK}.create_advisory.run_create_advisory",
            side_effect=ValueError("workflow broke"),
        ),
        mock.patch(f"{TASK}.create_advisory.logger.error") as log_error,
    ):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "workflow broke" in text
    log_error.assert_called()


def test_main_writes_failure_when_command_log_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Still write the workflow error when command-log inspection fails."""
    _setup_main_env(tmp_path, monkeypatch)
    with (
        mock.patch(
            f"{TASK}.create_advisory.run_create_advisory",
            side_effect=ValueError("workflow broke"),
        ),
        mock.patch(
            f"{TASK}.create_advisory.Path.is_file",
            side_effect=OSError("log vanished"),
        ),
        mock.patch(f"{TASK}.create_advisory.tekton.write_failure_result") as write_failure,
    ):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0
    write_failure.assert_called_once()
    assert write_failure.call_args.kwargs["command_log_path"] is None
    assert isinstance(write_failure.call_args.args[2], ValueError)


def test_main_check_step_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Include the `CheckStepError` action in the Tekton failure result."""
    paths = _setup_main_env(tmp_path, monkeypatch)
    err = tekton.CheckStepError("reading secrets", OSError("no mount"))
    with mock.patch(
        f"{TASK}.create_advisory.run_create_advisory",
        side_effect=err,
    ):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0
    assert "reading secrets" in paths["RESULT_RESULT"].read_text(encoding="utf-8")


def _catalog_schema() -> dict:
    """JSON schema matching the catalog Tekton mock (type and severity enums)."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["spec"],
        "properties": {
            "spec": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["RHEA", "RHBA", "RHSA"],
                    },
                    "severity": {
                        "type": "string",
                        "enum": ["Critical", "Important", "Moderate", "Low"],
                    },
                },
            }
        },
    }


def _write_image_advisory_yaml(
    path: Path,
    *,
    name: str,
    images: list[dict],
) -> None:
    """Write a minimal advisory YAML with *images* under `spec.content.images`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "metadata:\n"
        f"  name: '{name}'\n"
        "spec:\n"
        "  type: RHSA\n"
        "  content:\n"
        "    images:\n"
        + "".join(
            f"      - containerImage: {row['containerImage']}\n"
            f"        repository: {row['repository']}\n"
            f"        tags: {json.dumps(row['tags'])}\n"
            for row in images
        ),
        encoding="utf-8",
    )


def _idempotency_repo(tmp_path: Path, origin: str = "dev-tenant") -> tuple[Path, Path]:
    """Build a repo tree with catalog idempotency advisories (1442, 1601, 1602)."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / origin
    _write_image_advisory_yaml(
        base / "2024" / "1442" / "advisory.yaml",
        name="2024:1442",
        images=[
            {
                "containerImage": "quay.io/example/openstack@sha256:abde",
                "repository": "quay.io/example/openstack",
                "tags": ["v1.0", "latest"],
            }
        ],
    )
    _write_image_advisory_yaml(
        base / "2025" / "1601" / "advisory.yaml",
        name="2025:1601",
        images=[
            {
                "containerImage": "quay.io/example/release@sha256:alpha123",
                "repository": "example-stream/release",
                "tags": ["v1.0", "latest"],
            }
        ],
    )
    _write_image_advisory_yaml(
        base / "2025" / "1602" / "advisory.yaml",
        name="2025:1602",
        images=[
            {
                "containerImage": "quay.io/example/release@sha256:beta123",
                "repository": "example-stream/release",
                "tags": ["v2.0", "stable"],
            }
        ],
    )
    # Newest leaf mtime first (1602 > 1601 > 1442).
    os.utime(base / "2024" / "1442", (1704012342.0, 1704012342.0))
    os.utime(base / "2025" / "1601", (1712012344.0, 1712012344.0))
    os.utime(base / "2025" / "1602", (1712012345.0, 1712012345.0))
    return repo, base


def test_finish_idempotency_single_image_returns_existing_url(tmp_path: Path) -> None:
    """When the sole image is already published, return that advisory's portal URL."""
    repo, base = _idempotency_repo(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    content = work / "content.json"
    content.write_text(
        json.dumps(
            [
                {
                    "containerImage": "quay.io/example/openstack@sha256:abde",
                    "repository": "quay.io/example/openstack",
                    "tags": ["v1.0", "latest"],
                }
            ]
        ),
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    assert create_advisory.create_advisory._finish_if_all_content_already_published(
        repo_root=repo,
        advisory_base=base,
        content_file=content,
        content_list_path=".content.images",
        content_type="image",
        git_repo="https://gitlab.example.com/g/r.git",
        url_prefix="https://access.redhat.com/errata",
        stderr_path=tmp_path / "e.log",
        result_paths=results,
    )
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.redhat.com/errata/RHSA-2024:1442"
    )


def test_finish_all_images_returns_latest_advisory_url(tmp_path: Path) -> None:
    """When all images match existing advisories, pick the newest advisory URL."""
    repo, base = _idempotency_repo(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    content = work / "content.json"
    content.write_text(
        json.dumps(
            [
                {
                    "containerImage": "quay.io/example/release@sha256:alpha123",
                    "repository": "example-stream/release",
                    "tags": ["v1.0", "latest"],
                },
                {
                    "containerImage": "quay.io/example/release@sha256:beta123",
                    "repository": "example-stream/release",
                    "tags": ["v2.0", "stable"],
                },
            ]
        ),
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    assert create_advisory.create_advisory._finish_if_all_content_already_published(
        repo_root=repo,
        advisory_base=base,
        content_file=content,
        content_list_path=".content.images",
        content_type="image",
        git_repo="https://gitlab.example.com/g/r.git",
        url_prefix="https://access.redhat.com/errata",
        stderr_path=tmp_path / "e.log",
        result_paths=results,
    )
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.redhat.com/errata/RHSA-2025:1602"
    )


def test_run_create_advisory_partial_idempotency_creates_new(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Filter out published images and create a new advisory for the remainder."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    repo, base = _idempotency_repo(tmp_path)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    decoded = {
        "type": "RHSA",
        "content": {
            "images": [
                {
                    "containerImage": "quay.io/example/openstack@sha256:abde",
                    "repository": "quay.io/example/openstack",
                    "tags": ["v1.0", "latest"],
                },
                {
                    "containerImage": "quay.io/example/release@sha256:alpha123",
                    "repository": "example-stream/release",
                    "tags": ["v1.0", "latest"],
                },
                {
                    "containerImage": "quay.io/example/openstack@sha256:NEW",
                    "repository": "rhosp16-rhel8/openstack",
                    "tags": ["latest"],
                },
            ]
        },
    }
    with mock.patch(
        "create_advisory.create_advisory.gitlab.read_credentials_from_mount",
        return_value=creds,
    ):
        with mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(repo, base),
        ):
            with mock.patch(
                f"{TASK}.create_advisory.subprocess_cmd.run_cmd",
                return_value=mock.MagicMock(stdout=_configmap_signing_key_stdout()),
            ):
                with mock.patch(f"{TASK}.create_advisory.gitlab.client_from_credentials"):
                    with mock.patch(
                        f"{TASK}.create_advisory.git.rev_parse",
                        return_value="mainsha",
                    ):
                        with mock.patch(
                            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
                            return_value=False,
                        ):
                            with mock.patch(
                                f"{TASK}.create_advisory._claim_source_branch",
                                return_value=(True, "claimsha"),
                            ):
                                with mock.patch(
                                    (
                                        f"{TASK}.create_advisory."
                                        "_sync_main_and_finish_if_published"
                                    ),
                                    return_value=False,
                                ):
                                    with mock.patch(
                                        f"{TASK}.create_advisory._resolve_live_id_number",
                                        return_value=1234,
                                    ):
                                        with mock.patch(
                                            (
                                                f"{TASK}.create_advisory."
                                                "_ensure_advisory_number_unused"
                                            ),
                                        ):
                                            with mock.patch(
                                                (
                                                    f"{TASK}.create_advisory."
                                                    "_create_new_advisory"
                                                ),
                                            ) as create:
                                                mod = create_advisory.create_advisory
                                                mod.run_create_advisory(
                                                    advisory_secret=secret,
                                                    errata_mount=errata,
                                                    stderr_path=tmp_path / "e.log",
                                                    result_paths=results,
                                                    params={
                                                        "component_group": "g",
                                                        "origin": "dev-tenant",
                                                        "config_map_name": "cm",
                                                        "content_type": "image",
                                                        "internal_request_pr_name": "pr",
                                                        "task_run_name": "tr",
                                                    },
                                                    decoded=decoded,
                                                )
    merged_arg = create.call_args.kwargs["merged"]
    assert len(merged_arg["content"]["images"]) == 1
    assert "NEW" in merged_arg["content"]["images"][0]["containerImage"]
    create.assert_called_once()


def test_run_create_advisory_clone_failure(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """A sparse-clone failure propagates from `run_create_advisory`."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    with mock.patch(
        f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
        return_value=creds,
    ):
        with mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            side_effect=subprocess.CalledProcessError(1, "git clone"),
        ):
            with pytest.raises(subprocess.CalledProcessError):
                create_advisory.create_advisory.run_create_advisory(
                    advisory_secret=secret,
                    errata_mount=errata,
                    stderr_path=tmp_path / "e.log",
                    result_paths={
                        "result": tmp_path / "r",
                        "advisory_url": tmp_path / "u",
                        "advisory_internal_url": tmp_path / "i",
                        "internal_pr_name": tmp_path / "pr",
                        "internal_task_run_name": tmp_path / "tr",
                    },
                    params={
                        "component_group": "g",
                        "origin": "failing-tenant",
                        "config_map_name": "cm",
                        "content_type": "image",
                        "internal_request_pr_name": "pr",
                        "task_run_name": "tr",
                    },
                    decoded={"type": "RHSA", "content": {"images": []}},
                )


def test_render_schema_failure_wrong_type_and_severity(tmp_path: Path) -> None:
    """Invalid `type` / `severity` values fail schema validation with stderr detail."""
    repo = tmp_path / "repo"
    schema_path = repo / "schema" / "advisory.json"
    schema_path.parent.mkdir(parents=True)
    schema_doc = _catalog_schema()
    schema_doc["$id"] = schema_path.resolve().as_uri()
    schema_path.write_text(json.dumps(schema_doc), encoding="utf-8")
    new_dir = repo / "data" / "2025" / "0042"
    new_dir.mkdir(parents=True)
    log = tmp_path / "e.log"

    def _bad_render(out_path: Path, _tpl: Path, _vars: dict, **_) -> None:
        out_path.write_text(
            json.dumps({"spec": {"type": "wrongType", "severity": "wrongSeverity"}}),
            encoding="utf-8",
        )

    with mock.patch(
        f"{TASK}.create_advisory.render_template_to_json_file",
        side_effect=_bad_render,
    ):
        with pytest.raises(ValueError, match="schema validation failed"):
            create_advisory.create_advisory._render_and_validate_advisory_yaml(
                repo_root=repo,
                new_advisory_dir=new_dir,
                merged={"type": "RHSA", "content": {"images": []}},
                portal_advisory_id="2025:0042",
                ship_date="2025-01-01T00:00:00Z",
                work_dir=tmp_path,
                stderr_path=log,
            )
    log_text = log.read_text(encoding="utf-8")
    assert "wrongType" in log_text or "type" in log_text


def test_write_success_from_published_yaml_reads_synced_main(tmp_path: Path) -> None:
    """Load advisory metadata from the updated worktree after syncing main."""
    repo = tmp_path / "repo"
    yaml_path = repo / "data" / "advisories" / "t" / "2025" / "0042" / "advisory.yaml"
    yaml_path.parent.mkdir(parents=True)
    yaml_path.write_text(
        "metadata:\n  name: '2025:0042'\nspec:\n  type: RHSA\n",
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main") as sync_main:
        create_advisory.create_advisory._write_success_from_published_yaml(
            repo,
            "data/advisories/t/2025/0042/advisory.yaml",
            "https://gitlab.example.com/g/r.git",
            "https://access.stage.redhat.com/errata",
            results,
            stderr_path=tmp_path / "e.log",
        )
    sync_main.assert_called_once()
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.stage.redhat.com/errata/RHSA-2025:0042"
    )


@pytest.mark.parametrize("has_approved", [False, True])
def test_approve_advisory_merge_request(has_approved: bool) -> None:
    """Approve the refreshed SHA once within the remaining task budget."""
    merge_request = mock.Mock(state="opened", sha="old-sha")
    current_merge_request = merge_request.manager.get.return_value
    current_merge_request.state = "opened"
    current_merge_request.sha = "current-sha"
    current_merge_request.approvals.get.return_value.user_has_approved = has_approved

    with mock.patch(f"{TASK}.create_advisory.time.monotonic", return_value=50):
        create_advisory.create_advisory._approve_advisory_merge_request(
            merge_request, deadline=70
        )

    merge_request.manager.get.assert_called_once_with(
        merge_request.get_id(), timeout=20, max_retries=0
    )
    current_merge_request.approvals.get.assert_called_once_with(timeout=20, max_retries=0)
    if has_approved:
        current_merge_request.approve.assert_not_called()
    else:
        current_merge_request.approve.assert_called_once_with(
            sha="current-sha", timeout=20, max_retries=0
        )


@pytest.mark.parametrize("is_already_merged", [False, True])
def test_approve_advisory_merge_request_skips_merged(is_already_merged: bool) -> None:
    """Skip approval when the MR has already merged, including during a refresh."""
    merge_request = mock.Mock(state="merged" if is_already_merged else "opened")
    current_merge_request = merge_request.manager.get.return_value
    current_merge_request.state = "merged"

    create_advisory.create_advisory._approve_advisory_merge_request(
        merge_request, deadline=time.monotonic() + 100
    )

    current_merge_request.approvals.get.assert_not_called()
    current_merge_request.approve.assert_not_called()
    if is_already_merged:
        merge_request.manager.get.assert_not_called()


def test_approve_advisory_merge_request_rejects_missing_sha() -> None:
    """Never submit an approval without binding it to a commit."""
    merge_request = mock.Mock(state="opened")
    current_merge_request = merge_request.manager.get.return_value
    current_merge_request.state = "opened"
    current_merge_request.sha = None
    current_merge_request.approvals.get.return_value.user_has_approved = False

    with pytest.raises(tekton.CheckStepError, match="approving the advisory") as caught:
        create_advisory.create_advisory._approve_advisory_merge_request(
            merge_request, deadline=time.monotonic() + 100
        )
    assert isinstance(caught.value.__cause__, ValueError)
    assert "no HEAD SHA" in str(caught.value.__cause__)
    current_merge_request.approve.assert_not_called()


@pytest.mark.parametrize("response_code", [401, 403, 422])
def test_approve_advisory_merge_request_reports_failure(response_code: int) -> None:
    """Surface permanent approval failures without retrying."""
    merge_request = mock.Mock(state="opened")
    current_merge_request = merge_request.manager.get.return_value
    current_merge_request.state = "opened"
    current_merge_request.sha = "current-sha"
    current_merge_request.approvals.get.return_value.user_has_approved = False
    error = GitlabError("approval rejected", response_code=response_code)
    current_merge_request.approve.side_effect = error

    with pytest.raises(tekton.CheckStepError, match="approving the advisory") as caught:
        create_advisory.create_advisory._approve_advisory_merge_request(
            merge_request, deadline=time.monotonic() + 100
        )
    assert caught.value.__cause__ is error
    current_merge_request.approve.assert_called_once()


@pytest.mark.parametrize("failure_stage", ["refresh", "lookup", "submission"])
@pytest.mark.parametrize(
    "error",
    [
        GitlabError("unavailable", response_code=503),
        GitlabError("rate limited", response_code=429),
        requests.Timeout("response lost"),
    ],
)
def test_approve_advisory_merge_request_retries(failure_stage: str, error: Exception) -> None:
    """Retry lookups and submissions by refreshing the MR within the same budget."""
    merge_request = mock.Mock(state="opened")
    current_merge_request = mock.Mock(state="opened", sha="current-sha")
    current_merge_request.approvals.get.return_value.user_has_approved = False
    merge_request.manager.get.return_value = current_merge_request
    if failure_stage == "refresh":
        merge_request.manager.get.side_effect = [error, current_merge_request]
    elif failure_stage == "lookup":
        current_merge_request.approvals.get.side_effect = [
            error,
            mock.Mock(user_has_approved=False),
        ]
    else:
        current_merge_request.approve.side_effect = [error, None]

    with mock.patch(f"{TASK}.create_advisory.time.sleep") as sleep:
        create_advisory.create_advisory._approve_advisory_merge_request(
            merge_request, deadline=time.monotonic() + 100
        )
    assert merge_request.manager.get.call_count == 2
    sleep.assert_called_once_with(10)
    assert current_merge_request.approve.call_args.kwargs["sha"] == "current-sha"


def test_approve_advisory_merge_request_refreshes_changed_sha() -> None:
    """Approve the new revision after GitLab rejects an outdated SHA."""
    merge_request = mock.Mock(state="opened")
    original_request = mock.Mock(state="opened", sha="old-sha")
    updated_request = mock.Mock(state="opened", sha="new-sha")
    for request in (original_request, updated_request):
        request.approvals.get.return_value.user_has_approved = False
    original_request.approve.side_effect = GitlabError("SHA changed", response_code=409)
    merge_request.manager.get.side_effect = [original_request, updated_request]

    with mock.patch(f"{TASK}.create_advisory.time.sleep"):
        create_advisory.create_advisory._approve_advisory_merge_request(
            merge_request, deadline=time.monotonic() + 100
        )
    assert original_request.approve.call_args.kwargs["sha"] == "old-sha"
    assert updated_request.approve.call_args.kwargs["sha"] == "new-sha"


def test_approve_advisory_merge_request_recovers_lost_response() -> None:
    """Do not submit a duplicate approval after the first response is lost."""
    merge_request = mock.Mock(state="opened")
    current_merge_request = merge_request.manager.get.return_value
    current_merge_request.state = "opened"
    current_merge_request.sha = "current-sha"
    current_merge_request.approvals.get.side_effect = [
        mock.Mock(user_has_approved=False),
        mock.Mock(user_has_approved=True),
    ]
    current_merge_request.approve.side_effect = requests.ConnectionError("response lost")

    with mock.patch(f"{TASK}.create_advisory.time.sleep"):
        create_advisory.create_advisory._approve_advisory_merge_request(
            merge_request, deadline=time.monotonic() + 100
        )
    current_merge_request.approve.assert_called_once()
    assert merge_request.manager.get.call_count == 2


def test_approve_advisory_merge_request_retry_deadline() -> None:
    """Bound retries and sleeps by the shared deadline and report the approval step."""
    merge_request = mock.Mock(state="opened")
    merge_request.manager.get.side_effect = GitlabError("unavailable", response_code=503)
    current_time = 0.0

    def advance_time(seconds: float) -> None:
        nonlocal current_time
        current_time += seconds

    with (
        mock.patch(f"{TASK}.create_advisory.time.monotonic", side_effect=lambda: current_time),
        mock.patch(f"{TASK}.create_advisory.time.sleep", side_effect=advance_time) as sleep,
        pytest.raises(tekton.CheckStepError, match="approving the advisory") as caught,
    ):
        create_advisory.create_advisory._approve_advisory_merge_request(
            merge_request, deadline=35
        )
    assert isinstance(caught.value.__cause__, TimeoutError)
    assert sleep.call_args_list == [mock.call(10), mock.call(20), mock.call(5)]
    assert [call.kwargs["timeout"] for call in merge_request.manager.get.call_args_list] == [
        30,
        25,
        5,
    ]


def test_finish_from_merged_uses_known_advisory_metadata(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Write portal URLs from known path/type/name after verifying main."""
    client = mock.Mock()
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = None
    yaml_repo_path = "data/advisories/t/2025/0042/advisory.yaml"
    published = tmp_path / yaml_repo_path
    published.parent.mkdir(parents=True)
    published.write_text(
        "metadata:\n  name: '2025:0042'\nspec:\n  type: RHSA\n",
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory._approve_advisory_merge_request") as approve,
        mock.patch(
            f"{TASK}.create_advisory.gitlab.push_merge_request_to_main",
            side_effect=lambda *args, **kwargs: approve.assert_called_once(),
        ) as push_mr,
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main") as sync_main,
    ):
        out = create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=client,
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            yaml_repo_path=yaml_repo_path,
            advisory_type="RHSA",
            advisory_name="2025:0042",
            deadline=time.monotonic() + 100,
        )
    sync_main.assert_called_once()
    assert push_mr.call_args.kwargs["use_merge_train"] is True
    assert push_mr.call_args.kwargs["cleanup_on_conflict"] is True
    merge_request.changes.assert_not_called()
    assert out == yaml_repo_path
    assert results["result"].read_text(encoding="utf-8") == "Success"
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.stage.redhat.com/errata/RHSA-2025:0042"
    )
    assert "advisory.yaml" in results["advisory_internal_url"].read_text(encoding="utf-8")


@pytest.mark.parametrize("merged_during_cleanup", [False, True])
def test_finish_cancels_auto_merge_after_wait_failure(
    tmp_path: Path, creds: gitlab.GitLabCredentials, merged_during_cleanup: bool
) -> None:
    """Cancel a timed-out train or finish if it merged during cancellation."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/7"
    merge_request.state = "opened"
    merge_request.manager.get.return_value.state = (
        "merged" if merged_during_cleanup else "opened"
    )
    merge_request._update_attrs.side_effect = lambda attrs: setattr(
        merge_request, "state", attrs["state"]
    )
    yaml_repo_path = "data/advisories/t/2025/0042/advisory.yaml"
    published = tmp_path / yaml_repo_path
    published.parent.mkdir(parents=True)
    published.write_text("spec:\n  type: RHSA\n", encoding="utf-8")
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory._approve_advisory_merge_request"),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.push_merge_request_to_main",
            side_effect=_owned_train_timeout,
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.cancel_merge_request_auto_merge",
            return_value=True,
        ) as cancel,
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main") as sync_main,
    ):
        kwargs = dict(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
            yaml_repo_path=yaml_repo_path,
            advisory_type="RHSA",
            advisory_name="2025:0042",
        )
        if merged_during_cleanup:
            assert (
                create_advisory.create_advisory._finish_from_merged_merge_request(**kwargs)
                == yaml_repo_path
            )
            sync_main.assert_called_once()
            assert results["result"].read_text(encoding="utf-8") == "Success"
        else:
            with pytest.raises(TimeoutError, match="merge wait expired"):
                create_advisory.create_advisory._finish_from_merged_merge_request(**kwargs)
            sync_main.assert_not_called()
            assert not results["result"].exists()
    cancel.assert_called_once()
    merge_request.manager.get.assert_called_once_with(
        merge_request.get_id.return_value, timeout=30, max_retries=0
    )


def test_finish_reports_uncertain_auto_merge_after_cancel_failure(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Tell the operator when a failed task may still merge its advisory."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/7"
    merge_request.state = "opened"
    merge_request.manager.get.return_value.state = "opened"
    with (
        mock.patch(f"{TASK}.create_advisory._approve_advisory_merge_request"),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.push_merge_request_to_main",
            side_effect=_owned_train_timeout,
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.cancel_merge_request_auto_merge",
            side_effect=GitlabError("GitLab unavailable", response_code=503),
        ),
        pytest.raises(RuntimeError, match="may merge after this task fails"),
    ):
        create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={},
            deadline=time.monotonic() + 100,
        )


def test_finish_reports_unknown_merge_state_if_cleanup_refresh_fails(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Require an MR state check after canceling a timed-out train entry."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/7"
    merge_request.state = "opened"
    merge_request.manager.get.side_effect = GitlabError(
        "GitLab unavailable", response_code=503
    )
    with (
        mock.patch(f"{TASK}.create_advisory._approve_advisory_merge_request"),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.push_merge_request_to_main",
            side_effect=_owned_train_timeout,
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.cancel_merge_request_auto_merge",
            return_value=True,
        ),
        pytest.raises(RuntimeError, match="could not confirm whether"),
    ):
        create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={},
            deadline=time.monotonic() + 100,
        )


def test_finish_reused_mr_does_not_cancel_train_even_if_it_queued(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Leave auto-merge armed when a retry queues a reused MR and times out."""
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/7"
    merge_request.state = "opened"
    merge_request.manager.get.return_value.state = "opened"

    def _joining_wait_failure(*_args: Any, **kwargs: Any) -> None:
        assert kwargs["cleanup_on_conflict"] is False
        assert kwargs["retry_failed_pipeline"] is True
        kwargs["train_attempt"].queued_by_this_run = True
        raise TimeoutError("joining task timed out")

    with (
        mock.patch(f"{TASK}.create_advisory._approve_advisory_merge_request"),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.push_merge_request_to_main",
            side_effect=_joining_wait_failure,
        ),
        mock.patch(f"{TASK}.create_advisory.gitlab.cancel_merge_request_auto_merge") as cancel,
        pytest.raises(RuntimeError, match="may still merge after this task fails"),
    ):
        create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={},
            deadline=time.monotonic() + 100,
            owns_advisory_branch=False,
        )
    cancel.assert_not_called()


def test_finish_from_merged_fails_when_yaml_missing_on_main(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Do not write Success when the advisory is still absent from main."""
    client = mock.Mock()
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = "abc123"
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
        pytest.raises(tekton.CheckStepError, match="not found on main"),
    ):
        create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=client,
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            yaml_repo_path="data/advisories/t/2025/0042/advisory.yaml",
            advisory_type="RHSA",
            advisory_name="2025:0042",
            deadline=time.monotonic() + 100,
        )
    assert not results["result"].exists()


def test_finish_from_merged_fails_when_mr_still_open(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Do not write Success when push returns but the MR is still open."""
    client = mock.Mock()
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "opened"
    merge_request.merge_commit_sha = None
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        pytest.raises(tekton.CheckStepError, match="is not merged"),
    ):
        create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=client,
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            yaml_repo_path="data/advisories/t/2025/0042/advisory.yaml",
            advisory_type="RHSA",
            advisory_name="2025:0042",
            deadline=time.monotonic() + 100,
        )
    assert not results["result"].exists()


def test_render_preserves_tags_and_product_version(tmp_path: Path) -> None:
    """JSON-to-YAML keeps string tags and `product_version` like `1.20` intact."""
    repo = tmp_path / "repo"
    schema_path = repo / "schema" / "advisory.json"
    schema_path.parent.mkdir(parents=True)
    schema_doc = _catalog_schema()
    schema_doc["$id"] = schema_path.resolve().as_uri()
    schema_path.write_text(json.dumps(schema_doc), encoding="utf-8")
    new_dir = repo / "data" / "2025" / "0001"
    new_dir.mkdir(parents=True)
    template = Path(__file__).resolve().parents[5] / "templates" / "advisory.yaml.jinja"
    merged = {
        "product_id": 123,
        "product_name": "preserves data",
        "product_version": "1.20",
        "product_stream": "preserver-data-1.20",
        "cpe": "cpe:/a:test:product",
        "type": "RHEA",
        "synopsis": "Test synopsis",
        "topic": "Test topic",
        "description": "Test description",
        "solution": "Test solution",
        "references": ["https://example.com"],
        "content": {
            "images": [
                {
                    "containerImage": "quay.io/example/image@sha256:abc123",
                    "repository": "example/repo",
                    "tags": ["33158e1", "1.0.0", "latest", "v2.0e10"],
                }
            ]
        },
    }
    with mock.patch.object(
        create_advisory.create_advisory, "ADVISORY_TEMPLATE_PATH", template
    ):
        create_advisory.create_advisory._render_and_validate_advisory_yaml(
            repo_root=repo,
            new_advisory_dir=new_dir,
            merged=merged,
            portal_advisory_id="2025:0001",
            ship_date="2025-01-01T00:00:00Z",
            work_dir=tmp_path,
            stderr_path=tmp_path / "e.log",
        )
    import yaml

    doc = yaml.safe_load((new_dir / "advisory.yaml").read_text(encoding="utf-8"))
    assert doc["spec"]["product_version"] == "1.20"
    assert doc["spec"]["product_stream"] == "preserver-data-1.20"
    assert doc["spec"]["content"]["images"][0]["tags"] == [
        "33158e1",
        "1.0.0",
        "latest",
        "v2.0e10",
    ]


def test_render_jinja_cross_field_references(tmp_path: Path) -> None:
    """Jinja in synopsis/topic/description resolves other `advisory.spec` fields."""
    repo = tmp_path / "repo"
    schema_path = repo / "schema" / "advisory.json"
    schema_path.parent.mkdir(parents=True)
    schema_doc = _catalog_schema()
    schema_doc["$id"] = schema_path.resolve().as_uri()
    schema_path.write_text(json.dumps(schema_doc), encoding="utf-8")
    new_dir = repo / "data" / "2025" / "0001"
    new_dir.mkdir(parents=True)
    template = Path(__file__).resolve().parents[5] / "templates" / "advisory.yaml.jinja"
    merged = {
        "product_id": 123,
        "product_name": "Red Hat Product",
        "product_version": "9.0.1",
        "product_stream": "tp1",
        "cpe": "cpe:/a:example:product:el8",
        "type": "RHSA",
        "severity": "Moderate",
        "synopsis": (
            "{% set version_str = advisory.spec.product_version | string() %}"
            "{% set major = version_str.split('.')[0] %}"
            "{% if advisory.spec.type == 'RHSA' %}"
            "{{ advisory.spec.severity }}: RHEL {{ major }} security update"
            "{% else %}RHEL {{ major }} bug fix update{% endif %}"
        ),
        "topic": (
            "Updated {{ advisory.spec.product_name }} "
            "{{ advisory.spec.product_version }} available"
        ),
        "description": (
            "Security update for {{ advisory.spec.product_name }} "
            "for advisory {{ advisory_name }}"
        ),
        "solution": "Update your containers",
        "references": ["https://docs.example.com/notes"],
        "content": {"images": [{"containerImage": "q.io/i", "tags": ["latest"]}]},
    }
    with mock.patch.object(
        create_advisory.create_advisory, "ADVISORY_TEMPLATE_PATH", template
    ):
        create_advisory.create_advisory._render_and_validate_advisory_yaml(
            repo_root=repo,
            new_advisory_dir=new_dir,
            merged=merged,
            portal_advisory_id="2025:0001",
            ship_date="2025-01-01T00:00:00Z",
            work_dir=tmp_path,
            stderr_path=tmp_path / "e.log",
        )
    import yaml

    doc = yaml.safe_load((new_dir / "advisory.yaml").read_text(encoding="utf-8"))
    assert doc["spec"]["synopsis"] == "Moderate: RHEL 9 security update"
    assert doc["spec"]["topic"] == "Updated Red Hat Product 9.0.1 available"
    assert "Red Hat Product" in doc["spec"]["description"]
    assert "2025:0001" in doc["spec"]["description"]


def test_run_create_advisory_generic_content_type(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """`content_type=generic` uses the `.content.artifacts` content list path."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    repo = tmp_path / "repo"
    (repo / "data" / "advisories" / "t").mkdir(parents=True)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    decoded = {
        "type": "RHSA",
        "content": {"artifacts": [{"purl": "pkg:generic/example@1"}]},
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(repo, repo / "data" / "advisories" / "t"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="mainsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value=decoded,
        ) as build,
        mock.patch(f"{TASK}.create_advisory.gitlab.client_from_credentials"),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(True, "claimsha"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._resolve_live_id_number",
            return_value=1234,
        ),
        mock.patch(f"{TASK}.create_advisory._ensure_advisory_number_unused"),
        mock.patch(f"{TASK}.create_advisory._create_new_advisory"),
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "generic",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded=decoded,
        )
    assert build.call_args[0][2] == ".content.artifacts"


def test_run_create_advisory_happy_path_writes_portal_url(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """End-to-end create path writes Success and an `RHSA-*:1234` portal URL."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    repo = tmp_path / "repo"
    schema_dir = repo / "schema"
    schema_dir.mkdir(parents=True)
    schema_path = schema_dir / "advisory.json"
    schema_doc = _catalog_schema()
    schema_doc["$id"] = schema_path.resolve().as_uri()
    schema_path.write_text(json.dumps(schema_doc), encoding="utf-8")
    base = repo / "data" / "advisories" / "not-existing-origin"
    base.mkdir(parents=True)
    template = Path(__file__).resolve().parents[5] / "templates" / "advisory.yaml.jinja"
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    decoded = {
        "product_id": 123,
        "product_name": "Red Hat Product",
        "product_version": "1.2.3",
        "product_stream": "tp1",
        "cpe": "cpe:/a:example:product:el8",
        "type": "RHSA",
        "synopsis": "test synopsis",
        "topic": "test topic",
        "description": "test description",
        "solution": "test solution",
        "references": ["https://docs.example.com/notes"],
        "content": {
            "images": [
                {
                    "containerImage": "quay.io/example/openstack@sha256:abdeNEW",
                    "repository": "rhosp16-rhel8/openstack",
                    "tags": ["latest"],
                }
            ]
        },
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(repo, base),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="mainsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.subprocess_cmd.run_cmd",
            return_value=mock.MagicMock(stdout=_configmap_signing_key_stdout()),
        ),
        mock.patch(
            f"{TASK}.create_advisory._reserve_errata_live_id",
            return_value=1234,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.origin_main_has_path_matching",
            return_value=False,
        ),
        mock.patch.object(create_advisory.create_advisory, "ADVISORY_TEMPLATE_PATH", template),
        mock.patch(f"{TASK}.create_advisory.gitlab.client_from_credentials"),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(True, "claimsha"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=False,
        ),
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit"),
        mock.patch(f"{TASK}.create_advisory.git.push"),
        mock.patch(f"{TASK}.create_advisory.gitlab.get_or_create_merge_request"),
        mock.patch(
            f"{TASK}.create_advisory._finish_from_merged_merge_request",
            side_effect=lambda **kwargs: (
                create_advisory.create_advisory._write_success_results(
                    kwargs["result_paths"],
                    customer_portal_url=("https://access.redhat.com/errata/RHSA-2025:1234"),
                    gitlab_raw_url=(
                        "https://gitlab.example.com/g/r/-/raw/main/"
                        "data/advisories/not-existing-origin/2025/1234/advisory.yaml"
                    ),
                ),
                "data/advisories/not-existing-origin/2025/1234/advisory.yaml",
            )[1],
        ),
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "test-app",
                "origin": "not-existing-origin",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded=decoded,
        )
    assert results["result"].read_text(encoding="utf-8") == "Success"
    url = results["advisory_url"].read_text(encoding="utf-8")
    assert url.startswith("https://access.redhat.com/errata/RHSA-")
    assert url.endswith(":1234")


def test_try_finish_continues_when_branch_missing(tmp_path: Path) -> None:
    """Do not re-walk main when neither an MR nor the source branch exists."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t" / "2025" / "0001"
    base.mkdir(parents=True)
    (base / "advisory.yaml").write_text(
        "metadata:\n  name: '2025:0001'\nspec:\n  type: RHSA\n  content:\n"
        "    images:\n      - containerImage: q.io/i\n        tags: ['t']\n"
        "        repository: r\n",
        encoding="utf-8",
    )
    work = tmp_path / "work"
    work.mkdir()
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            return_value=None,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=False,
        ),
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main") as sync_main,
    ):
        finished = create_advisory.create_advisory._try_finish_via_existing_merge_request(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={
                "content": {
                    "images": [
                        {
                            "containerImage": "q.io/i",
                            "tags": ["t"],
                            "repository": "r",
                        }
                    ]
                }
            },
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )
    assert finished is False
    sync_main.assert_not_called()


def test_sync_main_skips_walk_when_main_sha_unchanged(tmp_path: Path) -> None:
    """Skip the published-content walk when origin/main did not move."""
    repo = tmp_path / "repo"
    repo.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="same-sha",
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
        ) as finish_published,
    ):
        finished = create_advisory.create_advisory._sync_main_and_finish_if_published(
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": []}},
            advisory_base=repo / "data",
            content_list_path=".content.images",
            content_type="image",
            git_repo="https://gitlab.example.com/g/r.git",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            skip_if_main_sha="same-sha",
        )
    assert finished is False
    finish_published.assert_not_called()


def test_claim_source_branch_uses_create_only_push(tmp_path: Path) -> None:
    """Claim the source branch with a create-only remote ref update."""
    call_order: list[str] = []

    def _rev_parse(*_args: object, **_kwargs: object) -> str:
        call_order.append("rev_parse")
        return "claimsha"

    def _push_new_branch(*_args: object, **_kwargs: object) -> None:
        call_order.append("push_new_branch")

    with (
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(
            f"{TASK}.create_advisory.git.push_new_branch",
            side_effect=_push_new_branch,
        ) as push_new,
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            side_effect=_rev_parse,
        ),
    ):
        claimed, claim_sha = create_advisory.create_advisory._claim_source_branch(
            tmp_path,
            "konflux-advisory-t-abc",
            stderr_path=tmp_path / "e.log",
        )
    assert claimed is True
    assert claim_sha == "claimsha"
    assert call_order == ["rev_parse", "push_new_branch"]
    push_new.assert_called_once_with(
        tmp_path,
        "konflux-advisory-t-abc",
        stderr_path=tmp_path / "e.log",
    )


def test_try_finish_waits_for_merge_request_when_branch_exists(tmp_path: Path) -> None:
    """Poll for an MR when the source branch exists without an open request."""
    repo = tmp_path / "repo"
    work = tmp_path / "work"
    work.mkdir()
    merge_request = mock.Mock()
    merge_request.changes.return_value = {
        "changes": [{"new_path": "data/advisories/t/2025/0001/advisory.yaml"}]
    }
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            return_value=None,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=True,
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.wait_for_open_merge_request_by_source_branch",
            return_value=merge_request,
        ) as wait_mr,
        mock.patch(
            f"{TASK}.create_advisory._finish_from_merged_merge_request",
            return_value="data/advisories/t/2025/0001/advisory.yaml",
        ) as finish,
        mock.patch(f"{TASK}.create_advisory.gitlab.get_or_create_merge_request") as create_mr,
    ):
        finished = create_advisory.create_advisory._try_finish_via_existing_merge_request(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": [{"x": 1}]}},
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )
    assert finished is True
    wait_mr.assert_called_once()
    create_mr.assert_not_called()
    assert finish.call_args.kwargs["owns_advisory_branch"] is False
    finish.assert_called_once()


def test_run_create_advisory_releases_claimed_branch_on_post_claim_idempotency(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Delete a claimed branch when refreshed idempotency finds published content."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    client = mock.Mock()
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(
                tmp_path / "repo",
                tmp_path / "repo" / "data" / "advisories" / "t",
            ),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="mainsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.client_from_credentials",
            return_value=client,
        ),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value={"type": "RHSA", "content": {"images": []}},
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(True, "claimsha"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=True,
        ),
        mock.patch(f"{TASK}.create_advisory._release_claimed_work") as release,
        mock.patch(f"{TASK}.create_advisory._resolve_live_id_number") as reserve,
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded={"content": {"images": [{"x": 1}]}},
        )
    release.assert_called_once_with(client, creds, mock.ANY)
    reserve.assert_not_called()


def test_commit_and_merge_push_race_joins_competitor_mr(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Report the merged advisory from a competing MR after a rejected push."""
    client = mock.Mock()
    claim_holder: dict[str, Any] = {"branch": "konflux-advisory-t-deadbeef0001"}
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    merge_request = mock.Mock()
    merge_request.changes.return_value = {
        "changes": [{"new_path": "data/advisories/t/2025/0999/advisory.yaml"}]
    }
    with (
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit"),
        mock.patch(
            f"{TASK}.create_advisory.git.push",
            side_effect=subprocess.CalledProcessError(1, "git push"),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="localsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_sha",
            return_value="remotesha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.wait_for_open_merge_request_by_source_branch",
            return_value=merge_request,
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_from_merged_merge_request",
            return_value="data/advisories/t/2025/0999/advisory.yaml",
        ) as finish,
    ):
        out = create_advisory.create_advisory._commit_and_merge_new_advisory(
            client,
            creds,
            tmp_path,
            "data/advisories/t/2025/1234/advisory.yaml",
            "konflux-advisory-t-deadbeef0001",
            "grp",
            "parent-pr",
            "task-run-1",
            "https://access.redhat.com/errata",
            results,
            advisory_type="RHSA",
            advisory_name="2025:1234",
            claim_commit_sha="claimsha",
            claim_holder=claim_holder,
            stderr_path=tmp_path / "e.log",
            deadline=time.monotonic() + 100,
        )
    finish.assert_called_once()
    assert claim_holder["branch"] is None
    assert out == "data/advisories/t/2025/0999/advisory.yaml"


def test_commit_and_merge_rejects_push_when_remote_still_at_claim(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Propagate a failed push when the remote branch never left the claim commit."""
    client = mock.Mock()
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit"),
        mock.patch(
            f"{TASK}.create_advisory.git.push",
            side_effect=subprocess.CalledProcessError(1, "git push"),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="localsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_sha",
            return_value="claimsha",
        ),
        pytest.raises(subprocess.CalledProcessError),
    ):
        create_advisory.create_advisory._commit_and_merge_new_advisory(
            client,
            creds,
            tmp_path,
            "data/advisories/t/2025/1234/advisory.yaml",
            "konflux-advisory-t-deadbeef0001",
            "grp",
            "parent-pr",
            "task-run-1",
            "https://access.redhat.com/errata",
            results,
            advisory_type="RHSA",
            advisory_name="2025:1234",
            claim_commit_sha="claimsha",
            stderr_path=tmp_path / "e.log",
            deadline=time.monotonic() + 100,
        )


def test_release_claimed_work_looks_up_open_merge_request(
    creds: gitlab.GitLabCredentials,
) -> None:
    """Preserve a remotely created MR even when this task lacks its object."""
    client = mock.Mock()
    merge_request = mock.Mock()
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            return_value=merge_request,
        ) as find_mr,
        mock.patch(
            f"{TASK}.create_advisory.gitlab.cleanup_merge_request_branch",
        ) as cleanup,
        mock.patch(
            f"{TASK}.create_advisory.gitlab.delete_remote_branch",
        ) as delete_branch,
    ):
        create_advisory.create_advisory._release_claimed_work(
            client,
            creds,
            "konflux-advisory-t-abc",
        )
    find_mr.assert_called_once_with(
        client,
        creds.git_repo,
        "konflux-advisory-t-abc",
    )
    cleanup.assert_not_called()
    delete_branch.assert_not_called()


def test_release_claimed_work_logs_merge_request_lookup_failure(
    creds: gitlab.GitLabCredentials,
) -> None:
    """Keep the branch when GitLab cannot establish whether an MR exists."""
    client = mock.Mock()
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            side_effect=GitlabError("lookup failed", response_code=503),
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.delete_remote_branch",
        ) as delete_branch,
        mock.patch(f"{TASK}.create_advisory.logger.warning") as warn,
    ):
        create_advisory.create_advisory._release_claimed_work(
            client,
            creds,
            "konflux-advisory-t-abc",
        )
    delete_branch.assert_not_called()
    assert warn.call_args.args[0] == (
        "failed to look up merge request for branch %s during cleanup: %s"
    )


@pytest.mark.parametrize("failure_kind", ["creation", "approval", "timeout", "other_step"])
def test_run_create_advisory_cleanup_after_failure(
    tmp_path: Path,
    creds: gitlab.GitLabCredentials,
    failure_kind: str,
) -> None:
    """Retain pushed advisory work after creation or merge failures."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t"
    base.mkdir(parents=True)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    client = mock.Mock()
    merge_request = mock.Mock()
    if failure_kind == "creation":
        error = RuntimeError("mr created remotely but not returned")
    else:
        action = (
            "another step"
            if failure_kind == "other_step"
            else ("approving the advisory merge request")
        )
        cause = (
            TimeoutError("deadline expired")
            if failure_kind == "timeout"
            else (GitlabError("forbidden", response_code=403))
        )
        error = tekton.CheckStepError(action, cause)
    with ExitStack() as stack:
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
                return_value=creds,
            )
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory._clone_advisory_repo", return_value=(repo, base)
            )
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory._finish_if_all_content_already_published",
                return_value=False,
            )
        )
        stack.enter_context(
            mock.patch(f"{TASK}.create_advisory.git.rev_parse", return_value="mainsha")
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory.gitlab.client_from_credentials", return_value=client
            )
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
                return_value=False,
            )
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory._claim_source_branch", return_value=(True, "claimsha")
            )
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory._sync_main_and_finish_if_published",
                return_value=False,
            )
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory.subprocess_cmd.run_cmd",
                return_value=mock.MagicMock(stdout=_configmap_signing_key_stdout()),
            )
        )
        stack.enter_context(
            mock.patch(f"{TASK}.create_advisory._reserve_errata_live_id", return_value=42)
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory.git.origin_main_has_path_matching", return_value=False
            )
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory._render_and_validate_advisory_yaml",
                return_value="data/advisories/t/2026/0042/advisory.yaml",
            )
        )
        stack.enter_context(mock.patch(f"{TASK}.create_advisory.git.checkout"))
        stack.enter_context(mock.patch(f"{TASK}.create_advisory.git.index_add_commit"))
        stack.enter_context(mock.patch(f"{TASK}.create_advisory.git.push"))
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory.gitlab.get_or_create_merge_request",
                side_effect=error if failure_kind == "creation" else None,
                return_value=merge_request,
            )
        )
        find_mr = stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
                return_value=merge_request,
            )
        )
        cleanup = stack.enter_context(
            mock.patch(f"{TASK}.create_advisory.gitlab.cleanup_merge_request_branch")
        )
        stack.enter_context(
            mock.patch(
                f"{TASK}.create_advisory._approve_advisory_merge_request", side_effect=error
            )
        )
        merge = stack.enter_context(
            mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main")
        )
        remove_local = stack.enter_context(mock.patch(f"{TASK}.create_advisory.shutil.rmtree"))
        caught = stack.enter_context(pytest.raises(type(error)))
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded={
                "type": "RHSA",
                "content": {"images": [{"x": 1}]},
            },
        )
    assert caught.value is error
    merge.assert_not_called()
    remove_local.assert_called_once()
    assert not results["result"].exists()
    find_mr.assert_not_called()
    cleanup.assert_not_called()


def test_release_claimed_work_logs_branch_delete_failure(
    creds: gitlab.GitLabCredentials,
) -> None:
    """Log branch cleanup failures without raising."""
    client = mock.Mock()
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            return_value=None,
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.delete_remote_branch",
            side_effect=GitlabError("boom", response_code=503),
        ),
        mock.patch(f"{TASK}.create_advisory.logger.warning") as warn,
    ):
        create_advisory.create_advisory._release_claimed_work(
            client,
            creds,
            "konflux-advisory-t-abc",
        )
    warn.assert_called_once()
    assert warn.call_args.args[0] == "failed to delete claimed branch %s: %s"


def test_recover_after_merge_request_wait_timeout_without_branch(
    tmp_path: Path,
) -> None:
    """Rerun idempotency when the source branch disappeared after a wait timeout."""
    repo = tmp_path / "repo"
    work = tmp_path / "work"
    work.mkdir()
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=True,
        ) as sync_finish,
        mock.patch(f"{TASK}.create_advisory._release_claimed_work") as release,
    ):
        finished = create_advisory.create_advisory._recover_after_merge_request_wait_timeout(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": [{"x": 1}]}},
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )
    assert finished is True
    sync_finish.assert_called_once()
    release.assert_not_called()


def test_recover_after_merge_request_wait_timeout_creates_merge_request(
    tmp_path: Path,
) -> None:
    """Open an MR for a remote advisory branch that has no open request."""
    repo = tmp_path / "repo"
    work = tmp_path / "work"
    work.mkdir()
    merge_request = mock.Mock()
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=True,
        ),
        mock.patch(
            f"{TASK}.create_advisory._is_claim_only_branch",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.get_or_create_merge_request",
            return_value=merge_request,
        ) as create_mr,
        mock.patch(
            f"{TASK}.create_advisory._finish_from_merged_merge_request",
            return_value="data/advisories/t/2025/0001/advisory.yaml",
        ) as finish,
        mock.patch(
            f"{TASK}.create_advisory.gitlab.wait_for_open_merge_request_by_source_branch",
        ) as wait_mr,
    ):
        finished = create_advisory.create_advisory._recover_after_merge_request_wait_timeout(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": [{"x": 1}]}},
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )
    assert finished is True
    create_mr.assert_called_once()
    finish.assert_called_once()
    assert finish.call_args.kwargs["owns_advisory_branch"] is False
    wait_mr.assert_not_called()


def test_run_create_advisory_joins_advanced_branch_when_claim_fails(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Join advisory work without a second MR wait when the branch is taken."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    client = mock.Mock()
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(
                tmp_path / "repo",
                tmp_path / "repo" / "data" / "advisories" / "t",
            ),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="mainsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.client_from_credentials",
            return_value=client,
        ),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value={"type": "RHSA", "content": {"images": []}},
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(False, None),
        ),
        mock.patch(
            f"{TASK}.create_advisory._recover_after_merge_request_wait_timeout",
            return_value=True,
        ) as recover,
        mock.patch(
            f"{TASK}.create_advisory.gitlab.wait_for_open_merge_request_by_source_branch",
        ) as wait_mr,
        mock.patch(f"{TASK}.create_advisory._resolve_live_id_number") as reserve,
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded={"content": {"images": [{"x": 1}]}},
        )
    recover.assert_called_once()
    wait_mr.assert_not_called()
    reserve.assert_not_called()


def test_rpm_epoch_survives_advisory_submission_and_rendering(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Preserve generated RPM epoch PURLs through both create-advisory tasks and YAML."""
    data = {
        "sign": {"configMapName": "signing-config"},
        "mapping": {"components": [{"name": "bind--main", "contentType": "rpm"}]},
        "pulp": {"domain": "public-hummingbird"},
        "signOptions": {"signKeyAlias": {"key": "hummingbird-signing-key"}},
        "releaseNotes": {
            "product_id": 123,
            "product_name": "Hummingbird",
            "product_version": "1.0",
            "product_stream": "hummingbird-1.0",
            "cpe": "cpe:/a:redhat:hummingbird:1.0",
            "type": "RHSA",
            "synopsis": "Test synopsis",
            "topic": "Test topic",
            "description": "Test description",
            "solution": "Test solution",
            "references": ["https://example.com/notes"],
            "cves": [
                {"key": "CVE-2026-1234", "component": "bind--main", "packages": ["bind"]}
            ],
        },
    }
    rpms = []
    for arch, repo_names in [
        ("x86_64", ["x86_64"]),
        ("aarch64", ["aarch64"]),
        ("noarch", ["x86_64", "aarch64"]),
        ("x86_64", ["source"]),
        ("x86_64", []),
    ]:
        rpms.append(
            {
                "rpmname": "bind",
                "epoch": "32",
                "version": "9.20.27",
                "release": "0.1.hum1",
                "arch": arch,
                "distro": "hummingbird",
                "sbomPath": "sboms/bind.sbom",
                "attestationPath": "attestations/bind.att",
                "targetRepos": [
                    {
                        "repository_id": f"hbird-{repo_name}-id",
                        "repository_name": repo_name,
                        "distro": "hummingbird",
                    }
                    for repo_name in repo_names
                ],
            }
        )
    snapshot = {
        "componentGroup": "hummingbird",
        "components": [{"name": "bind--main", "rpmsToPublish": rpms}],
    }
    populate_release_notes.populate_artifacts(data, snapshot)
    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert len(artifacts) == 6
    assert all("&epoch=32&" in artifact["purl"] for artifact in artifacts)
    (tmp_path / "data.json").write_text(json.dumps(data), encoding="utf-8")
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot), encoding="utf-8")
    (tmp_path / "rpa.json").write_text(
        json.dumps({"spec": {"origin": "test-origin"}}), encoding="utf-8"
    )
    request_params = request_advisory_creation.TaskParams(
        data_dir=tmp_path,
        data_path=Path("data.json"),
        snapshot_path=Path("snapshot.json"),
        release_plan_admission_path=Path("rpa.json"),
        results_dir_path=Path("results"),
        environment="stage",
        request_pipeline="create-advisory",
        synchronously="true",
        pipeline_run_uid="uid",
        task_git_url="https://example.com/catalog.git",
        task_git_revision="main",
        task_name="create-advisory",
        checksum_map="",
        dockerconfig_path=tmp_path / "dockerconfig",
        advisory_url_result=tmp_path / "request-url",
        advisory_internal_url_result=tmp_path / "request-internal-url",
    )
    with (
        mock.patch.object(internal_request, "create", return_value="ir-epoch") as submit,
        mock.patch.object(
            internal_request, "fetch_results", return_value={"result": "Success"}
        ),
    ):
        request_advisory_creation.run_request_advisory_creation(request_params)
    submit.assert_called_once()
    submitted_params = submit.call_args.kwargs["params"]
    assert submitted_params["contentType"] == "rpm"
    decoded = advisory_data.decode_advisory_param(submitted_params["advisory_json"])
    assert decoded["content"]["artifacts"] == artifacts
    assert json.loads((tmp_path / "data.json").read_text(encoding="utf-8")) == data

    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    repo = tmp_path / "repo"
    schema_dir = repo / "schema"
    schema_dir.mkdir(parents=True)
    (schema_dir / "advisory.json").write_text(json.dumps(_catalog_schema()), encoding="utf-8")
    base = repo / "data" / "advisories" / "test-origin"
    base.mkdir(parents=True)
    template = Path(__file__).resolve().parents[5] / "templates" / "advisory.yaml.jinja"
    results = {
        "result": tmp_path / "result",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal-url",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    with (
        mock.patch.object(
            create_advisory.create_advisory.gitlab,
            "read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch.object(
            create_advisory.create_advisory, "_clone_advisory_repo", return_value=(repo, base)
        ),
        mock.patch.object(
            create_advisory.create_advisory.subprocess_cmd,
            "run_cmd",
            return_value=mock.MagicMock(stdout=_configmap_signing_key_stdout("fallback-key")),
        ),
        mock.patch.object(
            create_advisory.create_advisory, "_reserve_errata_live_id", return_value=1234
        ),
        mock.patch.object(
            create_advisory.create_advisory.git,
            "origin_main_has_path_matching",
            return_value=False,
        ),
        mock.patch.object(create_advisory.create_advisory, "ADVISORY_TEMPLATE_PATH", template),
        mock.patch(f"{TASK}.create_advisory.git.rev_parse", return_value="mainsha"),
        mock.patch(f"{TASK}.create_advisory.gitlab.client_from_credentials"),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(True, "claimsha"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=False,
        ),
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit") as commit,
        mock.patch(f"{TASK}.create_advisory.git.push") as push,
        mock.patch(
            f"{TASK}.create_advisory.gitlab.get_or_create_merge_request",
            return_value=mock.Mock(state="merged", merge_commit_sha="abc123"),
        ),
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=tmp_path / "gitlab",
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": submitted_params["componentGroup"],
                "origin": submitted_params["origin"],
                "config_map_name": submitted_params["config_map_name"],
                "content_type": submitted_params["contentType"],
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded=decoded,
        )
    assert results["result"].read_text(encoding="utf-8") == "Success"
    push.assert_called_once()
    commit.assert_called_once()
    yaml_path = repo / commit.call_args.args[1][0]
    document = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    assert document["spec"]["content"]["artifacts"] == artifacts


def test_main_uses_secret_mount_env_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`ADVISORY_SECRET_MOUNT` and `ERRATA_SECRET_MOUNT` override default paths."""
    _setup_main_env(tmp_path, monkeypatch)
    advisory_mount = tmp_path / "adv"
    errata_mount = tmp_path / "err"
    advisory_mount.mkdir()
    errata_mount.mkdir()
    monkeypatch.setenv("ADVISORY_SECRET_MOUNT", str(advisory_mount))
    monkeypatch.setenv("ERRATA_SECRET_MOUNT", str(errata_mount))
    seen: list[tuple[Path, Path]] = []

    def _capture(**kwargs: Any) -> None:
        seen.append((kwargs["advisory_secret"], kwargs["errata_mount"]))

    with mock.patch(
        f"{TASK}.create_advisory.run_create_advisory",
        side_effect=_capture,
    ):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0
    assert seen == [(advisory_mount, errata_mount)]


def test_reraise_gitlab_api_error_insufficient_scope() -> None:
    """Rewrite insufficient-scope GitLab errors as CheckStepError."""
    with pytest.raises(tekton.CheckStepError, match="GitLab API"):
        create_advisory.create_advisory._reraise_gitlab_api_error(
            GitlabError("insufficient_scope", response_code=403),
        )


def test_reraise_gitlab_api_error_propagates_other_errors() -> None:
    """Re-raise unrelated GitLab API errors unchanged."""
    err = GitlabError("boom", response_code=500)
    try:
        raise err
    except GitlabError as exc:
        with pytest.raises(GitlabError, match="boom"):
            create_advisory.create_advisory._reraise_gitlab_api_error(exc)


def test_advisory_yaml_path_from_merge_request_returns_none() -> None:
    """Return None when MR changes do not include an advisory.yaml path."""
    merge_request = mock.Mock()
    merge_request.changes.return_value = {
        "changes": [{"new_path": "README.md"}],
    }
    assert (
        create_advisory.create_advisory._advisory_yaml_path_from_merge_request(merge_request)
        is None
    )


def test_sync_main_and_finish_runs_published_walk_when_main_moved(
    tmp_path: Path,
) -> None:
    """Walk published content when origin/main advanced since the last check."""
    repo = tmp_path / "repo"
    work = tmp_path / "work"
    work.mkdir()
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="new-sha",
        ),
        mock.patch(
            f"{TASK}.create_advisory._write_initial_content_file",
            return_value=work / "content.json",
        ) as write_content,
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=True,
        ) as finish_published,
    ):
        finished = create_advisory.create_advisory._sync_main_and_finish_if_published(
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": []}},
            advisory_base=repo / "data",
            content_list_path=".content.images",
            content_type="image",
            git_repo="https://gitlab.example.com/g/r.git",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            skip_if_main_sha="old-sha",
        )
    assert finished is True
    write_content.assert_called_once()
    finish_published.assert_called_once()


def test_release_claimed_work_noop_without_branch(
    creds: gitlab.GitLabCredentials,
) -> None:
    """Skip cleanup when no source branch was claimed."""
    with mock.patch(
        f"{TASK}.create_advisory.gitlab.cleanup_merge_request_branch",
    ) as cleanup:
        create_advisory.create_advisory._release_claimed_work(
            mock.Mock(),
            creds,
            None,
        )
    cleanup.assert_not_called()


def test_is_claim_only_branch_when_tip_is_ancestor_of_main(
    tmp_path: Path,
) -> None:
    """Treat a branch as claim-only when its tip is an ancestor of main."""
    with (
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_sha",
            side_effect=["claim-sha", "newer-main-sha"],
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.is_ancestor",
            return_value=True,
        ) as is_ancestor,
    ):
        assert create_advisory.create_advisory._is_claim_only_branch(
            tmp_path,
            "feat",
            stderr_path=tmp_path / "e.log",
        )
    is_ancestor.assert_called_once_with(
        tmp_path,
        "claim-sha",
        "origin/main",
        stderr_path=tmp_path / "e.log",
    )


def test_is_claim_only_branch_false_when_tip_not_ancestor(
    tmp_path: Path,
) -> None:
    """Return False when the branch has commits that are not on main."""
    with (
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_sha",
            side_effect=["work-sha", "main-sha"],
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.is_ancestor",
            return_value=False,
        ),
    ):
        assert not create_advisory.create_advisory._is_claim_only_branch(
            tmp_path,
            "feat",
            stderr_path=tmp_path / "e.log",
        )


def test_is_claim_only_branch_false_when_branch_or_main_missing(
    tmp_path: Path,
) -> None:
    """Return False when the branch or main SHA cannot be resolved."""
    with mock.patch(
        f"{TASK}.create_advisory.git.remote_branch_sha",
        return_value=None,
    ):
        assert not create_advisory.create_advisory._is_claim_only_branch(
            tmp_path,
            "feat",
            stderr_path=tmp_path / "e.log",
        )
    with mock.patch(
        f"{TASK}.create_advisory.git.remote_branch_sha",
        side_effect=["branch-sha", None],
    ):
        assert not create_advisory.create_advisory._is_claim_only_branch(
            tmp_path,
            "feat",
            stderr_path=tmp_path / "e.log",
        )


def test_recover_after_timeout_retains_unowned_claim_branch(tmp_path: Path) -> None:
    """Do not delete another task's claim branch while checking published content."""
    repo = tmp_path / "repo"
    work = tmp_path / "work"
    work.mkdir()
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory._is_claim_only_branch",
            return_value=True,
        ),
        mock.patch(f"{TASK}.create_advisory._release_claimed_work") as release,
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=True,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=False,
        ),
    ):
        finished = create_advisory.create_advisory._recover_after_merge_request_wait_timeout(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": [{"x": 1}]}},
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )
    assert finished is True
    release.assert_not_called()


def test_recover_after_timeout_returns_false_for_claim_only_after_sync(
    tmp_path: Path,
) -> None:
    """Stop when the remaining remote branch is still claim-only."""
    repo = tmp_path / "repo"
    work = tmp_path / "work"
    work.mkdir()
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory._is_claim_only_branch",
            return_value=True,
        ),
        mock.patch(f"{TASK}.create_advisory._release_claimed_work") as release,
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=True,
        ),
    ):
        finished = create_advisory.create_advisory._recover_after_merge_request_wait_timeout(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": [{"x": 1}]}},
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )
    assert finished is False
    release.assert_not_called()


def test_finish_from_merged_reads_published_yaml_without_known_metadata(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Load type/name from main when the caller did not pass them."""
    client = mock.Mock()
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = "abc123"
    yaml_repo_path = "data/advisories/t/2025/0042/advisory.yaml"
    published = tmp_path / yaml_repo_path
    published.parent.mkdir(parents=True)
    published.write_text(
        "metadata:\n  name: '2025:0042'\nspec:\n  type: RHSA\n",
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
    ):
        out = create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=client,
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            yaml_repo_path=yaml_repo_path,
            deadline=time.monotonic() + 100,
        )
    assert out == yaml_repo_path
    assert results["result"].read_text(encoding="utf-8") == "Success"
    assert "RHSA-2025:0042" in results["advisory_url"].read_text(encoding="utf-8")


def test_finish_from_merged_resolves_yaml_path_from_mr(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Read the advisory path from MR changes when it was not provided."""
    client = mock.Mock()
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = "abc123"
    yaml_repo_path = "data/advisories/t/2025/0042/advisory.yaml"
    merge_request.changes.return_value = {
        "changes": [{"new_path": yaml_repo_path}],
    }
    published = tmp_path / yaml_repo_path
    published.parent.mkdir(parents=True)
    published.write_text(
        "metadata:\n  name: '2025:0042'\nspec:\n  type: RHSA\n",
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
    ):
        out = create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=client,
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            advisory_type="RHSA",
            advisory_name="2025:0042",
            deadline=time.monotonic() + 100,
        )
    assert out == yaml_repo_path


def test_finish_from_merged_fails_when_mr_has_no_advisory_path(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Fail when neither a known path nor MR changes provide advisory.yaml."""
    client = mock.Mock()
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    merge_request.state = "merged"
    merge_request.merge_commit_sha = "abc123"
    merge_request.changes.return_value = {"changes": []}
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with (
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main"),
        pytest.raises(tekton.CheckStepError, match="does not contain an advisory.yaml"),
    ):
        create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=client,
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )


def test_try_finish_via_existing_mr_handles_transient_lookup(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Treat a transient MR lookup failure as no open merge request."""
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            side_effect=GitlabError("unavailable", response_code=503),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=False,
        ),
        mock.patch(f"{TASK}.create_advisory.logger.warning") as warn,
    ):
        finished = create_advisory.create_advisory._try_finish_via_existing_merge_request(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=tmp_path,
            work_dir=tmp_path / "work",
            decoded={"content": {"images": []}},
            advisory_base=tmp_path / "data",
            content_list_path=".content.images",
            content_type="image",
            source_branch="feat",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
            deadline=time.monotonic() + 100,
        )
    assert finished is False
    warn.assert_called_once()


def test_try_finish_via_existing_mr_reraises_permanent_lookup_error(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Propagate non-transient GitLab errors from the initial MR lookup."""
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            side_effect=GitlabError("insufficient_scope", response_code=403),
        ),
        pytest.raises(tekton.CheckStepError, match="GitLab API"),
    ):
        create_advisory.create_advisory._try_finish_via_existing_merge_request(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=tmp_path,
            work_dir=tmp_path / "work",
            decoded={"content": {"images": []}},
            advisory_base=tmp_path / "data",
            content_list_path=".content.images",
            content_type="image",
            source_branch="feat",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
            deadline=time.monotonic() + 100,
        )


def test_try_finish_via_existing_mr_recovers_after_wait_timeout(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Recover via published-content checks when waiting for an MR times out."""
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.find_open_merge_request_by_source_branch",
            return_value=None,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=True,
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.wait_for_open_merge_request_by_source_branch",
            side_effect=TimeoutError("still waiting"),
        ),
        mock.patch(
            f"{TASK}.create_advisory._recover_after_merge_request_wait_timeout",
            return_value=True,
        ) as recover,
    ):
        finished = create_advisory.create_advisory._try_finish_via_existing_merge_request(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=tmp_path,
            work_dir=tmp_path / "work",
            decoded={"content": {"images": []}},
            advisory_base=tmp_path / "data",
            content_list_path=".content.images",
            content_type="image",
            source_branch="feat",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths={
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
            deadline=time.monotonic() + 100,
        )
    assert finished is True
    recover.assert_called_once()


def test_claim_source_branch_returns_false_when_remote_already_exists(
    tmp_path: Path,
) -> None:
    """Treat a rejected create-only push as another run winning the claim."""
    with (
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="sha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.push_new_branch",
            side_effect=subprocess.CalledProcessError(1, ["git", "push"]),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=True,
        ),
    ):
        claimed, sha = create_advisory.create_advisory._claim_source_branch(
            tmp_path,
            "feat",
            stderr_path=tmp_path / "e.log",
        )
    assert claimed is False
    assert sha is None


def test_commit_and_merge_reraises_push_error_when_remote_branch_missing(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Propagate push failures when the remote branch disappeared."""
    with (
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit"),
        mock.patch(
            f"{TASK}.create_advisory.git.push",
            side_effect=subprocess.CalledProcessError(1, ["git", "push"]),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_sha",
            return_value=None,
        ),
        pytest.raises(subprocess.CalledProcessError),
    ):
        create_advisory.create_advisory._commit_and_merge_new_advisory(
            mock.Mock(),
            creds,
            tmp_path,
            "data/2025/0001/advisory.yaml",
            "feat",
            "grp",
            "parent-pr",
            "task-run-1",
            "https://access.redhat.com/errata",
            {
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
            advisory_type="RHSA",
            advisory_name="2025:0001",
            stderr_path=tmp_path / "e.log",
            deadline=time.monotonic() + 100,
        )


def test_commit_and_merge_fails_when_existing_mr_has_wrong_yaml(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Fail when another run owns the branch without the expected advisory path."""
    merge_request = mock.Mock()
    with (
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(f"{TASK}.create_advisory.git.index_add_commit"),
        mock.patch(
            f"{TASK}.create_advisory.git.push",
            side_effect=subprocess.CalledProcessError(1, ["git", "push"]),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_sha",
            return_value="other-sha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="local-sha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.wait_for_open_merge_request_by_source_branch",
            return_value=merge_request,
        ),
        mock.patch(
            f"{TASK}.create_advisory._advisory_yaml_path_from_merge_request",
            return_value=None,
        ),
        pytest.raises(tekton.CheckStepError, match="does not contain"),
    ):
        create_advisory.create_advisory._commit_and_merge_new_advisory(
            mock.Mock(),
            creds,
            tmp_path,
            "data/2025/0001/advisory.yaml",
            "feat",
            "grp",
            "parent-pr",
            "task-run-1",
            "https://access.redhat.com/errata",
            {
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
            advisory_type="RHSA",
            advisory_name="2025:0001",
            claim_commit_sha="claim-sha",
            stderr_path=tmp_path / "e.log",
            deadline=time.monotonic() + 100,
        )


def test_create_new_advisory_requires_type(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Require an advisory type before building customer-portal URLs."""
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t"
    base.mkdir(parents=True)
    with (
        mock.patch(
            f"{TASK}.create_advisory._render_and_validate_advisory_yaml",
            return_value="data/2025/0001/advisory.yaml",
        ),
        pytest.raises(ValueError, match="advisory type is required"),
    ):
        create_advisory.create_advisory._create_new_advisory(
            gitlab_client=mock.Mock(),
            credentials=creds,
            repo_root=repo,
            advisory_base=base,
            merged={},
            decoded={},
            year="2025",
            advisory_number_segment="0001",
            portal_advisory_id="2025:0001",
            ship_date="2025-01-01",
            source_branch="feat",
            url_prefix="https://access.redhat.com/errata",
            work_dir=tmp_path / "work",
            stderr_path=tmp_path / "e.log",
            result_paths={
                "result": tmp_path / "r",
                "advisory_url": tmp_path / "u",
                "advisory_internal_url": tmp_path / "i",
            },
            params={
                "component_group": "g",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            claim_commit_sha="sha",
            claim_holder={},
            deadline=time.monotonic() + 100,
        )


def test_main_logs_recent_command_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Include the command-log tail in failure diagnostics when present."""
    paths = _setup_main_env(tmp_path, monkeypatch)
    command_log_path = tmp_path / "create_advisory_command_log.txt"
    monkeypatch.setattr(
        f"{TASK}.create_advisory._COMMAND_LOG_PATH",
        command_log_path,
    )

    def _fail_and_write_log(*_args: object, **_kwargs: object) -> None:
        command_log_path.write_text("line-a\nline-b\n", encoding="utf-8")
        raise ValueError("workflow broke")

    with (
        mock.patch(
            f"{TASK}.create_advisory.run_create_advisory",
            side_effect=_fail_and_write_log,
        ),
        mock.patch(f"{TASK}.create_advisory.logger.error") as log_error,
    ):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0
    assert any(
        call.args and "recent command output" in str(call.args[0])
        for call in log_error.call_args_list
    )
    assert "workflow broke" in paths["RESULT_RESULT"].read_text(encoding="utf-8")


def test_recover_after_timeout_returns_false_when_branch_gone(
    tmp_path: Path,
) -> None:
    """Return False when published content is incomplete and the branch is gone."""
    repo = tmp_path / "repo"
    work = tmp_path / "work"
    work.mkdir()
    results = {
        "result": tmp_path / "res",
        "advisory_url": tmp_path / "url",
        "advisory_internal_url": tmp_path / "internal",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory._is_claim_only_branch",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._sync_main_and_finish_if_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=False,
        ),
    ):
        finished = create_advisory.create_advisory._recover_after_merge_request_wait_timeout(
            gitlab_client=mock.Mock(),
            credentials=mock.Mock(git_repo="https://gitlab.example.com/g/r.git"),
            repo_root=repo,
            work_dir=work,
            decoded={"content": {"images": [{"x": 1}]}},
            advisory_base=repo / "data" / "advisories" / "t",
            content_list_path=".content.images",
            content_type="image",
            source_branch="konflux-advisory-t-abc",
            component_group="g",
            internal_request_pr_name="pr",
            task_run_name="tr",
            url_prefix="https://access.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            deadline=time.monotonic() + 100,
        )
    assert finished is False


def test_claim_source_branch_reraises_when_remote_missing_after_push_failure(
    tmp_path: Path,
) -> None:
    """Propagate create-only push failures when the remote branch is still absent."""
    with (
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
        mock.patch(f"{TASK}.create_advisory.git.checkout"),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="sha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.push_new_branch",
            side_effect=subprocess.CalledProcessError(1, ["git", "push"]),
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.remote_branch_exists",
            return_value=False,
        ),
        pytest.raises(subprocess.CalledProcessError),
    ):
        create_advisory.create_advisory._claim_source_branch(
            tmp_path,
            "feat",
            stderr_path=tmp_path / "e.log",
        )


def test_run_create_advisory_raises_when_claim_and_recover_fail(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """Fail the workflow when claiming loses and recovery finds no MR."""
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    errata = tmp_path / "errata"
    _write_errata_mount(errata)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
        "internal_pr_name": tmp_path / "pr",
        "internal_task_run_name": tmp_path / "tr",
    }
    with (
        mock.patch(
            f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            f"{TASK}.create_advisory._clone_advisory_repo",
            return_value=(
                tmp_path / "repo",
                tmp_path / "repo" / "data" / "advisories" / "t",
            ),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory.git.rev_parse",
            return_value="mainsha",
        ),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.client_from_credentials",
            return_value=mock.Mock(),
        ),
        mock.patch(
            f"{TASK}.create_advisory._try_finish_via_existing_merge_request",
            return_value=False,
        ),
        mock.patch(
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value={"type": "RHSA", "content": {"images": []}},
        ),
        mock.patch(
            f"{TASK}.create_advisory._claim_source_branch",
            return_value=(False, None),
        ),
        mock.patch(
            f"{TASK}.create_advisory._recover_after_merge_request_wait_timeout",
            return_value=False,
        ),
        pytest.raises(tekton.CheckStepError, match="could not claim branch"),
    ):
        create_advisory.create_advisory.run_create_advisory(
            advisory_secret=secret,
            errata_mount=errata,
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            params={
                "component_group": "g",
                "origin": "t",
                "config_map_name": "cm",
                "content_type": "image",
                "internal_request_pr_name": "pr",
                "task_run_name": "tr",
            },
            decoded={"content": {"images": [{"x": 1}]}},
        )


def test_timeout_seconds_until_returns_remaining(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return seconds left until the shared merge deadline."""
    monkeypatch.setattr(
        create_advisory.create_advisory.time,
        "monotonic",
        lambda: 100.0,
    )
    assert create_advisory.create_advisory._timeout_seconds_until(130.0) == 30.0


def test_timeout_seconds_until_raises_when_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise when the shared advisory merge wait budget is spent."""
    monkeypatch.setattr(
        create_advisory.create_advisory.time,
        "monotonic",
        lambda: 200.0,
    )
    with pytest.raises(TimeoutError, match="timed out waiting"):
        create_advisory.create_advisory._timeout_seconds_until(150.0)


def test_finish_from_merged_passes_remaining_timeout(
    tmp_path: Path,
    creds: gitlab.GitLabCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use remaining shared-budget seconds for the merge wait."""
    client = mock.Mock()
    merge_request = mock.Mock()
    merge_request.web_url = "https://gitlab.example.com/g/r/-/merge_requests/1"
    yaml_repo_path = "data/advisories/t/2025/0042/advisory.yaml"
    published = tmp_path / yaml_repo_path
    published.parent.mkdir(parents=True)
    published.write_text(
        "metadata:\n  name: '2025:0042'\nspec:\n  type: RHSA\n",
        encoding="utf-8",
    )
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    monkeypatch.setattr(
        create_advisory.create_advisory.time,
        "monotonic",
        lambda: 50.0,
    )
    with (
        mock.patch(f"{TASK}.create_advisory.gitlab.push_merge_request_to_main") as push_merge,
        mock.patch(f"{TASK}.create_advisory.git.sync_to_origin_main"),
        mock.patch(
            f"{TASK}.create_advisory.gitlab.merge_request_is_merged",
            return_value=True,
        ),
    ):
        create_advisory.create_advisory._finish_from_merged_merge_request(
            gitlab_client=client,
            credentials=creds,
            repo_root=tmp_path,
            merge_request=merge_request,
            source_branch="feat",
            url_prefix="https://access.stage.redhat.com/errata",
            stderr_path=tmp_path / "e.log",
            result_paths=results,
            yaml_repo_path=yaml_repo_path,
            advisory_type="RHSA",
            advisory_name="2025:0042",
            deadline=90.0,
        )
    assert push_merge.call_args.kwargs["timeout_seconds"] == 40.0
