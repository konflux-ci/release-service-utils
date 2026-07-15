"""Tests for `create_advisory`."""

from __future__ import annotations

import base64
import gzip
import json
import os
import subprocess
from pathlib import Path
from unittest import mock

import pytest
import requests
from release_service_utils.helpers import tekton
from release_service_utils.tasks.internal import create_advisory
from release_service_utils.helpers.vcs import gitlab

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
        f"{TASK}.create_advisory.apply_template.render_template_to_json_file",
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
        f"{TASK}.create_advisory.apply_template.render_template_to_json_file",
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


def test_commit_and_push_new_advisory(tmp_path: Path) -> None:
    """Commit the new advisory file and push to the default branch."""
    with mock.patch(f"{TASK}.create_advisory.git.commit_and_push") as commit_push:
        create_advisory.create_advisory._commit_and_push_new_advisory(
            tmp_path, "path/y.yaml", "grp", stderr_path=tmp_path / "e.log"
        )
    commit_push.assert_called_once_with(
        tmp_path,
        ["path/y.yaml"],
        "[Konflux Release] new advisory for grp",
        gitlab.DEFAULT_BRANCH,
        retries=5,
        stderr_path=tmp_path / "e.log",
    )


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
        with mock.patch(f"{TASK}.create_advisory._commit_and_push_new_advisory"):
            create_advisory.create_advisory._create_new_advisory(
                credentials=creds,
                repo_root=repo,
                advisory_base=base,
                merged={"type": "RHSA", "content": {"images": []}},
                decoded={"type": "RHSA"},
                year="2025",
                advisory_number_segment="0042",
                portal_advisory_id="2025:0042",
                ship_date="2025-01-01T00:00:00Z",
                url_prefix="https://access.redhat.com/errata",
                work_dir=tmp_path,
                stderr_path=tmp_path / "e.log",
                result_paths=results,
                params={"component_group": "g"},
            )
    assert results["result"].read_text(encoding="utf-8") == "Success"


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
            with mock.patch(
                f"{TASK}.create_advisory._finish_if_all_content_already_published",
                return_value=True,
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
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value={"type": "RHSA", "content": {"images": []}},
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
    with mock.patch(
        f"{TASK}.create_advisory.run_create_advisory",
        side_effect=ValueError("workflow broke"),
    ):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "workflow broke" in text


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
        f"{TASK}.create_advisory.gitlab.read_credentials_from_mount",
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
                with mock.patch(
                    f"{TASK}.create_advisory._resolve_live_id_number",
                    return_value=1234,
                ):
                    with mock.patch(f"{TASK}.create_advisory._ensure_advisory_number_unused"):
                        with mock.patch(
                            f"{TASK}.create_advisory._create_new_advisory"
                        ) as create:
                            create_advisory.create_advisory.run_create_advisory(
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
        f"{TASK}.create_advisory.apply_template.render_template_to_json_file",
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


def test_create_new_advisory_stage_portal_url(
    tmp_path: Path,
) -> None:
    """`rhtap-release` repos use the staging customer portal errata URL prefix."""
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
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with mock.patch(
        f"{TASK}.create_advisory._render_and_validate_advisory_yaml",
        return_value="data/2025/0042/advisory.yaml",
    ):
        with mock.patch(f"{TASK}.create_advisory._commit_and_push_new_advisory"):
            create_advisory.create_advisory._create_new_advisory(
                credentials=creds,
                repo_root=repo,
                advisory_base=base,
                merged={"type": "RHSA", "content": {"images": []}},
                decoded={"type": "RHSA"},
                year="2025",
                advisory_number_segment="0042",
                portal_advisory_id="2025:0042",
                ship_date="2025-01-01T00:00:00Z",
                url_prefix="https://access.stage.redhat.com/errata",
                work_dir=tmp_path,
                stderr_path=tmp_path / "e.log",
                result_paths=results,
                params={"component_group": "g"},
            )
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.stage.redhat.com/errata/RHSA-2025:0042"
    )


def test_create_new_advisory_custom_live_id_portal_url(
    tmp_path: Path, creds: gitlab.GitLabCredentials
) -> None:
    """A pre-assigned `live_id` is reflected in the portal URL (e.g. `:0999`)."""
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
        return_value="data/2025/0999/advisory.yaml",
    ):
        with mock.patch(f"{TASK}.create_advisory._commit_and_push_new_advisory"):
            create_advisory.create_advisory._create_new_advisory(
                credentials=creds,
                repo_root=repo,
                advisory_base=base,
                merged={"type": "RHSA", "content": {"images": []}},
                decoded={"type": "RHSA", "live_id": 999},
                year="2025",
                advisory_number_segment="0999",
                portal_advisory_id="2025:0999",
                ship_date="2025-01-01T00:00:00Z",
                url_prefix="https://access.redhat.com/errata",
                work_dir=tmp_path,
                stderr_path=tmp_path / "e.log",
                result_paths=results,
                params={"component_group": "g"},
            )
    assert results["advisory_url"].read_text(encoding="utf-8") == (
        "https://access.redhat.com/errata/RHSA-2025:0999"
    )


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
            f"{TASK}.create_advisory._build_merged_advisory_with_signing_key",
            return_value=decoded,
        ) as build,
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
            "release_service_utils.tasks"
            + ".internal.create_advisory.create_advisory.gitlab.read_credentials_from_mount",
            return_value=creds,
        ),
        mock.patch(
            "release_service_utils.tasks"
            + ".internal.create_advisory.create_advisory._clone_advisory_repo",
            return_value=(repo, base),
        ),
        mock.patch(
            f"{TASK}.create_advisory._finish_if_all_content_already_published",
            return_value=False,
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
        mock.patch(f"{TASK}.create_advisory.git.commit_and_push"),
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

    def _capture(**kwargs) -> None:
        seen.append((kwargs["advisory_secret"], kwargs["errata_mount"]))

    with mock.patch(
        f"{TASK}.create_advisory.run_create_advisory",
        side_effect=_capture,
    ):
        assert create_advisory.create_advisory.main(["create_advisory.py"]) == 0
    assert seen == [(advisory_mount, errata_mount)]
