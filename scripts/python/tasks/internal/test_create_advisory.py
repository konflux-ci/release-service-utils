"""Tests for `create_advisory`."""

from __future__ import annotations

import base64
import gzip
import json
from pathlib import Path
from unittest import mock

import pytest
import requests
import tekton

import create_advisory
from vcs import gitlab


def _gzip_b64(obj: dict) -> str:
    raw = json.dumps(obj).encode("utf-8")
    return base64.standard_b64encode(gzip.compress(raw)).decode("ascii")


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
    secret = tmp_path / "gitlab"
    _write_gitlab_secret(secret)
    return gitlab.read_credentials_from_mount(secret)


def test_reserve_errata_live_id_posts_json(tmp_path: Path) -> None:
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n    default_realm = FOO\n", encoding="utf-8")

    class _Resp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"live_id": 42}

    with mock.patch("create_advisory.requests.Session") as sess_cls:
        sess = mock.MagicMock()
        sess_cls.return_value = sess
        sess.post.return_value = _Resp()
        out = create_advisory._reserve_errata_live_id(
            "https://errata/api/v1",
            mount,
            stderr_path=None,
            krb5_template=krb5,
            kinit_fn=lambda *_a, **_k: None,
        )
    assert out == 42


def test_reserve_errata_live_id_krb5_read_error(tmp_path: Path) -> None:
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    missing = tmp_path / "missing.conf"
    with pytest.raises(tekton.CheckStepError, match="Kerberos"):
        create_advisory._reserve_errata_live_id(
            "https://errata/api/v1",
            mount,
            stderr_path=None,
            krb5_template=missing,
            kinit_fn=lambda *_a, **_k: None,
        )


def test_reserve_errata_live_id_request_failure_logs_stderr(tmp_path: Path) -> None:
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n", encoding="utf-8")
    log = tmp_path / "log.txt"

    with mock.patch("create_advisory.requests.Session") as sess_cls:
        sess_cls.return_value.post.side_effect = requests.RequestException("net")
        with pytest.raises(requests.RequestException):
            create_advisory._reserve_errata_live_id(
                "https://errata/api/v1",
                mount,
                stderr_path=log,
                krb5_template=krb5,
                kinit_fn=lambda *_a, **_k: None,
            )
    assert "reserve_live_id" in log.read_text(encoding="utf-8")


def test_reserve_errata_live_id_missing_live_id(tmp_path: Path) -> None:
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n", encoding="utf-8")

    class _Resp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {}

    with mock.patch("create_advisory.requests.Session") as sess_cls:
        sess_cls.return_value.post.return_value = _Resp()
        with pytest.raises(ValueError, match="no live_id"):
            create_advisory._reserve_errata_live_id(
                "https://errata/api/v1",
                mount,
                stderr_path=None,
                krb5_template=krb5,
                kinit_fn=lambda *_a, **_k: None,
            )


def test_clone_advisory_repo(tmp_path: Path, creds: gitlab.GitLabCredentials) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "data" / "advisories" / "tenant").mkdir(parents=True)
    with mock.patch("create_advisory.gitlab.clone_project_sparse", return_value=repo):
        root, base = create_advisory._clone_advisory_repo(
            creds, "tenant", tmp_path, stderr_path=tmp_path / "e.log"
        )
    assert root == repo
    assert base == repo / "data" / "advisories" / "tenant"


def test_write_initial_content_file(tmp_path: Path) -> None:
    decoded = {"content": {"images": [{"a": 1}]}}
    path = create_advisory._write_initial_content_file(tmp_path, decoded, ".content.images")
    assert json.loads(path.read_text(encoding="utf-8")) == [{"a": 1}]


def test_customer_portal_url() -> None:
    assert (
        create_advisory._customer_portal_url(
            "https://access.redhat.com/errata", "RHSA", "2025:1"
        )
        == "https://access.redhat.com/errata/RHSA-2025:1"
    )


def test_write_success_results(tmp_path: Path) -> None:
    paths = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    create_advisory._write_success_results(
        paths,
        customer_portal_url="https://portal/x",
        gitlab_raw_url="https://gitlab/raw",
    )
    assert paths["result"].read_text(encoding="utf-8") == "Success"
    assert paths["advisory_url"].read_text(encoding="utf-8") == "https://portal/x"


def test_finish_if_all_content_already_published_true(tmp_path: Path) -> None:
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
    assert create_advisory._finish_if_all_content_already_published(
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
    assert not create_advisory._finish_if_all_content_already_published(
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
        create_advisory._finish_if_all_content_already_published(
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
    content = tmp_path / "c.json"
    content.write_text(json.dumps([{"a": 1}]), encoding="utf-8")
    decoded = {"type": "RHSA", "content": {"images": []}}
    with mock.patch("create_advisory.subprocess_cmd.run_cmd") as run:
        run.return_value = mock.MagicMock(stdout="key-name\n")
        merged = create_advisory._build_merged_advisory_with_signing_key(
            decoded, content, ".content.images", "cm", stderr_path=tmp_path / "e.log"
        )
    assert merged["content"]["images"][0]["signingKey"] == "key-name"


def test_resolve_live_id_from_decoded() -> None:
    assert (
        create_advisory._resolve_live_id_number(
            {"live_id": 7},
            Path("/unused"),
            stderr_path=Path("/dev/null"),
            krb5_template=Path("/etc/krb5.conf"),
        )
        == 7
    )


def test_resolve_live_id_reserves(tmp_path: Path) -> None:
    mount = tmp_path / "errata"
    _write_errata_mount(mount)
    with mock.patch("create_advisory._reserve_errata_live_id", return_value=99) as reserve:
        out = create_advisory._resolve_live_id_number(
            {},
            mount,
            stderr_path=tmp_path / "e.log",
            krb5_template=tmp_path / "k5.conf",
        )
    assert out == 99
    reserve.assert_called_once()


def test_ensure_advisory_number_unused_raises(tmp_path: Path) -> None:
    with mock.patch(
        "create_advisory.git.origin_ls_tree_name_only",
        return_value="data/advisories/t/2025/0042/advisory.yaml\n",
    ):
        with pytest.raises(ValueError, match="already exists"):
            create_advisory._ensure_advisory_number_unused(
                tmp_path, "2025", "0042", stderr_path=tmp_path / "e.log"
            )


def test_render_and_validate_advisory_yaml(tmp_path: Path) -> None:
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
        "create_advisory.apply_template.render_template_to_json_file",
        side_effect=_fake_render,
    ):
        rel = create_advisory._render_and_validate_advisory_yaml(
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
        "create_advisory.apply_template.render_template_to_json_file",
        side_effect=_bad_render,
    ):
        with pytest.raises(ValueError, match="schema validation failed"):
            create_advisory._render_and_validate_advisory_yaml(
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
    with mock.patch("create_advisory.git.index_add_commit") as add:
        with mock.patch("create_advisory.git.push_origin_with_rebase_retries") as push:
            create_advisory._commit_and_push_new_advisory(
                tmp_path, "path/y.yaml", "grp", stderr_path=tmp_path / "e.log"
            )
    add.assert_called_once()
    push.assert_called_once()


def test_create_new_advisory(tmp_path: Path, creds: gitlab.GitLabCredentials) -> None:
    repo = tmp_path / "repo"
    base = repo / "data" / "advisories" / "t"
    base.mkdir(parents=True)
    results = {
        "result": tmp_path / "r",
        "advisory_url": tmp_path / "u",
        "advisory_internal_url": tmp_path / "i",
    }
    with mock.patch(
        "create_advisory._render_and_validate_advisory_yaml",
        return_value="data/2025/0042/advisory.yaml",
    ):
        with mock.patch("create_advisory._commit_and_push_new_advisory"):
            create_advisory._create_new_advisory(
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
    with mock.patch("create_advisory.gitlab.read_credentials_from_mount", return_value=creds):
        with mock.patch(
            "create_advisory._clone_advisory_repo",
            return_value=(tmp_path / "repo", tmp_path / "repo" / "data" / "advisories" / "t"),
        ):
            with mock.patch(
                "create_advisory._finish_if_all_content_already_published",
                return_value=True,
            ):
                create_advisory.run_create_advisory(
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
    with mock.patch("create_advisory.gitlab.read_credentials_from_mount", return_value=creds):
        with mock.patch(
            "create_advisory._clone_advisory_repo",
            return_value=(repo, repo / "data" / "advisories" / "t"),
        ):
            with mock.patch(
                "create_advisory._finish_if_all_content_already_published",
                return_value=False,
            ):
                with mock.patch(
                    "create_advisory._build_merged_advisory_with_signing_key",
                    return_value={"type": "RHSA", "content": {"images": []}},
                ):
                    with mock.patch(
                        "create_advisory._resolve_live_id_number", return_value=42
                    ):
                        with mock.patch("create_advisory._ensure_advisory_number_unused"):
                            with mock.patch("create_advisory._create_new_advisory") as create:
                                create_advisory.run_create_advisory(
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
    _setup_main_env(tmp_path, monkeypatch)
    with mock.patch("create_advisory.run_create_advisory"):
        assert create_advisory.main(["create_advisory.py"]) == 0


def test_main_writes_failure_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _setup_main_env(tmp_path, monkeypatch)
    with mock.patch(
        "create_advisory.run_create_advisory",
        side_effect=ValueError("workflow broke"),
    ):
        assert create_advisory.main(["create_advisory.py"]) == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "workflow broke" in text


def test_main_check_step_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _setup_main_env(tmp_path, monkeypatch)
    err = tekton.CheckStepError("reading secrets", OSError("no mount"))
    with mock.patch("create_advisory.run_create_advisory", side_effect=err):
        assert create_advisory.main(["create_advisory.py"]) == 0
    assert "reading secrets" in paths["RESULT_RESULT"].read_text(encoding="utf-8")
