"""Tests for ``process_file_updates``."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest import mock

import process_file_updates
import pytest
import tekton
from gitlab.exceptions import GitlabError

FILE_UPDATES_SECRET_MOUNT = process_file_updates.FILE_UPDATES_SECRET_MOUNT_ENV

_VALID_ARGS = [
    "--upstream-repo",
    "https://gitlab.com/org/upstream.git",
    "--repo",
    "https://gitlab.com/org/repo.git",
    "--ref",
    "main",
    "--paths",
    '[{"path":"f.yaml","replacements":[]}]',
    "--component-group",
    "my-group",
    "--internal-request-pipeline-run-name",
    "pr-1",
    "--internal-request-task-run-name",
    "tr-1",
]


def _write_file_updates_secret(mount: Path) -> None:
    mount.mkdir(parents=True, exist_ok=True)
    (mount / "gitlab_host").write_text("gitlab.example.com", encoding="utf-8")
    (mount / "gitlab_access_token").write_text("token", encoding="utf-8")
    (mount / "git_author_name").write_text("Author", encoding="utf-8")
    (mount / "git_author_email").write_text("a@example.com", encoding="utf-8")


def _setup_tekton_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, secret_mount: Path
) -> tuple[Path, Path, Path, Path]:
    info = tmp_path / "info"
    state = tmp_path / "state"
    ir_pr = tmp_path / "ir_pr"
    ir_tr = tmp_path / "ir_tr"
    monkeypatch.setenv("RESULT_FILE_UPDATES_INFO", str(info))
    monkeypatch.setenv("RESULT_FILE_UPDATES_STATE", str(state))
    monkeypatch.setenv("RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME", str(ir_pr))
    monkeypatch.setenv("RESULT_INTERNAL_REQUEST_TASK_RUN_NAME", str(ir_tr))
    monkeypatch.setenv(FILE_UPDATES_SECRET_MOUNT, str(secret_mount))
    return info, state, ir_pr, ir_tr


def _mock_gitlab_client(
    *,
    list_pages: list[list[object]] | None = None,
    create_mr: object | None = None,
    get_error: GitlabError | None = None,
    list_error: GitlabError | None = None,
    create_error: GitlabError | None = None,
) -> mock.Mock:
    mock_mr = mock.Mock()
    mock_mr.web_url = "https://gitlab.com/org/up/-/merge_requests/99"
    mock_mr.iid = 99

    mock_mrs = mock.Mock()
    if list_error is not None:
        mock_mrs.list.side_effect = list_error
    elif list_pages is not None:

        def _list(**kwargs: object) -> list[object]:
            page = int(kwargs["page"])
            return list_pages[page - 1] if page <= len(list_pages) else []

        mock_mrs.list.side_effect = _list
    else:
        mock_mrs.list.return_value = []

    if create_error is not None:
        mock_mrs.create.side_effect = create_error
    else:
        mock_mrs.create.return_value = create_mr if create_mr is not None else mock_mr

    mock_project = mock.Mock()
    mock_project.mergerequests = mock_mrs
    mock_gl = mock.Mock()
    if get_error is not None:
        mock_gl.projects.get.side_effect = get_error
    else:
        mock_gl.projects.get.return_value = mock_project
    return mock_gl


@pytest.fixture
def secret_mount(tmp_path: Path) -> Path:
    """Provide a temp directory with file-updates secret files."""
    p = tmp_path / "secrets"
    _write_file_updates_secret(p)
    return p


def _require_yq() -> None:
    """Skip tests when ``yq`` is not on ``PATH`` (CI installs v4.34.1)."""
    if shutil.which("yq") is None:
        pytest.skip("yq is not installed")


def _write_repo_file(tmp_path: Path, rel_path: str, content: str) -> tuple[Path, Path]:
    """Create a file under ``tmp_path/repo`` and return ``(repo, target)``."""
    repo = tmp_path / "repo"
    repo.mkdir(exist_ok=True)
    target = repo / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return repo, target


_COMPLEX_DEPLOY_YAML = """\
# Managed tenant deploy manifest (representative fixture)
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo-app
  labels:
    app.kubernetes.io/name: demo
spec:
  replicas: 2
  selector:
    matchLabels:
      app: demo
  template:
    metadata:
      labels:
        app: demo
    spec:
      serviceAccountName: demo
      containers:
        - name: primary
          indexImage:


          releaseImage:


        - name: sidecar
          indexImage:


"""


def _large_deploy_yaml() -> str:
    """Return a 100+ line deploy manifest with nested keys at varied line offsets."""
    lines = [
        "# Managed-tenant deploy manifest (large deploy fixture)",
        "# Exercises comment/``---`` offsets and deep nested ``yq`` paths",
        "---",
        "apiVersion: apps/v1",
        "kind: Deployment",
        "metadata:",
        "  name: demo-app",
        "  namespace: managed-tenant",
        "  labels:",
        "    app.kubernetes.io/name: demo",
        "    app.kubernetes.io/part-of: release-service",
        "  annotations:",
        "    release.appstudio.openshift.io/component: demo",
        "spec:",
        "  replicas: 3",
        "  revisionHistoryLimit: 5",
        "  selector:",
        "    matchLabels:",
        "      app: demo",
        "  strategy:",
        "    type: RollingUpdate",
        "    rollingUpdate:",
        "      maxSurge: 1",
        "      maxUnavailable: 0",
        "  template:",
        "    metadata:",
        "      labels:",
        "        app: demo",
        "      annotations:",
        "        checksum/config: placeholder",
        "    spec:",
        "      serviceAccountName: demo",
        "      securityContext:",
        "        runAsNonRoot: true",
        "      affinity:",
        "        nodeAffinity:",
        "          requiredDuringSchedulingIgnoredDuringExecution:",
        "            nodeSelectorTerms:",
        "              - matchExpressions:",
        "                  - key: kubernetes.io/arch",
        "                    operator: In",
        "                    values:",
        "                      - amd64",
        "                      - arm64",
        "      volumes:",
    ]
    for volume_index in range(12):
        lines.extend(
            [
                f"        - name: config-{volume_index}",
                "          configMap:",
                f"            name: demo-config-{volume_index}",
            ]
        )
    lines.extend(
        [
            "      containers:",
            "        - name: primary",
            "          image: registry.example.com/placeholder:0.0.0",
            "          indexImage:",
            "",
            "",
            "          releaseImage:",
            "",
            "",
            "          env:",
        ]
    )
    for env_index in range(24):
        lines.extend(
            [
                f"            - name: DEMO_ENV_{env_index}",
                f"              value: placeholder-{env_index}",
            ]
        )
    lines.extend(
        [
            "          volumeMounts:",
        ]
    )
    for mount_index in range(12):
        lines.extend(
            [
                f"            - name: config-{mount_index}",
                f"              mountPath: /etc/demo/config-{mount_index}",
                "              readOnly: true",
            ]
        )
    lines.extend(
        [
            "        - name: sidecar",
            "          image: registry.example.com/sidecar:0.0.0",
            "          indexImage:",
            "",
            "",
            "        - name: metrics",
            "          image: registry.example.com/metrics:0.0.0",
            "          indexImage:",
            "",
            "",
        ]
    )
    content = "\n".join(lines) + "\n"
    assert (
        content.count("\n") >= 100
    ), "fixture must stay large enough to matter for line offsets"
    return content


def test_parse_args_help() -> None:
    """``-h`` prints usage and exits with code 1."""
    with pytest.raises(SystemExit) as exc:
        process_file_updates.parse_args(["-h"])
    assert exc.value.code == 1


def test_parse_args_missing_required() -> None:
    """Missing required flags print usage and exit with code 1."""
    with pytest.raises(SystemExit) as exc:
        process_file_updates.parse_args(["--upstream-repo", "u", "--repo", "r"])
    assert exc.value.code == 1


def test_parse_args_rejects_extra() -> None:
    """Extra argv tokens are rejected by argparse with exit 2."""
    with pytest.raises(SystemExit) as exc:
        process_file_updates.parse_args([*_VALID_ARGS, "extra"])
    assert exc.value.code == 2


def test_parse_args_ok() -> None:
    """Valid flags yield a populated namespace."""
    ns = process_file_updates.parse_args(_VALID_ARGS)
    assert ns.upstream_repo == "https://gitlab.com/org/upstream.git"
    assert ns.component_group == "my-group"


def test_parse_replacement_expression() -> None:
    """Replacement strings must match ``|search|replace|`` exactly."""
    assert process_file_updates.parse_replacement_expression("|a|b|") == ("a", "b")
    assert process_file_updates.parse_replacement_expression("|search|replace|") == (
        "search",
        "replace",
    )
    assert process_file_updates.parse_replacement_expression("|only-two") is None
    assert process_file_updates.parse_replacement_expression("|a|b|e") is None
    assert process_file_updates.parse_replacement_expression("|a|b|\n") is None
    assert process_file_updates.parse_replacement_expression("|a\n|b|") is None


def test_apply_replacement_block_rejects_bad_pipe_count(tmp_path: Path) -> None:
    """Invalid replacement pipe count returns an error."""
    target = tmp_path / "f.yaml"
    target.write_text("key: value\n", encoding="utf-8")
    err, diff_path = process_file_updates.apply_replacement_block(
        target, 1, 1, "|only-two", tmp_path
    )
    assert err == "Replace expression should be in '|search|replace|' format"
    assert diff_path is None


def test_apply_replacement_block_rejects_trailing_chars(tmp_path: Path) -> None:
    """Trailing characters after the closing delimiter are rejected."""
    target = tmp_path / "f.yaml"
    target.write_text("key: value\n", encoding="utf-8")
    err, diff_path = process_file_updates.apply_replacement_block(
        target, 1, 1, "|old|new|e", tmp_path
    )
    assert err == "Replace expression should be in '|search|replace|' format"
    assert diff_path is None


def test_apply_replacement_block_performs_regex_replacement(tmp_path: Path) -> None:
    """Regex replacement is applied to the target file in the specified line range."""
    target = tmp_path / "f.yaml"
    target.write_text("old\n", encoding="utf-8")
    (tmp_path / "found.txt").write_text("old\n", encoding="utf-8")

    err, _ = process_file_updates.apply_replacement_block(target, 1, 0, "|old|new|", tmp_path)
    assert err is None
    assert target.read_text(encoding="utf-8") == "new\n"


def test_load_file_updates_secrets(secret_mount: Path) -> None:
    """Secret mount files are read into a dict."""
    secrets = process_file_updates.load_file_updates_secrets(secret_mount)
    assert secrets["gitlab_host"] == "gitlab.example.com"
    assert secrets["gitlab_access_token"] == "token"


def test_load_file_updates_secrets_missing_file(tmp_path: Path) -> None:
    """Missing secret files raise ``CheckStepError``."""
    with pytest.raises(tekton.CheckStepError) as exc:
        process_file_updates.load_file_updates_secrets(tmp_path / "missing")
    assert "reading secret file gitlab_host" in str(exc.value)


def test_write_paths_manifest(tmp_path: Path) -> None:
    """Paths JSON is written and parsed from the temp dir."""
    paths_json = '[{"path":"a.yaml","replacements":[]}]'
    manifest, data = process_file_updates.write_paths_manifest(paths_json, tmp_path)
    assert manifest.read_text(encoding="utf-8") == paths_json + "\n"
    assert data == [{"path": "a.yaml", "replacements": []}]


def test_outcome_after_path_processing_replacement_error(tmp_path: Path) -> None:
    """Replacement errors return diff text and the error message."""
    diff_file = tmp_path / "diff.txt"
    diff_file.write_text("---\n+++", encoding="utf-8")
    state = process_file_updates.PathProcessingState(
        replacements_update_error="bad replace",
        diff_path=diff_file,
        replacements_performed=0,
    )
    body, err, code = process_file_updates.outcome_after_path_processing(state)
    assert code == 0
    assert err == "bad replace"
    assert body.startswith("---")


def test_outcome_after_path_processing_key_not_found() -> None:
    """Missing keys yield the no-replacements JSON error."""
    state = process_file_updates.PathProcessingState(
        replacements_performed=0,
        key_not_found=True,
    )
    body, err, code = process_file_updates.outcome_after_path_processing(state)
    assert code == 0
    assert body == err == '"no replacements were performed"'


def test_outcome_after_path_processing_no_changes() -> None:
    """No replacements performed yields Success with no changes."""
    state = process_file_updates.PathProcessingState(replacements_performed=0)
    body, state_name, code = process_file_updates.outcome_after_path_processing(state)
    assert (body, state_name, code) == ("nothing needs change\n", "Success", 0)


def test_outcome_after_path_processing_continue() -> None:
    """Successful replacements return ``None`` to continue."""
    state = process_file_updates.PathProcessingState(replacements_performed=2)
    assert process_file_updates.outcome_after_path_processing(state) is None


def test_write_error_result(tmp_path: Path) -> None:
    """Replacement failures write JSON and Failed state."""
    info = tmp_path / "info"
    state = tmp_path / "state"
    process_file_updates.write_error_result(info, state, "+diff line", "oops")
    payload = json.loads(info.read_text(encoding="utf-8"))
    assert payload["error"] == "oops"
    assert payload["str"] == "diff line"
    assert state.read_text(encoding="utf-8") == "Failed"


def test_gitlab_create_mr_returns_json() -> None:
    """MR creation returns merge request JSON for Tekton."""
    out = process_file_updates.gitlab_create_mr(
        head="branch-1",
        title="t",
        target_branch="main",
        description="d",
        upstream_repo="https://gitlab.com/org/up.git",
        gitlab_client=_mock_gitlab_client(),
    )
    assert json.loads(out) == {
        "merge_request": "https://gitlab.com/org/up/-/merge_requests/99"
    }


def test_gitlab_create_mr_raises_when_web_url_missing() -> None:
    """Empty ``web_url`` raises ``CheckStepError``."""
    bare_mr = mock.Mock()
    bare_mr.web_url = ""
    with pytest.raises(tekton.CheckStepError) as exc:
        process_file_updates.gitlab_create_mr(
            head="b",
            title="t",
            target_branch="main",
            description="d",
            upstream_repo="https://gitlab.com/org/up.git",
            gitlab_client=_mock_gitlab_client(create_mr=bare_mr),
        )
    assert "creating GitLab merge request" in str(exc.value)


def test_gitlab_create_mr_raises_on_gitlab_error() -> None:
    """GitLab API errors map to ``CheckStepError``."""
    with pytest.raises(tekton.CheckStepError) as exc:
        process_file_updates.gitlab_create_mr(
            head="b",
            title="t",
            target_branch="main",
            description="d",
            upstream_repo="https://gitlab.com/org/up.git",
            gitlab_client=_mock_gitlab_client(create_error=GitlabError("create failed")),
        )
    assert "creating GitLab merge request" in str(exc.value)


def test_list_konflux_open_mrs_raises_on_project_lookup_error() -> None:
    """Project lookup failures raise ``CheckStepError``."""
    with pytest.raises(tekton.CheckStepError) as exc:
        process_file_updates.list_konflux_open_mrs(
            "org/up", _mock_gitlab_client(get_error=GitlabError("project not found"))
        )
    assert "getting GitLab project" in str(exc.value)


def test_list_konflux_open_mrs_raises_on_list_error() -> None:
    """MR list failures raise ``CheckStepError``."""
    with pytest.raises(tekton.CheckStepError) as exc:
        process_file_updates.list_konflux_open_mrs(
            "org/up", _mock_gitlab_client(list_error=GitlabError("list failed"))
        )
    assert "listing GitLab merge requests" in str(exc.value)


def test_main_success_writes_results(
    tmp_path: Path, secret_mount: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Success appends info and writes Success state."""
    info, state, ir_pr, ir_tr = _setup_tekton_env(tmp_path, monkeypatch, secret_mount)
    monkeypatch.setenv("TEMP", str(tmp_path / "work"))

    def _fake_run(**_k: object) -> tuple[str, str, int]:
        return '{"merge_request":"https://x/mr/1"}\n', "Success", 0

    with mock.patch.object(process_file_updates, "run_file_updates", side_effect=_fake_run):
        rc = process_file_updates.main(["process_file_updates.py", *_VALID_ARGS])

    assert rc == 0
    assert state.read_text(encoding="utf-8") == "Success"
    assert "merge_request" in info.read_text(encoding="utf-8")
    assert ir_pr.read_text(encoding="utf-8") == "pr-1"
    assert ir_tr.read_text(encoding="utf-8") == "tr-1"


def test_main_maps_check_step_error_to_failed(
    tmp_path: Path, secret_mount: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``CheckStepError`` writes Failed state and exits 0."""
    info, state, _, _ = _setup_tekton_env(tmp_path, monkeypatch, secret_mount)

    def _fail(**_k: object) -> tuple[str, str, int]:
        raise tekton.CheckStepError("cloning", ValueError("network"))

    with mock.patch.object(process_file_updates, "run_file_updates", side_effect=_fail):
        rc = process_file_updates.main(["process_file_updates.py", *_VALID_ARGS])

    assert rc == 0
    assert state.read_text(encoding="utf-8") == "Failed"
    assert "cloning" in info.read_text(encoding="utf-8")


def test_main_yaml_error_exits_one(
    tmp_path: Path, secret_mount: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalid YAML exits 1 after writing info."""
    info, state, _, _ = _setup_tekton_env(tmp_path, monkeypatch, secret_mount)

    def _yaml_fail(**_k: object) -> tuple[str, str, int]:
        return "fileUpdates: not yaml", "", 1

    with mock.patch.object(process_file_updates, "run_file_updates", side_effect=_yaml_fail):
        rc = process_file_updates.main(["process_file_updates.py", *_VALID_ARGS])

    assert rc == 1
    assert "not yaml" in info.read_text(encoding="utf-8")
    assert not state.exists()


def test_main_subprocess_error_writes_failed(
    tmp_path: Path, secret_mount: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Subprocess failures write Failed state and exit 0."""
    info, state, _, _ = _setup_tekton_env(tmp_path, monkeypatch, secret_mount)

    def _cmd_fail(**_k: object) -> tuple[str, str, int]:
        raise subprocess.CalledProcessError(1, ["yq"], stderr="broken")

    with mock.patch.object(process_file_updates, "run_file_updates", side_effect=_cmd_fail):
        rc = process_file_updates.main(["process_file_updates.py", *_VALID_ARGS])

    assert rc == 0
    assert state.read_text(encoding="utf-8") == "Failed"
    assert "yq" in info.read_text(encoding="utf-8")


def test_run_file_updates_short_circuit_on_path_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Path outcome short-circuits before MR handling."""
    secrets = {
        "gitlab_host": "h",
        "gitlab_access_token": "t",
        "git_author_name": "n",
        "git_author_email": "e@x.com",
    }
    path_state = process_file_updates.PathProcessingState(
        replacements_performed=0,
        key_not_found=True,
    )

    with (
        mock.patch.object(process_file_updates, "configure_git_environment", return_value="t"),
        mock.patch.object(process_file_updates, "git_functions_init"),
        mock.patch.object(
            process_file_updates,
            "write_paths_manifest",
            return_value=(tmp_path / "p.json", []),
        ),
        mock.patch.object(
            process_file_updates, "prepare_repository", return_value=tmp_path / "repo"
        ),
        mock.patch.object(
            process_file_updates,
            "process_all_paths",
            return_value=(path_state, None),
        ),
    ):
        body, err, code = process_file_updates.run_file_updates(
            upstream_repo="u",
            repo="r",
            revision="main",
            paths_json="[]",
            component_group="g",
            temp_dir=tmp_path,
            secrets=secrets,
        )

    assert code == 0
    assert body == err == '"no replacements were performed"'


def test_git_functions_init_configures_identity() -> None:
    """Global git user.name and user.email are set via ``vcs.git``."""
    with mock.patch.object(process_file_updates.vcs_git, "configure_git_global_user") as cfg:
        process_file_updates.git_functions_init("Author", "a@example.com", "tok")
    cfg.assert_called_once_with("Author", "a@example.com")


def test_sparse_dirs_from_paths() -> None:
    """Sparse checkout paths cover parent dirs and root-level files."""
    paths = [
        {"path": "deploy/overlays/prod/kustomization.yaml"},
        {"path": "version.yaml"},
        {"path": "deploy/base/kustomization.yaml"},
    ]
    assert process_file_updates.sparse_dirs_from_paths(paths) == [
        "deploy/base",
        "deploy/overlays/prod",
        "version.yaml",
    ]


def test_sparse_dirs_from_paths_skips_missing_path() -> None:
    """Entries without a path are ignored."""
    assert process_file_updates.sparse_dirs_from_paths([{"seed": "x"}]) == []


def test_prepare_repository(tmp_path: Path) -> None:
    """Repository is sparse-cloned and rebased on upstream via ``vcs.git``."""
    repo_cwd = tmp_path / "cloned"
    repo_cwd.mkdir()
    paths = [{"path": "deploy/image.yaml", "replacements": []}]
    with (
        mock.patch.object(
            process_file_updates.vcs_git,
            "clone",
            return_value=repo_cwd,
        ) as clone,
        mock.patch.object(process_file_updates.vcs_git, "rebase_onto_remote") as rebase,
    ):
        out = process_file_updates.prepare_repository(
            "https://gitlab.com/org/repo.git",
            "main",
            "https://gitlab.com/org/up.git",
            tmp_path,
            paths,
        )
    assert out == repo_cwd
    clone.assert_called_once_with(
        tmp_path,
        "https://gitlab.com/org/repo.git",
        revision="main",
        sparse_dirs=["deploy"],
        shallow=True,
    )
    rebase.assert_called_once()


def test_prepare_repository_raises_when_no_paths(tmp_path: Path) -> None:
    """An empty path list fails before cloning."""
    with pytest.raises(tekton.CheckStepError, match="cloning repository"):
        process_file_updates.prepare_repository(
            "https://gitlab.com/org/repo.git",
            "main",
            "https://gitlab.com/org/up.git",
            tmp_path,
            [],
        )


def test_configure_git_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Secrets are exported to the process environment."""
    secrets = {
        "gitlab_host": "gitlab.example.com",
        "gitlab_access_token": "tok123",
        "git_author_name": "Author",
        "git_author_email": "a@example.com",
    }
    with mock.patch.object(
        process_file_updates.vcs_gitlab, "configure_git_oauth2_auth"
    ) as auth:
        token = process_file_updates.configure_git_environment(secrets)
    assert token == "tok123"
    assert os.environ["ACCESS_TOKEN"] == "tok123"
    assert os.environ["GITLAB_HOST"] == "gitlab.example.com"
    auth.assert_called_once_with("tok123")


def test_git_functions_init_raises_on_missing_fields() -> None:
    """Missing git identity fields raise ``CheckStepError``."""
    with pytest.raises(tekton.CheckStepError) as exc:
        process_file_updates.git_functions_init("", "a@x.com", "tok")
    assert "initializing git" in str(exc.value)


def test_blank_lines_before_yaml(tmp_path: Path) -> None:
    """Leading comment/blank lines are counted before YAML."""
    target = tmp_path / "f.yaml"
    target.write_text("# comment\nkey: v\n", encoding="utf-8")
    assert process_file_updates.blank_lines_before_yaml(target) == 1


def test_blank_lines_before_yaml_returns_zero_on_missing_file(tmp_path: Path) -> None:
    """Missing file returns zero offset."""
    target = tmp_path / "missing.yaml"
    assert process_file_updates.blank_lines_before_yaml(target) == 0


def test_blank_lines_before_yaml_returns_zero_when_no_alpha_lines(tmp_path: Path) -> None:
    """File with no alphabetic non-comment lines returns zero offset."""
    target = tmp_path / "f.yaml"
    target.write_text("# only comments\n# more comments\n", encoding="utf-8")
    assert process_file_updates.blank_lines_before_yaml(target) == 0


@pytest.mark.parametrize(
    ("content", "expected_offset"),
    [
        ("# comment\nkey: v\n", 1),
        ("\n\nkey: v\n", 2),
        ("---\napiVersion: v1\n", 1),
        ("# Release config\n---\napiVersion: v1\n", 2),
    ],
)
def test_blank_lines_before_yaml_cases(
    tmp_path: Path, content: str, expected_offset: int
) -> None:
    """Line offsets cover comments, blanks, and ``---`` document starts."""
    target = tmp_path / "f.yaml"
    target.write_text(content, encoding="utf-8")
    assert process_file_updates.blank_lines_before_yaml(target) == expected_offset


def test_apply_replacement_block_success(tmp_path: Path) -> None:
    """Valid replacement returns no error and writes diff."""
    target = tmp_path / "f.yaml"
    target.write_text("old\n", encoding="utf-8")
    (tmp_path / "found.txt").write_text("old\n", encoding="utf-8")

    err, diff_path = process_file_updates.apply_replacement_block(
        target, 1, 0, "|old|new|", tmp_path
    )
    assert err is None
    assert diff_path is not None
    assert target.read_text(encoding="utf-8") == "new\n"


def test_apply_replacement_block_size_mismatch(tmp_path: Path) -> None:
    """Block size mismatch returns an error when value_size exceeds file lines."""
    target = tmp_path / "f.yaml"
    target.write_text("x\n", encoding="utf-8")
    (tmp_path / "found.txt").write_text("x\nx\n", encoding="utf-8")

    err, _ = process_file_updates.apply_replacement_block(target, 1, 1, "|x|y|", tmp_path)
    assert err == "Text block size differs from the original"


def test_apply_replacement_block_too_greedy(tmp_path: Path) -> None:
    """Greedy replacements return an error when multiple lines match."""
    target = tmp_path / "f.yaml"
    target.write_text("a\na\n", encoding="utf-8")
    (tmp_path / "found.txt").write_text("a\na\n", encoding="utf-8")

    err, _ = process_file_updates.apply_replacement_block(target, 1, 1, "|a|b|", tmp_path)
    assert "Too many lines replaced" in (err or "")


def test_write_json_error_result(tmp_path: Path) -> None:
    """Logical failures write JSON to both result files."""
    info = tmp_path / "info"
    state = tmp_path / "state"
    process_file_updates.write_json_error_result(info, state, '"oops"')
    payload = json.loads(info.read_text(encoding="utf-8"))
    assert payload["error"] == '"oops"'
    assert state.read_text(encoding="utf-8") == "Failed"


def test_resolve_target_file_accepts_nested_relative_path(tmp_path: Path) -> None:
    """Relative paths under the repo root are accepted."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = process_file_updates.resolve_target_file(repo, "dir/file.yaml")
    assert target == (repo / "dir" / "file.yaml").resolve()


@pytest.mark.parametrize(
    "entry_path",
    [
        "",
        "/etc/passwd",
        "../outside.yaml",
        "foo/../../outside.yaml",
        "C:secret.yaml",
    ],
)
def test_resolve_target_file_rejects_unsafe_paths(tmp_path: Path, entry_path: str) -> None:
    """Absolute paths, ``..`` segments, and drive paths are rejected."""
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(ValueError):
        process_file_updates.resolve_target_file(repo, entry_path)


def test_process_all_paths_rejects_unsafe_path_entry(tmp_path: Path) -> None:
    """Unsafe path entries fail before seed or replacement side effects."""
    repo = tmp_path / "repo"
    repo.mkdir()
    manifest = tmp_path / "paths.json"
    manifest.write_text("[]\n", encoding="utf-8")
    paths = [{"path": "../evil.yaml", "seed": "pwned"}]

    with pytest.raises(tekton.CheckStepError) as exc:
        process_file_updates.process_all_paths(paths, manifest, repo, tmp_path)
    assert exc.value.action == "validating path entry"
    assert not (tmp_path / "evil.yaml").exists()


def test_main_rejects_unsafe_path_entry(
    tmp_path: Path, secret_mount: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Traversal paths write Failed state and exit 0 via ``CheckStepError``."""
    info, state, _, _ = _setup_tekton_env(tmp_path, monkeypatch, secret_mount)
    repo = tmp_path / "repo"
    repo.mkdir()
    paths_json = json.dumps([{"path": "../evil.yaml", "seed": "x"}])
    argv = [
        "process_file_updates.py",
        "--upstream-repo",
        "https://gitlab.com/org/upstream.git",
        "--repo",
        "https://gitlab.com/org/repo.git",
        "--ref",
        "main",
        "--paths",
        paths_json,
        "--component-group",
        "my-group",
        "--internal-request-pipeline-run-name",
        "pr-1",
        "--internal-request-task-run-name",
        "tr-1",
        "--temp-dir",
        str(tmp_path / "work"),
    ]

    with (
        mock.patch.object(
            process_file_updates,
            "load_file_updates_secrets",
            return_value={
                "gitlab_host": "h",
                "gitlab_access_token": "t",
                "git_author_name": "n",
                "git_author_email": "e@x.com",
            },
        ),
        mock.patch.object(process_file_updates, "configure_git_environment", return_value="t"),
        mock.patch.object(process_file_updates, "git_functions_init"),
        mock.patch.object(process_file_updates.gitlab, "Gitlab"),
        mock.patch.object(process_file_updates, "prepare_repository", return_value=repo),
    ):
        rc = process_file_updates.main(argv)

    assert rc == 0
    assert state.read_text(encoding="utf-8") == "Failed"
    assert "validating path entry" in info.read_text(encoding="utf-8")
    assert not (tmp_path / "evil.yaml").exists()


def test_seed_target_file_skips_when_no_seed(tmp_path: Path) -> None:
    """Entries without seed skip file creation."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "new.yaml"
    with mock.patch.object(process_file_updates.vcs_git, "index_add_commit") as stage:
        process_file_updates.seed_target_file({"path": "new.yaml"}, target, repo)
    stage.assert_not_called()


def test_seed_target_file_writes_and_stages(tmp_path: Path) -> None:
    """Seed content is written and staged with git."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "dir" / "new.yaml"
    with (
        mock.patch.object(process_file_updates.vcs_git, "index_add_commit") as stage,
        mock.patch.object(
            process_file_updates.vcs_git,
            "working_tree_status",
            return_value="",
        ) as status,
    ):
        process_file_updates.seed_target_file(
            {"path": "dir/new.yaml", "seed": "content"},
            target,
            repo,
        )
    assert target.read_text(encoding="utf-8") == "content\n"
    stage.assert_called_once_with(repo, ["dir/new.yaml"], "", commit=False)
    status.assert_called_once_with(repo)


def test_seed_target_file_propagates_git_add_failure(tmp_path: Path) -> None:
    """``seed-error`` Tekton: git add failure propagates (``main`` maps to Failed)."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "test" / "seed-error.yaml"
    with mock.patch.object(
        process_file_updates.vcs_git,
        "index_add_commit",
        side_effect=subprocess.CalledProcessError(
            1, ["git", "add"], stderr="simulating error"
        ),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            process_file_updates.seed_target_file(
                {"path": "test/seed-error.yaml", "seed": "indexImage: \\ntom:"},
                target,
                repo,
            )


def test_apply_replacements_for_entry_missing_file_returns_not_yaml(
    tmp_path: Path,
) -> None:
    """``replacements-missing-file`` Tekton: missing path is not valid YAML."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "addons" / "missing.yaml"
    state = process_file_updates.PathProcessingState()
    with mock.patch(
        "subprocess.run",
        return_value=subprocess.CompletedProcess(["yq"], 1, "", ""),
    ):
        early = process_file_updates.apply_replacements_for_entry(
            {
                "path": "addons/missing.yaml",
                "replacements": [{"key": ".indexImage", "replacement": "|a|b|"}],
            },
            target,
            repo,
            tmp_path,
            state,
        )
    assert early == (
        "fileUpdates: the targetFile addons/missing.yaml is not a yaml file",
        "",
        1,
    )


def test_apply_replacements_for_entry_invalid_yaml(tmp_path: Path) -> None:
    """Invalid YAML returns exit code 1."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "bad.yaml"
    target.write_text("not: [valid\n", encoding="utf-8")
    state = process_file_updates.PathProcessingState()
    with (
        mock.patch.object(process_file_updates, "blank_lines_before_yaml", return_value=0),
        mock.patch(
            "subprocess.run",
            return_value=subprocess.CompletedProcess(["yq"], 1, "", ""),
        ),
    ):
        early = process_file_updates.apply_replacements_for_entry(
            {"path": "bad.yaml", "replacements": [{"key": ".k", "replacement": "|a|b|"}]},
            target,
            repo,
            tmp_path,
            state,
        )
    assert early == ("fileUpdates: the targetFile bad.yaml is not a yaml file", "", 1)


def test_apply_replacements_for_entry_key_not_found(tmp_path: Path) -> None:
    """Missing keys set ``key_not_found`` on state."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "f.yaml"
    target.write_text("key: v\n", encoding="utf-8")
    state = process_file_updates.PathProcessingState()

    def _yq_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if "(line, .)" in cmd[1]:
            return subprocess.CompletedProcess(cmd, 0, "0\nnull\n", "")
        return subprocess.CompletedProcess(cmd, 0, "v\n", "")

    with (
        mock.patch.object(process_file_updates, "blank_lines_before_yaml", return_value=0),
        mock.patch("subprocess.run", side_effect=_yq_run),
    ):
        assert (
            process_file_updates.apply_replacements_for_entry(
                {
                    "path": "f.yaml",
                    "replacements": [{"key": ".missing", "replacement": "|v|w|"}],
                },
                target,
                repo,
                tmp_path,
                state,
            )
            is None
        )
    assert state.key_not_found is True
    assert state.replacements_performed == 0


def test_apply_replacements_for_entry_applies_replacement(tmp_path: Path) -> None:
    """Successful replacement increments the performed count."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "f.yaml"
    target.write_text("key: old\n", encoding="utf-8")
    state = process_file_updates.PathProcessingState()

    def _yq_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if "(line, .)" in cmd[1]:
            return subprocess.CompletedProcess(cmd, 0, "1\nold\n", "")
        return subprocess.CompletedProcess(cmd, 0, "old\n", "")

    with (
        mock.patch.object(process_file_updates, "blank_lines_before_yaml", return_value=0),
        mock.patch("subprocess.run", side_effect=_yq_run),
        mock.patch.object(
            process_file_updates,
            "apply_replacement_block",
            return_value=(None, tmp_path / "diff.txt"),
        ),
    ):
        assert (
            process_file_updates.apply_replacements_for_entry(
                {
                    "path": "f.yaml",
                    "replacements": [{"key": ".key", "replacement": "|old|new|"}],
                },
                target,
                repo,
                tmp_path,
                state,
            )
            is None
        )
    assert state.replacements_performed == 1


def test_process_all_paths_returns_early_on_yaml_error(tmp_path: Path) -> None:
    """Invalid YAML in paths returns early."""
    repo = tmp_path / "repo"
    repo.mkdir()
    manifest = tmp_path / "paths.json"
    manifest.write_text("[]\n", encoding="utf-8")
    paths = [
        {
            "path": "f.yaml",
            "replacements": [{"key": ".k", "replacement": "|a|b|"}],
        }
    ]

    with (
        mock.patch.object(process_file_updates, "seed_target_file"),
        mock.patch.object(
            process_file_updates,
            "apply_replacements_for_entry",
            return_value=("fileUpdates: not yaml", "", 1),
        ),
        mock.patch.object(process_file_updates.vcs_git, "index_add_commit") as stage,
    ):
        state, early = process_file_updates.process_all_paths(paths, manifest, repo, tmp_path)
    assert early is not None
    stage.assert_not_called()


def test_get_cached_diff(tmp_path: Path) -> None:
    """Cached diff is captured and written under the temp dir."""
    repo = tmp_path / "repo"
    repo.mkdir()
    with mock.patch.object(
        process_file_updates.vcs_git, "working_tree_diff", return_value="diff\n"
    ):
        assert process_file_updates.get_cached_diff(repo, tmp_path) == "diff\n"
    assert (tmp_path / "tempMRFile-cached.diff").read_text(encoding="utf-8") == "diff\n"


def test_list_konflux_open_mrs_paginates() -> None:
    """Open MR listing paginates until an empty page."""
    page1_mr = mock.Mock()
    page1_mr.iid = 1
    items = process_file_updates.list_konflux_open_mrs(
        "org/up",
        _mock_gitlab_client(list_pages=[[page1_mr], []]),
    )
    assert len(items) == 1
    assert items[0].iid == 1


def test_find_existing_mr_with_same_diff(tmp_path: Path) -> None:
    """Matching staged diff returns existing MR info."""
    repo = tmp_path / "repo"
    repo.mkdir()
    existing_mr = mock.Mock()
    existing_mr.iid = 99
    existing_mr.web_url = "https://gitlab.com/org/up/-/merge_requests/99"
    with (
        mock.patch.object(
            process_file_updates,
            "list_konflux_open_mrs",
            return_value=[existing_mr],
        ),
        mock.patch.object(
            process_file_updates,
            "_fetch_merge_request_head",
            return_value="mr_99",
        ),
        mock.patch.object(process_file_updates.vcs_git, "working_tree_diff", return_value=""),
    ):
        info = process_file_updates.find_existing_mr_with_same_diff(
            "org/up", repo, tmp_path, _mock_gitlab_client()
        )
    assert info is not None
    assert "merge_requests/99" in info


def test_commit_and_create_mr(tmp_path: Path) -> None:
    """Staged changes are pushed and a merge request is opened."""
    repo = tmp_path / "repo"
    repo.mkdir()
    with (
        mock.patch.object(process_file_updates.vcs_git, "checkout") as checkout,
        mock.patch.object(process_file_updates.vcs_git, "commit_staged") as commit,
        mock.patch.object(process_file_updates.vcs_git, "push") as push,
        mock.patch.object(
            process_file_updates,
            "gitlab_create_mr",
            return_value='{"merge_request":"https://x/mr/1"}',
        ),
        mock.patch.object(process_file_updates.uuid, "uuid4") as uid,
    ):
        uid.return_value.hex = "abcd1234efgh5678"
        body, state, code = process_file_updates.commit_and_create_mr(
            component_group="grp",
            revision="main",
            upstream_repo="org/up",
            repo_cwd=repo,
            gitlab_client=_mock_gitlab_client(),
        )
    checkout.assert_called_once_with(repo, "abcd1234")
    commit.assert_called_once_with(repo, "fileUpdates changes")
    push.assert_called_once_with(repo, "abcd1234")
    assert code == 0 and state == "Success"
    assert "merge_request" in body


def test_run_file_updates_creates_mr(tmp_path: Path) -> None:
    """Workflow creates a merge request when changes exist."""
    secrets = {
        "gitlab_host": "h",
        "gitlab_access_token": "t",
        "git_author_name": "n",
        "git_author_email": "e@x.com",
    }
    path_state = process_file_updates.PathProcessingState(replacements_performed=2)

    with (
        mock.patch.object(process_file_updates, "configure_git_environment", return_value="t"),
        mock.patch.object(process_file_updates, "git_functions_init"),
        mock.patch.object(
            process_file_updates,
            "write_paths_manifest",
            return_value=(tmp_path / "p.json", []),
        ),
        mock.patch.object(
            process_file_updates, "prepare_repository", return_value=tmp_path / "repo"
        ),
        mock.patch.object(
            process_file_updates,
            "process_all_paths",
            return_value=(path_state, None),
        ),
        mock.patch.object(process_file_updates, "get_cached_diff", return_value="diff\n"),
        mock.patch.object(
            process_file_updates, "find_existing_mr_with_same_diff", return_value=None
        ),
        mock.patch.object(
            process_file_updates,
            "commit_and_create_mr",
            return_value=('{"merge_request":"u"}\n', "Success", 0),
        ),
    ):
        body, state, code = process_file_updates.run_file_updates(
            upstream_repo="u",
            repo="r",
            revision="main",
            paths_json="[]",
            component_group="g",
            temp_dir=tmp_path,
            secrets=secrets,
        )
    assert state == "Success" and "merge_request" in body


def test_run_file_updates_no_cached_diff(tmp_path: Path) -> None:
    """Empty cached diff short-circuits with Success."""
    secrets = {
        "gitlab_host": "h",
        "gitlab_access_token": "t",
        "git_author_name": "n",
        "git_author_email": "e@x.com",
    }
    path_state = process_file_updates.PathProcessingState(replacements_performed=1)

    with (
        mock.patch.object(process_file_updates, "configure_git_environment", return_value="t"),
        mock.patch.object(process_file_updates, "git_functions_init"),
        mock.patch.object(
            process_file_updates,
            "write_paths_manifest",
            return_value=(tmp_path / "p.json", []),
        ),
        mock.patch.object(
            process_file_updates, "prepare_repository", return_value=tmp_path / "repo"
        ),
        mock.patch.object(
            process_file_updates,
            "process_all_paths",
            return_value=(path_state, None),
        ),
        mock.patch.object(process_file_updates, "get_cached_diff", return_value=""),
    ):
        body, state, code = process_file_updates.run_file_updates(
            upstream_repo="u",
            repo="r",
            revision="main",
            paths_json="[]",
            component_group="g",
            temp_dir=tmp_path,
            secrets=secrets,
        )
    assert (body, state, code) == ("nothing needs change\n", "Success", 0)


def test_main_writes_replacement_error_result(
    tmp_path: Path, secret_mount: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Replacement errors write JSON Failed results."""
    info, state, _, _ = _setup_tekton_env(tmp_path, monkeypatch, secret_mount)

    def _fail(**_k: object) -> tuple[str, str, int]:
        return "diff", "block size differs", 0

    with mock.patch.object(process_file_updates, "run_file_updates", side_effect=_fail):
        rc = process_file_updates.main(["process_file_updates.py", *_VALID_ARGS])

    assert rc == 0
    assert state.read_text(encoding="utf-8") == "Failed"
    payload = json.loads(info.read_text(encoding="utf-8"))
    assert payload["error"] == "block size differs"


def test_main_writes_json_error_result(
    tmp_path: Path, secret_mount: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """JSON logical errors write Failed results."""
    info, state, _, _ = _setup_tekton_env(tmp_path, monkeypatch, secret_mount)
    err = '"no replacements were performed"'

    def _fail(**_k: object) -> tuple[str, str, int]:
        return err, err, 0

    with mock.patch.object(process_file_updates, "run_file_updates", side_effect=_fail):
        rc = process_file_updates.main(["process_file_updates.py", *_VALID_ARGS])

    assert rc == 0
    payload = json.loads(info.read_text(encoding="utf-8"))
    assert payload["error"] == err
    assert state.read_text(encoding="utf-8") == "Failed"


def test_main_missing_result_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing result env vars exit before updates run."""
    monkeypatch.delenv("RESULT_FILE_UPDATES_INFO", raising=False)
    with pytest.raises(SystemExit) as exc:
        process_file_updates.main(["process_file_updates.py", *_VALID_ARGS])
    assert exc.value.code == 1


def test_run_file_updates_builds_gitlab_client_when_not_provided(tmp_path: Path) -> None:
    """``run_file_updates`` builds a client when omitted."""
    secrets = {
        "gitlab_host": "gitlab.example.com",
        "gitlab_access_token": "t",
        "git_author_name": "n",
        "git_author_email": "e@x.com",
    }
    mock_gl = mock.Mock()
    with (
        mock.patch.object(process_file_updates, "configure_git_environment", return_value="t"),
        mock.patch.object(process_file_updates.gitlab, "Gitlab", return_value=mock_gl) as mk,
        mock.patch.object(process_file_updates, "git_functions_init"),
        mock.patch.object(
            process_file_updates,
            "write_paths_manifest",
            return_value=(tmp_path / "p.json", []),
        ),
        mock.patch.object(
            process_file_updates, "prepare_repository", return_value=tmp_path / "repo"
        ),
        mock.patch.object(
            process_file_updates,
            "process_all_paths",
            return_value=(process_file_updates.PathProcessingState(), ("early", "", 1)),
        ),
    ):
        process_file_updates.run_file_updates(
            upstream_repo="u",
            repo="r",
            revision="main",
            paths_json="[]",
            component_group="g",
            temp_dir=tmp_path,
            secrets=secrets,
        )
    mk.assert_called_once_with("gitlab.example.com", private_token="t")


def test_apply_replacements_for_entry_skips_empty_replacements(
    tmp_path: Path,
) -> None:
    """Empty replacement lists are skipped."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "f.yaml"
    target.write_text("k: v\n", encoding="utf-8")
    state = process_file_updates.PathProcessingState()
    assert (
        process_file_updates.apply_replacements_for_entry(
            {"path": "f.yaml", "replacements": []},
            target,
            repo,
            tmp_path,
            state,
        )
        is None
    )


def test_apply_replacements_for_entry_records_block_error(tmp_path: Path) -> None:
    """Replacement block errors update state."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "f.yaml"
    target.write_text("key: old\n", encoding="utf-8")
    state = process_file_updates.PathProcessingState()
    diff_path = tmp_path / "diff.txt"
    diff_path.write_text("---\n", encoding="utf-8")

    def _yq_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if "(line, .)" in cmd[1]:
            return subprocess.CompletedProcess(cmd, 0, "1\nold\n", "")
        return subprocess.CompletedProcess(cmd, 0, "old\n", "")

    with (
        mock.patch.object(process_file_updates, "blank_lines_before_yaml", return_value=0),
        mock.patch("subprocess.run", side_effect=_yq_run),
        mock.patch.object(
            process_file_updates,
            "apply_replacement_block",
            return_value=("greedy replace", diff_path),
        ),
    ):
        assert (
            process_file_updates.apply_replacements_for_entry(
                {
                    "path": "f.yaml",
                    "replacements": [{"key": ".key", "replacement": "|old|new|"}],
                },
                target,
                repo,
                tmp_path,
                state,
            )
            is None
        )
    assert state.replacements_update_error == "greedy replace"
    assert state.diff_path == diff_path


def test_process_all_paths_stages_then_breaks_on_replacement_error(
    tmp_path: Path,
) -> None:
    """Replacement errors stop after staging the file."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "f.yaml"
    target.write_text("k: v\n", encoding="utf-8")
    manifest = tmp_path / "paths.json"
    manifest.write_text("[]\n", encoding="utf-8")
    paths = [{"path": "f.yaml", "replacements": [{"key": ".k", "replacement": "|a|b|"}]}]

    def _apply(
        entry: dict,
        target_file: Path,
        repo_cwd: Path,
        temp_dir: Path,
        st: process_file_updates.PathProcessingState,
    ) -> None:
        st.replacements_update_error = "sed failed"
        st.diff_path = temp_dir / "diff.txt"

    with (
        mock.patch.object(process_file_updates, "seed_target_file"),
        mock.patch.object(
            process_file_updates, "apply_replacements_for_entry", side_effect=_apply
        ),
        mock.patch.object(process_file_updates.vcs_git, "index_add_commit") as stage,
    ):
        out_state, early = process_file_updates.process_all_paths(
            paths, manifest, repo, tmp_path
        )

    assert early is None
    assert out_state.replacements_update_error == "sed failed"
    stage.assert_called_once_with(repo, ["f.yaml"], "", commit=False)


def test_find_existing_mr_returns_none_when_diff_differs(tmp_path: Path) -> None:
    """Non-matching MR diffs return ``None``."""
    repo = tmp_path / "repo"
    repo.mkdir()
    existing_mr = mock.Mock()
    existing_mr.iid = 1
    existing_mr.web_url = "https://gitlab.com/org/up/-/merge_requests/1"
    with (
        mock.patch.object(
            process_file_updates,
            "list_konflux_open_mrs",
            return_value=[existing_mr],
        ),
        mock.patch.object(
            process_file_updates,
            "_fetch_merge_request_head",
            return_value="mr_1",
        ),
        mock.patch.object(
            process_file_updates.vcs_git,
            "working_tree_diff",
            return_value="not empty\n",
        ),
    ):
        assert (
            process_file_updates.find_existing_mr_with_same_diff(
                "org/up", repo, tmp_path, mock.Mock()
            )
            is None
        )


def test_run_file_updates_returns_early_from_process_all_paths(
    tmp_path: Path,
) -> None:
    """Early path processing result is returned."""
    secrets = {
        "gitlab_host": "h",
        "gitlab_access_token": "t",
        "git_author_name": "n",
        "git_author_email": "e@x.com",
    }
    with (
        mock.patch.object(process_file_updates, "configure_git_environment", return_value="t"),
        mock.patch.object(process_file_updates, "git_functions_init"),
        mock.patch.object(
            process_file_updates,
            "write_paths_manifest",
            return_value=(tmp_path / "p.json", []),
        ),
        mock.patch.object(
            process_file_updates, "prepare_repository", return_value=tmp_path / "repo"
        ),
        mock.patch.object(
            process_file_updates,
            "process_all_paths",
            return_value=(
                process_file_updates.PathProcessingState(),
                ("not yaml", "", 1),
            ),
        ),
    ):
        body, state, code = process_file_updates.run_file_updates(
            upstream_repo="u",
            repo="r",
            revision="main",
            paths_json="[]",
            component_group="g",
            temp_dir=tmp_path,
            secrets=secrets,
        )
    assert code == 1 and body == "not yaml"


def test_run_file_updates_returns_existing_mr(tmp_path: Path) -> None:
    """Existing MR info is returned when found."""
    secrets = {
        "gitlab_host": "h",
        "gitlab_access_token": "t",
        "git_author_name": "n",
        "git_author_email": "e@x.com",
    }
    path_state = process_file_updates.PathProcessingState(replacements_performed=1)
    existing = "existing MR\n"

    with (
        mock.patch.object(process_file_updates, "configure_git_environment", return_value="t"),
        mock.patch.object(process_file_updates, "git_functions_init"),
        mock.patch.object(
            process_file_updates,
            "write_paths_manifest",
            return_value=(tmp_path / "p.json", []),
        ),
        mock.patch.object(
            process_file_updates, "prepare_repository", return_value=tmp_path / "repo"
        ),
        mock.patch.object(
            process_file_updates,
            "process_all_paths",
            return_value=(path_state, None),
        ),
        mock.patch.object(process_file_updates, "get_cached_diff", return_value="diff\n"),
        mock.patch.object(
            process_file_updates,
            "find_existing_mr_with_same_diff",
            return_value=existing,
        ),
    ):
        body, state, code = process_file_updates.run_file_updates(
            upstream_repo="u",
            repo="r",
            revision="main",
            paths_json="[]",
            component_group="g",
            temp_dir=tmp_path,
            secrets=secrets,
        )
    assert (body, state, code) == (existing, "Success", 0)


def test_main_parse_args_non_int_exit_code() -> None:
    """Non-int ``SystemExit`` from argparse maps to exit 1."""
    with mock.patch.object(
        process_file_updates,
        "parse_args",
        side_effect=SystemExit("usage"),
    ):
        assert process_file_updates.main(["process_file_updates.py"]) == 1


def test_apply_replacements_yaml_tooling_leading_comments_and_document_start(
    tmp_path: Path,
) -> None:
    """Replacements honor leading comments and ``---`` via real ``yq``."""
    _require_yq()
    repo, target = _write_repo_file(
        tmp_path, "addons/my-addon.yaml", "# Konflux addon\n---\nindexImage:\n\n\n"
    )
    state = process_file_updates.PathProcessingState()
    assert (
        process_file_updates.apply_replacements_for_entry(
            {
                "path": "addons/my-addon.yaml",
                "replacements": [
                    {"key": ".indexImage", "replacement": "|indexImage:.*|indexImage: Tom|"},
                ],
            },
            target,
            repo,
            tmp_path,
            state,
        )
        is None
    )
    assert state.replacements_performed == 1
    assert "indexImage: Tom" in target.read_text(encoding="utf-8")


def test_apply_replacements_yaml_tooling_nested_key_path(tmp_path: Path) -> None:
    """Nested ``yq`` paths such as ``.spec.containers[0].indexImage`` work end-to-end."""
    _require_yq()
    repo, target = _write_repo_file(
        tmp_path,
        "deploy.yaml",
        "---\nspec:\n  containers:\n    - name: app\n      indexImage:\n\n\n",
    )
    state = process_file_updates.PathProcessingState()
    assert (
        process_file_updates.apply_replacements_for_entry(
            {
                "path": "deploy.yaml",
                "replacements": [
                    {
                        "key": ".spec.containers[0].indexImage",
                        "replacement": (
                            "|indexImage:.*|indexImage: registry.example.com/demo:1.0|"
                        ),
                    },
                ],
            },
            target,
            repo,
            tmp_path,
            state,
        )
        is None
    )
    assert state.replacements_performed == 1
    assert "registry.example.com/demo:1.0" in target.read_text(encoding="utf-8")


def test_apply_replacements_yaml_tooling_multiline_index_image_block(
    tmp_path: Path,
) -> None:
    """Multiline ``indexImage`` blocks preserve line count after replacement."""
    _require_yq()
    repo, target = _write_repo_file(
        tmp_path, "addon.yaml", "# header\nindexImage:\n\n\nreleaseImage:\n\n\n"
    )
    state = process_file_updates.PathProcessingState()
    assert (
        process_file_updates.apply_replacements_for_entry(
            {
                "path": "addon.yaml",
                "replacements": [
                    {
                        "key": ".indexImage",
                        "replacement": "|indexImage:.*|indexImage: multi:1.0|",
                    },
                ],
            },
            target,
            repo,
            tmp_path,
            state,
        )
        is None
    )
    updated = target.read_text(encoding="utf-8")
    assert state.replacements_performed == 1
    assert "indexImage: multi:1.0" in updated
    assert updated.index("indexImage: multi:1.0") < updated.index("releaseImage:")


def test_apply_replacements_yaml_tooling_multiple_replacements_same_file(
    tmp_path: Path,
) -> None:
    """Multiple replacements in one path entry are applied sequentially."""
    _require_yq()
    content = (
        "---\nspec:\n  containers:\n    - name: app\n      indexImage:\n\n\n"
        "      releaseImage:\n\n\n"
    )
    repo, target = _write_repo_file(tmp_path, "addon.yaml", content)
    state = process_file_updates.PathProcessingState()
    assert (
        process_file_updates.apply_replacements_for_entry(
            {
                "path": "addon.yaml",
                "replacements": [
                    {
                        "key": ".spec.containers[0].indexImage",
                        "replacement": "|indexImage:.*|indexImage: first:1.0|",
                    },
                    {
                        "key": ".spec.containers[0].releaseImage",
                        "replacement": "|releaseImage:.*|releaseImage: second:2.0|",
                    },
                ],
            },
            target,
            repo,
            tmp_path,
            state,
        )
        is None
    )
    updated = target.read_text(encoding="utf-8")
    assert state.replacements_performed == 2
    assert "indexImage: first:1.0" in updated
    assert "releaseImage: second:2.0" in updated


def test_process_all_paths_yaml_tooling_multiple_files(tmp_path: Path) -> None:
    """``process_all_paths`` applies replacements across multiple files."""
    _require_yq()
    repo, first = _write_repo_file(tmp_path, "addons/one.yaml", "indexImage:\n\n\n")
    _, second = _write_repo_file(tmp_path, "addons/two.yaml", "indexImage:\n\n\n")
    manifest = tmp_path / "paths.json"
    manifest.write_text("[]\n", encoding="utf-8")
    paths = [
        {
            "path": "addons/one.yaml",
            "replacements": [
                {"key": ".indexImage", "replacement": "|indexImage:.*|indexImage: one:1.0|"},
            ],
        },
        {
            "path": "addons/two.yaml",
            "replacements": [
                {"key": ".indexImage", "replacement": "|indexImage:.*|indexImage: two:2.0|"},
            ],
        },
    ]

    with mock.patch.object(process_file_updates.vcs_git, "index_add_commit") as stage:
        state, early = process_file_updates.process_all_paths(paths, manifest, repo, tmp_path)

    assert early is None
    assert state.replacements_performed == 2
    assert "indexImage: one:1.0" in first.read_text(encoding="utf-8")
    assert "indexImage: two:2.0" in second.read_text(encoding="utf-8")
    assert stage.call_count == 2
    staged_paths = {call.args[1][0] for call in stage.call_args_list}
    assert staged_paths == {"addons/one.yaml", "addons/two.yaml"}


def test_process_all_paths_yaml_tooling_cumulative_count_after_later_missing_key(
    tmp_path: Path,
) -> None:
    """Earlier replacements stay counted when a later path entry misses keys."""
    _require_yq()
    repo, first = _write_repo_file(tmp_path, "addons/one.yaml", "indexImage:\n\n\n")
    _, second = _write_repo_file(tmp_path, "addons/two.yaml", "releaseImage:\n\n\n")
    manifest = tmp_path / "paths.json"
    manifest.write_text("[]\n", encoding="utf-8")
    paths = [
        {
            "path": "addons/one.yaml",
            "replacements": [
                {"key": ".indexImage", "replacement": "|indexImage:.*|indexImage: one:1.0|"},
            ],
        },
        {
            "path": "addons/two.yaml",
            "replacements": [
                {
                    "key": ".missingIndexImage",
                    "replacement": "|indexImage:.*|indexImage: two:2.0|",
                },
            ],
        },
    ]

    with mock.patch.object(process_file_updates.vcs_git, "index_add_commit"):
        state, early = process_file_updates.process_all_paths(paths, manifest, repo, tmp_path)

    assert early is None
    assert state.replacements_performed == 1
    assert state.key_not_found is True
    assert "indexImage: one:1.0" in first.read_text(encoding="utf-8")
    assert process_file_updates.outcome_after_path_processing(state) is None


def test_process_all_paths_yaml_tooling_seed_then_replacement(tmp_path: Path) -> None:
    """``process_all_paths`` seeds a file then applies replacements with real tooling."""
    _require_yq()
    repo = tmp_path / "repo"
    repo.mkdir(exist_ok=True)
    target = repo / "addons" / "new-addon.yaml"
    manifest = tmp_path / "paths.json"
    manifest.write_text("[]\n", encoding="utf-8")
    paths = [
        {
            "path": "addons/new-addon.yaml",
            "seed": "indexImage:\n\n\nrelatedImages: []\n",
            "replacements": [
                {
                    "key": ".indexImage",
                    "replacement": "|indexImage:.*|indexImage: seeded:1.0|",
                },
            ],
        }
    ]

    with (
        mock.patch.object(process_file_updates.vcs_git, "index_add_commit") as stage,
        mock.patch.object(
            process_file_updates.vcs_git,
            "working_tree_status",
            return_value="",
        ),
    ):
        state, early = process_file_updates.process_all_paths(paths, manifest, repo, tmp_path)

    assert early is None
    assert state.replacements_performed == 1
    updated = target.read_text(encoding="utf-8")
    assert "indexImage: seeded:1.0" in updated
    assert "relatedImages: []" in updated
    assert stage.call_count == 2
    staged_paths = [call.args[1][0] for call in stage.call_args_list]
    assert staged_paths == ["addons/new-addon.yaml", "addons/new-addon.yaml"]


def test_apply_replacements_yaml_tooling_complex_deploy_fixture(tmp_path: Path) -> None:
    """A representative nested deploy manifest accepts several nested replacements."""
    _require_yq()
    repo, target = _write_repo_file(tmp_path, "data/deploy.yaml", _COMPLEX_DEPLOY_YAML)
    state = process_file_updates.PathProcessingState()
    assert (
        process_file_updates.apply_replacements_for_entry(
            {
                "path": "data/deploy.yaml",
                "replacements": [
                    {
                        "key": ".spec.template.spec.containers[0].indexImage",
                        "replacement": "|indexImage:.*|indexImage: primary:1.0|",
                    },
                    {
                        "key": ".spec.template.spec.containers[0].releaseImage",
                        "replacement": "|releaseImage:.*|releaseImage: primary-rel:1.0|",
                    },
                    {
                        "key": ".spec.template.spec.containers[1].indexImage",
                        "replacement": "|indexImage:.*|indexImage: sidecar:1.0|",
                    },
                ],
            },
            target,
            repo,
            tmp_path,
            state,
        )
        is None
    )
    updated = target.read_text(encoding="utf-8")
    assert state.replacements_performed == 3
    assert "indexImage: primary:1.0" in updated
    assert "releaseImage: primary-rel:1.0" in updated
    assert "indexImage: sidecar:1.0" in updated
    assert updated.count("indexImage:") == 2


def test_apply_replacements_yaml_tooling_large_deploy_fixture(tmp_path: Path) -> None:
    """A 100+ line deploy manifest accepts nested replacements at deep line offsets."""
    _require_yq()
    large_yaml = _large_deploy_yaml()
    assert large_yaml.count("\n") >= 100
    repo, target = _write_repo_file(tmp_path, "data/deploy.yaml", large_yaml)
    state = process_file_updates.PathProcessingState()
    assert (
        process_file_updates.apply_replacements_for_entry(
            {
                "path": "data/deploy.yaml",
                "replacements": [
                    {
                        "key": ".spec.template.spec.containers[0].indexImage",
                        "replacement": "|indexImage:.*|indexImage: primary:9.9|",
                    },
                    {
                        "key": ".spec.template.spec.containers[0].releaseImage",
                        "replacement": "|releaseImage:.*|releaseImage: primary-rel:9.9|",
                    },
                    {
                        "key": ".spec.template.spec.containers[2].indexImage",
                        "replacement": "|indexImage:.*|indexImage: metrics:9.9|",
                    },
                ],
            },
            target,
            repo,
            tmp_path,
            state,
        )
        is None
    )
    updated = target.read_text(encoding="utf-8")
    assert state.replacements_performed == 3
    assert "indexImage: primary:9.9" in updated
    assert "releaseImage: primary-rel:9.9" in updated
    assert "indexImage: metrics:9.9" in updated
    assert updated.count("indexImage:") == 3
    assert len(updated.splitlines()) == len(large_yaml.splitlines())


def test_apply_replacement_block_yaml_tooling_multiline_size_mismatch(
    tmp_path: Path,
) -> None:
    """Block validation rejects multiline scalar size drift."""
    _require_yq()
    target = tmp_path / "f.yaml"
    target.write_text(
        """\
# comment
description: |
  line one
  line two
""",
        encoding="utf-8",
    )
    key = ".description"
    proc = subprocess.run(
        ["yq", f"{key} | (line, .)", str(target)],
        text=True,
        capture_output=True,
        check=True,
    )
    blank_offset = process_file_updates.blank_lines_before_yaml(target)
    found_at = int(proc.stdout.splitlines()[0])
    value_proc = subprocess.run(
        ["yq", key, str(target)],
        text=True,
        capture_output=True,
        check=True,
    )
    value_size = len(value_proc.stdout.splitlines())
    start_block = found_at + blank_offset
    found_path = tmp_path / "found.txt"
    found_lines = proc.stdout.splitlines()
    found_path.write_text(
        "\n".join(found_lines[1:]) + ("\n" if len(found_lines) > 1 else ""),
        encoding="utf-8",
    )

    err, diff_path = process_file_updates.apply_replacement_block(
        target,
        start_block,
        value_size,
        "|line one|line ONE|",
        tmp_path,
    )
    assert err == "Text block size differs from the original"
    assert diff_path is not None


def test_apply_replacement_block_yaml_tooling_greedy_replace(tmp_path: Path) -> None:
    """Greedy replacements that touch multiple lines are detected."""
    _require_yq()
    target = tmp_path / "f.yaml"
    target.write_text("values:\n  - alpha\n  - alpha\n", encoding="utf-8")
    key = ".values[0]"
    proc = subprocess.run(
        ["yq", f"{key} | (line, .)", str(target)],
        text=True,
        capture_output=True,
        check=True,
    )
    blank_offset = process_file_updates.blank_lines_before_yaml(target)
    found_at = int(proc.stdout.splitlines()[0])
    value_proc = subprocess.run(
        ["yq", key, str(target)],
        text=True,
        capture_output=True,
        check=True,
    )
    value_size = len(value_proc.stdout.splitlines())
    start_block = found_at + blank_offset
    found_path = tmp_path / "found.txt"
    found_lines = proc.stdout.splitlines()
    found_path.write_text(
        "\n".join(found_lines[1:]) + ("\n" if len(found_lines) > 1 else ""),
        encoding="utf-8",
    )

    err, diff_path = process_file_updates.apply_replacement_block(
        target,
        start_block,
        value_size,
        "|alpha|beta|",
        tmp_path,
    )
    assert err is not None
    assert "Too many lines replaced" in err
    assert diff_path is not None
