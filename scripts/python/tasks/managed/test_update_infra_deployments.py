"""Tests for `update_infra_deployments`."""

from __future__ import annotations

import json
import typing
from pathlib import Path
from unittest import mock

import pytest

import update_infra_deployments as task
from vcs import github


def _write_snapshot(data_dir: Path) -> None:
    snap = {
        "components": [
            {
                "containerImage": "quay.io/org/img@sha256:abc",
                "source": {
                    "git": {
                        "revision": "rev123",
                        "url": "https://github.com/org/my-app.git",
                    }
                },
            }
        ]
    }
    (data_dir / "snap.json").write_text(json.dumps(snap), encoding="utf-8")


def _task_params(tmp_path: Path, data_dir: Path) -> task.TaskParams:
    return task.TaskParams(
        work_dir=tmp_path / "work",
        data_dir=data_dir,
        data_json_path=Path("data.json"),
        snapshot_path=Path("snap.json"),
        default_target_repo="org/infra",
        default_app_id="1",
        default_installation_id="2",
        github_api_url="https://api.github.com",
        github_app_key_path=tmp_path / "key",
    )


def test_extract_old_revision_new_tag() -> None:
    """Read the removed `newTag` or `digest` value from a unified diff."""
    diff = " context\n-    newTag: abc123\n+    newTag: def456\n"
    assert task._extract_old_revision_from_diff(diff) == "abc123"


def test_extract_old_revision_ignores_version() -> None:
    """Ignore `version` field changes when extracting the old revision."""
    diff = "-    version: 1.2.3\n+    version: 1.3.0\n"
    assert task._extract_old_revision_from_diff(diff) == ""


def test_bash_script_from_data_empty() -> None:
    """Return `None` when the legacy bash script key is missing or blank."""
    assert task._bash_script_from_data({}) is None
    assert task._bash_script_from_data({"infra-deployment-update-script": ""}) is None


def test_sandboxed_script_from_data_empty() -> None:
    """Return `None` when the sandboxed script key is missing or blank."""
    assert task._sandboxed_script_from_data({}) is None
    assert task._sandboxed_script_from_data({"infraDeploymentUpdates": ""}) is None


def test_sandboxed_script_from_data_returns_text() -> None:
    """Return the stripped script text when the key is present."""
    data = {"infraDeploymentUpdates": "  replace('f', r'x', 'y')  "}
    assert task._sandboxed_script_from_data(data) == "replace('f', r'x', 'y')"


def test_github_app_ids() -> None:
    """Read GitHub App ID and installation ID from data JSON."""
    data = {
        "githubAppID": 9,
        "githubAppInstallationID": 8,
    }
    assert task._github_app_ids(
        data,
        default_app_id="1",
        default_installation_id="2",
    ) == ("9", "8")


def test_run_update_script_prints_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Forward bash stdout from the update script to the process stdout."""
    task._run_update_script("echo hello\n", tmp_path)
    assert "hello" in capsys.readouterr().out


def test_run_update_script_prints_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Forward bash stderr from the update script to the process stderr."""
    task._run_update_script("echo err 1>&2\n", tmp_path)
    assert "err" in capsys.readouterr().err


def test_collect_apply_result() -> None:
    """Collect old revision and changed paths from the clone after the script."""
    snap = task.SnapshotContext("rev", "https://github.com/org/app", "img")
    with (
        mock.patch(
            "update_infra_deployments.git.working_tree_diff",
            return_value="-    newTag: x\n",
        ),
        mock.patch(
            "update_infra_deployments.git.changed_paths_from_status",
            return_value=["a.yaml"],
        ),
    ):
        result = task._collect_apply_result(snap, Path("/repo"))
    assert result.snap is snap
    assert result.old_revision == "x"
    assert result.changed_paths == ["a.yaml"]


def test_build_pr_description_appends_changelog() -> None:
    """Keep prior changelog entries and append new commits plus the PR link."""
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with (
        mock.patch(
            "update_infra_deployments.github.pull_request_url_for_commit_sha",
            return_value="https://github.com/o/r/pull/1",
        ),
        mock.patch(
            "update_infra_deployments.github.compare_changelog",
            return_value="## Changelog\n- new item",
        ),
        mock.patch(
            "update_infra_deployments.image_ref.resolve_quay_digest_to_git_sha",
            return_value="oldrev",
        ),
    ):
        body = task._build_pr_description(
            session,
            existing_body="Included PRs:\r\n- old\r\n\r\n## Changelog\r\n- stale",
            origin_repo="https://github.com/org/repo",
            revision="newrev",
            old_revision="sha256:abc",
            container_image="quay.io/org/img",
        )
    assert "stale" in body
    assert "new item" in body
    assert "## Changelog" in body
    assert "pull/1" in body


def test_merge_changelog_section_dedupes_lines() -> None:
    """Do not duplicate changelog list lines already in the PR body."""
    body = "Links\n\n## Changelog\n- same"
    merged = task._merge_changelog_section(body, "## Changelog\n- same\n- other")
    assert merged.count("- same") == 1
    assert "- other" in merged


def test_merge_changelog_section_finds_lf_only_marker() -> None:
    """Detect an existing LF-only changelog section in a CRLF PR body."""
    body = "Links\r\n\r\n## Changelog\r\n- stale"
    merged = task._merge_changelog_section(body, "## Changelog\n- new")
    assert merged.count("## Changelog") == 1
    assert "- stale" in merged
    assert "- new" in merged


def test_merge_changelog_section_no_new_items() -> None:
    """Return the body unchanged when the new changelog has no list items."""
    body = "Links\n\n## Changelog\n- existing"
    merged = task._merge_changelog_section(body, "just a header, no items")
    assert merged == body


def test_build_pr_description_without_changelog_rev() -> None:
    """Omit the changelog block when there is no resolvable old revision."""
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with mock.patch(
        "update_infra_deployments.github.pull_request_url_for_commit_sha",
        return_value="https://github.com/o/r/pull/2",
    ):
        body = task._build_pr_description(
            session,
            existing_body=None,
            origin_repo="https://github.com/org/repo",
            revision="newrev",
            old_revision="",
            container_image="img",
        )
    assert body.startswith("Included PRs:")
    assert "## Changelog" not in body


def test_build_pr_description_missing_pr_link() -> None:
    """Omit the PR link line when no source PR is found for the revision."""
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with mock.patch(
        "update_infra_deployments.github.pull_request_url_for_commit_sha",
        return_value=None,
    ):
        body = task._build_pr_description(
            session,
            existing_body="Included PRs:\r\n- old-link",
            origin_repo="https://github.com/org/repo",
            revision="newrev",
            old_revision="",
            container_image="img",
        )
    assert "old-link" in body
    assert "None" not in body
    assert "newrev" not in body


def test_build_pr_description_uses_tag_old_revision() -> None:
    """Pass a non-digest old revision directly to the compare API."""
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with (
        mock.patch(
            "update_infra_deployments.github.pull_request_url_for_commit_sha",
            return_value="https://github.com/o/r/pull/3",
        ),
        mock.patch(
            "update_infra_deployments.github.compare_changelog",
            return_value="## Changelog\n- item",
        ) as compare,
    ):
        task._build_pr_description(
            session,
            existing_body=None,
            origin_repo="https://github.com/org/repo",
            revision="newrev",
            old_revision="v1.0",
            container_image="img",
        )
    compare.assert_called_once_with(
        session,
        "https://github.com/org/repo",
        "v1.0",
        "newrev",
    )


def test_snapshot_from_params(tmp_path: Path) -> None:
    """Load snapshot context from the configured snapshot file path."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_snapshot(data_dir)
    params = _task_params(tmp_path, data_dir)
    snap = task._snapshot_from_params(params)
    assert snap.revision == "rev123"
    assert snap.origin_repo == "https://github.com/org/my-app"


def test_run_patched_script(tmp_path: Path) -> None:
    """Replace `{{ revision }}` and invoke bash in the clone directory."""
    with mock.patch("update_infra_deployments._run_update_script") as run:
        task._run_patched_script('echo "{{ revision }}"', "rev123", tmp_path)
    run.assert_called_once_with('echo "rev123"', tmp_path)


def test_create_or_update_pr_no_changed_paths() -> None:
    """Skip GitHub calls when the update script changed no files."""
    params = _task_params(Path("/tmp"), Path("/tmp"))
    apply_result = task.ApplyResult(
        snap=task.SnapshotContext("r", "https://github.com/o/a", "img"),
        old_revision="",
        changed_paths=[],
    )
    with mock.patch("update_infra_deployments.github.open_session") as open_session:
        task._create_or_update_pr(
            params,
            {},
            target_repo="org/infra",
            clone_dir=Path("/cloned"),
            apply_result=apply_result,
        )
    open_session.assert_not_called()


def test_create_or_update_pr_happy_path(tmp_path: Path) -> None:
    """Push commits, open or refresh PR, and update the body."""
    params = _task_params(tmp_path, tmp_path)
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with (
        mock.patch("update_infra_deployments.github.open_session", return_value=session),
        mock.patch("update_infra_deployments.github.force_push_updated_files"),
        mock.patch(
            "update_infra_deployments.github.create_pull_request",
            return_value={"url": "https://api.github.com/pull/1", "body": "old"},
        ),
        mock.patch(
            "update_infra_deployments._build_pr_description",
            return_value="new body",
        ),
        mock.patch("update_infra_deployments.github.update_pull_request_body") as update,
    ):
        task._create_or_update_pr(
            params,
            {"githubAppID": "1"},
            target_repo="org/infra",
            clone_dir=tmp_path,
            apply_result=task.ApplyResult(
                snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
                old_revision="old",
                changed_paths=["a.yaml"],
            ),
        )
    update.assert_called_once()


def test_create_or_update_pr_finds_existing_when_create_missing_url(tmp_path: Path) -> None:
    """Reuse an existing bot PR when create returns no `url` field."""
    params = _task_params(tmp_path, tmp_path)
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with (
        mock.patch("update_infra_deployments.github.open_session", return_value=session),
        mock.patch("update_infra_deployments.github.force_push_updated_files"),
        mock.patch(
            "update_infra_deployments.github.create_pull_request",
            return_value={"message": "already exists"},
        ),
        mock.patch(
            "update_infra_deployments.github.find_open_pull_request_by_branch",
            return_value={"url": "https://api.github.com/pull/9", "body": "b"},
        ),
        mock.patch("update_infra_deployments._build_pr_description", return_value="nb"),
        mock.patch("update_infra_deployments.github.update_pull_request_body"),
    ):
        task._create_or_update_pr(
            params,
            {},
            target_repo="org/infra",
            clone_dir=tmp_path,
            apply_result=task.ApplyResult(
                snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
                old_revision="",
                changed_paths=["a.yaml"],
            ),
        )


def test_create_or_update_pr_raises_when_pr_missing(tmp_path: Path) -> None:
    """Raise when create fails and no matching bot PR exists."""
    params = _task_params(tmp_path, tmp_path)
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with (
        mock.patch("update_infra_deployments.github.open_session", return_value=session),
        mock.patch("update_infra_deployments.github.force_push_updated_files"),
        mock.patch(
            "update_infra_deployments.github.create_pull_request",
            return_value={"message": "nope"},
        ),
        mock.patch(
            "update_infra_deployments.github.find_open_pull_request_by_branch",
            return_value=None,
        ),
    ):
        with pytest.raises(RuntimeError, match="PR not created"):
            task._create_or_update_pr(
                params,
                {},
                target_repo="org/infra",
                clone_dir=tmp_path,
                apply_result=task.ApplyResult(
                    snap=task.SnapshotContext("r", "https://github.com/o/a", "img"),
                    old_revision="",
                    changed_paths=["a.yaml"],
                ),
            )


def test_create_or_update_pr_raises_when_body_missing(tmp_path: Path) -> None:
    """Raise when the PR JSON has a URL but no `body` field."""
    params = _task_params(tmp_path, tmp_path)
    session = github.GitHubAppSession(api_url="https://api.github.com", token="t")
    with (
        mock.patch("update_infra_deployments.github.open_session", return_value=session),
        mock.patch("update_infra_deployments.github.force_push_updated_files"),
        mock.patch(
            "update_infra_deployments.github.create_pull_request",
            return_value={"url": "https://api.github.com/pull/1"},
        ),
    ):
        with pytest.raises(RuntimeError, match="PR not created"):
            task._create_or_update_pr(
                params,
                {},
                target_repo="org/infra",
                clone_dir=tmp_path,
                apply_result=task.ApplyResult(
                    snap=task.SnapshotContext("r", "https://github.com/o/a", "img"),
                    old_revision="",
                    changed_paths=["a.yaml"],
                ),
            )


def test_run_update_infra_deployments_syncs_main(tmp_path: Path) -> None:
    """Sync the clone to origin/main before running the update script."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_snapshot(data_dir)
    (data_dir / "data.json").write_text(
        json.dumps({"infra-deployment-update-script": "true"}),
        encoding="utf-8",
    )
    params = _task_params(tmp_path, data_dir)
    clone_dir = tmp_path / "cloned"
    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            return_value=clone_dir,
        ),
        mock.patch("update_infra_deployments.git.sync_to_origin_main") as sync,
        mock.patch.object(task, "_run_patched_script"),
        mock.patch.object(
            task,
            "_collect_apply_result",
            return_value=task.ApplyResult(
                snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
                old_revision="",
                changed_paths=[],
            ),
        ),
        mock.patch.object(task, "_create_or_update_pr"),
    ):
        task.run_update_infra_deployments(params)
    sync.assert_called_once_with(clone_dir)


def test_run_update_infra_deployments_creates_work_dir(tmp_path: Path) -> None:
    """Create ``PARAM_WORK_DIR`` when missing and remove a stale ``cloned`` tree."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_snapshot(data_dir)
    (data_dir / "data.json").write_text(
        json.dumps({"infra-deployment-update-script": "true"}),
        encoding="utf-8",
    )
    work_dir = tmp_path / "missing" / "work"
    stale_clone = work_dir / "cloned"
    stale_clone.mkdir(parents=True)
    (stale_clone / "old.txt").write_text("stale", encoding="utf-8")
    params = task.TaskParams(
        work_dir=work_dir,
        data_dir=data_dir,
        data_json_path=Path("data.json"),
        snapshot_path=Path("snap.json"),
        default_target_repo="org/infra",
        default_app_id="1",
        default_installation_id="2",
        github_api_url="https://api.github.com",
        github_app_key_path=tmp_path / "key",
    )
    clone_dir = work_dir / "cloned"
    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            return_value=clone_dir,
        ) as clone,
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_run_patched_script"),
        mock.patch.object(
            task,
            "_collect_apply_result",
            return_value=task.ApplyResult(
                snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
                old_revision="",
                changed_paths=[],
            ),
        ),
        mock.patch.object(task, "_create_or_update_pr"),
    ):
        task.run_update_infra_deployments(params)
    assert work_dir.is_dir()
    assert not stale_clone.exists()
    clone.assert_called_once_with(
        work_dir,
        "https://github.com/org/infra.git",
        directory_name="cloned",
    )


def test_run_update_infra_deployments_no_script(tmp_path: Path) -> None:
    """Exit early without cloning when no update script is configured."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "data.json").write_text("{}", encoding="utf-8")
    params = _task_params(tmp_path, data_dir)
    with mock.patch("update_infra_deployments.git.clone") as clone:
        with mock.patch.object(task, "_create_or_update_pr") as create_pr:
            task.run_update_infra_deployments(params)
    clone.assert_not_called()
    create_pr.assert_not_called()


def test_run_update_infra_deployments_bash_deprecation_warning(
    tmp_path: Path,
) -> None:
    """Emit a deprecation warning when using the legacy bash script field."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_snapshot(data_dir)
    (data_dir / "data.json").write_text(
        json.dumps({"infra-deployment-update-script": "true"}),
        encoding="utf-8",
    )
    params = _task_params(tmp_path, data_dir)
    clone_dir = tmp_path / "cloned"
    with (
        mock.patch("update_infra_deployments.git.clone", return_value=clone_dir),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_run_patched_script"),
        mock.patch.object(
            task,
            "_collect_apply_result",
            return_value=task.ApplyResult(
                snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
                old_revision="",
                changed_paths=[],
            ),
        ),
        mock.patch.object(task, "_create_or_update_pr"),
        pytest.warns(DeprecationWarning, match="infra-deployment-update-script"),
    ):
        task.run_update_infra_deployments(params)


def test_run_update_infra_deployments_sandboxed_script(tmp_path: Path) -> None:
    """Use the sandboxed path when infraDeploymentUpdates is present."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_snapshot(data_dir)
    (data_dir / "data.json").write_text(
        json.dumps({"infraDeploymentUpdates": "replace('f', r'x', 'y')"}),
        encoding="utf-8",
    )
    params = _task_params(tmp_path, data_dir)
    clone_dir = tmp_path / "cloned"
    with (
        mock.patch("update_infra_deployments.git.clone", return_value=clone_dir),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "execute_infra_updates") as execute,
        mock.patch.object(
            task,
            "_collect_apply_result",
            return_value=task.ApplyResult(
                snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
                old_revision="",
                changed_paths=[],
            ),
        ),
        mock.patch.object(task, "_create_or_update_pr"),
    ):
        task.run_update_infra_deployments(params)
    execute.assert_called_once()


def test_run_update_infra_deployments_sandboxed_preferred_over_bash(
    tmp_path: Path,
) -> None:
    """Use the sandboxed script when both fields are present."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_snapshot(data_dir)
    (data_dir / "data.json").write_text(
        json.dumps(
            {
                "infraDeploymentUpdates": "replace('f', r'x', 'y')",
                "infra-deployment-update-script": "echo hello",
            }
        ),
        encoding="utf-8",
    )
    params = _task_params(tmp_path, data_dir)
    clone_dir = tmp_path / "cloned"
    with (
        mock.patch("update_infra_deployments.git.clone", return_value=clone_dir),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "execute_infra_updates") as execute,
        mock.patch.object(task, "_run_patched_script") as run_bash,
        mock.patch.object(
            task,
            "_collect_apply_result",
            return_value=task.ApplyResult(
                snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
                old_revision="",
                changed_paths=[],
            ),
        ),
        mock.patch.object(task, "_create_or_update_pr"),
    ):
        task.run_update_infra_deployments(params)
    execute.assert_called_once()
    run_bash.assert_not_called()


def test_run_update_infra_deployments_full_flow(tmp_path: Path) -> None:
    """Run clone, apply, and PR steps when a script is present."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_snapshot(data_dir)
    (data_dir / "data.json").write_text(
        json.dumps({"infra-deployment-update-script": "true"}),
        encoding="utf-8",
    )
    params = _task_params(tmp_path, data_dir)
    apply_result = task.ApplyResult(
        snap=task.SnapshotContext("rev", "https://github.com/org/my-app", "img"),
        old_revision="old",
        changed_paths=["a.yaml"],
    )
    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            return_value=tmp_path / "cloned",
        ),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_snapshot_from_params", return_value=apply_result.snap),
        mock.patch.object(task, "_run_patched_script"),
        mock.patch.object(task, "_collect_apply_result", return_value=apply_result),
        mock.patch.object(task, "_create_or_update_pr") as create_pr,
    ):
        task.run_update_infra_deployments(params)
    create_pr.assert_called_once()


def test_params_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Build `TaskParams` from Tekton-style environment variables."""
    monkeypatch.setenv("PARAM_WORK_DIR", str(tmp_path / "w"))
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path / "d"))
    monkeypatch.setenv("PARAM_DATA_JSON_PATH", "data.json")
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DEFAULT_TARGET_GH_REPO", "org/infra")
    monkeypatch.setenv("PARAM_DEFAULT_GITHUB_APP_ID", "1")
    monkeypatch.setenv("PARAM_DEFAULT_GITHUB_APP_INSTALLATION_ID", "2")
    monkeypatch.setenv("GITHUB_API_URL", "https://api.github.com")
    monkeypatch.setenv("GITHUBAPP_KEY_PATH", str(tmp_path / "key"))
    params = task._params_from_env()
    assert params.work_dir == tmp_path / "w"


def test_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """`main` loads params from the environment and runs the workflow."""
    monkeypatch.setattr(task, "run_update_infra_deployments", lambda _p: None)
    monkeypatch.setattr(task, "_params_from_env", lambda: mock.MagicMock())
    assert task.main() == 0


def test_main_entrypoint_exits_zero() -> None:
    """The module entrypoint exits with the return code from `main`."""
    fake_params = mock.MagicMock()
    with (
        mock.patch.object(task, "_params_from_env", return_value=fake_params),
        mock.patch.object(task, "run_update_infra_deployments") as run,
    ):
        with pytest.raises(SystemExit) as exc:
            raise SystemExit(task.main())
    assert exc.value.code == 0
    run.assert_called_once_with(fake_params)


# --- Integration tests ---
# These exercise run_update_infra_deployments end-to-end: real data.json,
# real snapshot, real sandbox execution, real file mutations.  Only git
# and GitHub calls are mocked (no repo to clone, no remote to push to).


def _setup_integration(
    tmp_path: Path,
    data: dict,
    snapshot: dict,
    clone_files: dict[str, str],
) -> tuple[task.TaskParams, Path]:
    """Write data.json, snapshot, and return params + clone_dir path.

    Files in *clone_files* are NOT written yet — use ``_mock_clone`` to
    create a ``git.clone`` side-effect that populates them when called
    (the real flow does ``rmtree`` before cloning).
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "data.json").write_text(json.dumps(data), encoding="utf-8")
    (data_dir / "snap.json").write_text(json.dumps(snapshot), encoding="utf-8")
    clone_dir = tmp_path / "work" / "cloned"
    params = _task_params(tmp_path, data_dir)
    return params, clone_dir


def _mock_clone(clone_dir: Path, clone_files: dict[str, str]) -> typing.Callable[..., Path]:
    """Return a ``git.clone`` side-effect that writes *clone_files* into *clone_dir*."""

    def _side_effect(*_args: typing.Any, **_kwargs: typing.Any) -> Path:
        for rel_path, content in clone_files.items():
            target = clone_dir / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        return clone_dir

    return _side_effect


_INTEGRATION_SNAPSHOT = {
    "components": [
        {
            "name": "my-component",
            "containerImage": "quay.io/org/img:v1.0_rc1@sha256:abc",
            "source": {
                "git": {
                    "revision": "newrev789",
                    "url": "https://github.com/org/my-app.git",
                }
            },
        }
    ]
}


def test_integration_sandboxed_git_ref_and_tag(tmp_path: Path) -> None:
    """End-to-end: data.json with sandboxed script updates git ref and newTag."""
    kustomization = (
        "resources:\n"
        "  - https://github.com/org/repo/config?ref=old123\n"
        "images:\n"
        "  - name: quay.io/org/component\n"
        "    newTag: old123\n"
    )
    script = (
        "replace('comp/kustomization.yaml',"
        " r'(config\\?ref=)\\S+', r'\\1{{ revision }}')\n"
        "replace('comp/kustomization.yaml',"
        " r'(newTag: ).*', r'\\1{{ revision }}')\n"
    )
    clone_files = {"comp/kustomization.yaml": kustomization}
    params, clone_dir = _setup_integration(
        tmp_path,
        data={"infraDeploymentUpdates": script},
        snapshot=_INTEGRATION_SNAPSHOT,
        clone_files=clone_files,
    )

    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            side_effect=_mock_clone(clone_dir, clone_files),
        ),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_create_or_update_pr"),
        mock.patch(
            "update_infra_deployments.git.working_tree_diff",
            return_value="",
        ),
        mock.patch(
            "update_infra_deployments.git.changed_paths_from_status",
            return_value=[],
        ),
    ):
        task.run_update_infra_deployments(params)

    text = (clone_dir / "comp/kustomization.yaml").read_text(encoding="utf-8")
    assert "?ref=newrev789" in text
    assert "newTag: newrev789" in text
    assert "old123" not in text


def test_integration_sandboxed_context_aware(tmp_path: Path) -> None:
    """End-to-end: context-aware replacement leaves unrelated images alone."""
    kustomization = (
        "images:\n"
        "  - newName: quay.io/org/other\n"
        "    newTag: keep\n"
        "  - newName: quay.io/org/target\n"
        "    newTag: old123\n"
    )
    script = (
        "replace('comp/kustomization.yaml',"
        " r'(newTag:\\s*).*', r'\\1{{ revision }}',"
        " after=r'newName:\\s*quay\\.io/org/target')\n"
    )
    clone_files = {"comp/kustomization.yaml": kustomization}
    params, clone_dir = _setup_integration(
        tmp_path,
        data={"infraDeploymentUpdates": script},
        snapshot=_INTEGRATION_SNAPSHOT,
        clone_files=clone_files,
    )

    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            side_effect=_mock_clone(clone_dir, clone_files),
        ),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_create_or_update_pr"),
        mock.patch(
            "update_infra_deployments.git.working_tree_diff",
            return_value="",
        ),
        mock.patch(
            "update_infra_deployments.git.changed_paths_from_status",
            return_value=[],
        ),
    ):
        task.run_update_infra_deployments(params)

    text = (clone_dir / "comp/kustomization.yaml").read_text(encoding="utf-8")
    assert "newTag: newrev789" in text
    assert "newTag: keep" in text
    assert text.count("newTag: newrev789") == 1


def test_integration_sandboxed_dynamic_tag(tmp_path: Path) -> None:
    """End-to-end: tag() reads from the real snapshot file."""
    generator = "spec:\n  version: 0.1.0\n"
    script = (
        "v = tag('my-component', r'.*\\..*')\n"
        "replace('gen.yaml', r'(version: ).*', r'\\g<1>' + v)\n"
    )
    clone_files = {"gen.yaml": generator}
    params, clone_dir = _setup_integration(
        tmp_path,
        data={"infraDeploymentUpdates": script},
        snapshot=_INTEGRATION_SNAPSHOT,
        clone_files=clone_files,
    )

    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            side_effect=_mock_clone(clone_dir, clone_files),
        ),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_create_or_update_pr"),
        mock.patch(
            "update_infra_deployments.git.working_tree_diff",
            return_value="",
        ),
        mock.patch(
            "update_infra_deployments.git.changed_paths_from_status",
            return_value=[],
        ),
    ):
        task.run_update_infra_deployments(params)

    text = (clone_dir / "gen.yaml").read_text(encoding="utf-8")
    assert "version: v1.0+rc1" in text
    assert "version: 0.1.0" not in text


def test_integration_sandboxed_multi_file_loop(tmp_path: Path) -> None:
    """End-to-end: sandboxed for-loop updates the same pattern across files."""
    kustomization = "images:\n  - newTag: old123\n"
    script = (
        "for env in ['dev', 'stg']:\n"
        "    replace(f'comp/{env}/k.yaml',"
        " r'(newTag: ).*', r'\\1{{ revision }}')\n"
    )
    clone_files = {
        "comp/dev/k.yaml": kustomization,
        "comp/stg/k.yaml": kustomization,
    }
    params, clone_dir = _setup_integration(
        tmp_path,
        data={"infraDeploymentUpdates": script},
        snapshot=_INTEGRATION_SNAPSHOT,
        clone_files=clone_files,
    )

    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            side_effect=_mock_clone(clone_dir, clone_files),
        ),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_create_or_update_pr"),
        mock.patch(
            "update_infra_deployments.git.working_tree_diff",
            return_value="",
        ),
        mock.patch(
            "update_infra_deployments.git.changed_paths_from_status",
            return_value=[],
        ),
    ):
        task.run_update_infra_deployments(params)

    for env in ["dev", "stg"]:
        text = (clone_dir / f"comp/{env}/k.yaml").read_text(encoding="utf-8")
        assert "newTag: newrev789" in text
        assert "old123" not in text


def test_integration_bash_fallback(tmp_path: Path) -> None:
    """End-to-end: legacy bash path still works when no sandboxed script."""
    kustomization = "images:\n  - newTag: old123\n"
    clone_files = {"comp/k.yaml": kustomization}
    params, clone_dir = _setup_integration(
        tmp_path,
        data={"infra-deployment-update-script": ("sed -i 's/old123/newrev789/' comp/k.yaml")},
        snapshot=_INTEGRATION_SNAPSHOT,
        clone_files=clone_files,
    )

    with (
        mock.patch(
            "update_infra_deployments.git.clone",
            side_effect=_mock_clone(clone_dir, clone_files),
        ),
        mock.patch("update_infra_deployments.git.sync_to_origin_main"),
        mock.patch.object(task, "_create_or_update_pr"),
        mock.patch(
            "update_infra_deployments.git.working_tree_diff",
            return_value="",
        ),
        mock.patch(
            "update_infra_deployments.git.changed_paths_from_status",
            return_value=[],
        ),
        pytest.warns(DeprecationWarning, match="infra-deployment-update-script"),
    ):
        task.run_update_infra_deployments(params)

    text = (clone_dir / "comp/k.yaml").read_text(encoding="utf-8")
    assert "newTag: newrev789" in text
    assert "old123" not in text


# --- Sandbox tests ---


def test_validate_path_rejects_absolute(tmp_path: Path) -> None:
    """Reject absolute paths."""
    with pytest.raises(task.InfraReplacementError, match="absolute"):
        task._validate_path("/etc/passwd", tmp_path)


def test_validate_path_rejects_traversal(tmp_path: Path) -> None:
    """Reject paths that escape the clone directory."""
    with pytest.raises(task.InfraReplacementError, match="traversal"):
        task._validate_path("../escape", tmp_path)


def test_validate_path_accepts_relative(tmp_path: Path) -> None:
    """Accept a valid relative path inside the clone directory."""
    target = tmp_path / "sub" / "file.yaml"
    target.parent.mkdir()
    target.touch()
    result = task._validate_path("sub/file.yaml", tmp_path)
    assert result == target.resolve()


def test_extract_image_tag_with_digest() -> None:
    """Extract the tag from a reference that has both tag and digest."""
    ref = "quay.io/org/img:v1.2.3@sha256:abc"
    assert task._extract_image_tag(ref) == "v1.2.3"


def test_extract_image_tag_no_digest() -> None:
    """Extract the tag from a reference without a digest."""
    assert task._extract_image_tag("quay.io/org/img:latest") == "latest"


def test_extract_image_tag_digest_only() -> None:
    """Return empty when only a digest is present."""
    assert task._extract_image_tag("quay.io/org/img@sha256:abc") == ""


def test_extract_image_tag_no_tag_no_digest() -> None:
    """Return empty when there is no tag or digest."""
    assert task._extract_image_tag("quay.io/org/img") == ""


def test_extract_image_tag_port_in_registry() -> None:
    """Return empty when the colon belongs to a registry port, not a tag."""
    assert task._extract_image_tag("localhost:5000/org/img") == ""


def test_normalize_oci_tag() -> None:
    """Replace underscores with plus signs for OCI semver normalization."""
    assert task._normalize_oci_tag("1.2.3_beta") == "1.2.3+beta"


def test_replace_on_line_after() -> None:
    """Replace only on lines immediately following the after-pattern match."""
    content = "name: foo\nnewTag: old\nname: bar\nnewTag: old\n"
    result = task._replace_on_line_after(
        content,
        r"(newTag:\s*).*",
        r"\1new",
        r"name: foo",
    )
    assert "name: foo\nnewTag: new\n" in result
    assert result.count("newTag: new") == 1


def test_replace_nth_match() -> None:
    """Replace only the nth occurrence of a regex match."""
    content = "tag: a\ntag: b\ntag: c\n"
    result = task._replace_nth_match(content, r"(tag: ).", r"\1X", 2)
    assert result == "tag: a\ntag: X\ntag: c\n"


def test_replace_simple_sed(tmp_path: Path) -> None:
    """Pattern 1: simple regex substitution via the sandbox."""
    target = tmp_path / "kustomization.yaml"
    target.write_text("  newTag: oldrev\n", encoding="utf-8")
    script = "replace('kustomization.yaml'," " r'(newTag: ).*', r'\\1{{ revision }}')"
    task.execute_infra_updates(script, tmp_path, {}, "newrev")
    assert "newTag: newrev" in target.read_text(encoding="utf-8")


def test_replace_context_aware(tmp_path: Path) -> None:
    """Pattern 2: replace only after a matching context line."""
    target = tmp_path / "k.yaml"
    target.write_text(
        "newName: quay.io/org/img-a\nnewTag: old\n"
        "newName: quay.io/org/img-b\nnewTag: keep\n",
        encoding="utf-8",
    )
    script = (
        "replace('k.yaml', r'(newTag:\\s*).*', r'\\1{{ revision }}',"
        " after=r'newName:\\s*quay\\.io/org/img-a')"
    )
    task.execute_infra_updates(script, tmp_path, {}, "new")
    text = target.read_text(encoding="utf-8")
    assert "newTag: new" in text
    assert "newTag: keep" in text


def test_replace_nth_occurrence(tmp_path: Path) -> None:
    """Pattern 3: replace only the Nth match."""
    target = tmp_path / "k.yaml"
    target.write_text(
        "  newTag: a\n  newTag: b\n  newTag: c\n",
        encoding="utf-8",
    )
    script = "replace('k.yaml', r'(newTag: ).*', r'\\1{{ revision }}'," " occurrence=2)"
    task.execute_infra_updates(script, tmp_path, {}, "new")
    text = target.read_text(encoding="utf-8")
    assert text == "  newTag: a\n  newTag: new\n  newTag: c\n"


def test_replace_with_tag_lookup(tmp_path: Path) -> None:
    """Pattern 4: use tag() to look up a version from the snapshot."""
    target = tmp_path / "gen.yaml"
    target.write_text("  version: old\n", encoding="utf-8")
    snapshot_data = {
        "components": [
            {
                "name": "caching-helm",
                "containerImage": "quay.io/org/caching-helm:1.2.3@sha256:abc",
            }
        ]
    }
    script = (
        "v = tag('caching-helm', r'.*\\..*')\n"
        "replace('gen.yaml', r'(version: ).*', r'\\g<1>' + v)"
    )
    task.execute_infra_updates(script, tmp_path, snapshot_data, "unused")
    assert "version: 1.2.3" in target.read_text(encoding="utf-8")


def test_tag_oci_normalization() -> None:
    """Apply OCI semver normalization when looking up a tag."""
    snapshot_data = {
        "components": [
            {
                "name": "comp",
                "containerImage": "quay.io/org/comp:1.0_beta",
            }
        ]
    }
    result = task._tag("comp", r".*", snapshot_data=snapshot_data)
    assert result == "1.0+beta"


def test_tag_component_not_found() -> None:
    """Raise when the component name is not in the snapshot."""
    with pytest.raises(task.InfraReplacementError, match="not found"):
        task._tag("missing", r".*", snapshot_data={"components": []})


def test_tag_no_image_tag() -> None:
    """Raise when the component has a digest-only image reference."""
    snapshot_data = {
        "components": [{"name": "comp", "containerImage": "quay.io/org/img@sha256:abc"}]
    }
    with pytest.raises(task.InfraReplacementError, match="no image tag"):
        task._tag("comp", r".*", snapshot_data=snapshot_data)


def test_tag_filter_no_match() -> None:
    """Raise when the tag does not match the filter regex."""
    snapshot_data = {
        "components": [{"name": "comp", "containerImage": "quay.io/org/comp:latest"}]
    }
    with pytest.raises(task.InfraReplacementError, match="does not match"):
        task._tag("comp", r"\d+\.\d+", snapshot_data=snapshot_data)


def test_tag_invalid_filter_regex() -> None:
    """Raise when the filter regex is invalid."""
    snapshot_data = {"components": [{"name": "comp", "containerImage": "quay.io/org/comp:v1"}]}
    with pytest.raises(task.InfraReplacementError, match="invalid filter regex"):
        task._tag("comp", r"[bad", snapshot_data=snapshot_data)


def test_tag_components_not_a_list() -> None:
    """Treat non-list components as empty and raise not-found."""
    with pytest.raises(task.InfraReplacementError, match="not found"):
        task._tag("comp", r".*", snapshot_data={"components": "bogus"})


def test_tag_skips_non_dict_entries() -> None:
    """Skip non-dict entries in the components list."""
    snapshot_data = {
        "components": [
            "not-a-dict",
            {"name": "comp", "containerImage": "quay.io/org/comp:v1"},
        ]
    }
    assert task._tag("comp", r".*", snapshot_data=snapshot_data) == "v1"


def test_tag_skips_non_matching_components() -> None:
    """Skip components whose name does not match."""
    snapshot_data = {
        "components": [
            {"name": "other", "containerImage": "quay.io/org/other:v2"},
            {"name": "comp", "containerImage": "quay.io/org/comp:v1"},
        ]
    }
    assert task._tag("comp", r".*", snapshot_data=snapshot_data) == "v1"


def test_sandbox_blocks_import(tmp_path: Path) -> None:
    """Block import statements in user scripts."""
    with pytest.raises(task.InfraReplacementError, match="execution failed"):
        task.execute_infra_updates("import os", tmp_path, {}, "rev")


def test_sandbox_blocks_open(tmp_path: Path) -> None:
    """Block open() calls in user scripts."""
    with pytest.raises(task.InfraReplacementError, match="execution failed"):
        task.execute_infra_updates("open('/etc/passwd')", tmp_path, {}, "rev")


def test_sandbox_blocks_double_underscore_attributes(tmp_path: Path) -> None:
    """Block access to double-underscore attributes like __class__."""
    with pytest.raises(task.InfraReplacementError, match="compilation failed"):
        task.execute_infra_updates("x = ''.__class__", tmp_path, {}, "rev")


def test_sandbox_allows_string_methods(tmp_path: Path) -> None:
    """Allow normal string methods like split() and strip()."""
    target = tmp_path / "f.yaml"
    target.write_text("  hello  \n  world  \n", encoding="utf-8")
    script = (
        "content = read('f.yaml')\n"
        "content = content.strip().split('\\n')\n"
        "write('f.yaml', '\\n'.join(content) + '\\n')\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, "rev")
    assert target.read_text(encoding="utf-8") == "hello  \n  world\n"


def test_read_file(tmp_path: Path) -> None:
    """Read a file from the cloned repo."""
    target = tmp_path / "data.txt"
    target.write_text("hello\n", encoding="utf-8")
    result = task._read("data.txt", clone_dir=tmp_path)
    assert result == "hello\n"


def test_read_file_not_found(tmp_path: Path) -> None:
    """Raise when reading a file that does not exist."""
    with pytest.raises(task.InfraReplacementError, match="file not found"):
        task._read("missing.txt", clone_dir=tmp_path)


def test_read_path_traversal(tmp_path: Path) -> None:
    """Reject path traversal in read()."""
    with pytest.raises(task.InfraReplacementError, match="traversal"):
        task._read("../escape", clone_dir=tmp_path)


def test_write_file(tmp_path: Path) -> None:
    """Write a file in the cloned repo."""
    task._write("output.txt", "content\n", clone_dir=tmp_path)
    assert (tmp_path / "output.txt").read_text(encoding="utf-8") == "content\n"


def test_write_creates_parent_dirs(tmp_path: Path) -> None:
    """Create parent directories when writing to a nested path."""
    task._write("sub/dir/file.txt", "nested\n", clone_dir=tmp_path)
    assert (tmp_path / "sub/dir/file.txt").read_text(encoding="utf-8") == "nested\n"


def test_write_path_traversal(tmp_path: Path) -> None:
    """Reject path traversal in write()."""
    with pytest.raises(task.InfraReplacementError, match="traversal"):
        task._write("../escape", "bad", clone_dir=tmp_path)


def test_read_write_roundtrip_in_sandbox(tmp_path: Path) -> None:
    """Use read() and write() together in a sandboxed script."""
    target = tmp_path / "k.yaml"
    target.write_text("newTag: old\n", encoding="utf-8")
    script = (
        "content = read('k.yaml')\n"
        "content = content.replace('old', 'new')\n"
        "write('k.yaml', content)\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, "rev")
    assert "newTag: new" in target.read_text(encoding="utf-8")


def test_replace_path_traversal_in_sandbox(tmp_path: Path) -> None:
    """Reject path traversal attempts from within the sandbox."""
    with pytest.raises(task.InfraReplacementError, match="traversal"):
        task.execute_infra_updates("replace('../escape', r'x', 'y')", tmp_path, {}, "rev")


def test_replace_invalid_regex(tmp_path: Path) -> None:
    """Reject invalid regex patterns from within the sandbox."""
    target = tmp_path / "file.yaml"
    target.write_text("content\n", encoding="utf-8")
    with pytest.raises(task.InfraReplacementError, match="invalid regex"):
        task.execute_infra_updates(
            "replace('file.yaml', r'[invalid', 'x')", tmp_path, {}, "rev"
        )


def test_replace_invalid_after_regex(tmp_path: Path) -> None:
    """Reject an invalid ``after`` regex pattern."""
    target = tmp_path / "file.yaml"
    target.write_text("content\n", encoding="utf-8")
    with pytest.raises(task.InfraReplacementError, match="invalid after regex"):
        task._replace("file.yaml", r"x", "y", clone_dir=tmp_path, revision="r", after=r"[bad")


def test_replace_file_not_found(tmp_path: Path) -> None:
    """Raise when the target file does not exist."""
    with pytest.raises(task.InfraReplacementError, match="file not found"):
        task.execute_infra_updates("replace('missing.yaml', r'x', 'y')", tmp_path, {}, "rev")


def test_replace_occurrence_zero(tmp_path: Path) -> None:
    """Reject occurrence < 1."""
    target = tmp_path / "f.yaml"
    target.write_text("x\n", encoding="utf-8")
    with pytest.raises(task.InfraReplacementError, match="occurrence"):
        task.execute_infra_updates(
            "replace('f.yaml', r'x', 'y', occurrence=0)",
            tmp_path,
            {},
            "rev",
        )


def test_replace_after_and_occurrence_mutually_exclusive(tmp_path: Path) -> None:
    """Reject when both after and occurrence are provided."""
    target = tmp_path / "f.yaml"
    target.write_text("x\n", encoding="utf-8")
    with pytest.raises(task.InfraReplacementError, match="mutually exclusive"):
        task._replace(
            "f.yaml",
            r"x",
            "y",
            clone_dir=tmp_path,
            revision="r",
            after=r"z",
            occurrence=1,
        )


# --- RPA scenario tests ---
# Each test mirrors a distinct pattern found across production
# ReleasePlanAdmissions, using realistic kustomization content.

_REV = "abc1234def5678"

_KUSTOMIZATION_SINGLE = (
    "apiVersion: kustomize.config.k8s.io/v1beta1\n"
    "kind: Kustomization\n"
    "resources:\n"
    "  - https://github.com/org/repo/config/default?ref=old123\n"
    "images:\n"
    "  - name: quay.io/org/component\n"
    "    newTag: old123\n"
)

_KUSTOMIZATION_MULTI_IMAGE = (
    "images:\n"
    "  - name: quay.io/org/other-image\n"
    "    newName: quay.io/org/other-image\n"
    "    newTag: keep-this\n"
    "  - name: quay.io/org/target-image\n"
    "    newName: quay.io/org/target-image\n"
    "    newTag: old123\n"
)


def test_scenario_git_ref_and_tag(tmp_path: Path) -> None:
    """Replace a git ref and newTag in a single kustomization file."""
    kdir = tmp_path / "components/comp/development"
    kdir.mkdir(parents=True)
    kfile = kdir / "kustomization.yaml"
    kfile.write_text(_KUSTOMIZATION_SINGLE, encoding="utf-8")

    script = (
        "replace('components/comp/development/kustomization.yaml',\n"
        "        r'(config/default\\?ref=)\\S+', r'\\1{{ revision }}')\n"
        "replace('components/comp/development/kustomization.yaml',\n"
        "        r'(newTag: ).*', r'\\1{{ revision }}')\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, _REV)

    text = kfile.read_text(encoding="utf-8")
    assert f"?ref={_REV}" in text
    assert f"newTag: {_REV}" in text
    assert "old123" not in text


def test_scenario_same_replacement_across_multiple_files(tmp_path: Path) -> None:
    """Apply identical git-ref + newTag replacements to two env directories."""
    for env in ["development", "staging/base"]:
        kdir = tmp_path / f"components/comp/{env}"
        kdir.mkdir(parents=True)
        (kdir / "kustomization.yaml").write_text(_KUSTOMIZATION_SINGLE, encoding="utf-8")

    script = (
        "for env in ['development', 'staging/base']:\n"
        "    path = f'components/comp/{env}/kustomization.yaml'\n"
        "    replace(path, r'(config/default\\?ref=)\\S+', r'\\1{{ revision }}')\n"
        "    replace(path, r'(newTag: ).*', r'\\1{{ revision }}')\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, _REV)

    for env in ["development", "staging/base"]:
        text = (tmp_path / f"components/comp/{env}/kustomization.yaml").read_text(
            encoding="utf-8"
        )
        assert f"?ref={_REV}" in text
        assert f"newTag: {_REV}" in text
        assert "old123" not in text


def test_scenario_context_aware_single_image(tmp_path: Path) -> None:
    """Replace newTag only after the line matching a specific newName."""
    kdir = tmp_path / "components/comp/staging"
    kdir.mkdir(parents=True)
    kfile = kdir / "kustomization.yaml"
    kfile.write_text(_KUSTOMIZATION_MULTI_IMAGE, encoding="utf-8")

    script = (
        "replace('components/comp/staging/kustomization.yaml',\n"
        "        r'(newTag:\\s*).*', r'\\1{{ revision }}',\n"
        "        after=r'newName:\\s*quay\\.io/org/target-image')\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, _REV)

    text = kfile.read_text(encoding="utf-8")
    assert f"newTag: {_REV}" in text
    assert "newTag: keep-this" in text
    assert text.count(f"newTag: {_REV}") == 1


def test_scenario_context_aware_multiple_images(tmp_path: Path) -> None:
    """Update several image tags in one file, each identified by newName."""
    content = (
        "images:\n"
        "  - newName: quay.io/org/app\n"
        "    newTag: old1\n"
        "  - newName: quay.io/org/app-init\n"
        "    newTag: old2\n"
        "  - newName: quay.io/org/app-background\n"
        "    newTag: old3\n"
    )
    kdir = tmp_path / "components/comp/development"
    kdir.mkdir(parents=True)
    kfile = kdir / "kustomization.yaml"
    kfile.write_text(content, encoding="utf-8")

    script = (
        "images = ['app', 'app-init', 'app-background']\n"
        "for img in images:\n"
        "    replace('components/comp/development/kustomization.yaml',\n"
        "            r'(newTag:\\s*).*', r'\\1{{ revision }}',\n"
        "            after=r'newName:\\s*quay\\.io/org/' + img + r'$')\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, _REV)

    text = kfile.read_text(encoding="utf-8")
    assert text.count(f"newTag: {_REV}") == 3
    assert "old1" not in text
    assert "old2" not in text
    assert "old3" not in text


def test_scenario_nth_occurrence_two_images(tmp_path: Path) -> None:
    """Replace the 1st and 2nd newTag independently via occurrence."""
    content = (
        "resources:\n"
        "  - https://github.com/org/repo/config?ref=old123\n"
        "images:\n"
        "  - name: quay.io/org/operator\n"
        "    newTag: old-op\n"
        "  - name: quay.io/org/oauth\n"
        "    newTag: old-oauth\n"
    )
    kdir = tmp_path / "components/comp/staging/base"
    kdir.mkdir(parents=True)
    kfile = kdir / "kustomization.yaml"
    kfile.write_text(content, encoding="utf-8")

    script = (
        "path = 'components/comp/staging/base/kustomization.yaml'\n"
        "replace(path,\n"
        "        r'(config\\?ref=)\\S+', r'\\1{{ revision }}')\n"
        "replace(path, r'(newTag: ).*', r'\\1{{ revision }}', occurrence=1)\n"
        "replace(path, r'(newTag: ).*', r'\\1{{ revision }}', occurrence=2)\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, _REV)

    text = kfile.read_text(encoding="utf-8")
    assert f"?ref={_REV}" in text
    assert text.count(f"newTag: {_REV}") == 2
    assert "old-op" not in text
    assert "old-oauth" not in text


def test_scenario_dynamic_tag_from_snapshot(tmp_path: Path) -> None:
    """Look up an OCI tag from the snapshot and write it to multiple files."""
    generator = "spec:\n" "  chart: oci://quay.io/org/helm-chart\n" "  version: 0.1.0\n"
    snapshot_data = {
        "components": [
            {
                "name": "helm-chart",
                "containerImage": "quay.io/org/helm-chart" ":0.2.0_rc1@sha256:abc",
            },
        ]
    }
    for env in ["development", "staging"]:
        gdir = tmp_path / f"components/cache/{env}"
        gdir.mkdir(parents=True)
        (gdir / "generator.yaml").write_text(generator, encoding="utf-8")

    script = (
        "version = tag('helm-chart', r'.*\\..*')\n"
        "for env in ['development', 'staging']:\n"
        "    replace(f'components/cache/{env}/generator.yaml',\n"
        "            r'(version: ).*', r'\\g<1>' + version)\n"
    )
    task.execute_infra_updates(script, tmp_path, snapshot_data, "unused")

    for env in ["development", "staging"]:
        text = (tmp_path / f"components/cache/{env}/generator.yaml").read_text(
            encoding="utf-8"
        )
        assert "version: 0.2.0+rc1" in text
        assert "version: 0.1.0" not in text


def test_scenario_context_aware_digest_to_tag(tmp_path: Path) -> None:
    """Replace a digest field with newTag after matching the image name."""
    content = (
        "images:\n"
        "  - newName: quay.io/org/unrelated\n"
        "    newTag: keep-this\n"
        "  - newName: quay.io/org/proxy\n"
        "    digest: sha256:olddigest\n"
    )
    kdir = tmp_path / "components/comp/staging/base"
    kdir.mkdir(parents=True)
    kfile = kdir / "kustomization.yaml"
    kfile.write_text(content, encoding="utf-8")

    script = (
        "replace('components/comp/staging/base/kustomization.yaml',\n"
        "        r'(digest|newTag):\\s*.*',\n"
        "        r'newTag: {{ revision }}',\n"
        "        after=r'newName:\\s*quay\\.io/org/proxy')\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, _REV)

    text = kfile.read_text(encoding="utf-8")
    assert f"newTag: {_REV}" in text
    assert "newTag: keep-this" in text
    assert "digest:" not in text


def test_scenario_context_aware_with_git_refs(tmp_path: Path) -> None:
    """Combine git ref replacements with context-aware tag updates."""
    content = (
        "resources:\n"
        "  - https://github.com/org/ctrl/deploy/operator?ref=old123\n"
        "  - https://github.com/org/ctrl/deploy/otp?ref=old123\n"
        "images:\n"
        "  - newName: quay.io/org/controller\n"
        "    newTag: old123\n"
        "  - newName: quay.io/org/controller-otp\n"
        "    newTag: old123\n"
    )
    kdir = tmp_path / "components/ctrl/base"
    kdir.mkdir(parents=True)
    kfile = kdir / "kustomization.yaml"
    kfile.write_text(content, encoding="utf-8")

    script = (
        "kfile = 'components/ctrl/base/kustomization.yaml'\n"
        "replace(kfile,\n"
        "        r'(deploy/operator\\?ref=)\\S+', r'\\1{{ revision }}')\n"
        "replace(kfile,\n"
        "        r'(deploy/otp\\?ref=)\\S+', r'\\1{{ revision }}')\n"
        "replace(kfile,\n"
        "        r'(newTag:\\s*).*', r'\\1{{ revision }}',\n"
        "        after=r'newName:\\s*quay\\.io/org/controller$')\n"
        "replace(kfile,\n"
        "        r'(newTag:\\s*).*', r'\\1{{ revision }}',\n"
        "        after=r'newName:\\s*quay\\.io/org/controller-otp$')\n"
    )
    task.execute_infra_updates(script, tmp_path, {}, _REV)

    text = kfile.read_text(encoding="utf-8")
    assert text.count(f"?ref={_REV}") == 2
    assert text.count(f"newTag: {_REV}") == 2
    assert "old123" not in text
