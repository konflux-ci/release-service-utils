"""Tests for `update_infra_deployments`."""

from __future__ import annotations

import json
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


def test_update_script_from_data_empty() -> None:
    """Return `None` when the update script key is missing or blank."""
    assert task._update_script_from_data({}) is None
    assert task._update_script_from_data({"infra-deployment-update-script": ""}) is None


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
