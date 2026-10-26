"""Tests for ``filter_already_released_advisory_rpms``."""

from __future__ import annotations

import json
import subprocess
import sys
import tarfile
from pathlib import Path
from unittest import mock

from git.exc import GitCommandError
import pytest

from release_service_utils.helpers import file
from release_service_utils.helpers import tekton
from release_service_utils.tasks.internal.filter_already_released_advisory_rpms import (
    filter_already_released_advisory_rpms as task,
)

_AUTH_JSON = "release_service_utils.helpers.oci_auth.oci_auth.auth_json"

_RELEASED = "pkg:rpm/redhat/released-rpm@1.0-1.fc44?arch=x86_64"
_NEW = "pkg:rpm/redhat/new-rpm@2.0-1.fc44?arch=x86_64"
_ALL_A = "pkg:rpm/redhat/all-released-a@1.0-1.fc44?arch=x86_64"
_ALL_B = "pkg:rpm/redhat/all-released-b@2.0-1.fc44?arch=x86_64"
_BIND_LEGACY = "pkg:rpm/redhat/bind@9.20.27-0.1.hum1?arch=x86_64&distro=hb&repository_id=repo"
_BIND_EPOCH_32 = (
    "pkg:rpm/redhat/bind@9.20.27-0.1.hum1" "?arch=x86_64&epoch=32&distro=hb&repository_id=repo"
)


def _encode(rows: object) -> str:
    return file.encode_json_gzip_b64(rows)


def _write_secret(mount: Path, *, git_repo: str = "https://gitlab.com/org/repo.git") -> None:
    mount.mkdir(parents=True, exist_ok=True)
    (mount / "git_repo").write_text(git_repo, encoding="utf-8")
    (mount / "gitlab_host").write_text("gitlab.example.com", encoding="utf-8")
    (mount / "gitlab_access_token").write_text("token", encoding="utf-8")
    (mount / "git_author_name").write_text("tester", encoding="utf-8")
    (mount / "git_author_email").write_text("tester@tester", encoding="utf-8")


def _write_advisory(
    repo: Path,
    origin: str,
    subdir: str,
    purls: list[str],
    *,
    spec_type: str = "RHBA",
    name: str = "2025:1601",
) -> None:
    directory = repo / "data" / "advisories" / origin / subdir
    directory.mkdir(parents=True, exist_ok=True)
    artifacts = "\n".join(f"      - purl: {purl}" for purl in purls)
    (directory / "advisory.yaml").write_text(
        f"metadata:\n  name: {name}\n"
        f"spec:\n  type: {spec_type}\n  content:\n    artifacts:\n{artifacts}\n",
        encoding="utf-8",
    )


def _result_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Path]:
    paths = {
        "RESULT_RESULT": tmp_path / "result",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME": tmp_path / "pr",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME": tmp_path / "tr",
        "RESULT_FILTER_RESULTS_ARTIFACT": tmp_path / "artifact",
        "RESULT_ADVISORY_URL": tmp_path / "url",
        "RESULT_ADVISORY_INTERNAL_URL": tmp_path / "iurl",
    }
    for key, path in paths.items():
        monkeypatch.setenv(key, str(path))
    monkeypatch.setattr(task, "COMMAND_LOG_PATH", tmp_path / "stderr.txt")
    return paths


def _param_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRANSFORMED_SNAPSHOT", "abc")
    monkeypatch.setenv("ORIGIN", "test-origin")
    monkeypatch.setenv("OCI_STORAGE", "quay.io/org/repo")
    monkeypatch.setenv("INTERNAL_REQUEST_PIPELINE_RUN_NAME", "pr-1")
    monkeypatch.setenv("TASK_RUN_NAME", "tr-1")


def _patch_run_filter_deps(repo: Path) -> tuple[object, object, object]:
    return (
        mock.patch.object(task.gitlab, "clone_project_sparse", return_value=repo),
        mock.patch.multiple(
            task.gitlab,
            export_env_for_image_helpers=mock.DEFAULT,
            configure_git_oauth2_auth=mock.DEFAULT,
        ),
        mock.patch.object(
            task,
            "push_results_to_oci",
            return_value="oci:quay.io/org/repo@sha256:abc",
        ),
    )


def test_write_results_archive_roundtrip() -> None:
    """Archive contains compact unreleased and in-advisory JSON files."""
    unreleased = [{"purl": _NEW}]
    in_advisory = [{"purl": _RELEASED, "legacy_advisory_purl": False}]
    results_dir = task.write_results_archive(unreleased, in_advisory)
    tar_path = results_dir / "filter-results.tar.gz"
    assert tar_path.is_file()
    assert json.loads((results_dir / "unreleased_rpms.json").read_text()) == unreleased
    assert json.loads((results_dir / "in_advisory_rpms.json").read_text()) == in_advisory
    with tarfile.open(tar_path, "r:gz") as tar:
        names = tar.getnames()
    assert "unreleased_rpms.json" in names
    assert "in_advisory_rpms.json" in names


def test_push_results_to_oci_returns_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Push uses oci_auth, oras options, and returns an oci digest ref."""
    monkeypatch.setattr(task.time, "time", lambda: 1700000000)
    log = tmp_path / "stderr.txt"
    calls: list[list[str]] = []
    stderr_paths: list[Path | None] = []
    auth_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_auth_json(*args: object, **kwargs: object) -> str:
        auth_calls.append((args, kwargs))
        return '{"auths":{}}'

    def fake_run(cmd, **kwargs):
        calls.append([str(x) for x in cmd])
        stderr_paths.append(kwargs.get("stderr_path"))
        if cmd[0] == "oras" and cmd[1] == "push":
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[0] == "oras" and cmd[1] == "manifest":
            return subprocess.CompletedProcess(
                cmd, 0, stdout='{"digest":"sha256:deadbeef"}', stderr=""
            )
        raise AssertionError(cmd)

    monkeypatch.setattr(_AUTH_JSON, fake_auth_json)
    with mock.patch.object(task.subprocess_cmd, "run_cmd", side_effect=fake_run):
        ref = task.push_results_to_oci(
            tmp_path, "quay.io/org/repo", "--insecure", stderr_path=log
        )
    assert ref == "oci:quay.io/org/repo@sha256:deadbeef"
    assert auth_calls == [(("quay.io/org/repo",), {"check": True, "stderr_path": log})]
    assert calls[0][0:2] == ["oras", "push"]
    assert "--insecure" in calls[0]
    assert "filter-results.tar.gz:application/gzip" in calls[0]
    assert "--registry-config" in calls[0]
    assert "--descriptor" in calls[1]
    assert stderr_paths == [log, log]


def test_push_results_to_oci_cleans_auth_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The temporary oras auth file is removed when push fails."""
    seen: list[Path] = []

    def fake_run(cmd, **kwargs):
        if cmd[0] == "oras":
            auth = Path(cmd[cmd.index("--registry-config") + 1])
            seen.append(auth)
            assert auth.is_file()
            raise subprocess.CalledProcessError(1, cmd)
        raise AssertionError(cmd)

    monkeypatch.setattr(_AUTH_JSON, lambda *args, **kwargs: '{"auths":{}}')
    with mock.patch.object(task.subprocess_cmd, "run_cmd", side_effect=fake_run):
        with pytest.raises(tekton.CheckStepError, match="pushing filter results to OCI"):
            task.push_results_to_oci(
                tmp_path, "quay.io/org/repo", "", stderr_path=tmp_path / "stderr.txt"
            )
    assert seen
    assert not seen[0].exists()


def test_run_filter_rejects_non_array_snapshot(tmp_path: Path) -> None:
    """A gzip+base64 object payload is rejected."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    with pytest.raises(ValueError, match="JSON array"):
        task.run_filter(
            _encode({"not": "a-list"}),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
        )


def test_run_filter_rejects_malformed_snapshot_row(tmp_path: Path) -> None:
    """Non-object snapshot rows fail once existing advisories are scanned."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    repo = tmp_path / "repo"
    _write_advisory(repo, "test-origin", "2025/1601", [_RELEASED])
    clone, auth, push = _patch_run_filter_deps(repo)
    with (
        mock.patch.object(task.authentication, "setup_docker_config"),
        clone,
        auth,
        push,
        pytest.raises(TypeError, match="entry 1 must be an object"),
    ):
        task.run_filter(
            _encode(
                [
                    {"name": "released", "purl": _RELEASED, "epoch": "0"},
                    "skip-me",
                ]
            ),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
        )


def test_run_filter_secret_error(tmp_path: Path) -> None:
    """Missing advisory secret files become a CheckStepError."""
    with pytest.raises(tekton.CheckStepError, match="reading the mounted advisory secret"):
        task.run_filter(
            _encode([{"purl": _NEW}]),
            "test-origin",
            "quay.io/org/repo",
            tmp_path / "missing",
            "",
        )


def test_run_filter_clone_git_command_error(tmp_path: Path) -> None:
    """GitPython clone failures are wrapped as a CheckStepError."""
    secret = tmp_path / "secret"
    log = tmp_path / "stderr.txt"
    _write_secret(secret)
    with (
        mock.patch.object(task.authentication, "setup_docker_config"),
        mock.patch.object(task.gitlab, "export_env_for_image_helpers"),
        mock.patch.object(task.gitlab, "configure_git_oauth2_auth"),
        mock.patch.object(
            task.gitlab,
            "clone_project_sparse",
            side_effect=GitCommandError(["git"], 1, "clone failed"),
        ) as clone,
        pytest.raises(tekton.CheckStepError, match="cloning the advisory Git repository"),
    ):
        task.run_filter(
            _encode([{"purl": _NEW}]),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
            stderr_path=log,
        )
    assert clone.call_args.args[1] is None
    assert clone.call_args.kwargs["stderr_path"] == log


def test_run_filter_clone_called_process_error(tmp_path: Path) -> None:
    """CLI clone failures are wrapped as a CheckStepError."""
    secret = tmp_path / "secret"
    log = tmp_path / "stderr.txt"
    _write_secret(secret)
    with (
        mock.patch.object(task.authentication, "setup_docker_config"),
        mock.patch.object(task.gitlab, "export_env_for_image_helpers"),
        mock.patch.object(task.gitlab, "configure_git_oauth2_auth"),
        mock.patch.object(
            task.gitlab,
            "clone_project_sparse",
            side_effect=subprocess.CalledProcessError(1, "git clone"),
        ) as clone,
        pytest.raises(tekton.CheckStepError, match="cloning the advisory Git repository"),
    ):
        task.run_filter(
            _encode([{"purl": _NEW}]),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
            stderr_path=log,
        )
    assert clone.call_args.args[1] is None
    assert clone.call_args.kwargs["stderr_path"] == log


def test_run_filter_no_advisories(tmp_path: Path) -> None:
    """When no advisory dirs exist, every snapshot RPM is unreleased."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    repo = tmp_path / "repo"
    (repo / "data" / "advisories" / "test-origin").mkdir(parents=True)
    clone, auth, push = _patch_run_filter_deps(repo)
    with (
        mock.patch.object(task.authentication, "setup_docker_config"),
        clone as mock_clone,
        auth as gitlab_mocks,
        push as mock_push,
    ):
        output = task.run_filter(
            _encode([{"name": "new", "purl": _NEW}]),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
        )
    gitlab_mocks["export_env_for_image_helpers"].assert_called_once()
    gitlab_mocks["configure_git_oauth2_auth"].assert_called_once()
    assert mock_clone.call_args.args[1] is None
    assert mock_clone.call_args.kwargs["stderr_path"] == task.COMMAND_LOG_PATH
    assert mock_push.call_args.kwargs["stderr_path"] == task.COMMAND_LOG_PATH
    assert output.result == "Success"
    assert output.filter_results_artifact == "oci:quay.io/org/repo@sha256:abc"
    assert output.advisory_url == ""
    unreleased = json.loads(
        (Path(mock_push.call_args[0][0]) / "unreleased_rpms.json").read_text()
    )
    assert unreleased == [{"name": "new", "purl": _NEW}]


def test_run_filter_partial_release(tmp_path: Path) -> None:
    """A mix of released and new RPMs leaves the new RPM unreleased."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    repo = tmp_path / "repo"
    _write_advisory(repo, "test-origin", "2025/1601", [_RELEASED])
    clone, auth, push = _patch_run_filter_deps(repo)
    with mock.patch.object(task.authentication, "setup_docker_config"), clone, auth, push:
        output = task.run_filter(
            _encode(
                [
                    {"name": "released", "purl": _RELEASED, "epoch": "0"},
                    {"name": "new", "purl": _NEW, "epoch": "0"},
                ]
            ),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "--insecure",
        )
    assert output.advisory_url == ""
    assert output.advisory_internal_url == ""
    assert output.filter_results_artifact.endswith("sha256:abc")


def test_run_filter_all_released_single_advisory(tmp_path: Path) -> None:
    """All RPMs from one advisory set public and internal URLs."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    repo = tmp_path / "repo"
    _write_advisory(
        repo,
        "test-origin",
        "2025/1601",
        [_ALL_A, _ALL_B],
        name="2025:1601",
    )
    clone, auth, push = _patch_run_filter_deps(repo)
    with mock.patch.object(task.authentication, "setup_docker_config"), clone, auth, push:
        output = task.run_filter(
            _encode(
                [
                    {"name": "a", "purl": _ALL_A, "epoch": "0"},
                    {"name": "b", "purl": _ALL_B, "epoch": "0"},
                ]
            ),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
        )
    assert output.advisory_url == "https://access.redhat.com/errata/RHBA-2025:1601"
    assert output.advisory_internal_url.endswith(
        "data/advisories/test-origin/2025/1601/advisory.yaml"
    )


def test_run_filter_all_released_multiple_advisories(tmp_path: Path) -> None:
    """RPMs split across advisories do not set advisory_url."""
    secret = tmp_path / "secret"
    _write_secret(secret, git_repo="https://gitlab.com/rhtap-release/org/repo.git")
    repo = tmp_path / "repo"
    _write_advisory(repo, "test-origin", "2025/1601", [_ALL_A], name="2025:1601")
    _write_advisory(repo, "test-origin", "2025/1602", [_ALL_B], name="2025:1602")
    clone, auth, push = _patch_run_filter_deps(repo)
    with mock.patch.object(task.authentication, "setup_docker_config"), clone, auth, push:
        output = task.run_filter(
            _encode(
                [
                    {"name": "a", "purl": _ALL_A, "epoch": "0"},
                    {"name": "b", "purl": _ALL_B, "epoch": "0"},
                ]
            ),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
        )
    assert output.advisory_url == ""
    assert output.advisory_internal_url == ""


def test_run_filter_epoch_qualified_match(tmp_path: Path) -> None:
    """Header epoch 32 matches a qualified advisory PURL; epoch 33 does not."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    repo = tmp_path / "repo"
    _write_advisory(repo, "test-origin", "2025/1601", [_BIND_EPOCH_32], name="2025:1601")
    clone, auth, push = _patch_run_filter_deps(repo)
    with mock.patch.object(task.authentication, "setup_docker_config"), clone, auth, push:
        matched = task.run_filter(
            _encode([{"name": "bind", "purl": _BIND_LEGACY, "epoch": "32"}]),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
        )
        unmatched = task.run_filter(
            _encode([{"name": "bind", "purl": _BIND_LEGACY, "epoch": "33"}]),
            "test-origin",
            "quay.io/org/repo",
            secret,
            "",
        )
    assert matched.advisory_url.endswith("RHBA-2025:1601")
    assert unmatched.advisory_url == ""


def test_main_writes_results(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``main`` writes all Tekton result files and returns 0."""
    paths = _result_env(monkeypatch, tmp_path)
    _param_env(monkeypatch)
    expected = task.FilterOutput(
        result="Success",
        filter_results_artifact="oci:quay.io/r@sha256:abc",
        advisory_url="pub",
        advisory_internal_url="int",
    )
    with mock.patch.object(task, "run_filter", return_value=expected):
        rc = task.main()
    assert rc == 0
    assert paths["RESULT_RESULT"].read_text(encoding="utf-8") == "Success"
    assert paths["RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME"].read_text() == "pr-1"
    assert (
        paths["RESULT_FILTER_RESULTS_ARTIFACT"].read_text(encoding="utf-8")
        == "oci:quay.io/r@sha256:abc"
    )
    assert paths["RESULT_ADVISORY_URL"].read_text(encoding="utf-8") == "pub"
    assert paths["RESULT_ADVISORY_INTERNAL_URL"].read_text(encoding="utf-8") == "int"


def test_main_failure_still_exits_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Operational failures are written to ``RESULT_RESULT`` and exit code stays 0."""
    paths = _result_env(monkeypatch, tmp_path)
    _param_env(monkeypatch)
    with mock.patch.object(
        task,
        "run_filter",
        side_effect=tekton.CheckStepError(
            "reading the mounted advisory secret", OSError("boom")
        ),
    ):
        rc = task.main()
    assert rc == 0
    assert "Failed while reading the mounted advisory secret" in paths[
        "RESULT_RESULT"
    ].read_text(encoding="utf-8")


def test_main_unexpected_exception(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Unexpected exceptions are wrapped and written to ``RESULT_RESULT``."""
    paths = _result_env(monkeypatch, tmp_path)
    _param_env(monkeypatch)
    with mock.patch.object(task, "run_filter", side_effect=RuntimeError("kaboom")):
        rc = task.main()
    assert rc == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "Failed while filtering already released advisory rpms" in text


def test_main_passes_oras_options(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``ORAS_OPTIONS`` from the environment is forwarded to ``run_filter``."""
    _result_env(monkeypatch, tmp_path)
    _param_env(monkeypatch)
    monkeypatch.setenv("ORAS_OPTIONS", "--insecure")
    with mock.patch.object(
        task,
        "run_filter",
        return_value=task.FilterOutput("Success", "oci:x@sha256:a", "", ""),
    ) as run:
        assert task.main() == 0
    assert run.call_args.args == (
        "abc",
        "test-origin",
        "quay.io/org/repo",
        task.ADVISORY_SECRET_MOUNT_DEFAULT,
    )
    assert run.call_args.kwargs["oras_options"] == "--insecure"
    assert run.call_args.kwargs["stderr_path"] == task.COMMAND_LOG_PATH


def test_main_failure_includes_clone_stderr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Clone stderr captured in the command log is appended to ``RESULT_RESULT``."""
    paths = _result_env(monkeypatch, tmp_path)
    _param_env(monkeypatch)
    git_stderr = "fatal: Authentication failed for 'https://gitlab.example.com/org/repo.git'"

    def fail_clone(*_args: object, **_kwargs: object) -> None:
        task.COMMAND_LOG_PATH.write_text(git_stderr + "\n", encoding="utf-8")
        raise tekton.CheckStepError(
            "cloning the advisory Git repository",
            subprocess.CalledProcessError(128, "git clone"),
        )

    with mock.patch.object(task, "run_filter", side_effect=fail_clone):
        assert task.main() == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "Failed while cloning the advisory Git repository" in text
    assert git_stderr in text


def test_main_missing_param_env_raises_system_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Missing input env vars fail fast via ``tekton.require_env``."""
    _result_env(monkeypatch, tmp_path)
    with pytest.raises(SystemExit):
        task.main()


def test_main_missing_result_env_raises_system_exit() -> None:
    """Missing ``RESULT_*`` env vars fail fast via ``tekton.result_paths_from_env``."""
    with pytest.raises(SystemExit):
        task.main()


def test_main_uses_prog_when_argv_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``PROG`` is used when ``sys.argv`` is empty."""
    _result_env(monkeypatch, tmp_path)
    _param_env(monkeypatch)
    monkeypatch.setattr(sys, "argv", [])
    with mock.patch.object(
        task,
        "run_filter",
        return_value=task.FilterOutput("Success", "oci:x@sha256:a", "", ""),
    ):
        assert task.main() == 0


def test_package_main_exports_main() -> None:
    """Importing the package ``__main__`` exposes the task ``main`` function."""
    from release_service_utils.tasks.internal.filter_already_released_advisory_rpms import (
        __main__ as package_main,
    )

    assert package_main.main is task.main


def test_module_entrypoint_requires_env() -> None:
    """``python3 -m`` reaches the package ``__main__`` and exits when env is missing."""
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "release_service_utils.tasks.internal.filter_already_released_advisory_rpms",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 1
    assert "RESULT_RESULT" in proc.stderr
