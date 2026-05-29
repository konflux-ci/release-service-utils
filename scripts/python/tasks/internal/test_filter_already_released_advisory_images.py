"""Tests for `filter_already_released_advisory_images`."""

from __future__ import annotations

import base64
import gzip
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from unittest import mock

import filter_already_released_advisory_images as filt
import pytest
import tekton
from git.exc import GitCommandError


def _encode_snapshot(rows: list[dict[str, object]] | dict[str, object]) -> str:
    payload = json.dumps(rows, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(gzip.compress(payload)).decode("ascii")


def _decode_components(value: str) -> list[str]:
    raw = base64.b64decode(value)
    return json.loads(gzip.decompress(raw).decode("utf-8"))


def _write_secret(mount: Path, *, git_repo: str = "https://gitlab.com/org/repo.git") -> None:
    mount.mkdir(parents=True, exist_ok=True)
    (mount / "git_repo").write_text(git_repo, encoding="utf-8")
    (mount / "gitlab_host").write_text("gitlab.example.com", encoding="utf-8")
    (mount / "gitlab_access_token").write_text("token", encoding="utf-8")
    (mount / "git_author_name").write_text("tester", encoding="utf-8")
    (mount / "git_author_email").write_text("tester@tester", encoding="utf-8")


def _advisory_images_by_num(advisory_num: str) -> list[dict[str, object]]:
    """Mirror catalog ``tests/mocks.sh`` advisory image payloads."""
    mapping: dict[str, list[dict[str, object]]] = {
        "1601": [
            {
                "containerImage": "registry.redhat.io/test@sha256:releasedarch123",
                "tags": ["v1.0"],
                "repository": "registry.redhat.io/test",
            },
            {
                "containerImage": "registry.redhat.io/test@sha256:amd64digest123",
                "tags": ["v1.0"],
                "repository": "registry.redhat.io/test",
            },
            {
                "containerImage": "registry.redhat.io/test@sha256:arm64digest456",
                "tags": ["v1.0"],
                "repository": "registry.redhat.io/test",
            },
        ],
        "1602": [
            {
                "containerImage": "quay.io/test/other-image:1.0.0",
                "tags": ["stable"],
                "repository": "quay.io/test",
            }
        ],
        "1452": [
            {
                "containerImage": "quay.io/test/legacy-image:2.0.0",
                "tags": ["old"],
                "repository": "quay.io/legacy",
            }
        ],
    }
    return mapping.get(advisory_num, [])


def _catalog_yq_fake(args: list[str], *, cwd: Path | None = None) -> str:
    """Return mock ``yq`` JSON for advisory paths under ``test-origin``."""
    if args[0] != "yq":
        return ""
    expression = args[2]
    advisory_path = args[3]
    advisory_num = Path(advisory_path).parent.name
    if expression == ".spec.type":
        return json.dumps("RHBA")
    if expression == ".metadata.name":
        year = Path(advisory_path).parent.parent.name
        return json.dumps(f"{year}:{advisory_num}")
    if expression == ".spec.content.images // []":
        return json.dumps(_advisory_images_by_num(advisory_num))
    return ""


def _setup_repo_tree(repo: Path, origin: str, advisory_nums: list[str]) -> None:
    """Create advisory layout under ``repo/data/advisories/<origin>/``."""
    base = repo / "data" / "advisories" / origin
    if not advisory_nums:
        base.mkdir(parents=True, exist_ok=True)
        return
    for sub in advisory_nums:
        (base / sub).mkdir(parents=True, exist_ok=True)
        (base / sub / "advisory.yaml").write_text(
            "spec: {}\nmetadata: {}\n",
            encoding="utf-8",
        )


def _make_clone_sparse(
    tmp_path: Path,
    *,
    origin: str = "test-origin",
    advisory_nums: list[str] | None = None,
) -> object:
    """Return a ``clone_sparse`` callable that materializes a fake advisory repo tree."""
    repo = tmp_path / "work" / "repo"
    nums = advisory_nums if advisory_nums is not None else ["2025/1601"]

    def _clone() -> Path:
        _setup_repo_tree(repo, origin, nums)
        return repo

    return _clone


def _make_yq_run_cmd(*, fail: bool = False) -> object:
    """Return a ``run_cmd`` fake for ``yq`` invocations."""

    def _run_cmd(args: list[str], *, cwd: Path | None = None) -> str:
        if fail:
            raise subprocess.CalledProcessError(1, "yq", output="yq failed")
        if args[:1] == ["yq"]:
            return _catalog_yq_fake(args, cwd=cwd)
        return ""

    return _run_cmd


def _result_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Path]:
    paths = {
        "RESULT_RESULT": tmp_path / "result",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME": tmp_path / "pr",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME": tmp_path / "tr",
        "RESULT_UNRELEASED_COMPONENTS": tmp_path / "unreleased",
        "RESULT_ADVISORY_URL": tmp_path / "url",
        "RESULT_ADVISORY_INTERNAL_URL": tmp_path / "iurl",
    }
    for key, path in paths.items():
        monkeypatch.setenv(key, str(path))
    return paths


def test_parse_args_ok() -> None:
    """Required flags parse into a namespace."""
    ns = filt.parse_args(
        [
            "--transformed-snapshot",
            "abc",
            "--origin",
            "org",
            "--internal-request-pipeline-run-name",
            "pr",
            "--internal-request-task-run-name",
            "tr",
        ]
    )
    assert ns.transformed_snapshot == "abc"
    assert ns.origin == "org"


def test_parse_args_help_exits() -> None:
    """``--help`` prints usage and exits with code 1."""
    with pytest.raises(SystemExit) as exc:
        filt.parse_args(["--help"])
    assert exc.value.code == 1


def test_parse_args_missing_required_exits() -> None:
    """Missing required flags print usage and exit with code 1."""
    with pytest.raises(SystemExit) as exc:
        filt.parse_args(["--origin", "only-origin"])
    assert exc.value.code == 1


def test_main_rejects_extra_argv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Extra positional arguments are rejected with exit code 2."""
    _result_env(monkeypatch, tmp_path)
    rc = filt.main(
        [
            "prog",
            "--transformed-snapshot",
            "x",
            "--origin",
            "o",
            "--internal-request-pipeline-run-name",
            "pr",
            "--internal-request-task-run-name",
            "tr",
            "extra",
        ]
    )
    assert rc == 2


def test_decode_transformed_snapshot_ok() -> None:
    """Round-trip gzip+base64 snapshot encoding decodes to the original rows."""
    rows = [
        {
            "name": "a",
            "containerImage": "r.io/i:1",
            "tags": ["v1"],
            "repository": "r.io",
        }
    ]
    assert filt.decode_transformed_snapshot(_encode_snapshot(rows)) == rows


def test_decode_transformed_snapshot_invalid_payload() -> None:
    """Malformed snapshot payloads raise `ValueError`."""
    with pytest.raises(ValueError, match="invalid transformed snapshot"):
        filt.decode_transformed_snapshot("not-valid")


def test_decode_transformed_snapshot_not_array() -> None:
    """Non-array JSON raises `ValueError`."""
    with pytest.raises(ValueError, match="must be a JSON array"):
        filt.decode_transformed_snapshot(_encode_snapshot({"x": 1}))


def test_encode_gzipped_base64_json_roundtrip() -> None:
    """Encoded component lists decode back to the same JSON value."""
    value = ["a", "b"]
    encoded = filt.encode_gzipped_base64_json(value)
    assert _decode_components(encoded) == value


def test_unique_component_names_dedupes_and_skips_blank() -> None:
    """Duplicate and blank names are skipped; order is first-seen."""
    rows = [
        {"name": "a"},
        {"name": "a"},
        {"name": ""},
        {"name": 1},
        {"name": "b"},
    ]
    assert filt.unique_component_names(rows) == ["a", "b"]


def test_filter_arch_images_triple_match() -> None:
    """Rows matching containerImage/tags/repository triples are removed."""
    existing = [
        {
            "containerImage": "registry.redhat.io/test@sha256:abc",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        }
    ]
    rows = [
        {
            "name": "released",
            "containerImage": "registry.redhat.io/test@sha256:abc",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        },
        {
            "name": "new",
            "containerImage": "registry.redhat.io/test@sha256:new",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        },
    ]
    out = filt.filter_arch_images(rows, existing)
    assert [row["name"] for row in out] == ["new"]


def test_filter_arch_images_tag_mismatch_keeps_row() -> None:
    """Different tags prevent a triple match."""
    existing = [
        {
            "containerImage": "r.io/i@sha256:abc",
            "tags": ["v1.0"],
            "repository": "r.io/i",
        }
    ]
    row = {
        "name": "x",
        "containerImage": "r.io/i@sha256:abc",
        "tags": ["v2.0"],
        "repository": "r.io/i",
    }
    assert filt.filter_arch_images([row], existing) == [row]


def test_list_advisory_subdirs_newest_first(tmp_path: Path) -> None:
    """Advisories are listed by directory mtime, newest first."""
    base = tmp_path / "data/advisories/origin"
    old = base / "2024" / "100"
    new = base / "2025" / "200"
    old.mkdir(parents=True)
    new.mkdir(parents=True)
    (old / "advisory.yaml").write_text("old: true\n", encoding="utf-8")
    (new / "advisory.yaml").write_text("new: true\n", encoding="utf-8")
    old_time = time.time() - 100
    new_time = time.time()
    os.utime(old, (old_time, old_time))
    os.utime(new, (new_time, new_time))
    assert filt.list_advisory_subdirs(base) == ["2025/200", "2024/100"]


def test_list_advisory_subdirs_missing_dir() -> None:
    """A missing advisory base directory yields an empty list."""
    assert filt.list_advisory_subdirs(Path("/does/not/exist")) == []


def test_list_advisory_subdirs_skips_shallow_paths(tmp_path: Path) -> None:
    """Advisory paths with fewer than two relative segments are ignored."""
    base = tmp_path / "data/advisories/origin"
    shallow = base / "2025"
    shallow.mkdir(parents=True)
    (shallow / "advisory.yaml").write_text("x: 1\n", encoding="utf-8")
    assert filt.list_advisory_subdirs(base) == []


def test_advisory_errata_url_prefixes() -> None:
    """Production and stage errata prefixes are selected from the repo URL."""
    assert "access.redhat.com" in filt.advisory_errata_url_prefix(
        "https://gitlab.com/org/repo.git"
    )
    assert "access.stage.redhat.com" in filt.advisory_errata_url_prefix(
        "https://gitlab.com/rhtap-release/advisories.git"
    )


def test_build_advisory_urls_stage_and_prod() -> None:
    """Public and internal advisory URLs are built for stage and production repos."""
    stage_public, stage_internal = filt.build_advisory_urls(
        "https://gitlab.com/rhtap-release/advisories.git",
        Path("data/advisories/x/2025/1/advisory.yaml"),
        "RHBA",
        "2025:1",
    )
    assert stage_public == "https://access.stage.redhat.com/errata/RHBA-2025:1"
    assert stage_internal == (
        "https://gitlab.com/rhtap-release/advisories/-/raw/main/"
        "data/advisories/x/2025/1/advisory.yaml"
    )

    prod_public, _ = filt.build_advisory_urls(
        "https://gitlab.com/org/advisories.git",
        Path("data/advisories/x/2025/1/advisory.yaml"),
        "RHBA",
        "2025:1",
    )
    assert prod_public == "https://access.redhat.com/errata/RHBA-2025:1"


def test_run_filter_empty_advisory_directory(tmp_path: Path) -> None:
    """An existing but empty advisory tree returns all component names."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    out = filt.run_filter(
        _encode_snapshot([{"name": "c1"}, {"name": "c2"}]),
        "test-origin",
        secret,
        work_dir=tmp_path / "work",
        clone_sparse=_make_clone_sparse(tmp_path, advisory_nums=[]),
    )
    assert _decode_components(out.unreleased_components_b64) == ["c1", "c2"]


def test_run_filter_partial_release(tmp_path: Path) -> None:
    """Released triples are filtered out and unreleased component names remain."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    rows = [
        {
            "name": "released-component",
            "containerImage": "registry.redhat.io/test@sha256:releasedarch123",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        },
        {
            "name": "new-component",
            "containerImage": "registry.redhat.io/test@sha256:newarch456",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        },
    ]
    out = filt.run_filter(
        _encode_snapshot(rows),
        "test-origin",
        secret,
        work_dir=tmp_path / "work",
        clone_sparse=_make_clone_sparse(tmp_path),
        run_cmd=_make_yq_run_cmd(),
    )
    assert out.result == "Success"
    assert _decode_components(out.unreleased_components_b64) == ["new-component"]
    assert out.advisory_url == ""


def test_run_filter_all_released(tmp_path: Path) -> None:
    """When every row is filtered out, advisory URLs are returned."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    rows = [
        {
            "name": "released-component",
            "containerImage": "registry.redhat.io/test@sha256:releasedarch123",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        }
    ]
    out = filt.run_filter(
        _encode_snapshot(rows),
        "test-origin",
        secret,
        work_dir=tmp_path / "work",
        clone_sparse=_make_clone_sparse(tmp_path),
        run_cmd=_make_yq_run_cmd(),
    )
    assert out.result == "Success"
    assert _decode_components(out.unreleased_components_b64) == []
    assert out.advisory_url == "https://access.redhat.com/errata/RHBA-2025:1601"
    assert "2025/1601/advisory.yaml" in out.advisory_internal_url


def test_run_filter_multi_arch_all_released(tmp_path: Path) -> None:
    """Both arch-specific rows matching advisory 1601 yield empty unreleased output."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    rows = [
        {
            "name": "multi-arch-component",
            "containerImage": "registry.redhat.io/test@sha256:amd64digest123",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        },
        {
            "name": "multi-arch-component",
            "containerImage": "registry.redhat.io/test@sha256:arm64digest456",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        },
    ]
    out = filt.run_filter(
        _encode_snapshot(rows),
        "test-origin",
        secret,
        work_dir=tmp_path / "work",
        clone_sparse=_make_clone_sparse(tmp_path),
        run_cmd=_make_yq_run_cmd(),
    )
    assert _decode_components(out.unreleased_components_b64) == []
    assert out.advisory_url.endswith("RHBA-2025:1601")


def test_run_filter_multi_advisory_progressive(tmp_path: Path) -> None:
    """Filtering continues across multiple advisories until rows remain."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    rows = [
        {
            "name": "released-component",
            "containerImage": "registry.redhat.io/test@sha256:releasedarch123",
            "tags": ["v1.0"],
            "repository": "registry.redhat.io/test",
        },
        {
            "name": "other-component",
            "containerImage": "quay.io/test/other-image:1.0.0",
            "tags": ["stable"],
            "repository": "quay.io/test",
        },
        {
            "name": "still-new",
            "containerImage": "quay.io/test/brand-new:9.9.9",
            "tags": ["latest"],
            "repository": "quay.io/test",
        },
    ]
    out = filt.run_filter(
        _encode_snapshot(rows),
        "test-origin",
        secret,
        work_dir=tmp_path / "work",
        clone_sparse=_make_clone_sparse(tmp_path, advisory_nums=["2025/1601", "2025/1602"]),
        run_cmd=_make_yq_run_cmd(),
    )
    assert _decode_components(out.unreleased_components_b64) == ["still-new"]


def test_run_filter_yq_non_list_images_coerced(tmp_path: Path) -> None:
    """Non-list ``yq`` image payloads are treated as empty advisory content."""

    def _bad_yq(args: list[str], *, cwd: Path | None = None) -> str:
        if args[:1] == ["yq"] and ".spec.content.images" in args[2]:
            return json.dumps({"bad": True})
        return _catalog_yq_fake(args, cwd=cwd)

    secret = tmp_path / "secret"
    _write_secret(secret)
    rows = [
        {
            "name": "only-one",
            "containerImage": "r.io/i:1",
            "tags": [],
            "repository": "r.io",
        }
    ]
    out = filt.run_filter(
        _encode_snapshot(rows),
        "test-origin",
        secret,
        work_dir=tmp_path / "work",
        clone_sparse=_make_clone_sparse(tmp_path),
        run_cmd=_bad_yq,
    )
    assert _decode_components(out.unreleased_components_b64) == ["only-one"]


def test_run_filter_secret_read_failure(tmp_path: Path) -> None:
    """Mounted secret read errors are wrapped as ``CheckStepError``."""
    with (
        mock.patch.object(
            filt.gitlab,
            "read_credentials_from_mount",
            side_effect=OSError("permission denied"),
        ),
        pytest.raises(tekton.CheckStepError, match="reading the mounted advisory secret"),
    ):
        filt.run_filter(
            _encode_snapshot([{"name": "x"}]),
            "test-origin",
            tmp_path / "secret",
        )


def test_run_filter_uses_gitlab_clone_sparse(tmp_path: Path) -> None:
    """Default path delegates sparse clone to ``gitlab.clone_project_sparse``."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    repo = tmp_path / "work" / "repo"
    _setup_repo_tree(repo, "test-origin", [])

    with (
        mock.patch.object(filt.gitlab, "configure_git_oauth2_auth"),
        mock.patch.object(filt.gitlab, "clone_project_sparse", return_value=repo) as clone,
    ):
        out = filt.run_filter(
            _encode_snapshot([{"name": "c1"}]),
            "test-origin",
            secret,
            work_dir=tmp_path / "work",
            run_cmd=_make_yq_run_cmd(),
        )
    clone.assert_called_once()
    assert _decode_components(out.unreleased_components_b64) == ["c1"]


def test_run_filter_git_failure(tmp_path: Path) -> None:
    """Git clone failures are wrapped as ``CheckStepError``."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    with (
        mock.patch.object(
            filt.gitlab,
            "clone_project_sparse",
            side_effect=GitCommandError(["git"], 1, "clone failed"),
        ),
        pytest.raises(tekton.CheckStepError, match="cloning the advisory Git repository"),
    ):
        filt.run_filter(
            _encode_snapshot([{"name": "x"}]),
            "test-origin",
            secret,
            work_dir=tmp_path / "work",
        )


def test_run_filter_yq_failure(tmp_path: Path) -> None:
    """``yq`` failures propagate as ``CalledProcessError``."""
    secret = tmp_path / "secret"
    _write_secret(secret)
    with pytest.raises(subprocess.CalledProcessError):
        filt.run_filter(
            _encode_snapshot([{"name": "x"}]),
            "test-origin",
            secret,
            work_dir=tmp_path / "work",
            clone_sparse=_make_clone_sparse(tmp_path),
            run_cmd=_make_yq_run_cmd(fail=True),
        )


def test_main_writes_results(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``main`` writes all Tekton result files and returns 0."""
    paths = _result_env(monkeypatch, tmp_path)
    expected = filt.FilterOutput(
        result="Success",
        unreleased_components_b64="abc",
        advisory_url="pub",
        advisory_internal_url="int",
    )
    with mock.patch.object(filt, "run_filter", return_value=expected):
        rc = filt.main(
            [
                "filter_already_released_advisory_images.py",
                "--transformed-snapshot",
                "x",
                "--origin",
                "o",
                "--internal-request-pipeline-run-name",
                "pr-1",
                "--internal-request-task-run-name",
                "tr-1",
            ]
        )
    assert rc == 0
    assert paths["RESULT_RESULT"].read_text(encoding="utf-8") == "Success"
    assert (
        paths["RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME"].read_text(encoding="utf-8")
        == "pr-1"
    )
    assert paths["RESULT_UNRELEASED_COMPONENTS"].read_text(encoding="utf-8") == "abc"
    assert paths["RESULT_ADVISORY_URL"].read_text(encoding="utf-8") == "pub"
    assert paths["RESULT_ADVISORY_INTERNAL_URL"].read_text(encoding="utf-8") == "int"


def test_main_failure_still_exits_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Operational failures are written to ``RESULT_RESULT`` and exit code stays 0."""
    paths = _result_env(monkeypatch, tmp_path)
    with mock.patch.object(
        filt,
        "run_filter",
        side_effect=tekton.CheckStepError(
            "reading the mounted advisory secret", OSError("boom")
        ),
    ):
        rc = filt.main(
            [
                "filter_already_released_advisory_images.py",
                "--transformed-snapshot",
                "x",
                "--origin",
                "o",
                "--internal-request-pipeline-run-name",
                "pr-1",
                "--internal-request-task-run-name",
                "tr-1",
            ]
        )
    assert rc == 0
    assert "Failed while reading the mounted advisory secret" in paths[
        "RESULT_RESULT"
    ].read_text(encoding="utf-8")


def test_main_unexpected_exception(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Unexpected exceptions are wrapped and written to ``RESULT_RESULT``."""
    paths = _result_env(monkeypatch, tmp_path)
    with mock.patch.object(filt, "run_filter", side_effect=RuntimeError("kaboom")):
        rc = filt.main(
            [
                "filter_already_released_advisory_images.py",
                "--transformed-snapshot",
                "x",
                "--origin",
                "o",
                "--internal-request-pipeline-run-name",
                "pr-1",
                "--internal-request-task-run-name",
                "tr-1",
            ]
        )
    assert rc == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "Failed while filtering already released advisory images" in text


def test_main_subprocess_failure_still_exits_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Subprocess failures from ``run_filter`` are written to ``RESULT_RESULT``."""
    paths = _result_env(monkeypatch, tmp_path)
    with mock.patch.object(
        filt,
        "run_filter",
        side_effect=subprocess.CalledProcessError(1, "git clone", output="clone failed"),
    ):
        rc = filt.main(
            [
                "filter_already_released_advisory_images.py",
                "--transformed-snapshot",
                "x",
                "--origin",
                "o",
                "--internal-request-pipeline-run-name",
                "pr-1",
                "--internal-request-task-run-name",
                "tr-1",
            ]
        )
    assert rc == 0
    assert "Failed while filtering already released advisory images" in paths[
        "RESULT_RESULT"
    ].read_text(encoding="utf-8")


def test_main_missing_required_flags_returns_one() -> None:
    """Bad CLI usage returns 1 before result env is required."""
    assert filt.main(["filter_already_released_advisory_images.py", "--origin", "o"]) == 1


def test_script_help_via_subprocess() -> None:
    """The script entrypoint handles ``--help`` when invoked as ``__main__``."""
    script = Path(__file__).with_name("filter_already_released_advisory_images.py")
    proc = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(Path(__file__).resolve().parents[2] / "helpers"),
                    str(Path(__file__).resolve().parent),
                ]
            ),
        },
    )
    assert proc.returncode == 1
    assert "--transformed-snapshot" in proc.stderr


def test_main_missing_result_env_raises_system_exit() -> None:
    """Missing ``RESULT_*`` env vars fail fast via ``tekton.result_paths_from_env``."""
    with pytest.raises(SystemExit):
        filt.main(
            [
                "filter_already_released_advisory_images.py",
                "--transformed-snapshot",
                "x",
                "--origin",
                "o",
                "--internal-request-pipeline-run-name",
                "pr-1",
                "--internal-request-task-run-name",
                "tr-1",
            ]
        )
