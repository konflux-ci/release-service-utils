"""Tests for push_oot_kmods_to_git."""

from __future__ import annotations

import hashlib
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.tasks.managed.push_oot_kmods_to_git import main, run
from release_service_utils.tasks.managed.push_oot_kmods_to_git.push_oot_kmods_to_git import (
    DEFAULT_GIT_TOKEN_MOUNT,
    _ArchTarget,
    _max_push_attempts,
)

TASK = "release_service_utils.tasks.managed.push_oot_kmods_to_git.push_oot_kmods_to_git"


def _write_envfile(
    arch_dir: Path,
    *,
    vendor: str = "acme",
    version: str = "1.0.0",
    kernel: str = "6.5.0-test.x86_64",
    arch: str | None = None,
    batch_size: str | None = None,
) -> None:
    """Write a kmod envfile into *arch_dir*."""
    lines = [
        f"DRIVER_VENDOR={vendor}",
        f"DRIVER_VERSION={version}",
        f"KERNEL_VERSION={kernel}",
    ]
    if arch is not None:
        lines.append(f"ARCH={arch}")
    if batch_size is not None:
        lines.append(f"BATCH_SIZE={batch_size}")
    (arch_dir / "envfile").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_valid_checksum(arch_dir: Path, checksum_name: str) -> None:
    """Write a checksum file that verifies against the .ko files in *arch_dir*."""
    lines = []
    for ko_file in sorted(arch_dir.rglob("*.ko")):
        digest = hashlib.sha256(ko_file.read_bytes()).hexdigest()
        lines.append(f"{digest}  {ko_file.relative_to(arch_dir).as_posix()}")
    (arch_dir / checksum_name).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _single_arch_tree(
    data_dir: Path,
    *,
    with_checksum: bool = True,
    checksum_valid: bool = True,
    with_ko: bool = True,
    arch: str | None = None,
    kernel: str = "6.5.0-test.x86_64",
) -> Path:
    """Create a single arch signed kmods tree and return the arch directory."""
    arch_dir = data_dir / "signed-kmods" / "x86_64"
    arch_dir.mkdir(parents=True)
    if with_ko:
        (arch_dir / "mod1.ko").write_text("mod1", encoding="utf-8")
        nested = arch_dir / "driver" / "subdir"
        nested.mkdir(parents=True)
        (nested / "nested.ko").write_text("nested", encoding="utf-8")
    _write_envfile(arch_dir, arch=arch, kernel=kernel)
    if with_checksum:
        checksum_name = "signed_kmods_checksums_x86_64.txt"
        if checksum_valid:
            _write_valid_checksum(arch_dir, checksum_name)
        else:
            (arch_dir / checksum_name).write_text("deadbeef  mod1.ko\n", encoding="utf-8")
    return arch_dir


def _token_mount(tmp_path: Path) -> Path:
    """Create a mock git token mount and return it."""
    mount = tmp_path / "secrets"
    mount.mkdir()
    (mount / "gitlab-gr-maintenance-token").write_text("tok123\n", encoding="utf-8")
    return mount


def test_run_single_arch_pushes(tmp_path: Path) -> None:
    """Single arch run copies .ko files, checksums and envfile, then commits and pushes."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with (
        patch(f"{TASK}.gitlab.configure_git_oauth2_auth") as mock_auth,
        patch(f"{TASK}.git.clone", return_value=repo_dir) as mock_clone,
        patch(f"{TASK}.git.configure_git_global_user") as mock_identity,
        patch(f"{TASK}.git.index_add_commit") as mock_add,
        patch(f"{TASK}.git.changed_paths_from_status", return_value=["x86_64"]),
        patch(f"{TASK}.git.commit_staged") as mock_commit,
        patch(f"{TASK}.git.push") as mock_push,
    ):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            git_repo_url="gitlab.example.com/group/project.git",
            git_branch="main",
            git_token_mount=_token_mount(tmp_path),
        )

    mock_auth.assert_called_once_with("tok123")
    mock_clone.assert_called_once()
    args, kwargs = mock_clone.call_args
    assert args[0] == data_dir / "git-work"
    assert args[1] == "https://gitlab.example.com/group/project.git"
    assert kwargs["directory_name"] == "local-artifacts"
    assert kwargs["revision"] == "main"
    assert kwargs["sparse_dirs"] == ["acme/1.0.0/6.5.0-test/x86_64"]
    assert kwargs["shallow"] is True

    target = repo_dir / "acme/1.0.0/6.5.0-test/x86_64"
    assert (target / "mod1.ko").read_text(encoding="utf-8") == "mod1"
    assert (target / "driver/subdir/nested.ko").is_file()
    assert (target / "signed_kmods_checksums_x86_64.txt").is_file()
    assert (target / "envfile").is_file()

    mock_identity.assert_called_once_with(
        "Partner Accelerators CI Bot",
        "partner-accelerators@konflux-ci.redhat.com",
    )
    mock_add.assert_called_once_with(
        repo_dir,
        ["acme/1.0.0/6.5.0-test/x86_64"],
        "",
        commit=False,
    )
    mock_commit.assert_called_once_with(
        repo_dir,
        "Automated push from Build and Sign pipeline",
    )
    mock_push.assert_called_once_with(
        repo_dir,
        "main",
        remote="origin",
        retries=0,
        rebase_branch="main",
    )


def test_run_extracts_tarball_before_push(tmp_path: Path) -> None:
    """signed-kmods.tar.gz is extracted when present."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    signed = data_dir / "signed-kmods"
    archive = data_dir / "signed-kmods.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(signed, arcname="signed-kmods")
    for child in sorted(signed.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            child.rmdir()
    signed.rmdir()
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with (
        patch(f"{TASK}.gitlab.configure_git_oauth2_auth"),
        patch(f"{TASK}.git.clone", return_value=repo_dir),
        patch(f"{TASK}.git.configure_git_global_user"),
        patch(f"{TASK}.git.index_add_commit"),
        patch(f"{TASK}.git.changed_paths_from_status", return_value=["x86_64"]),
        patch(f"{TASK}.git.commit_staged"),
        patch(f"{TASK}.git.push"),
    ):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            git_repo_url="gitlab.example.com/g/p.git",
            git_branch="main",
            git_token_mount=_token_mount(tmp_path),
        )

    assert (repo_dir / "acme/1.0.0/6.5.0-test/x86_64/mod1.ko").is_file()


def test_run_multi_arch_pushes_summaries(tmp_path: Path) -> None:
    """Multi arch run copies each architecture's files and the signing/extraction summaries."""
    data_dir = tmp_path / "data"
    signed = data_dir / "signed-kmods"
    x86 = signed / "x86_64"
    arm = signed / "aarch64"
    x86.mkdir(parents=True)
    arm.mkdir(parents=True)
    (x86 / "a.ko").write_text("a", encoding="utf-8")
    (arm / "b.ko").write_text("b", encoding="utf-8")
    _write_envfile(x86, kernel="6.5.0-test.x86_64")
    _write_envfile(arm, kernel="6.5.0-test.aarch64")
    (signed / "signing_summary.txt").write_text("sign", encoding="utf-8")
    (signed / "extraction_summary.txt").write_text("extract", encoding="utf-8")
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with (
        patch(f"{TASK}.gitlab.configure_git_oauth2_auth"),
        patch(f"{TASK}.git.clone", return_value=repo_dir),
        patch(f"{TASK}.git.configure_git_global_user"),
        patch(f"{TASK}.git.index_add_commit"),
        patch(f"{TASK}.git.changed_paths_from_status", return_value=["x86_64", "aarch64"]),
        patch(f"{TASK}.git.commit_staged") as mock_commit,
        patch(f"{TASK}.git.push"),
    ):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            git_repo_url="gitlab.example.com/g/p.git",
            git_branch="main",
            git_token_mount=_token_mount(tmp_path),
        )

    assert (repo_dir / "acme/1.0.0/6.5.0-test/x86_64/a.ko").is_file()
    assert (repo_dir / "acme/1.0.0/6.5.0-test/aarch64/b.ko").is_file()
    summary = repo_dir / "acme/1.0.0/6.5.0-test/multi-arch-summary"
    assert (summary / "signing_summary.txt").read_text(encoding="utf-8") == "sign"
    assert (summary / "extraction_summary.txt").read_text(encoding="utf-8") == "extract"
    mock_commit.assert_called_once_with(
        repo_dir,
        "Automated push from Build and Sign pipeline (multi-arch: 2 architectures)",
    )


def test_run_multi_arch_skips_summary_without_signing_file(tmp_path: Path) -> None:
    """Multi arch summary copying is skipped when signing_summary.txt is absent."""
    data_dir = tmp_path / "data"
    signed = data_dir / "signed-kmods"
    x86 = signed / "x86_64"
    arm = signed / "aarch64"
    x86.mkdir(parents=True)
    arm.mkdir(parents=True)
    (x86 / "a.ko").write_text("a", encoding="utf-8")
    (arm / "b.ko").write_text("b", encoding="utf-8")
    _write_envfile(x86)
    _write_envfile(arm)
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with (
        patch(f"{TASK}.gitlab.configure_git_oauth2_auth"),
        patch(f"{TASK}.git.clone", return_value=repo_dir),
        patch(f"{TASK}.git.configure_git_global_user"),
        patch(f"{TASK}.git.index_add_commit"),
        patch(f"{TASK}.git.changed_paths_from_status", return_value=["x86_64", "aarch64"]),
        patch(f"{TASK}.git.commit_staged"),
        patch(f"{TASK}.git.push"),
    ):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            git_repo_url="gitlab.example.com/g/p.git",
            git_branch="main",
            git_token_mount=_token_mount(tmp_path),
        )

    assert not (repo_dir / "acme/1.0.0/6.5.0-test/multi-arch-summary").exists()


def test_run_no_changes_skips_commit_and_push(tmp_path: Path) -> None:
    """When there is nothing to commit, run() logs and returns without pushing."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with (
        patch(f"{TASK}.gitlab.configure_git_oauth2_auth"),
        patch(f"{TASK}.git.clone", return_value=repo_dir),
        patch(f"{TASK}.git.configure_git_global_user"),
        patch(f"{TASK}.git.index_add_commit"),
        patch(f"{TASK}.git.changed_paths_from_status", return_value=[]),
        patch(f"{TASK}.git.commit_staged") as mock_commit,
        patch(f"{TASK}.git.push") as mock_push,
    ):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            git_repo_url="gitlab.example.com/g/p.git",
            git_branch="main",
            git_token_mount=_token_mount(tmp_path),
        )

    mock_commit.assert_not_called()
    mock_push.assert_not_called()


def test_run_single_arch_missing_checksum_raises(tmp_path: Path) -> None:
    """A single arch build without a checksum file fails before cloning."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, with_checksum=False)

    with patch(f"{TASK}.git.clone") as mock_clone:
        with pytest.raises(FileNotFoundError, match="not found for single architecture"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                git_repo_url="gitlab.example.com/g/p.git",
                git_branch="main",
                git_token_mount=_token_mount(tmp_path),
            )
    mock_clone.assert_not_called()


def test_run_single_arch_checksum_mismatch_raises(tmp_path: Path) -> None:
    """A single arch build with an invalid checksum fails before cloning."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, checksum_valid=False)

    with patch(f"{TASK}.git.clone") as mock_clone:
        with pytest.raises(subprocess.CalledProcessError):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                git_repo_url="gitlab.example.com/g/p.git",
                git_branch="main",
                git_token_mount=_token_mount(tmp_path),
            )
    mock_clone.assert_not_called()


def test_run_no_ko_files_raises(tmp_path: Path) -> None:
    """An architecture directory with no .ko files fails."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    arch_dir = data_dir / "signed-kmods" / "x86_64"
    arch_dir.mkdir(parents=True)
    _write_envfile(arch_dir)
    # Checksum a file other than a .ko so verification passes before the
    # missing-.ko-files check is reached.
    envfile = arch_dir / "envfile"
    digest = hashlib.sha256(envfile.read_bytes()).hexdigest()
    (arch_dir / "signed_kmods_checksums_x86_64.txt").write_text(
        f"{digest}  envfile\n",
        encoding="utf-8",
    )
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with (
        patch(f"{TASK}.gitlab.configure_git_oauth2_auth"),
        patch(f"{TASK}.git.clone", return_value=repo_dir),
        patch(f"{TASK}.git.configure_git_global_user"),
    ):
        with pytest.raises(FileNotFoundError, match="No .ko files"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                git_repo_url="gitlab.example.com/g/p.git",
                git_branch="main",
                git_token_mount=_token_mount(tmp_path),
            )


def test_run_no_arch_directories_raises(tmp_path: Path) -> None:
    """An empty signed-kmods path fails before any git operation."""
    data_dir = tmp_path / "data"
    (data_dir / "signed-kmods").mkdir(parents=True)

    with pytest.raises(RuntimeError, match="No architecture directories"):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            git_repo_url="gitlab.example.com/g/p.git",
            git_branch="main",
            git_token_mount=_token_mount(tmp_path),
        )


def test_max_push_attempts_defaults_to_one() -> None:
    """No BATCH_SIZE in any envfile defaults to one attempt."""
    targets = [_ArchTarget(arch_dir=Path("x"), target_dir="x", env={})]
    assert _max_push_attempts(targets) == 1


def test_max_push_attempts_uses_last_set_value() -> None:
    """The last architecture whose envfile sets BATCH_SIZE wins."""
    targets = [
        _ArchTarget(arch_dir=Path("x"), target_dir="x", env={"BATCH_SIZE": "3"}),
        _ArchTarget(arch_dir=Path("y"), target_dir="y", env={}),
    ]
    assert _max_push_attempts(targets) == 3


def test_main_wires_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads PARAMS env vars and calls run()."""
    token_mount = _token_mount(tmp_path)
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", "signed-kmods")
    monkeypatch.setenv("PARAM_GIT_REPO_URL", "gitlab.example.com/g/p.git")
    monkeypatch.setenv("PARAM_GIT_BRANCH", "main")
    monkeypatch.setenv("PARAM_GIT_TOKEN_MOUNT", str(token_mount))
    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0
    mock_run.assert_called_once_with(
        data_dir=tmp_path / "data",
        signed_kmods_path="signed-kmods",
        git_repo_url="gitlab.example.com/g/p.git",
        git_branch="main",
        git_token_mount=token_mount,
    )


def test_main_default_git_token_mount(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() defaults the token mount to /var/secrets/git-token."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", "signed-kmods")
    monkeypatch.setenv("PARAM_GIT_REPO_URL", "gitlab.example.com/g/p.git")
    monkeypatch.setenv("PARAM_GIT_BRANCH", "main")
    monkeypatch.delenv("PARAM_GIT_TOKEN_MOUNT", raising=False)
    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0
    assert mock_run.call_args.kwargs["git_token_mount"] == DEFAULT_GIT_TOKEN_MOUNT
