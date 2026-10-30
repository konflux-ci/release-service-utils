#!/usr/bin/env python3
"""Upload signed out-of-tree kernel modules to a private vendor git repo."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from release_service_utils.helpers import (
    authentication,
    file,
    oot_kmods,
    subprocess_cmd,
    tekton,
)
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.vcs import git, gitlab

DEFAULT_GIT_TOKEN_MOUNT = Path("/var/secrets/git-token")
_GIT_TOKEN_FILE = "gitlab-gr-maintenance-token"
_GIT_CLONE_DIR_NAME = "local-artifacts"
_GIT_AUTHOR_NAME = "Partner Accelerators CI Bot"
_GIT_AUTHOR_EMAIL = "partner-accelerators@konflux-ci.redhat.com"
_COMMIT_MESSAGE = "Automated push from Build and Sign pipeline"
_DEFAULT_MAX_PUSH_ATTEMPTS = 1


@dataclass(frozen=True)
class _ArchTarget:
    """One architecture's source directory and its destination path in the repo."""

    arch_dir: Path
    target_dir: str
    env: dict[str, str]


def _arch_target(arch_dir: Path) -> _ArchTarget:
    """Read one architecture's envfile and find where its files go in the repo."""
    env = oot_kmods.load_kmod_envfile(arch_dir / oot_kmods.ENVFILE_NAME)
    kernel = oot_kmods.clean_kernel_version(env["KERNEL_VERSION"])
    final_arch = oot_kmods.arch_from_env(env, arch_dir.name)
    target_dir = oot_kmods.destination_prefix(
        env["DRIVER_VENDOR"],
        env["DRIVER_VERSION"],
        kernel,
        final_arch,
    ).rstrip("/")
    logger.info("Target directory for %s: %s", arch_dir.name, target_dir)
    return _ArchTarget(arch_dir=arch_dir, target_dir=target_dir, env=env)


def _multi_arch_summary_dir(env: dict[str, str]) -> str:
    """Build the multi arch summary folder's path in the repo, from *env*."""
    kernel = oot_kmods.clean_kernel_version(env["KERNEL_VERSION"])
    return oot_kmods.destination_prefix(
        env["DRIVER_VENDOR"],
        env["DRIVER_VERSION"],
        kernel,
        oot_kmods.MULTI_ARCH_SUFFIX,
    ).rstrip("/")


def _verify_single_arch_checksum(target: _ArchTarget) -> None:
    """Verify the signed checksums for a single architecture build."""
    checksum_name = oot_kmods.checksum_filename(target.arch_dir.name)
    checksum_path = target.arch_dir / checksum_name
    if not checksum_path.is_file():
        raise FileNotFoundError(f"{checksum_name} not found for single architecture build")
    subprocess_cmd.run_cmd(["sha256sum", "-c", checksum_name], cwd=target.arch_dir)


def _copy_arch_files(repo_dir: Path, target: _ArchTarget) -> None:
    """Copy one architecture's `.ko` files, checksums and envfile into the clone."""
    arch_dir = target.arch_dir
    dest_root = repo_dir / target.target_dir
    ko_files = sorted(path for path in arch_dir.rglob("*.ko") if path.is_file())
    if not ko_files:
        raise FileNotFoundError(f"No .ko files found for architecture {arch_dir.name}")
    dest_root.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Copying %d .ko files with preserved directory structure for %s",
        len(ko_files),
        arch_dir.name,
    )
    for ko_file in ko_files:
        dest = dest_root / ko_file.relative_to(arch_dir)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ko_file, dest)

    checksum = arch_dir / oot_kmods.checksum_filename(arch_dir.name)
    if checksum.is_file():
        shutil.copy2(checksum, dest_root / checksum.name)
        logger.info("Copied architecture-specific checksums for %s", arch_dir.name)

    shutil.copy2(arch_dir / oot_kmods.ENVFILE_NAME, dest_root / oot_kmods.ENVFILE_NAME)
    logger.info("Copied envfile for %s", arch_dir.name)


def _copy_multi_arch_summary(repo_dir: Path, signed_kmods: Path, summary_dir: str) -> None:
    """Copy the signing and optional summary into the clone."""
    logger.info("Creating multi-architecture summary in Git: %s", summary_dir)
    dest_root = repo_dir / summary_dir
    dest_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(
        signed_kmods / oot_kmods.SIGNING_SUMMARY_FILE,
        dest_root / oot_kmods.SIGNING_SUMMARY_FILE,
    )

    extraction_summary = signed_kmods / oot_kmods.EXTRACTION_SUMMARY_FILE
    if extraction_summary.is_file():
        shutil.copy2(extraction_summary, dest_root / oot_kmods.EXTRACTION_SUMMARY_FILE)


def _sparse_checkout_dirs(
    signed_kmods: Path,
    targets: list[_ArchTarget],
) -> tuple[list[str], str | None]:
    """Return the repo dirs to sparse-checkout and the multi arch summary dir, if any."""
    sparse_dirs = [target.target_dir for target in targets]
    if len(targets) <= 1:
        return sparse_dirs, None

    signing_summary = signed_kmods / oot_kmods.SIGNING_SUMMARY_FILE
    if not signing_summary.is_file():
        return sparse_dirs, None

    logger.info("Signing summary found:\n%s", signing_summary.read_text(encoding="utf-8"))
    summary_dir = _multi_arch_summary_dir(targets[0].env)
    sparse_dirs.append(summary_dir)
    return sparse_dirs, summary_dir


def _max_push_attempts(targets: list[_ArchTarget]) -> int:
    """Return the most push attempts to make, from a `BATCH_SIZE` envfile key."""
    max_attempts = _DEFAULT_MAX_PUSH_ATTEMPTS
    for target in targets:
        raw = target.env.get("BATCH_SIZE")
        if raw:
            max_attempts = int(raw)
    return max_attempts


def run(
    data_dir: Path,
    signed_kmods_path: str,
    git_repo_url: str,
    git_branch: str,
    git_token_mount: Path,
) -> None:
    """Extract signed kmods if needed and push them to a vendor git repo."""
    signed_kmods = file.resolve_path_under_base(data_dir, signed_kmods_path)
    oot_kmods.extract_signed_kmods_archive(data_dir, signed_kmods)
    arch_dirs = oot_kmods.arch_directories(signed_kmods)
    logger.info("Detected %d architecture(s) from directory structure", len(arch_dirs))

    targets = [_arch_target(arch_dir) for arch_dir in arch_dirs]
    if len(targets) == 1:
        _verify_single_arch_checksum(targets[0])
    sparse_dirs, summary_dir = _sparse_checkout_dirs(signed_kmods, targets)

    os.environ["GIT_LFS_SKIP_SMUDGE"] = "1"
    token = authentication.read_mounted_text(git_token_mount, _GIT_TOKEN_FILE)
    gitlab.configure_git_oauth2_auth(token)
    git.configure_git_global_user(_GIT_AUTHOR_NAME, _GIT_AUTHOR_EMAIL)

    repo_dir = git.clone(
        data_dir / "git-work",
        f"https://{git_repo_url}",
        directory_name=_GIT_CLONE_DIR_NAME,
        revision=git_branch,
        sparse_dirs=sparse_dirs,
        shallow=True,
    )

    for target in targets:
        logger.info("Processing Git upload for architecture: %s", target.arch_dir.name)
        _copy_arch_files(repo_dir, target)
    if summary_dir is not None:
        _copy_multi_arch_summary(repo_dir, signed_kmods, summary_dir)

    git.index_add_commit(repo_dir, sparse_dirs, "", commit=False)
    if not git.changed_paths_from_status(repo_dir):
        logger.info("No changes to commit.")
        return

    message = _COMMIT_MESSAGE
    if len(targets) > 1:
        message += f" (multi-arch: {len(targets)} architectures)"
    git.commit_staged(repo_dir, message)

    max_attempts = _max_push_attempts(targets)
    logger.info("Using BATCH_SIZE=%d for push retry attempts", max_attempts)
    git.push(
        repo_dir,
        git_branch,
        remote="origin",
        retries=max(max_attempts - 1, 0),
        rebase_branch=git_branch,
    )
    logger.info(
        "Successfully pushed artifacts for %d architecture(s) to Git.",
        len(targets),
    )


def main() -> int:
    """Read Tekton environment variables and push signed kmods to Git."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    signed_kmods_path = tekton.require_env("PARAM_SIGNED_KMODS_PATH")
    git_repo_url = tekton.require_env("PARAM_GIT_REPO_URL")
    git_branch = tekton.require_env("PARAM_GIT_BRANCH")
    git_token_mount = file.path_from_env_variable(
        "PARAM_GIT_TOKEN_MOUNT",
        DEFAULT_GIT_TOKEN_MOUNT,
    )
    run(
        data_dir=data_dir,
        signed_kmods_path=signed_kmods_path,
        git_repo_url=git_repo_url,
        git_branch=git_branch,
        git_token_mount=git_token_mount,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
