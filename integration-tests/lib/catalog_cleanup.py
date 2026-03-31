#!/usr/bin/env python3
"""Compare catalog branch head to CATALOG_BASE_SHA; warn if it moved. Delete temp GitHub repo.

Uses release-service-catalog's ``delete-repository.sh`` (same API as catalog e2e).

Required env:
  GITHUB_TOKEN
  TEMP_REPO_NAME  (full org/repo of the temporary fork, e.g.
  hacbs-release-tests/catalog-utils-e2e-<uid>)

Optional env:
  CATALOG_BASE_SHA
  CATALOG_REPO     (default: konflux-ci/release-service-catalog)
  CATALOG_REF      (default: development)
  INTEGRATION_TESTS_SCRIPTS_DIR  If set and contains delete-repository.sh,
  skip cloning catalog.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from catalog_e2e_helpers import require_env


def _ls_remote_head(*, catalog_repo: str, catalog_ref: str) -> str:
    """SHA of refs/heads/<ref> from GitHub (no auth; public konflux-ci catalog)."""
    url = f"https://github.com/{catalog_repo}.git"
    proc = subprocess.run(
        ["git", "ls-remote", url, f"refs/heads/{catalog_ref}"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    if not lines:
        return ""
    return lines[0].split()[0]


def _warn_catalog_drift(*, catalog_repo: str, catalog_ref: str, catalog_base_sha: str) -> None:
    current = _ls_remote_head(catalog_repo=catalog_repo, catalog_ref=catalog_ref)
    if current and current != catalog_base_sha:
        print()
        print("=" * 80)
        print(f"  WARNING: release-service-catalog branch '{catalog_ref}' has new commits")
        print("  since this test started.")
        print(f"    SHA at clone: {catalog_base_sha}")
        print(f"    SHA now:      {current}")
        print(
            "  E2E ran against the older catalog snapshot. Re-run if you need latest catalog."
        )
        print()


def _acquire_delete_repository_script_dir(
    *, catalog_repo: str, catalog_ref: str
) -> tuple[Path, Path | None]:
    """Fetch directory with ``delete-repository.sh``; optional temp clone root to remove."""
    override = os.environ.get("INTEGRATION_TESTS_SCRIPTS_DIR", "").strip()
    if override:
        d = Path(override)
        if (d / "delete-repository.sh").is_file():
            return d, None

    td = Path(tempfile.mkdtemp(prefix="catalog_cleanup-"))
    clone_dest = td / "catalog"
    url = f"https://github.com/{catalog_repo}.git"
    try:
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                catalog_ref,
                url,
                str(clone_dest),
            ],
            check=True,
            timeout=600,
        )
    except (OSError, subprocess.SubprocessError) as e:
        shutil.rmtree(td, ignore_errors=True)
        print(f"ERROR: git clone failed: {e}", file=sys.stderr)
        sys.exit(1)

    scripts = clone_dest / "integration-tests" / "scripts"
    if not (scripts / "delete-repository.sh").is_file():
        shutil.rmtree(td, ignore_errors=True)
        missing = scripts / "delete-repository.sh"
        print(f"ERROR: delete-repository.sh missing after clone: {missing}", file=sys.stderr)
        sys.exit(1)
    return scripts, td


def main() -> None:
    # delete-repository.sh uses this from the environment; do not embed it in clone URLs.
    require_env("GITHUB_TOKEN")
    temp_repo_name = require_env("TEMP_REPO_NAME")
    catalog_repo = os.environ.get("CATALOG_REPO", "konflux-ci/release-service-catalog").strip()
    catalog_ref = os.environ.get("CATALOG_REF", "development").strip()
    base_sha = os.environ.get("CATALOG_BASE_SHA", "").strip()

    if base_sha:
        _warn_catalog_drift(
            catalog_repo=catalog_repo,
            catalog_ref=catalog_ref,
            catalog_base_sha=base_sha,
        )

    scripts_dir, clone_root = _acquire_delete_repository_script_dir(
        catalog_repo=catalog_repo,
        catalog_ref=catalog_ref,
    )
    delete_script = scripts_dir / "delete-repository.sh"

    try:
        print(f"Deleting temporary repo {temp_repo_name}...")
        subprocess.run(
            ["bash", str(delete_script), temp_repo_name],
            check=True,
            env=os.environ.copy(),
            timeout=300,
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: delete-repository.sh exited {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    except subprocess.TimeoutExpired as e:
        print(
            f"ERROR: delete-repository.sh timed out after {e.timeout}s",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        if clone_root is not None:
            shutil.rmtree(clone_root, ignore_errors=True)


if __name__ == "__main__":
    main()
