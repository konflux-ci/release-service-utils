#!/usr/bin/env python3
"""Collect three parameters for the create-github-release task.

The githubSecret from the Data file, the repository from the
snapshot file, and the release_version from the binaries of the
extract-checksums-from-image task.
"""

from __future__ import annotations

from pathlib import Path

import tekton
from file import load_json_dict


def collect_params(
    *,
    data_file: Path,
    snapshot_file: Path,
    binaries_path: Path,
    result_repository: Path,
    result_release_version: Path,
    result_github_secret: Path,
) -> int:
    """Collect github parameters and write to results."""
    data = load_json_dict(data_file)
    snapshot = load_json_dict(snapshot_file)

    github_secret = data["github"]["githubSecret"]
    if not github_secret:
        raise RuntimeError(
            "No valid secret was provided via 'github.githubSecret' key in data."
        )

    sha_file = next(binaries_path.glob("*_SHA256SUMS"))
    stem = sha_file.name.removesuffix("_SHA256SUMS")
    parts = stem.rsplit("_", 1)
    if len(parts) < 2:
        raise RuntimeError(
            f"Malformed SHA256SUMS filename: '{sha_file.name}'."
            + " Expected format: '<name>_<version>_SHA256SUMS'"
        )
    release_version = parts[-1]

    repository = snapshot["components"][0]["source"]["git"]["url"]

    result_release_version.write_text(release_version, encoding="utf-8")
    result_github_secret.write_text(github_secret, encoding="utf-8")
    result_repository.write_text(repository, encoding="utf-8")

    return 0


def main() -> int:
    """Parse environment variables and collect github parameters."""
    data_dir = Path(tekton.require_env("DATA_DIR"))
    data_path = tekton.require_env("DATA_PATH")
    snapshot_path = tekton.require_env("SNAPSHOT_PATH")
    binaries_path = tekton.require_env("BINARIES_PATH")

    (
        result_repository,
        result_release_version,
        result_github_secret,
    ) = tekton.result_paths_from_env(
        "RESULT_REPOSITORY", "RESULT_RELEASE_VERSION", "RESULT_GITHUB_SECRET"
    )

    return collect_params(
        data_file=data_dir / data_path,
        snapshot_file=data_dir / snapshot_path,
        binaries_path=data_dir / binaries_path,
        result_repository=result_repository,
        result_release_version=result_release_version,
        result_github_secret=result_github_secret,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
