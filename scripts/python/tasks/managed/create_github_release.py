#!/usr/bin/env python3
"""Create a GitHub release with binaries extracted from a container image.

Extract binary files from the container image layers, then upload them
along with SHA256SUMS and signature files (from the Trusted Artifacts chain)
to a new GitHub release. If the release already exists, writes the existing
release URL to the result file.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import extract_artifacts
import skopeo
import snapshot
import tekton
from logger import logger
from vcs import github


def check_release_exists(
    owner_repo: str,
    release_version: str,
    gh_token: str,
) -> str | None:
    """Check if a release with the given version tag exists.

    Return the release URL if it exists, or None if it does not.
    Raise RuntimeError on API errors to avoid incorrectly attempting creation.
    """
    tag = f"v{release_version}"
    result = github.run_gh_command(
        ["gh", "api", f"repos/{owner_repo}/releases/tags/{tag}"],
        gh_token=gh_token,
        check=False,
    )
    if result.returncode != 0:
        if "HTTP 404" in result.stderr or "Not Found" in result.stderr:
            logger.info("Release %s does not exist (404)", tag)
            return None
        msg = f"Failed to check release {tag}: {result.stderr.strip()}"
        logger.error(msg)
        raise RuntimeError(msg)
    try:
        release = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        msg = f"Invalid JSON response when checking release {tag}: {e}"
        logger.error(msg)
        raise RuntimeError(msg) from e
    html_url = release.get("html_url")
    if html_url:
        return html_url
    return f"https://github.com/{owner_repo}/releases/tag/{tag}"


def copy_binaries_to_temp(
    source_dir: Path,
    dest_dir: Path,
    *,
    strict: bool = False,
) -> int:
    """Copy binary files (not SHA256SUMS or .sig) from source to dest.

    Return the number of files copied. If *strict* is True, raise ValueError
    when *source_dir* does not exist.
    """
    if not source_dir.is_dir():
        if strict:
            raise ValueError(f"Source directory does not exist: {source_dir}")
        return 0
    count = 0
    for item in source_dir.iterdir():
        if item.is_file():
            name = item.name
            if not name.endswith("SHA256SUMS") and not name.endswith(".sig"):
                shutil.copy2(item, dest_dir)
                count += 1
    return count


def create_release(
    repository: str,
    release_version: str,
    binaries_dir: Path,
    content_dir: Path,
    gh_token: str,
) -> str:
    """Create a GitHub release and return the release URL."""
    tag = f"v{release_version}"
    title = f"Release {release_version}"

    zip_files = list(binaries_dir.glob("*.zip"))
    json_files = list(binaries_dir.glob("*.json"))
    sha256sums_files = list(content_dir.glob("*SHA256SUMS"))
    sig_files = list(content_dir.glob("*.sig"))

    files_to_upload = zip_files + json_files + sha256sums_files + sig_files
    file_args = [str(f) for f in files_to_upload]

    cmd = [
        "gh",
        "release",
        "create",
        tag,
        *file_args,
        "--repo",
        repository,
        "--title",
        title,
    ]

    result = github.run_gh_command(cmd, gh_token=gh_token, check=True)
    release_url = result.stdout.strip()
    return release_url


def write_results_json(
    results_file: Path,
    release_url: str,
) -> None:
    """Write the JSON results file with the release URL."""
    results_file.parent.mkdir(parents=True, exist_ok=True)
    results = {"github-release": {"url": release_url}}
    results_file.write_text(json.dumps(results), encoding="utf-8")
    logger.info("Results written to %s", results_file)


def run_create_github_release(
    repository: str,
    release_version: str,
    content_directory: str,
    snapshot_path: Path,
    image_binaries_path: str,
    results_dir_path: str,
    data_dir: Path,
    gh_token: str,
    result_url_path: Path,
) -> str:
    """Create a GitHub release from container image binaries.

    Return the release URL.
    """
    owner_repo = github.owner_repo_from_url(repository)
    logger.info("Processing release for %s v%s", owner_repo, release_version)

    existing_url = check_release_exists(owner_repo, release_version, gh_token)
    if existing_url:
        logger.info("Release v%s already exists: %s", release_version, existing_url)
        result_url_path.write_text(existing_url, encoding="utf-8")
        results_file = data_dir / results_dir_path / "create-github-release-results.json"
        write_results_json(results_file, existing_url)
        return existing_url

    image_url = snapshot.first_component(snapshot_path)["container_image"]
    if not image_url:
        raise ValueError("Unable to get image url from snapshot: containerImage is empty")
    logger.info("Extracting binaries from image: %s", image_url)

    tmp_dir = Path(tempfile.mkdtemp())
    binaries_tmp = Path(tempfile.mkdtemp())
    try:
        result = skopeo.copy(f"docker://{image_url}", f"dir:{tmp_dir}")
        if result.returncode != 0:
            logger.error("skopeo copy failed: %s", result.stderr)
            raise subprocess.CalledProcessError(
                result.returncode,
                result.args,
                output=result.stdout,
                stderr=result.stderr,
            )

        extract_artifacts.extract_binaries_from_layers(tmp_dir, image_binaries_path)

        extracted_dir = tmp_dir / image_binaries_path
        if not extracted_dir.is_dir():
            raise ValueError(
                f"Image {image_url} does not contain the '{image_binaries_path}' directory"
            )

        copied_count = copy_binaries_to_temp(extracted_dir, binaries_tmp)
        logger.info("Copied %d binary files to staging directory", copied_count)

        content_dir = data_dir / content_directory
        logger.info("Creating release with files from %s and %s", binaries_tmp, content_dir)

        release_url = create_release(
            repository,
            release_version,
            binaries_tmp,
            content_dir,
            gh_token,
        )
        logger.info("Release created: %s", release_url)

        result_url_path.write_text(release_url, encoding="utf-8")
        results_file = data_dir / results_dir_path / "create-github-release-results.json"
        write_results_json(results_file, release_url)

        return release_url
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        shutil.rmtree(binaries_tmp, ignore_errors=True)


def main() -> int:
    """Read environment variables and create the GitHub release."""
    (result_url_path,) = tekton.result_paths_from_env("RESULT_URL")

    repository = tekton.require_env("REPOSITORY")
    release_version = tekton.require_env("RELEASE_VERSION")
    content_directory = tekton.require_env("CONTENT_DIRECTORY")
    data_dir = Path(tekton.require_env("DATA_DIR"))
    snapshot_rel_path = tekton.require_env("SNAPSHOT_PATH")
    image_binaries_path = os.environ.get("IMAGE_PATH", "releases")
    results_dir_path = tekton.require_env("RESULTS_DIR_PATH")
    gh_token_path = Path(os.environ.get("GH_TOKEN_PATH", "/etc/secrets/token"))

    if not gh_token_path.is_file():
        raise ValueError(f"GitHub token file not found: {gh_token_path}")
    gh_token = gh_token_path.read_text(encoding="utf-8").strip()

    snapshot_path = data_dir / snapshot_rel_path

    run_create_github_release(
        repository=repository,
        release_version=release_version,
        content_directory=content_directory,
        snapshot_path=snapshot_path,
        image_binaries_path=image_binaries_path,
        results_dir_path=results_dir_path,
        data_dir=data_dir,
        gh_token=gh_token,
        result_url_path=result_url_path,
    )

    logger.info("create-github-release completed successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
