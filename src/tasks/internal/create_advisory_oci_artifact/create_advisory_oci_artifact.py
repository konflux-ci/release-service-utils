#!/usr/bin/env python3
"""Create an OCI artifact from an advisory YAML URL.

Downloads the advisory YAML from GitLab's repository-files API using a
mounted access token, packs it as a gzip tar, and pushes the archive to
the release-service trusted-artifacts store. The returned URI uses the
tar-file SHA-256 so callers can ``oras blob fetch`` the blob.

After a valid invocation with Tekton result env vars, always exits ``0``;
success or failure is recorded in the result files.
"""

from __future__ import annotations

import gzip
import os
import tarfile
import tempfile
from pathlib import Path
from urllib.parse import quote, urlparse

from release_service_utils.helpers import (
    authentication,
    file,
    http_client,
    oci_auth,
    tekton,
)
from release_service_utils.helpers.internal_request import internal_request_results
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.oras_utils import oras_push

PROG = "create_advisory_oci_artifact.py"
STORE = "quay.io/konflux-ci/release-service-trusted-artifacts"
ARCHIVE_NAME = "advisory"
ADVISORY_FILENAME = "advisory.yaml"
DEFAULT_DOCKERCONFIG = "/mnt/trusted_artifacts_dockerconfig/.dockerconfigjson"
DOCKERCONFIG_ENV = "TA_DOCKERCONFIG_PATH"
ADVISORY_SECRET_MOUNT_ENV = "ADVISORY_SECRET_MOUNT"
DEFAULT_ADVISORY_SECRET_MOUNT = Path("/mnt/advisory_secret")
GITLAB_TOKEN_HEADER = "PRIVATE-TOKEN"
_RAW_FILE_MARKER = "/-/raw/"


def _raw_file_url_to_api_url(raw_url: str) -> str:
    """Translate a GitLab ``/-/raw/`` URL into the REST raw-file API URL.

    GitLab's web raw route does not honor ``PRIVATE-TOKEN`` for private
    projects: it redirects to sign-in and returns HTML with HTTP 200. The
    ``/repository/files/:path/raw`` API endpoint does honor the header.

    The first path segment after ``/-/raw/`` is the git ref; the remainder
    is the file path. Query strings on *raw_url* are ignored. Scheme and
    host from *raw_url* are preserved.
    """
    parsed = urlparse(raw_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"advisory_url is not an HTTP(S) URL: {raw_url}")
    if _RAW_FILE_MARKER not in parsed.path:
        raise ValueError(f"advisory_url is not a GitLab raw-file URL: {raw_url}")
    project_path, rest = parsed.path.split(_RAW_FILE_MARKER, 1)
    project_path = project_path.lstrip("/")
    git_ref, _sep, file_path = rest.lstrip("/").partition("/")
    if not project_path or not git_ref or not file_path:
        raise ValueError(f"advisory_url is missing project, git ref, or file path: {raw_url}")
    encoded_project = quote(project_path, safe="")
    encoded_file = quote(file_path, safe="")
    encoded_ref = quote(git_ref, safe="")
    return (
        f"{parsed.scheme}://{parsed.netloc}/api/v4/projects/{encoded_project}"
        f"/repository/files/{encoded_file}/raw?ref={encoded_ref}"
    )


def _write_gzip_tar(source_dir: Path, archive: Path) -> None:
    """Write a gzip-compressed tar of *source_dir* to *archive*.

    Gzip headers omit the original filename and use ``mtime=0``, matching
    ``gzip -n`` so the archive digest is independent of wall-clock time.
    """
    with (
        archive.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz,
        tarfile.open(fileobj=gz, mode="w") as tar,
    ):
        tar.add(str(source_dir), arcname=".")


def run(advisory_url: str, access_token: str) -> str:
    """Download, pack, and push the advisory YAML; return the OCI artifact URI."""
    api_url = _raw_file_url_to_api_url(advisory_url)
    with tempfile.TemporaryDirectory() as yaml_tmpdir:
        yaml_dir = Path(yaml_tmpdir)
        yaml_path = yaml_dir / ADVISORY_FILENAME
        try:
            body = http_client.get_text(
                api_url,
                headers={GITLAB_TOKEN_HEADER: access_token},
            )
        except Exception as exc:
            raise tekton.CheckStepError("downloading the advisory YAML", exc) from exc
        yaml_path.write_text(body, encoding="utf-8")
        logger.info("Downloaded advisory YAML (%s bytes)", len(body.encode("utf-8")))

        with tempfile.TemporaryDirectory() as archive_tmpdir:
            archive_dir = Path(archive_tmpdir)
            archive = archive_dir / ARCHIVE_NAME
            _write_gzip_tar(yaml_dir, archive)
            digest = file.sha256(archive)
            logger.info("Prepared artifact from %s (sha256:%s)", yaml_dir, digest)

            try:
                with oci_auth.auth_file(STORE) as auth_file:
                    oras_push(
                        STORE,
                        archive_dir,
                        ARCHIVE_NAME,
                        "advisory",
                        extra_args=["--registry-config", str(auth_file)],
                    )
            except Exception as exc:
                raise tekton.CheckStepError("pushing the advisory archive", exc) from exc
            return f"oci:{STORE}@sha256:{digest}"


def main() -> int:
    """Read Tekton env, create the artifact, and always return 0 after writing results."""
    (
        path_result,
        path_artifact,
        path_internal_pr,
        path_internal_task_run,
    ) = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_ADVISORY_OCI_ARTIFACT",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME",
    )
    result_paths = {
        "internal_pr_name": path_internal_pr,
        "internal_task_run_name": path_internal_task_run,
    }
    path_artifact.write_text("", encoding="utf-8")

    pipeline_run_name = tekton.require_env("INTERNAL_REQUEST_PIPELINE_RUN_NAME")
    task_run_name = tekton.require_env("INTERNAL_REQUEST_TASK_RUN_NAME")
    internal_request_results.write_result_paths(
        result_paths,
        pipeline_run_name=pipeline_run_name,
        task_run_name=task_run_name,
    )

    dockerconfig = file.path_from_env_variable(DOCKERCONFIG_ENV, DEFAULT_DOCKERCONFIG)
    advisory_secret = file.path_from_env_variable(
        ADVISORY_SECRET_MOUNT_ENV, DEFAULT_ADVISORY_SECRET_MOUNT
    )

    try:
        advisory_url = os.environ.get("PARAM_ADVISORY_URL", "").strip()
        if not advisory_url:
            raise ValueError("PARAM_ADVISORY_URL must be set")
        access_token = authentication.read_mounted_text(advisory_secret, "gitlab_access_token")
        if not access_token:
            raise ValueError("gitlab_access_token is empty")
        authentication.setup_ca_cert()
        authentication.setup_docker_config(dockerconfig, optional=True)
        uri = run(advisory_url, access_token)
        path_result.write_text("Success", encoding="utf-8")
        path_artifact.write_text(uri, encoding="utf-8")
    except Exception as exc:
        tekton.write_failure_result(
            path_result,
            PROG,
            exc,
            workflow_action="creating the advisory OCI artifact",
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
