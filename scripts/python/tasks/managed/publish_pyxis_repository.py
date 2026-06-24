#!/usr/bin/env python3
"""Mark snapshot repositories as published in Pyxis and record catalog URLs."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pyxis_api
import tekton
from logger import logger
from snapshot import component_push_source_container, default_push_source_container

PROG = "publish_pyxis_repository.py"
RESULTS_FILENAME = "publish-pyxis-repository-results.json"
SIGN_REGISTRY_ACCESS_FILENAME = "sign-registry-access.txt"


def resolve_pyxis_api_url() -> str:
    """Use `PYXIS_URL` when set; otherwise map `PARAM_SERVER` to a Pyxis API URL."""
    override = os.environ.get("PYXIS_URL", "").strip()
    if override:
        return override.rstrip("/")
    return pyxis_api.pyxis_api_url_for_server(tekton.require_env("PARAM_SERVER"))


def skip_repo_publishing(data: dict[str, Any]) -> bool:
    """Return whether `pyxis.skipRepoPublishing` is enabled in the data JSON."""
    pyxis = data.get("pyxis")
    if not isinstance(pyxis, dict):
        return False
    return bool(pyxis.get("skipRepoPublishing", False))


def build_publish_payload(source_enabled: bool) -> dict[str, bool]:
    """Build the Pyxis PATCH body for marking a repository published."""
    payload: dict[str, bool] = {"published": True}
    if source_enabled:
        payload["source_container_image_enabled"] = True
    return payload


def should_patch_repository(
    *,
    skip_publishing: bool,
    publish_on_push: Any,
    pyxis_registry: str,
    pyxis_repo: str,
) -> bool:
    """Return whether to PATCH Pyxis with the published flag for this repo."""
    if skip_publishing:
        logger.info("skipRepoPublishing is set to true, skipping publishing...")
        return False
    if publish_on_push is not True:
        logger.warning(
            "repository %s/%s is marked as publish_on_push = false",
            pyxis_registry,
            pyxis_repo,
        )
        logger.warning("Skipping the setting of the published flag.")
        return False
    return True


def should_record_catalog_url(
    *,
    repository_published: bool,
    should_patch: bool,
) -> bool:
    """Return whether to add a catalog URL for this repository."""
    return repository_published or should_patch


def should_add_sign_registry_access(
    pyxis_registry: str,
    requires_terms: Any,
) -> bool:
    """Return whether a repo belongs on the sign-registry-access list."""
    return pyxis_registry == "registry.access.redhat.com" and requires_terms is False


def publish_repositories(
    *,
    snapshot: dict[str, Any],
    pyxis_api_url: str,
    cert: tuple[str, str],
    sign_registry_access_file: Path,
    skip_publishing: bool,
    default_push_source_container: bool,
) -> dict[str, Any]:
    """Process snapshot components and return the results JSON payload."""
    results: dict[str, list[dict[str, str]]] = {"catalog_urls": []}
    components = snapshot.get("components")
    if not isinstance(components, list):
        msg = "snapshot components must be a JSON array"
        raise ValueError(msg)

    for component in components:
        if not isinstance(component, dict):
            continue
        component_name = str(component.get("name", ""))
        repositories = component.get("repositories")
        if not isinstance(repositories, list):
            continue

        push_source_container = component_push_source_container(
            component,
            default_push_source_container,
        )
        payload = build_publish_payload(push_source_container)

        for repository_row in repositories:
            if not isinstance(repository_row, dict):
                continue
            repository_url = repository_row.get("url")
            if not isinstance(repository_url, str) or not repository_url.strip():
                continue

            pyxis_registry = pyxis_api.pyxis_registry_for_quay_url(repository_url)
            pyxis_repo = pyxis_api.pyxis_repository_from_quay_url(repository_url)
            logger.info(
                "Processing repository %s as Pyxis %s/%s",
                repository_url,
                pyxis_registry,
                pyxis_repo,
            )

            repository_json = pyxis_api.get_repository_json(
                pyxis_api_url,
                pyxis_registry,
                pyxis_repo,
                cert=cert,
            )
            repository_id = repository_json.get("_id")
            if not repository_id:
                logger.error(
                    "Pyxis response for %s/%s: %s",
                    pyxis_registry,
                    pyxis_repo,
                    repository_json,
                )
                msg = "Unable to get Container Repository object id from Pyxis"
                raise ValueError(msg)

            logger.info(
                "Found Pyxis repository id %s for %s/%s",
                repository_id,
                pyxis_registry,
                pyxis_repo,
            )

            if should_add_sign_registry_access(
                pyxis_registry,
                repository_json.get("requires_terms"),
            ):
                with sign_registry_access_file.open("a", encoding="utf-8") as fh:
                    fh.write(f"{pyxis_repo}\n")
                logger.info(
                    "Added %s to sign-registry-access list",
                    pyxis_repo,
                )

            repository_published = repository_json.get("published", False) is True
            should_patch = should_patch_repository(
                skip_publishing=skip_publishing,
                publish_on_push=repository_json.get("publish_on_push", False),
                pyxis_registry=pyxis_registry,
                pyxis_repo=pyxis_repo,
            )

            if should_patch:
                pyxis_api.patch_repository_json(
                    pyxis_api_url,
                    str(repository_id),
                    payload,
                    cert=cert,
                )
                logger.info(
                    "Published %s/%s (id %s)",
                    pyxis_registry,
                    pyxis_repo,
                    repository_id,
                )

            if should_record_catalog_url(
                repository_published=repository_published,
                should_patch=should_patch,
            ):
                catalog_url = pyxis_api.catalog_url_for_repository(
                    repository_url,
                    pyxis_repo,
                    str(repository_id),
                )
                results["catalog_urls"].append(
                    {"name": component_name, "url": catalog_url},
                )

    return results


def run_publish_pyxis_repository(
    *,
    data_dir: Path,
    snapshot_path: Path,
    data_path: Path,
    results_dir_path: Path,
    sign_registry_access_result_path: Path,
    pyxis_secret_mount: Path,
    pyxis_api_url: str,
    task_name: str,
) -> None:
    """Load inputs, publish repositories in Pyxis, and write workspace outputs."""
    if not snapshot_path.is_file():
        msg = "No valid snapshot file was provided."
        raise FileNotFoundError(msg)
    if not data_path.is_file():
        msg = "No data JSON was provided."
        raise FileNotFoundError(msg)

    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    data = json.loads(data_path.read_text(encoding="utf-8"))

    sign_registry_relative = (data_path.parent / SIGN_REGISTRY_ACCESS_FILENAME).relative_to(
        data_dir
    )
    sign_registry_access_result_path.write_text(
        sign_registry_relative.as_posix(),
        encoding="utf-8",
    )
    sign_registry_access_file = data_dir / sign_registry_relative
    sign_registry_access_file.parent.mkdir(parents=True, exist_ok=True)
    sign_registry_access_file.write_text("", encoding="utf-8")

    component_group = snapshot.get("componentGroup", "")
    logger.info('Beginning "%s" for "%s"', task_name, component_group)
    logger.info("Using Pyxis API URL: %s", pyxis_api_url)

    skip_publishing = skip_repo_publishing(data)
    if skip_publishing:
        logger.info(
            "skipRepoPublishing is enabled; Pyxis PATCH will be skipped",
        )
    push_source = default_push_source_container(data)
    logger.info("Default pushSourceContainer: %s", str(push_source).lower())

    cert_path = pyxis_secret_mount / "cert"
    key_path = pyxis_secret_mount / "key"
    cert = (str(cert_path), str(key_path))

    results = publish_repositories(
        snapshot=snapshot,
        pyxis_api_url=pyxis_api_url,
        cert=cert,
        sign_registry_access_file=sign_registry_access_file,
        skip_publishing=skip_publishing,
        default_push_source_container=push_source,
    )

    results_dir_path.mkdir(parents=True, exist_ok=True)
    results_file = results_dir_path / RESULTS_FILENAME
    results_file.write_text(json.dumps(results) + "\n", encoding="utf-8")

    logger.info(
        'Completed "%s" for "%s": %d published, results in %s',
        task_name,
        component_group,
        len(results["catalog_urls"]),
        results_file,
    )


def main() -> int:
    """Run the publish workflow; exit non-zero on failure."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    run_publish_pyxis_repository(
        data_dir=data_dir,
        snapshot_path=data_dir / tekton.require_env("PARAM_SNAPSHOT_PATH"),
        data_path=data_dir / tekton.require_env("PARAM_DATA_PATH"),
        results_dir_path=data_dir / tekton.require_env("PARAM_RESULTS_DIR_PATH"),
        sign_registry_access_result_path=tekton.result_paths_from_env(
            "RESULT_SIGN_REGISTRY_ACCESS_PATH",
        )[0],
        pyxis_secret_mount=Path(
            os.environ.get("PYXIS_SECRET_MOUNT", "/etc/secrets"),
        ),
        pyxis_api_url=resolve_pyxis_api_url(),
        task_name=os.environ.get("TASK_NAME", PROG),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
