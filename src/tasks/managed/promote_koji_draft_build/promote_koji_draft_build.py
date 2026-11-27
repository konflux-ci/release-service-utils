#!/usr/bin/env python3
"""Promote draft Koji builds for snapshot components in the release list."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file as file_helper
from release_service_utils.helpers import koji as koji_helper
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

DEFAULT_SECRET_MOUNT = Path("/etc/secret")
DEFAULT_KINIT_RETRIES = 10


@dataclass(frozen=True)
class PromoteConfig:
    """Configuration for promoting draft Koji builds."""

    snapshot_path: Path
    data_path: Path
    secret_mount: Path
    kinit_retries: int
    koji_config_source: Path | None


@dataclass(frozen=True)
class PromoteOptions:
    """Parsed koji promote options from data.json."""

    principal: str
    keytab_file: str
    koji_profile: str
    release_components: list[str]


def parse_promote_options(data: dict[str, Any]) -> PromoteOptions:
    """Parse koji promote options from data.json."""
    push_opts = data.get("pushOptions", {})

    keytab_info = push_opts.get("pushKeytab", {})
    principal = keytab_info.get("principal")
    keytab_file = keytab_info.get("name")

    if not principal or not keytab_file:
        raise ValueError("pushOptions.pushKeytab.principal and name are required")

    koji_profile = push_opts.get("koji_profile")
    if not koji_profile:
        raise ValueError("pushOptions.koji_profile is required")

    explicit_components = push_opts.get("components")
    if explicit_components:
        release_components = list(explicit_components)
        logger.info("Using explicit release component list from pushOptions")
    else:
        mapping_components = data.get("mapping", {}).get("components", [])
        release_components = [c.get("name", "") for c in mapping_components if c.get("name")]
        logger.info("Using release component list from mapping.components")

    logger.info(
        "Parsed koji promote options: profile=%s principal=%s components=%s",
        koji_profile,
        principal,
        release_components,
    )
    return PromoteOptions(
        principal=principal,
        keytab_file=keytab_file,
        koji_profile=koji_profile,
        release_components=release_components,
    )


def git_source_url(component: dict[str, Any]) -> str:
    """Return the koji source URL for a snapshot component."""
    git = component["source"]["git"]
    source = f"git+{git['url']}#{git['revision']}"
    logger.info(
        "Koji source URL for %s: %s",
        component.get("name", "<unnamed>"),
        source,
    )
    return source


def latest_build_id(builds: list[dict[str, Any]], source: str) -> int:
    """Return the build_id of the newest draft build for *source*."""
    if not builds:
        raise RuntimeError(f"No draft Koji builds found for source {source}")
    latest = max(builds, key=lambda build: build["creation_time"])
    build_id = latest.get("build_id")
    if build_id is None:
        raise RuntimeError(f"Draft Koji build for source {source} has no build_id")
    logger.info(
        "Latest draft build for %s is %s (creation_time=%s, %d candidate(s))",
        source,
        build_id,
        latest.get("creation_time"),
        len(builds),
    )
    return int(build_id)


def list_draft_builds(profile: str, user_name: str, source: str) -> list[dict[str, Any]]:
    """List complete draft builds for *source* owned by *user_name*."""
    logger.info("Listing complete draft builds for %s (user=%s)", source, user_name)
    result = koji_helper.run_koji_cmd(
        profile,
        "call",
        "--json-output",
        "listBuilds",
        f"userID={user_name}",
        "state=1",
        f"source={source}",
        "draft=True",
    )
    builds = json.loads(result.stdout)
    if not isinstance(builds, list):
        raise RuntimeError(f"Unexpected listBuilds result for source {source}: {builds!r}")
    logger.info("Found %d complete draft build(s) for %s", len(builds), source)
    return builds


def promote_component(profile: str, user_name: str, component: dict[str, Any]) -> None:
    """Promote the latest draft build for *component*."""
    name = component.get("name", "<unnamed>")
    logger.info("Promoting latest draft build for component %s", name)
    source = git_source_url(component)
    builds = list_draft_builds(profile, user_name, source)
    build_id = latest_build_id(builds, source)
    logger.info("Promoting build %s for component %s", build_id, name)
    koji_helper.run_koji_cmd(profile, "call", "promoteBuild", str(build_id))


def run(config: PromoteConfig) -> None:
    """Promote draft Koji builds for snapshot components in the release list."""
    snapshot = file_helper.load_json_dict(config.snapshot_path)
    data = file_helper.load_json_dict(config.data_path)
    opts = parse_promote_options(data)

    if config.koji_config_source is not None:
        koji_helper.install_koji_client_config(config.koji_config_source)

    component_group = snapshot.get("componentGroup", "unknown")
    components = snapshot.get("components", [])
    logger.info(
        "Processing %d components for application '%s'",
        len(components),
        component_group,
    )

    working_dir = config.snapshot_path.parent
    working_dir.mkdir(parents=True, exist_ok=True)

    keytab_path = koji_helper.prepare_keytab(
        config.secret_mount, opts.keytab_file, working_dir
    )
    original_ccname = os.environ.get("KRB5CCNAME")
    ccache_path: str | None = None
    try:
        logger.info("Logging in with Kerberos as %s", opts.principal)
        ccache_path = koji_helper.kinit(
            opts.principal, keytab_path, max_attempts=config.kinit_retries
        )
        os.environ["KRB5CCNAME"] = ccache_path
        user_name = opts.principal.split("@")[0]

        koji_helper.test_koji_connection(opts.koji_profile)
        logger.info(
            'Start task "promote-koji-draft-build" for Application "%s"',
            component_group,
        )

        for component in components:
            name = component.get("name", "")
            if name not in opts.release_components:
                logger.info(
                    "Skip the component %s as it is not in the release list.",
                    name,
                )
                continue
            promote_component(opts.koji_profile, user_name, component)

        logger.info(
            'Completed "promote-koji-draft-build" for "%s"',
            component_group,
        )
    finally:
        keytab_path.unlink(missing_ok=True)
        if ccache_path:
            Path(ccache_path).unlink(missing_ok=True)
        if original_ccname is not None:
            os.environ["KRB5CCNAME"] = original_ccname
        elif "KRB5CCNAME" in os.environ:
            del os.environ["KRB5CCNAME"]


def main() -> int:
    """Read environment variables and promote draft Koji builds."""
    snapshot_spec_file = tekton.require_env("SNAPSHOT_SPEC_FILE")
    data_file = tekton.require_env("DATA_FILE")
    logger.info(
        "Starting promote-koji-draft-build (snapshot=%s, data=%s)",
        snapshot_spec_file,
        data_file,
    )
    koji_source_raw = os.environ.get("KOJI_CONFIG_SOURCE", "").strip()
    koji_config_source = Path(koji_source_raw) if koji_source_raw else None

    run(
        PromoteConfig(
            snapshot_path=Path(snapshot_spec_file),
            data_path=Path(data_file),
            secret_mount=file_helper.path_from_env_variable(
                "SECRET_MOUNT", DEFAULT_SECRET_MOUNT
            ),
            kinit_retries=int(os.environ.get("KINIT_RETRIES", str(DEFAULT_KINIT_RETRIES))),
            koji_config_source=koji_config_source,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
