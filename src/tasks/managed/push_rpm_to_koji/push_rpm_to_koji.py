#!/usr/bin/env python3
"""Push Konflux build RPMs to a Koji instance."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file as file_helper
from release_service_utils.helpers import oci_auth
from release_service_utils.helpers import oras_utils
from release_service_utils.helpers import tekton
from release_service_utils.helpers.koji import (
    install_koji_client_config,
    kinit,
    prepare_keytab,
    redact_koji_output,
    run_koji_cmd,
    test_koji_connection,
)
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd

DEFAULT_DATA_DIR = Path("/var/workdir/release")
DEFAULT_SECRET_MOUNT = Path("/etc/secret")
DEFAULT_RPM_DOWNLOAD_DIR = Path("/var/workdir/rpm-extract")
DEFAULT_KINIT_RETRIES = 10


@dataclass(frozen=True)
class KojiConfig:
    """Configuration for pushing RPMs to Koji."""

    snapshot_path: Path
    data_path: Path
    secret_mount: Path
    data_dir: Path
    rpm_download_dir: Path
    kinit_retries: int
    koji_config_source: Path | None = None


@dataclass(frozen=True)
class PushOptions:
    """Parsed push options from data.json."""

    principal: str
    keytab_file: str
    koji_profile: str
    koji_tags: list[str]
    koji_import_draft: bool
    release_components: list[str]


def parse_push_options(data: dict[str, Any]) -> PushOptions:
    """Parse push options from data.json."""
    push_opts = data.get("pushOptions", {})

    keytab_info = push_opts.get("pushKeytab", {})
    principal = keytab_info.get("principal")
    keytab_file = keytab_info.get("name")

    if not principal or not keytab_file:
        raise ValueError("pushOptions.pushKeytab.principal and name are required")

    koji_profile = push_opts.get("koji_profile")
    if not koji_profile:
        raise ValueError("pushOptions.koji_profile is required")

    koji_tags = push_opts.get("koji_tags", [])
    if not isinstance(koji_tags, list):
        koji_tags = []

    koji_import_draft_raw = push_opts.get("koji_import_draft")
    # An omitted koji_import_draft selects a draft import.
    koji_import_draft = koji_import_draft_raw not in (False, "false")

    # Fetch component list from pushOptions.components or mapping.components
    # Fall back to mapping.components if explicit list is absent or empty
    explicit_components = push_opts.get("components")
    if explicit_components:
        release_components = list(explicit_components)
    else:
        mapping_components = data.get("mapping", {}).get("components", [])
        release_components = [c.get("name", "") for c in mapping_components if c.get("name")]

    return PushOptions(
        principal=principal,
        keytab_file=keytab_file,
        koji_profile=koji_profile,
        koji_tags=koji_tags,
        koji_import_draft=koji_import_draft,
        release_components=release_components,
    )


def get_tag_info(profile: str, tag: str) -> dict[str, Any]:
    """Get tag information from Koji."""
    result = run_koji_cmd(profile, "call", "--json-output", "getTag", tag, noauth=True)
    return json.loads(result.stdout)


def is_sidetag(profile: str, tag: str) -> bool | None:
    """Return whether a tag is a sidetag, or None when Koji omits the field."""
    logger.info("Getting tag info and sidetag value for '%s'", tag)
    tag_info = get_tag_info(profile, tag)
    extra = tag_info.get("extra")
    if not isinstance(extra, dict) or "sidetag" not in extra:
        logger.info("Tag info: %s, sidetag: absent", tag_info)
        return None
    sidetag_value = extra["sidetag"]
    logger.info("Tag info: %s, sidetag: %s", tag_info, sidetag_value)
    if sidetag_value is True or sidetag_value == "true":
        return True
    if sidetag_value is False or sidetag_value == "false":
        return False
    return None


def get_dest_tag(profile: str, build_target: str) -> str:
    """Get the destination tag from a build target."""
    result = run_koji_cmd(
        profile, "call", "--json-output", "getBuildTarget", build_target, noauth=True
    )
    target_info = json.loads(result.stdout)
    dest_tag = target_info.get("dest_tag_name")
    if not dest_tag:
        err = RuntimeError(
            f"Failed to resolve build target '{build_target}' to a destination tag"
        )
        raise tekton.CheckStepError("resolving the Koji destination tag", err) from err
    return dest_tag


def get_existing_build(profile: str, nvr: str) -> dict[str, Any] | None:
    """Check if a build already exists in Koji."""
    result = run_koji_cmd(profile, "call", "--json-output", "getBuild", nvr, noauth=True)
    build_info = json.loads(result.stdout)
    if build_info == "null" or build_info is None:
        return None
    return build_info


def pull_component_image(container_image: str, download_dir: Path, auth_file: Path) -> None:
    """Pull a component image with an existing registry config."""
    download_dir.mkdir(parents=True, exist_ok=True)
    run_cmd(
        [
            "oras",
            "pull",
            "--registry-config",
            str(auth_file),
            container_image,
        ],
        cwd=download_dir,
        check=True,
        stream_stdout=True,
    )


def get_koji_target_from_manifest(container_image: str, auth_file: Path) -> str:
    """Fetch the koji build target from the container image manifest annotations."""
    manifest_json = oras_utils.oras_manifest_fetch(container_image, auth_file)
    manifest = json.loads(manifest_json)
    annotations = manifest.get("annotations", {})
    koji_target = annotations.get("koji.build-target")

    if not koji_target:
        err = RuntimeError("No Koji build target found in the container image annotations")
        raise tekton.CheckStepError("reading the Koji build target annotation", err) from err

    return koji_target


def find_srpm(download_dir: Path) -> Path:
    """Find the source RPM in the download directory."""
    srpms = list(download_dir.glob("*.src.rpm"))
    if not srpms:
        raise FileNotFoundError(f"No source RPM found in {download_dir}")
    if len(srpms) > 1:
        logger.warning("Multiple source RPMs found, using first: %s", srpms[0])
    return srpms[0]


def parse_cg_import(download_dir: Path) -> dict[str, Any]:
    """Parse the cg_import.json file."""
    return file_helper.load_json_dict(download_dir / "cg_import.json")


def init_koji_build(
    profile: str,
    build_name: str,
    build_version: str,
    build_release: str,
    build_epoch: int | None,
    draft: bool,
) -> tuple[int, str]:
    """Initialize a Koji CG build and return (build_id, token)."""
    import_build_data = json.dumps(
        {
            "name": build_name,
            "version": build_version,
            "release": build_release,
            "epoch": build_epoch,
            "draft": draft,
        }
    )
    logger.info("Initializing Koji build: %s", import_build_data)

    result = run_koji_cmd(
        profile,
        "call",
        "--json-output",
        "--json",
        "CGInitBuild",
        '"konflux"',
        import_build_data,
    )
    build_info = json.loads(result.stdout)
    build_id = build_info["build_id"]
    token = build_info["token"]
    logger.info("Initialized build ID: %s", build_id)
    return build_id, token


def import_cg_build(
    profile: str, build_id: int, token: str, cg_import_path: Path, draft: bool
) -> None:
    """Import a CG build to Koji."""
    cmd_args = ["import-cg"]
    if draft:
        cmd_args.append("--draft")
    cmd_args.extend(
        [
            str(cg_import_path),
            f"--token={token}",
            f"--build-id={build_id}",
            ".",
        ]
    )

    try:
        run_koji_cmd(profile, *cmd_args)
        logger.info("Successfully imported build %s", build_id)
    except subprocess.CalledProcessError as e:
        stderr = redact_koji_output(e.stderr or "", cmd_args).strip()
        if stderr:
            logger.error(
                "Import failed (exit code %s), refunding build %s: %s",
                e.returncode,
                build_id,
                stderr,
            )
        else:
            logger.error(
                "Import failed (exit code %s), refunding build %s",
                e.returncode,
                build_id,
            )
        refund_args = [
            "call",
            "--json",
            "CGRefundBuild",
            '"konflux"',
            str(build_id),
            f'"{token}"',
        ]
        try:
            run_koji_cmd(profile, *refund_args)
        except subprocess.CalledProcessError as refund_err:
            detail = redact_koji_output(refund_err.stderr or "", refund_args).strip()
            if detail:
                logger.error(
                    "Refund of build %s failed (exit code %s): %s",
                    build_id,
                    refund_err.returncode,
                    detail,
                )
            else:
                logger.error(
                    "Refund of build %s failed (exit code %s)",
                    build_id,
                    refund_err.returncode,
                )
        raise tekton.CheckStepError("importing the Koji CG build", e) from e


def ensure_package_in_tag(profile: str, tag: str, package_name: str, owner: str) -> None:
    """Add the package when ``list-pkgs`` exits nonzero."""
    try:
        run_koji_cmd(profile, "list-pkgs", "--tag", tag, "--package", package_name)
    except subprocess.CalledProcessError:
        logger.info("Adding package %s to tag %s", package_name, tag)
        run_koji_cmd(profile, "add-pkg", "--force", tag, package_name, "--owner", owner)


def tag_build(profile: str, tag: str, build_id: int) -> None:
    """Tag a build in Koji."""
    logger.info("Tagging build %s with tag %s", build_id, tag)
    run_koji_cmd(profile, "call", "tagBuild", tag, str(build_id))


def process_component(
    component: dict[str, Any],
    push_opts: PushOptions,
    config: KojiConfig,
    user_name: str,
    applied_tags: list[str] | None = None,
) -> None:
    """Process a single component: pull, import to Koji, and tag.

    *applied_tags* starts as the configured extra tags. Each component
    prepends its destination tag, and later components keep the earlier
    destination tags.
    """
    container_image = component.get("containerImage", "")
    component_name = component.get("name", "")
    if not isinstance(container_image, str):
        container_image = ""

    if component_name not in push_opts.release_components:
        logger.info("Skip component %s as it is not in the release list", component_name)
        return

    logger.info("Processing component: %s", component_name)

    if applied_tags is None:
        applied_tags = list(push_opts.koji_tags)

    # Resolve before chdir so a relative download path stays anchored.
    rpm_dir = config.rpm_download_dir.resolve()
    rpm_dir.mkdir(parents=True, exist_ok=True)
    original_cwd = os.getcwd()
    changed_dir = False
    with oci_auth.auth_file(container_image) as auth_file:
        try:
            pull_component_image(container_image, rpm_dir, auth_file)
            os.chdir(rpm_dir)
            changed_dir = True

            srpm = find_srpm(rpm_dir)
            package_nvr = srpm.name.removesuffix(".src.rpm")

            # Parse cg_import.json for build metadata
            cg_import = parse_cg_import(rpm_dir)
            build_name = cg_import["build"]["name"]
            build_version = cg_import["build"]["version"]
            build_release = cg_import["build"]["release"]
            build_epoch = cg_import["build"].get("epoch")

            # Check if we should skip import
            skip_import = False
            existing_build_id: int | None = None

            if not push_opts.koji_import_draft:
                existing = get_existing_build(push_opts.koji_profile, package_nvr)
                if existing:
                    existing_build_id = existing.get("id")
                    logger.info(
                        "Build %s already exists in Koji (ID %s), skipping import",
                        package_nvr,
                        existing_build_id,
                    )
                    skip_import = True

            draft = push_opts.koji_import_draft

            # Get koji build target from image annotations
            koji_target = get_koji_target_from_manifest(container_image, auth_file)
            logger.info("Koji build target from annotations: %s", koji_target)

            # Resolve destination tag
            koji_tag = get_dest_tag(push_opts.koji_profile, koji_target)
            logger.info("Destination tag: %s", koji_tag)

            # Handle draft suffix. Rewrite only when Koji explicitly marks the tag
            # as not a sidetag. An omitted extra.sidetag leaves the tag unchanged.
            if draft and not koji_tag.endswith("-draft"):
                tag_is_sidetag = is_sidetag(push_opts.koji_profile, koji_tag)
                if tag_is_sidetag is False:
                    koji_tag = koji_tag.removesuffix("-candidate") + "-draft"
                    logger.info("Modified tag for draft: %s", koji_tag)

            applied_tags.insert(0, koji_tag)

            if skip_import and existing_build_id is not None:
                build_id = existing_build_id
                logger.info(
                    "Tag existing build %s (ID %s) with tags %s",
                    package_nvr,
                    build_id,
                    applied_tags,
                )
            else:
                logger.info("Import rpm %s with tags %s", package_nvr, applied_tags)

                # Reserve build ID and import
                build_id, token = init_koji_build(
                    push_opts.koji_profile,
                    build_name,
                    build_version,
                    build_release,
                    build_epoch,
                    draft,
                )

                import_cg_build(
                    push_opts.koji_profile,
                    build_id,
                    token,
                    rpm_dir / "cg_import.json",
                    draft,
                )

            # Tag the build
            for tag in applied_tags:
                ensure_package_in_tag(push_opts.koji_profile, tag, build_name, user_name)
                tag_build(push_opts.koji_profile, tag, build_id)
        finally:
            if changed_dir:
                os.chdir(original_cwd)
    shutil.rmtree(rpm_dir)


def run(config: KojiConfig) -> None:
    """Push RPMs to Koji."""
    if config.koji_config_source is not None:
        install_koji_client_config(config.koji_config_source)

    # Load configuration files
    snapshot = file_helper.load_json_dict(config.snapshot_path)
    data = file_helper.load_json_dict(config.data_path)
    push_opts = parse_push_options(data)

    component_group = snapshot.get("componentGroup", "unknown")
    components = snapshot.get("components", [])
    num_components = len(components)

    logger.info(
        "Processing %d components for application '%s'", num_components, component_group
    )

    # The keytab is written next to the snapshot file.
    working_dir = config.snapshot_path.parent
    working_dir.mkdir(parents=True, exist_ok=True)

    # Prepare keytab and authenticate
    keytab_path = prepare_keytab(config.secret_mount, push_opts.keytab_file, working_dir)
    original_ccname = os.environ.get("KRB5CCNAME")
    ccache_path: str | None = None
    try:
        ccache_path = kinit(
            push_opts.principal, keytab_path, max_attempts=config.kinit_retries
        )
        os.environ["KRB5CCNAME"] = ccache_path

        # Extract username from principal
        user_name = push_opts.principal.split("@")[0]

        # Test Koji connection
        test_koji_connection(push_opts.koji_profile)

        logger.info('Start task "push-rpm-to-koji" for Application "%s"', component_group)

        # Destination tags accumulate across components, then the configured tags.
        applied_tags = list(push_opts.koji_tags)
        for i, component in enumerate(components):
            logger.info("Processing component %d/%d", i + 1, num_components)
            process_component(component, push_opts, config, user_name, applied_tags)

        logger.info('Completed "push-rpm-to-koji" for "%s"', component_group)
    finally:
        keytab_path.unlink(missing_ok=True)
        if ccache_path:
            Path(ccache_path).unlink(missing_ok=True)
        if original_ccname is not None:
            os.environ["KRB5CCNAME"] = original_ccname
        elif "KRB5CCNAME" in os.environ:
            del os.environ["KRB5CCNAME"]


def main() -> int:
    """Read environment variables and run the push workflow."""
    snapshot_spec_file = os.environ.get("SNAPSHOT_SPEC_FILE", "")
    data_file = os.environ.get("DATA_FILE", "")

    if not snapshot_spec_file:
        err = ValueError("SNAPSHOT_SPEC_FILE environment variable must be set")
        raise tekton.CheckStepError("reading configuration", err) from err
    if not data_file:
        err = ValueError("DATA_FILE environment variable must be set")
        raise tekton.CheckStepError("reading configuration", err) from err

    config = KojiConfig(
        snapshot_path=Path(snapshot_spec_file),
        data_path=Path(data_file),
        secret_mount=file_helper.path_from_env_variable("SECRET_MOUNT", DEFAULT_SECRET_MOUNT),
        data_dir=file_helper.path_from_env_variable("DATA_DIR", DEFAULT_DATA_DIR),
        rpm_download_dir=file_helper.path_from_env_variable(
            "RPM_DOWNLOAD_DIR", DEFAULT_RPM_DOWNLOAD_DIR
        ),
        kinit_retries=int(os.environ.get("KINIT_RETRIES", str(DEFAULT_KINIT_RETRIES))),
        koji_config_source=(
            Path(os.environ["KOJI_CONFIG_SOURCE"])
            if os.environ.get("KOJI_CONFIG_SOURCE")
            else None
        ),
    )

    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
