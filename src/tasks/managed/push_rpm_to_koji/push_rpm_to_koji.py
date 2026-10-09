#!/usr/bin/env python3
"""Push Konflux build RPMs to a Koji instance."""

from __future__ import annotations

import base64
import errno
import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file as file_helper
from release_service_utils.helpers import oras_utils
from release_service_utils.helpers import tekton
from release_service_utils.helpers.authentication import kinit_with_retry
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.redact import redact_secrets
from release_service_utils.helpers.subprocess_cmd import run_cmd

DEFAULT_DATA_DIR = Path("/var/workdir/release")
DEFAULT_SECRET_MOUNT = Path("/etc/secret")
DEFAULT_RPM_DOWNLOAD_DIR = Path("/var/workdir/rpm-extract")
DEFAULT_KINIT_RETRIES = 10
_TOKEN_ARG = re.compile(r"--token=\S+")


@dataclass(frozen=True)
class KojiConfig:
    """Configuration for pushing RPMs to Koji."""

    snapshot_path: Path
    data_path: Path
    secret_mount: Path
    data_dir: Path
    rpm_download_dir: Path
    kinit_retries: int


@dataclass(frozen=True)
class PushOptions:
    """Parsed push options from data.json."""

    principal: str
    keytab_file: str
    koji_profile: str
    koji_tags: list[str]
    koji_import_draft: bool
    release_components: list[str]


def load_snapshot(path: Path) -> dict[str, Any]:
    """Load and validate the snapshot spec file."""
    if not path.is_file():
        raise FileNotFoundError(f"No valid snapshot file was provided: {path}")
    return file_helper.load_json_dict(path)


def load_data(path: Path) -> dict[str, Any]:
    """Load and validate the data JSON file."""
    if not path.is_file():
        raise FileNotFoundError(f"No data JSON was provided: {path}")
    return file_helper.load_json_dict(path)


def parse_push_options(data: dict[str, Any]) -> PushOptions:
    """Parse push options from data.json.

    This task only imports builds. Promotion is a separate catalog task, so
    any ``pushType`` other than ``import`` (the schema default) is rejected
    before Koji is contacted.
    """
    push_opts = data.get("pushOptions", {})
    push_type = push_opts.get("pushType", "import")
    if push_type != "import":
        raise ValueError(
            f'pushOptions.pushType "{push_type}" is not supported by this task; '
            'only "import" is accepted'
        )

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


def _command_secrets(cmd: list[str]) -> list[str]:
    """Return reservation-token values embedded in a Koji command."""
    secrets: list[str] = []
    for arg in cmd:
        if arg.startswith("--token="):
            secret = arg.removeprefix("--token=")
        elif arg.startswith('"') and arg.endswith('"') and arg != '"konflux"':
            secret = arg[1:-1]
        else:
            continue
        if secret:
            secrets.append(secret)
    return secrets


def _redact_koji_output(text: str, cmd: list[str]) -> str:
    """Redact reservation tokens and other credentials from Koji output."""
    redacted = redact_secrets(text)
    for secret in _command_secrets(cmd):
        redacted = redacted.replace(secret, "<REDACTED>")
    return _TOKEN_ARG.sub("--token=<REDACTED>", redacted)


def _redact_cmd(cmd: list[str]) -> list[str]:
    """Return a copy of a Koji command with build tokens removed.

    Pipeline logs are a public record. ``import-cg`` takes the token as
    ``--token=``, and ``CGRefundBuild`` takes it as a quoted argument. The
    content-generator name ``"konflux"`` is the only other quoted argument
    this task sends, so every other quoted argument is treated as a token.
    """
    redacted: list[str] = []
    for arg in cmd:
        if arg.startswith("--token="):
            redacted.append("--token=<REDACTED>")
        elif arg.startswith('"') and arg.endswith('"') and arg != '"konflux"':
            redacted.append('"<REDACTED>"')
        else:
            redacted.append(arg)
    return redacted


def run_koji_cmd(
    profile: str,
    *args: str,
    check: bool = True,
    capture_output: bool = True,
    noauth: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a koji command with the specified profile.

    Stdout and stderr are captured so Koji does not write them straight to
    the pipeline log. The logged command has build tokens removed. On
    failure, stderr is logged after reservation tokens and other credentials
    are redacted. The raised ``CalledProcessError`` keeps that redacted
    stderr, and its ``cmd`` is redacted so a traceback cannot reprint the
    token.
    """
    cmd = ["koji", f"--profile={profile}"]
    if noauth:
        cmd.append("--noauth")
    cmd.extend(args)
    logger.info("Running: %s", " ".join(_redact_cmd(cmd)))
    try:
        return subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            check=check,
        )
    except subprocess.CalledProcessError as exc:
        if exc.stderr:
            exc.stderr = _redact_koji_output(exc.stderr, cmd)
            if exc.stderr.strip():
                logger.error("Koji stderr: %s", exc.stderr.strip())
        exc.cmd = _redact_cmd(cmd)
        raise


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


def _reject_escaped_keytab_path(base: Path, relative: str) -> Path:
    """Return *relative* resolved under *base*, or reject an escaping path."""
    try:
        confined = file_helper.resolve_path_under_base(base, relative)
    except ValueError as e:
        raise ValueError(f"keytab path must stay under {base}: {relative!r}") from e
    if confined == base.resolve():
        raise ValueError(f"keytab path must name a file under {base}: {relative!r}")
    return confined


def _confined_keytab_path(base: Path, relative: str) -> Path:
    """Return *relative* under *base* without following symlinks.

    Absolute paths and ``..`` traversal are rejected. A symlink at any
    component is rejected so a later write cannot truncate the link target.
    """
    confined = _reject_escaped_keytab_path(base, relative)
    current = base.resolve()
    for part in Path(str(relative).strip()).parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"keytab path must not be a symlink: {relative!r}")
    return confined


def _confined_keytab_source(base: Path, relative: str) -> Path:
    """Resolve a Secret keytab path, following links that stay inside *base*.

    Kubernetes Secret volumes expose each key as a symlink through ``..data``.
    Those links are accepted when the target remains inside the mount. A link
    that escapes the mount is rejected.
    """
    return _reject_escaped_keytab_path(base, relative)


def _is_regular_file(path: Path) -> bool:
    """Return whether *path* is a regular file, without following symlinks."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return False
    return stat.S_ISREG(mode)


def _read_keytab_bytes(path: Path) -> bytes:
    """Read *path* without following a symlink."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as e:
        if e.errno == errno.ELOOP:
            raise ValueError(f"keytab path must not be a symlink: {path}") from e
        raise
    with os.fdopen(fd, "rb") as handle:
        return handle.read()


def _write_keytab_secure(keytab_path: Path, data: bytes) -> None:
    """Write keytab data with mode 0600, without following symlinks.

    ``O_CREAT`` applies the mode only when the file is created. An existing
    destination is forced to 0600 before the keytab bytes are written.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW
    try:
        fd = os.open(keytab_path, flags, 0o600)
    except OSError as e:
        if e.errno == errno.ELOOP:
            raise ValueError(f"keytab path must not be a symlink: {keytab_path}") from e
        raise
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, data)
    finally:
        os.close(fd)


def prepare_keytab(secret_mount: Path, keytab_file: str, working_dir: Path) -> Path:
    """Copy the keytab into *working_dir*.

    The destination is a regular file under *working_dir* and is never a
    symlink. Source paths may be Secret-volume symlinks, including ``..data``
    links, when they resolve to a regular file inside *secret_mount*. Absolute
    paths, ``..`` traversal, and source links that leave the mount are rejected.
    """
    keytab_path = _confined_keytab_path(working_dir, keytab_file)
    base64_keytab = _confined_keytab_source(secret_mount, "base64_keytab")
    if _is_regular_file(base64_keytab):
        logger.info("Reading base64-encoded keytab from secret")
        encoded = _read_keytab_bytes(base64_keytab).decode("utf-8").strip()
        _write_keytab_secure(keytab_path, base64.b64decode(encoded))
    else:
        source_keytab = _confined_keytab_source(secret_mount, keytab_file)
        if not _is_regular_file(source_keytab):
            raise FileNotFoundError(f"Keytab file not found: {source_keytab}")
        logger.info("Copying keytab from secret mount")
        _write_keytab_secure(keytab_path, _read_keytab_bytes(source_keytab))

    return keytab_path


def kinit(principal: str, keytab_path: Path, max_attempts: int = 10) -> str:
    """Perform kinit with retries and return the ccache path."""
    ccache_path = file_helper.make_tempfile_path("ccache-")
    authenticated = False
    try:
        kenv = {"KRB5CCNAME": str(ccache_path)}
        kinit_with_retry(principal, keytab_path, kenv, max_attempts=max_attempts)
        authenticated = True
        return str(ccache_path)
    except subprocess.CalledProcessError as e:
        raise tekton.CheckStepError("logging in with Kerberos (kinit)", e) from e
    finally:
        if not authenticated:
            ccache_path.unlink(missing_ok=True)


def test_koji_connection(profile: str) -> None:
    """Test the Koji connection."""
    logger.info("Testing Koji connection...")
    run_koji_cmd(profile, "hello")
    logger.info("Koji connection successful")


def pull_component_image(container_image: str, download_dir: Path) -> None:
    """Pull a component image using oras."""
    download_dir.mkdir(parents=True, exist_ok=True)
    oras_utils.oras_pull(container_image, download_dir)


def get_koji_target_from_manifest(container_image: str) -> str:
    """Fetch the koji build target from the container image manifest annotations."""
    auth_file = file_helper.make_tempfile_path("oras-auth-")
    try:
        auth_result = run_cmd(["select-oci-auth", container_image], check=True)
        auth_file.write_text(auth_result.stdout, encoding="utf-8")

        manifest_json = oras_utils.oras_manifest_fetch(container_image, auth_file)
        manifest = json.loads(manifest_json)
        annotations = manifest.get("annotations", {})
        koji_target = annotations.get("koji.build-target")

        if not koji_target:
            err = RuntimeError("No Koji build target found in the container image annotations")
            raise tekton.CheckStepError(
                "reading the Koji build target annotation", err
            ) from err

        return koji_target
    finally:
        auth_file.unlink(missing_ok=True)


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
    cg_import_path = download_dir / "cg_import.json"
    if not cg_import_path.is_file():
        raise FileNotFoundError(f"cg_import.json not found in {download_dir}")
    return file_helper.load_json_dict(cg_import_path)


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
        stderr = _redact_koji_output(e.stderr or "", cmd_args).strip()
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
            detail = _redact_koji_output(refund_err.stderr or "", refund_args).strip()
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


def _package_listed(stdout: str, package_name: str) -> bool:
    """Return whether a list-pkgs response includes package_name."""
    for line in stdout.splitlines():
        fields = line.split()
        if not fields or fields[0] == "Package" or fields[0].startswith("-"):
            continue
        if fields[0] == package_name:
            return True
    return False


def ensure_package_in_tag(profile: str, tag: str, package_name: str, owner: str) -> None:
    """Ensure a package exists in the tag's package list.

    A nonzero ``list-pkgs`` status counts as not listed, so ``add-pkg`` is
    attempted. A successful listing that omits the package also attempts
    ``add-pkg``.
    """
    try:
        result = run_koji_cmd(profile, "list-pkgs", "--tag", tag, "--package", package_name)
    except subprocess.CalledProcessError:
        listed = False
    else:
        listed = _package_listed(result.stdout or "", package_name)
    if listed:
        return
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
) -> None:
    """Process a single component: pull, import to Koji, and tag."""
    container_image = component.get("containerImage", "")
    component_name = component.get("name", "")

    if component_name not in push_opts.release_components:
        logger.info("Skip component %s as it is not in the release list", component_name)
        return

    if not container_image or not container_image.strip():
        logger.info("Skip component %s: missing or blank containerImage", component_name)
        return

    logger.info("Processing component: %s", component_name)

    # Clean up and create download directory (resolve to absolute path before chdir)
    rpm_dir = config.rpm_download_dir.resolve()
    if rpm_dir.exists():
        import shutil

        shutil.rmtree(rpm_dir)
    rpm_dir.mkdir(parents=True)

    # Pull the container image
    pull_component_image(container_image, rpm_dir)

    # Change to RPM directory for koji import
    original_cwd = os.getcwd()
    os.chdir(rpm_dir)

    try:
        # Find SRPM (needed for import)
        find_srpm(rpm_dir)

        # Parse cg_import.json for build metadata
        cg_import = parse_cg_import(rpm_dir)
        build_name = cg_import["build"]["name"]
        build_version = cg_import["build"]["version"]
        build_release = cg_import["build"]["release"]
        build_epoch = cg_import["build"].get("epoch")

        # Construct NVR from build fields
        package_nvr = f"{build_name}-{build_version}-{build_release}"

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
        koji_target = get_koji_target_from_manifest(container_image)
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

        # Build list of tags to apply
        all_tags = [koji_tag] + list(push_opts.koji_tags)

        if skip_import and existing_build_id is not None:
            build_id = existing_build_id
            logger.info(
                "Tag existing build %s (ID %s) with tags %s", package_nvr, build_id, all_tags
            )
        else:
            logger.info("Import rpm %s with tags %s", package_nvr, all_tags)

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
        for tag in all_tags:
            ensure_package_in_tag(push_opts.koji_profile, tag, build_name, user_name)
            tag_build(push_opts.koji_profile, tag, build_id)

    finally:
        os.chdir(original_cwd)


def run(config: KojiConfig) -> None:
    """Push RPMs to Koji."""
    # Load configuration files
    snapshot = load_snapshot(config.snapshot_path)
    data = load_data(config.data_path)
    push_opts = parse_push_options(data)

    component_group = snapshot.get("componentGroup", "unknown")
    components = snapshot.get("components", [])
    num_components = len(components)

    logger.info(
        "Processing %d components for application '%s'", num_components, component_group
    )

    # Prepare working directory
    working_dir = config.data_dir / config.snapshot_path.parent.name
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

        # Process each component
        for i, component in enumerate(components):
            logger.info("Processing component %d/%d", i + 1, num_components)
            process_component(component, push_opts, config, user_name)

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
    )

    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
