#!/usr/bin/env python3
"""Push disk images with Pulp and publish metadata to the Developer Portal / CGW.

Reads snapshot JSON and credentials from mounted secrets, pulls OCI artifacts with
oras, stages files, invokes pulp_push_wrapper and developer_portal_wrapper.

Environment variables (set by the pulp-push-disk-images Tekton task):
  SNAPSHOT_JSON, CERT_EXPIRATION_WARN_DAYS, CONCURRENT_LIMIT, EXODUS_GW_ENV,
  CGW_HOSTNAME, RESULT_RESULT

Mount paths default to /mnt/exodusGwSecret, /mnt/pulpSecret, /mnt/udcacheSecret,
/mnt/redhat-workloads-token, /mnt/cgwSecret and can be overridden in tests.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import authentication
import file
import oras_utils
import push_artifacts
import subprocess_cmd
import tekton
from logger import logger

PROG = "pulp_push_disk_images.py"

DEFAULT_EXODUS_MOUNT = Path("/mnt/exodusGwSecret")
DEFAULT_PULP_MOUNT = Path("/mnt/pulpSecret")
DEFAULT_UDCACHE_MOUNT = Path("/mnt/udcacheSecret")
DEFAULT_WORKLOADS_MOUNT = Path("/mnt/redhat-workloads-token")
DEFAULT_CGW_MOUNT = Path("/mnt/cgwSecret")

RunCmd = Callable[..., subprocess.CompletedProcess[str]]


def _validate_certificates(
    warn_days: int,
    *,
    exodus_mount: Path,
    pulp_mount: Path,
    udcache_mount: Path,
) -> None:
    """Validate Exodus, Pulp, and UDCache certificate expiration."""
    logger.info("=== Checking certificate expiration ===")
    for label, cert_path in (
        ("Exodus Gateway", exodus_mount / "cert"),
        ("Pulp", pulp_mount / "konflux-release-rhsm-pulp.crt"),
        ("UDCache", udcache_mount / "cert"),
    ):
        logger.info("Checking %s certificate", label)
        push_artifacts._check_cert_expiration(str(cert_path), warn_days)
    logger.info("=== All certificates are valid ===")


def normalize_docker_config(raw: str) -> str:
    """Strip extra quoted fields from a dockerconfigjson secret payload."""
    return re.sub(r"(^|\})[^{}]+(\{|$)", r"\1\2", raw)


def build_staged_payload(
    disk_image_dir: Path,
    version: str,
) -> dict[str, Any]:
    """Build the staged.json payload listing every file under *disk_image_dir*."""
    payload: dict[str, Any] = {"header": {"version": "0.2"}, "payload": {"files": []}}
    files: list[dict[str, str]] = []
    for path in sorted(disk_image_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(disk_image_dir).as_posix()
        files.append(
            {
                "filename": path.name,
                "relative_path": rel,
                "version": version,
            }
        )
    payload["payload"]["files"] = files
    return payload


def require_json_field(data: dict[str, Any], *keys: str) -> Any:
    """Return ``data[keys...]`` or raise ``ValueError`` with a clear message."""
    cur: Any = data
    path = ""
    for key in keys:
        path = f"{path}.{key}" if path else key
        if not isinstance(cur, dict) or key not in cur:
            raise ValueError(f"Missing {path} value for component")
        cur = cur[key]
    if cur in (None, ""):
        raise ValueError(f"Missing {path} value for component")
    return cur


def require_staged_files_field(entry: dict[str, Any], field: str) -> Any:
    """Validate one ``staged.files[]`` mapping entry (bash ``jq -er`` equivalent)."""
    label = f"staged.files[].{field}"
    if not isinstance(entry, dict) or field not in entry or entry[field] in (None, ""):
        raise ValueError(f"Missing {label} value for component")
    return entry[field]


def process_component(
    component: dict[str, Any],
    disk_image_dir: Path,
    *,
    stderr_path: Path,
    run_cmd: RunCmd = subprocess_cmd.run_cmd,
) -> None:
    """Pull one OCI artifact and stage mapped files for Pulp upload."""
    pull_spec = require_json_field(component, "containerImage")
    destination_name = require_json_field(component, "staged", "destination")
    destination = disk_image_dir / str(destination_name) / "FILES"
    destination.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as download_dir:
        download = Path(download_dir)
        oras_utils.oras_pull(
            str(pull_spec),
            download,
            stderr_path=stderr_path,
        )

        staged_files = component.get("staged", {}).get("files", [])
        if not isinstance(staged_files, list):
            raise ValueError("staged.files must be a list")

        for entry in staged_files:
            if not isinstance(entry, dict):
                raise ValueError("staged.files entries must be objects")
            source = require_staged_files_field(entry, "source")
            filename = require_staged_files_field(entry, "filename")
            source_path = download / str(source)
            gz_path = Path(str(source_path) + ".gz")
            if gz_path.is_file():
                run_cmd(["gzip", "-d", str(gz_path)], cwd=download, check=True)
            dest_file = destination / str(filename)
            if dest_file.exists():
                raise ValueError(
                    "Multiple files use the same destination value: "
                    f"{destination} and filename value: {filename}. Failing..."
                )
            if source_path.is_file():
                shutil.move(str(source_path), dest_file)
            else:
                logger.warning("didn't find mapped file: %s", source)


def process_component_for_developer_portal(
    component: dict[str, Any],
    content_directory: Path,
    cgw_hostname: str,
    *,
    env: Mapping[str, str] | None = None,
    stderr_path: Path,
    run_cmd: RunCmd = subprocess_cmd.run_cmd,
) -> None:
    """Upload staged files for one component via developer_portal_wrapper."""
    product_name = require_json_field(component, "contentGateway", "productName")
    product_code = require_json_field(component, "contentGateway", "productCode")
    product_version_name = require_json_field(
        component,
        "contentGateway",
        "productVersionName",
    )
    file_prefix = require_json_field(component, "contentGateway", "filePrefix")

    cmd_env = dict(env or {})
    if "developers.qa.redhat.com" in cgw_hostname:
        cmd_env["HTTP_PROXY"] = "http://squid.corp.redhat.com:3128"
        cmd_env["HTTPS_PROXY"] = "http://squid.corp.redhat.com:3128"
        logger.info("Using squid proxy for preprod CGW access")

    run_cmd(
        [
            "developer_portal_wrapper",
            "--debug",
            "--product-name",
            str(product_name),
            "--product-code",
            str(product_code),
            "--product-version-name",
            str(product_version_name),
            "--cgw-hostname",
            cgw_hostname,
            "--content-directory",
            str(content_directory),
            "--file-prefix",
            str(file_prefix),
        ],
        env=cmd_env,
        stderr_path=stderr_path,
        check=True,
    )


def run_push(
    snapshot: dict[str, Any],
    *,
    concurrent_limit: int,
    exodus_gw_env: str,
    cgw_hostname: str,
    cert_warn_days: int,
    exodus_mount: Path,
    pulp_mount: Path,
    udcache_mount: Path,
    workloads_mount: Path,
    cgw_mount: Path,
    run_cmd: RunCmd = subprocess_cmd.run_cmd,
) -> None:
    """Execute the full pulp disk-image push workflow."""
    _validate_certificates(
        cert_warn_days,
        exodus_mount=exodus_mount,
        pulp_mount=pulp_mount,
        udcache_mount=udcache_mount,
    )

    exodus_cert = authentication.read_mounted_text(exodus_mount, "cert")
    exodus_key = authentication.read_mounted_text(exodus_mount, "key")
    exodus_url = authentication.read_mounted_text(exodus_mount, "url")
    pulp_url = authentication.read_mounted_text(pulp_mount, "pulp_url")
    pulp_cert = authentication.read_mounted_text(pulp_mount, "konflux-release-rhsm-pulp.crt")
    pulp_key = authentication.read_mounted_text(pulp_mount, "konflux-release-rhsm-pulp.key")
    udc_url = authentication.read_mounted_text(udcache_mount, "url")
    udc_cert = authentication.read_mounted_text(udcache_mount, "cert")
    udc_key = authentication.read_mounted_text(udcache_mount, "key")
    docker_config = authentication.read_mounted_text(workloads_mount, ".dockerconfigjson")
    cgw_username = authentication.read_mounted_text(cgw_mount, "username")
    cgw_password = authentication.read_mounted_text(cgw_mount, "token")

    stderr_path = Path("/tmp/stderr.txt")
    stderr_path.write_text("", encoding="utf-8")

    exodus_gw_cert = file.make_tempfile_path("exodus-", exodus_cert.encode())
    exodus_gw_key = file.make_tempfile_path("exodus-key-", exodus_key.encode())
    pulp_cert_file = file.make_tempfile_path("pulp-", pulp_cert.encode())
    pulp_key_file = file.make_tempfile_path("pulp-key-", pulp_key.encode())
    udcache_cert = file.make_tempfile_path("udc-", udc_cert.encode())
    udcache_key = file.make_tempfile_path("udc-key-", udc_key.encode())

    env = {
        **os.environ,
        "CGW_USERNAME": cgw_username,
        "CGW_PASSWORD": cgw_password,
        "EXODUS_GW_CERT": str(exodus_gw_cert),
        "EXODUS_GW_KEY": str(exodus_gw_key),
        "PULP_CERT_FILE": str(pulp_cert_file),
        "PULP_KEY_FILE": str(pulp_key_file),
        "UDCACHE_CERT": str(udcache_cert),
        "UDCACHE_KEY": str(udcache_key),
        "EXODUS_GW_ENV": exodus_gw_env,
        "EXODUS_GW_URL": exodus_url,
        "EXODUS_PULP_HOOK_ENABLED": "True",
        "EXODUS_GW_TIMEOUT": "7200",
    }

    docker_dir = Path.home() / ".docker"
    docker_dir.mkdir(parents=True, exist_ok=True)
    (docker_dir / "config.json").write_text(
        normalize_docker_config(docker_config),
        encoding="utf-8",
    )

    components = snapshot.get("components")
    if not isinstance(components, list) or not components:
        raise ValueError("snapshot must contain a non-empty components list")

    version = (
        components[0].get("staged", {}).get("version")
        if isinstance(components[0], dict)
        else None
    )
    if not version:
        msg = (
            "Error: version not specified in .components[0].staged.version. "
            "Needed to publish to customer portal"
        )
        with stderr_path.open("a", encoding="utf-8") as errf:
            errf.write(msg + "\n")
        raise tekton.CheckStepError("validating staged version", ValueError(msg))

    with tempfile.TemporaryDirectory() as disk_dir_name:
        disk_image_dir = Path(disk_dir_name)

        def _run_one(component: dict[str, Any]) -> None:
            process_component(
                component,
                disk_image_dir,
                stderr_path=stderr_path,
                run_cmd=run_cmd,
            )

        with ThreadPoolExecutor(max_workers=max(1, concurrent_limit)) as pool:
            futures = [
                pool.submit(_run_one, component)
                for component in components
                if isinstance(component, dict)
            ]
            for future in as_completed(futures):
                future.result()

        staged = build_staged_payload(disk_image_dir, str(version))
        staged_yaml = disk_image_dir / "staged.yaml"
        staged_yaml.write_text(
            run_cmd(
                ["yq", "-P", "-I", "4"],
                stdin=json.dumps(staged),
                check=True,
            ).stdout,
            encoding="utf-8",
        )

        run_cmd(
            [
                "pulp_push_wrapper",
                "--debug",
                "--source",
                str(disk_image_dir),
                "--pulp-url",
                pulp_url,
                "--pulp-cert",
                str(pulp_cert_file),
                "--pulp-key",
                str(pulp_key_file),
                "--udcache-url",
                udc_url,
            ],
            env=env,
            stderr_path=stderr_path,
            check=True,
        )

        for component in components:
            if not isinstance(component, dict):
                continue
            destination_name = require_json_field(component, "staged", "destination")
            content_directory = disk_image_dir / str(destination_name) / "FILES"
            process_component_for_developer_portal(
                component,
                content_directory,
                cgw_hostname,
                env=env,
                stderr_path=stderr_path,
                run_cmd=run_cmd,
            )


def main() -> int:
    """Write RESULT_RESULT and always return exit code 0 for Tekton."""
    rpath = tekton.result_paths_from_env("RESULT_RESULT")[0]
    snapshot_raw = os.environ.get("SNAPSHOT_JSON", "").strip()
    if not snapshot_raw:
        print(f"{PROG}: SNAPSHOT_JSON must be set", file=sys.stderr)
        raise SystemExit(1)

    cert_warn_days = int(os.environ.get("CERT_EXPIRATION_WARN_DAYS", "7"))
    concurrent_limit = int(os.environ.get("CONCURRENT_LIMIT", "3"))
    exodus_gw_env = os.environ.get("EXODUS_GW_ENV", "").strip()
    cgw_hostname = os.environ.get("CGW_HOSTNAME", "").strip()
    if not exodus_gw_env:
        print(f"{PROG}: EXODUS_GW_ENV must be set", file=sys.stderr)
        raise SystemExit(1)
    if not cgw_hostname:
        print(f"{PROG}: CGW_HOSTNAME must be set", file=sys.stderr)
        raise SystemExit(1)

    exodus_mount = file.path_from_env_variable("EXODUS_GW_SECRET_MOUNT", DEFAULT_EXODUS_MOUNT)
    pulp_mount = file.path_from_env_variable("PULP_SECRET_MOUNT", DEFAULT_PULP_MOUNT)
    udcache_mount = file.path_from_env_variable("UDCACHE_SECRET_MOUNT", DEFAULT_UDCACHE_MOUNT)
    workloads_mount = file.path_from_env_variable(
        "REDHAT_WORKLOADS_TOKEN_MOUNT",
        DEFAULT_WORKLOADS_MOUNT,
    )
    cgw_mount = file.path_from_env_variable("CGW_SECRET_MOUNT", DEFAULT_CGW_MOUNT)

    stderr_path = Path("/tmp/stderr.txt")
    stderr_path.write_text("", encoding="utf-8")
    try:
        snapshot = json.loads(snapshot_raw)
        if not isinstance(snapshot, dict):
            raise ValueError("SNAPSHOT_JSON must decode to a JSON object")
        run_push(
            snapshot,
            concurrent_limit=concurrent_limit,
            exodus_gw_env=exodus_gw_env,
            cgw_hostname=cgw_hostname,
            cert_warn_days=cert_warn_days,
            exodus_mount=exodus_mount,
            pulp_mount=pulp_mount,
            udcache_mount=udcache_mount,
            workloads_mount=workloads_mount,
            cgw_mount=cgw_mount,
        )
    except (ValueError, OSError, subprocess.CalledProcessError, tekton.CheckStepError) as exc:
        tekton.write_failure_result(
            rpath,
            PROG,
            exc,
            command_log_path=stderr_path,
            workflow_action="pushing disk images",
        )
        return 0

    rpath.write_text("Success", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
