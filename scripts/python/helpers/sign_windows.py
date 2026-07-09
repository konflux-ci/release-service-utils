#!/usr/bin/env python3
"""Sign Windows binaries on a remote Windows host via SSH.

For each component with a ``has_windows`` flag file:
* Generates a Windows batch script that pulls unsigned OCI artifacts, runs
  ``signtool`` to sign and verify all binaries, pushes signed content back to
  Quay, and writes the digest to a file.
* SCPs the script to the Windows host and executes it via SSH.
* Copies the resulting signed digest back, normalises line endings, and writes
  it to ``<component_dir>/signed_windows_digest.txt``.
* Always cleans up the remote temporary directory.

CLI arguments:
  ``--quay-url``
  ``--pipeline-run-uid``

Secret mounts:
  ``WINDOWS_SSH_KEY_MOUNT``      (default: ``/mnt/secrets``)
  ``WINDOWS_CREDS_MOUNT``        (default: ``/mnt/windowsCredentials``)
  ``QUAY_SECRET_MOUNT``          (default: ``/mnt/quaySecret``)

Other env vars:
  ``SNAPSHOT_JSON``   – JSON string of the Snapshot spec
  ``CONTENT_DIR``     – override base directory (default: ``/shared/artifacts``)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

PROG = "sign_windows.py"

WINDOWS_SSH_KEY_MOUNT = Path(os.environ.get("WINDOWS_SSH_KEY_MOUNT", "/mnt/secrets"))
WINDOWS_CREDS_MOUNT = Path(os.environ.get("WINDOWS_CREDS_MOUNT", "/mnt/windowsCredentials"))
QUAY_SECRET_MOUNT = Path(os.environ.get("QUAY_SECRET_MOUNT", "/mnt/quaySecret"))
DEST_QUAY_SECRET_MOUNT = Path(os.environ.get("DEST_QUAY_SECRET_MOUNT", "/mnt/destQuaySecret"))
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument("--quay-url", required=True, help="Quay repository URL base")
    p.add_argument("--pipeline-run-uid", required=True, help="Unique ID for this pipeline run")
    return p.parse_args(argv)


def _build_batch_script(
    *,
    quay_url: str,
    quay_user: str,
    quay_pass: str,
    component_name: str,
    unsigned_digest: str,
    pipeline_run_uid: str,
    windows_temp_dir: str,
) -> str:
    """Build the remote batch script that pulls, signs, verifies, and pushes binaries."""
    return f"""
mkdir %TEMP%\\{windows_temp_dir} && cd /d %TEMP%\\{windows_temp_dir}
@echo off
oras login quay.io -u {quay_user} -p {quay_pass}
@echo on
oras pull {quay_url}/unsigned/{component_name}@{unsigned_digest} -o unsigned
REM The content is extracted to unsigned\\windows with os/arch/ subdirectories

REM Recursively sign all files in unsigned\\windows directory tree
for /r unsigned\\windows %%f in (*) do (
  signtool sign /v /n "Red Hat" /fd SHA256 /tr http://timestamp.digicert.com /td SHA256 "%%f"
  if errorlevel 1 (
    echo Signing of %%f failed
    exit /B %ERRORLEVEL%
  )
)

REM Recursively verify all signed files
for /r unsigned\\windows %%f in (*) do (
  signtool verify /v /pa "%%f"
  if errorlevel 1 (
    echo Verification of %%f failed
    exit /B %ERRORLEVEL%
  )
)

echo [%DATE% %TIME%] Signing of Windows binaries for {component_name} completed successfully

cd unsigned
oras push ^
  --annotation=quay.expires-after=1d ^
  {quay_url}/signed/{component_name}:{pipeline_run_uid}-windows ^
  windows
if %ERRORLEVEL% neq 0 (
  echo ERROR: oras push failed with error %ERRORLEVEL%
  exit /B %ERRORLEVEL%
)
"""


def _run_custom_script(
    *,
    signing_script: str,
    signing_args: list[str],
    ssh_opts: list[str],
    scp_opts: list[str],
    windows_user: str,
    windows_host: str,
    name: str,
    origin: str,
    commit_sha: str,
    component_dir: Path,
    quay_url: str,
    dest_quay_url: str,
    quay_user: str,
    quay_pass: str,
    dest_quay_user: str,
    dest_quay_pass: str,
    unsigned_digest: str,
    pipeline_run_uid: str,
    win_cert_thumbprint: str = "",
) -> None:
    """Run a custom signing script already present on the remote Windows host."""
    win_temp_base = f"C:/Users/{windows_user}/AppData/Local/Temp"
    digest_file = f"{win_temp_base}/signed_digest_{pipeline_run_uid}_{name}.txt"
    tag = f"{pipeline_run_uid[:8]}-{commit_sha[:8]}-windows"

    env_vars = {
        "QUAY_USER": quay_user,
        "QUAY_PASS": quay_pass,
        "QUAY_DEST_USER": dest_quay_user,
        "QUAY_DEST_PASS": dest_quay_pass,
        "UNSIGNED_REF": f"{quay_url}/unsigned/{name}@{unsigned_digest}",
        "SIGNED_REF": f"{dest_quay_url}/{origin}/{name}:{tag}",
        "OUTPUT_DIGEST": digest_file,
        "WIN_CERT_THUMBPRINT": win_cert_thumbprint,
    }
    args_str = " ".join(f"'{a.replace(chr(39), chr(39)*2)}'" for a in signing_args)
    stdin_script = ""
    for k, v in env_vars.items():
        escaped_v = v.replace("'", "''")
        stdin_script += f"$env:{k}='{escaped_v}'\n"
    stdin_script += f"& '{signing_script}' {args_str}\n"

    ssh_exit = 0
    failed_op = ""
    result = subprocess.run(
        ["ssh"] + ssh_opts + [f"{windows_user}@{windows_host}", "powershell", "-Command", "-"],
        input=stdin_script,
        text=True,
    )
    if result.returncode != 0:
        ssh_exit = result.returncode
        failed_op = "signing"

    if ssh_exit == 0:
        scp_result = subprocess.run(
            ["scp"]
            + scp_opts
            + [
                f"{windows_user}@{windows_host}:{digest_file}",
                str(component_dir / "signed_windows_digest.txt"),
            ]
        )
        if scp_result.returncode != 0:
            ssh_exit = scp_result.returncode
            failed_op = "scp of signed digest"

    digest_file_escaped = digest_file.replace("/", "\\\\")
    cleanup = subprocess.run(
        ["ssh"]
        + ssh_opts
        + [
            f"{windows_user}@{windows_host}",
            f"Remove-Item -LiteralPath '{digest_file_escaped}' -Force",
        ],
        check=False,
    )
    if cleanup.returncode != 0:
        logger.warning(
            "Remote cleanup of digest file failed for %s (exit code: %d) — "
            "%s may remain on the Windows host",
            name,
            cleanup.returncode,
            digest_file,
        )

    if ssh_exit != 0:
        raise RuntimeError(
            f"Windows {failed_op} failed for component: {name}" f" (exit code: {ssh_exit})"
        )


def _run_default_script(
    *,
    ssh_opts: list[str],
    scp_opts: list[str],
    windows_user: str,
    windows_host: str,
    name: str,
    component_dir: Path,
    quay_url: str,
    quay_user: str,
    quay_pass: str,
    unsigned_digest: str,
    pipeline_run_uid: str,
) -> None:
    """Build, upload, and run the default signtool script on the remote Windows host."""
    windows_temp_dir = f"{pipeline_run_uid}_{name}"
    win_temp_base = f"C:/Users/{windows_user}/AppData/Local/Temp"
    windows_script_path = f"{win_temp_base}/windows_signing_script_file_{windows_temp_dir}.bat"

    script_content = _build_batch_script(
        quay_url=quay_url,
        quay_user=quay_user,
        quay_pass=quay_pass,
        component_name=name,
        unsigned_digest=unsigned_digest,
        pipeline_run_uid=pipeline_run_uid,
        windows_temp_dir=windows_temp_dir,
    )

    fd, script_path = tempfile.mkstemp(suffix=".bat")
    local_script = Path(script_path)
    os.write(fd, script_content.encode())
    os.close(fd)

    try:
        subprocess.check_call(
            ["scp"]
            + scp_opts
            + [
                str(local_script),
                f"{windows_user}@{windows_host}:{windows_script_path}",
            ]
        )

        ssh_exit = 0
        failed_op = ""
        result = subprocess.run(
            ["ssh"] + ssh_opts + [f"{windows_user}@{windows_host}", windows_script_path],
            capture_output=True,
            text=True,
        )
        logger.info("%s", result.stdout)
        if result.stderr:
            logger.info("%s", result.stderr)
        if result.returncode != 0:
            ssh_exit = result.returncode
            failed_op = "signing"

        if ssh_exit == 0:
            digest = None
            for line in result.stdout.splitlines():
                if line.strip().startswith("Digest:"):
                    digest = line.strip().split()[-1]
            if digest:
                (component_dir / "signed_windows_digest.txt").write_text(
                    digest, encoding="utf-8"
                )
            else:
                ssh_exit = 1
                failed_op = "parsing Digest from oras push output"

        cleanup_path = (
            f"C:\\\\Users\\\\{windows_user}\\\\AppData\\\\Local\\\\Temp"
            f"\\\\{windows_temp_dir}"
        )
        cleanup = subprocess.run(
            ["ssh"]
            + ssh_opts
            + [
                f"{windows_user}@{windows_host}",
                f"Remove-Item -LiteralPath '{cleanup_path}' -Force -Recurse; "
                f"Remove-Item -LiteralPath '{windows_script_path}' -Force",
            ],
            check=False,
        )
        if cleanup.returncode != 0:
            logger.warning(
                "Remote cleanup failed for %s (exit code: %d) — "
                "%s and %s may remain on the Windows host",
                name,
                cleanup.returncode,
                cleanup_path,
                windows_script_path,
            )

        if ssh_exit != 0:
            raise RuntimeError(
                f"Windows {failed_op} failed for component: {name}" f" (exit code: {ssh_exit})"
            )
    finally:
        local_script.unlink(missing_ok=True)


def run(
    quay_url: str,
    pipeline_run_uid: str,
    signing_script: str | None = None,
    signing_args: list[str] | None = None,
    dest_quay_url: str | None = None,
    origin: str = "",
) -> None:
    """Sign Windows binaries on the remote host for every component with a has_windows flag."""
    snapshot = json.loads(os.environ["SNAPSHOT_JSON"])
    quay_url = quay_url.rstrip("/")

    windows_user = (WINDOWS_CREDS_MOUNT / "username").read_text().strip()
    windows_port = (WINDOWS_CREDS_MOUNT / "port").read_text().strip()
    windows_host = (WINDOWS_CREDS_MOUNT / "host").read_text().strip()
    quay_user = (QUAY_SECRET_MOUNT / "username").read_text().strip()
    quay_pass = (QUAY_SECRET_MOUNT / "password").read_text().strip()

    dest_quay_user_path = DEST_QUAY_SECRET_MOUNT / "username"
    dest_quay_pass_path = DEST_QUAY_SECRET_MOUNT / "password"
    if dest_quay_user_path.exists() and dest_quay_pass_path.exists():
        dest_quay_user = dest_quay_user_path.read_text().strip()
        dest_quay_pass = dest_quay_pass_path.read_text().strip()
    else:
        dest_quay_user = quay_user
        dest_quay_pass = quay_pass

    ssh_dir = Path("/tmp/.ssh")
    ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    id_rsa = ssh_dir / "id_rsa"
    known_hosts = ssh_dir / "known_hosts"
    shutil.copy2(str(WINDOWS_SSH_KEY_MOUNT / "windows_id_rsa"), str(id_rsa))
    shutil.copy2(str(WINDOWS_SSH_KEY_MOUNT / "windows_fingerprint"), str(known_hosts))
    id_rsa.chmod(0o600)
    known_hosts.chmod(0o600)

    ssh_opts = [
        "-i",
        str(id_rsa),
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "IdentitiesOnly=yes",
        "-p",
        windows_port,
    ]
    scp_opts = [
        "-i",
        str(id_rsa),
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "IdentitiesOnly=yes",
        "-P",
        windows_port,
    ]

    effective_dest_quay_url = (dest_quay_url or quay_url).rstrip("/")

    common_kwargs = dict(
        ssh_opts=ssh_opts,
        scp_opts=scp_opts,
        windows_user=windows_user,
        windows_host=windows_host,
        quay_url=quay_url,
        quay_user=quay_user,
        quay_pass=quay_pass,
        pipeline_run_uid=pipeline_run_uid,
    )

    for component in snapshot.get("components", []):
        name = component.get("name", "")
        component_dir = CONTENT_DIR / name

        if not (component_dir / "has_windows").exists():
            logger.info(
                "No Windows content for component %s, skipping Windows signing...", name
            )
            continue

        logger.info("Signing Windows binaries for component: %s", name)

        unsigned_digest = (component_dir / "unsigned_windows_digest.txt").read_text().strip()
        commit_sha = component.get("source", {}).get("git", {}).get("revision", "")

        if signing_script:
            thumbprint_path = WINDOWS_CREDS_MOUNT / "cert_thumbprint"
            win_cert_thumbprint = ""
            if thumbprint_path.exists():
                win_cert_thumbprint = thumbprint_path.read_text().strip()

            _run_custom_script(
                signing_script=signing_script,
                signing_args=signing_args or [],
                name=name,
                origin=origin,
                commit_sha=commit_sha,
                component_dir=component_dir,
                dest_quay_url=effective_dest_quay_url,
                unsigned_digest=unsigned_digest,
                win_cert_thumbprint=win_cert_thumbprint,
                dest_quay_user=dest_quay_user,
                dest_quay_pass=dest_quay_pass,
                **common_kwargs,
            )
        else:
            _run_default_script(
                name=name,
                component_dir=component_dir,
                unsigned_digest=unsigned_digest,
                **common_kwargs,
            )


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run Windows signing; return exit code."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv[1:] if argv is not None else None)
    try:
        run(args.quay_url, args.pipeline_run_uid)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
