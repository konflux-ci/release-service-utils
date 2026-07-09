#!/usr/bin/env python3
"""Sign macOS binaries on a remote Mac host via SSH.

For each component with a ``has_mac`` flag file:
* Generates a shell script that pulls the unsigned OCI artifact from Quay,
  signs with ``xcrun codesign``, notarizes with ``xcrun notarytool``, and pushes
  the signed content back to Quay.
* SCPs the script to the Mac host and executes it via SSH.
* Copies the resulting signed digest back and writes it to
  ``<component_dir>/signed_mac_digest.txt``.
* Always cleans up the remote temporary directory.

CLI arguments:
  ``--quay-url``
  ``--pipeline-run-uid``

Secret mounts:
  ``MAC_SSH_KEY_MOUNT``         (default: ``/mnt/secrets``)
  ``MAC_HOST_CREDS_MOUNT``      (default: ``/mnt/macHostCredentials``)
  ``MAC_SIGNING_CREDS_MOUNT``   (default: ``/mnt/macSigningCredentials``)
  ``QUAY_SECRET_MOUNT``         (default: ``/mnt/quaySecret``)

Other env vars:
  ``SNAPSHOT_JSON``   – JSON string of the Snapshot spec
  ``CONTENT_DIR``     – override base directory (default: ``/shared/artifacts``)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

PROG = "sign_mac.py"

MAC_SSH_KEY_MOUNT = Path(os.environ.get("MAC_SSH_KEY_MOUNT", "/mnt/secrets"))
MAC_HOST_CREDS_MOUNT = Path(os.environ.get("MAC_HOST_CREDS_MOUNT", "/mnt/macHostCredentials"))
MAC_SIGNING_CREDS_MOUNT = Path(
    os.environ.get("MAC_SIGNING_CREDS_MOUNT", "/mnt/macSigningCredentials")
)
QUAY_SECRET_MOUNT = Path(os.environ.get("QUAY_SECRET_MOUNT", "/mnt/quaySecret"))
DEST_QUAY_SECRET_MOUNT = Path(os.environ.get("DEST_QUAY_SECRET_MOUNT", "/mnt/destQuaySecret"))
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))

logger = logging.getLogger(__name__)


def _build_signing_script(
    *,
    quay_url: str,
    quay_user: str,
    quay_pass: str,
    component_name: str,
    unsigned_digest: str,
    pipeline_run_uid: str,
    temp_dir: str,
    binary_path: str,
    zip_path: str,
    digest_file: str,
    keychain_password: str,
    signing_identity: str,
    apple_id: str,
    team_id: str,
    app_specific_password: str,
) -> str:
    """Build the remote shell script that pulls, signs, notarizes, and pushes Mac binaries."""
    return f"""#!/bin/bash
set -eux

mkdir -p {temp_dir}
mkdir -p {binary_path}

cd {temp_dir}
set +x
export QUAY_PASS={shlex.quote(quay_pass)}
echo "$QUAY_PASS" | /usr/local/bin/oras login quay.io -u {quay_user} --password-stdin
unset QUAY_PASS
set -x
/usr/local/bin/oras pull {quay_url}/unsigned/{component_name}@{unsigned_digest} -o "{binary_path}"  # noqa: E501
CONTENT_DIR_MAC="{binary_path}/macos"

set +x
export KEYCHAIN_PASSWORD={shlex.quote(keychain_password)}
security unlock-keychain -p "$KEYCHAIN_PASSWORD" login.keychain
unset KEYCHAIN_PASSWORD
set -x
echo "Signing files in the $CONTENT_DIR_MAC directory..."
find "$CONTENT_DIR_MAC" -type f | while IFS= read -r file; do
    echo "Signing: $file"
    if ! xcrun codesign --sign "Developer ID Application: {signing_identity}" \\
        --options runtime --timestamp --force "$file"; then
        echo "Failed to sign file: $file"
        exit 1
    fi
done

cd "{binary_path}"
zip -r "{zip_path}" macos

echo "Submitting ZIP file to Apple notary service..."
set +x
export NOTARY_PASSWORD={shlex.quote(app_specific_password)}
xcrun notarytool submit "{zip_path}" \\
    --wait \\
    --apple-id "{apple_id}" \\
    --team-id "{team_id}" \\
    --password "$NOTARY_PASSWORD"
unset NOTARY_PASSWORD
set -x

SIGNED_TAG="{pipeline_run_uid}-mac"
PUSH_OUTPUT=$(/usr/local/bin/oras push --annotation=quay.expires-after=1d \\
  "{quay_url}/signed/{component_name}:$SIGNED_TAG" macos)
SIGNED_DIGEST=$(echo "$PUSH_OUTPUT" | grep 'Digest:' | awk '{{print $2}}')
echo -n "$SIGNED_DIGEST" >> "{digest_file}"
echo "Process completed successfully."
"""


def _ssh_opts(key_path: str, known_hosts: str) -> list[str]:
    """Return SSH option flags for key-based authentication with a fixed known_hosts file."""
    return [
        "-i",
        key_path,
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "IdentitiesOnly=yes",
    ]


def _run_custom_script(
    *,
    signing_script: str,
    signing_args: list[str],
    ssh_opts: list[str],
    mac_user: str,
    mac_host: str,
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
    keychain_password: str,
    signing_identity: str,
) -> None:
    """Run a custom signing script already present on the remote Mac host."""
    digest_file = f"/tmp/signed_digest_{pipeline_run_uid}_{name}.txt"
    tag = f"{pipeline_run_uid[:8]}-{commit_sha[:8]}-mac"

    env_vars = {
        "QUAY_USER": quay_user,
        "QUAY_PASS": quay_pass,
        "QUAY_DEST_USER": dest_quay_user,
        "QUAY_DEST_PASS": dest_quay_pass,
        "CSC_KEY_PASSWORD": keychain_password,
        "CSC_NAME": signing_identity,
        "UNSIGNED_REF": f"{quay_url}/unsigned/{name}@{unsigned_digest}",
        "SIGNED_REF": f"{dest_quay_url}/{origin}/{name}:{tag}",
        "OUTPUT_DIGEST": digest_file,
    }
    args_str = " ".join(shlex.quote(a) for a in signing_args)
    stdin_script = "#!/bin/bash\nset -eu\n"
    for k, v in env_vars.items():
        stdin_script += f"export {k}={shlex.quote(v)}\n"
    stdin_script += f"exec bash {shlex.quote(signing_script)} {args_str}\n"

    ssh_exit = 0
    failed_op = ""
    result = subprocess.run(
        ["ssh"] + ssh_opts + [f"{mac_user}@{mac_host}", "bash", "-s"],
        input=stdin_script,
        text=True,
    )
    if result.returncode != 0:
        ssh_exit = result.returncode
        failed_op = "signing"

    if ssh_exit == 0:
        scp_result = subprocess.run(
            ["scp"]
            + ssh_opts
            + [
                f"{mac_user}@{mac_host}:{digest_file}",
                str(component_dir / "signed_mac_digest.txt"),
            ]
        )
        if scp_result.returncode != 0:
            ssh_exit = scp_result.returncode
            failed_op = "scp of signed digest"

    cleanup = subprocess.run(
        ["ssh"] + ssh_opts + [f"{mac_user}@{mac_host}", "rm -f " + shlex.quote(digest_file)],
        check=False,
    )
    if cleanup.returncode != 0:
        logger.warning(
            "Remote cleanup of digest file failed for %s (exit code: %d) — "
            "%s may remain on the Mac host",
            name,
            cleanup.returncode,
            digest_file,
        )

    if ssh_exit != 0:
        raise RuntimeError(
            f"Mac {failed_op} failed for component: {name} (exit code: {ssh_exit})"
        )


def _run_default_script(
    *,
    ssh_opts: list[str],
    mac_user: str,
    mac_host: str,
    name: str,
    component_dir: Path,
    quay_url: str,
    quay_user: str,
    quay_pass: str,
    unsigned_digest: str,
    pipeline_run_uid: str,
    keychain_password: str,
    signing_identity: str,
    apple_id: str,
    team_id: str,
    app_specific_password: str,
) -> None:
    """Build, upload, and run the default codesign/notarytool script on the remote Mac host."""
    mac_script_path = f"/tmp/mac_signing_script_{pipeline_run_uid}_{name}.sh"
    temp_dir = f"/tmp/{pipeline_run_uid}_{name}"
    binary_path = f"{temp_dir}/unsigned"
    zip_path = f"{temp_dir}/signed_content.zip"
    digest_file = f"{temp_dir}/push_digest.txt"

    script_content = _build_signing_script(
        quay_url=quay_url,
        quay_user=quay_user,
        quay_pass=quay_pass,
        component_name=name,
        unsigned_digest=unsigned_digest,
        pipeline_run_uid=pipeline_run_uid,
        temp_dir=temp_dir,
        binary_path=binary_path,
        zip_path=zip_path,
        digest_file=digest_file,
        keychain_password=keychain_password,
        signing_identity=signing_identity,
        apple_id=apple_id,
        team_id=team_id,
        app_specific_password=app_specific_password,
    )

    fd, script_path = tempfile.mkstemp(suffix=".sh")
    local_script = Path(script_path)
    os.write(fd, script_content.encode())
    os.close(fd)

    try:
        subprocess.check_call(
            ["scp"]
            + ssh_opts
            + [str(local_script), f"{mac_user}@{mac_host}:{mac_script_path}"]
        )

        ssh_exit = 0
        failed_op = ""
        result = subprocess.run(
            ["ssh"] + ssh_opts + [f"{mac_user}@{mac_host}", "bash", mac_script_path]
        )
        if result.returncode != 0:
            ssh_exit = result.returncode
            failed_op = "signing"

        if ssh_exit == 0:
            scp_result = subprocess.run(
                ["scp"]
                + ssh_opts
                + [
                    f"{mac_user}@{mac_host}:{temp_dir}/push_digest.txt",
                    str(component_dir / "signed_mac_digest.txt"),
                ]
            )
            if scp_result.returncode != 0:
                ssh_exit = scp_result.returncode
                failed_op = "scp of signed digest"

        cleanup = subprocess.run(
            ["ssh"]
            + ssh_opts
            + [
                f"{mac_user}@{mac_host}",
                "rm -rf " + shlex.quote(temp_dir) + " " + shlex.quote(mac_script_path),
            ],
            check=False,
        )
        if cleanup.returncode != 0:
            logger.warning(
                "Remote cleanup failed for %s (exit code: %d) — "
                "%s and %s may remain on the Mac host",
                name,
                cleanup.returncode,
                temp_dir,
                mac_script_path,
            )

        if ssh_exit != 0:
            raise RuntimeError(
                f"Mac {failed_op} failed for component: {name}" f" (exit code: {ssh_exit})"
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
    """Sign macOS binaries on the remote Mac host for every component with a has_mac flag."""
    snapshot = json.loads(os.environ["SNAPSHOT_JSON"])
    quay_url = quay_url.rstrip("/")

    mac_user = (MAC_HOST_CREDS_MOUNT / "username").read_text().strip()
    mac_host = (MAC_HOST_CREDS_MOUNT / "host").read_text().strip()
    keychain_password = (MAC_SIGNING_CREDS_MOUNT / "keychain_password").read_text().strip()
    signing_identity = (MAC_SIGNING_CREDS_MOUNT / "signing_identity").read_text().strip()
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

    if not signing_script:
        apple_id = (MAC_SIGNING_CREDS_MOUNT / "apple_id").read_text().strip()
        team_id = (MAC_SIGNING_CREDS_MOUNT / "team_id").read_text().strip()
        app_specific_password = (
            (MAC_SIGNING_CREDS_MOUNT / "app_specific_password").read_text().strip()
        )

    ssh_dir = Path("/tmp/.ssh")
    ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    id_rsa = ssh_dir / "id_rsa"
    known_hosts = ssh_dir / "known_hosts"
    shutil.copy2(str(MAC_SSH_KEY_MOUNT / "mac_id_rsa"), str(id_rsa))
    shutil.copy2(str(MAC_SSH_KEY_MOUNT / "mac_fingerprint"), str(known_hosts))
    id_rsa.chmod(0o600)
    known_hosts.chmod(0o600)

    ssh_opts = _ssh_opts(str(id_rsa), str(known_hosts))

    effective_dest_quay_url = (dest_quay_url or quay_url).rstrip("/")

    common_kwargs = dict(
        ssh_opts=ssh_opts,
        mac_user=mac_user,
        mac_host=mac_host,
        quay_url=quay_url,
        quay_user=quay_user,
        quay_pass=quay_pass,
        pipeline_run_uid=pipeline_run_uid,
        keychain_password=keychain_password,
        signing_identity=signing_identity,
    )

    for component in snapshot.get("components", []):
        name = component.get("name", "")
        component_dir = CONTENT_DIR / name

        if not (component_dir / "has_mac").exists():
            logger.info("No macOS content for component %s, skipping Mac signing...", name)
            continue

        logger.info("Signing Mac binaries for component: %s", name)

        unsigned_digest = (component_dir / "unsigned_mac_digest.txt").read_text().strip()
        commit_sha = component.get("source", {}).get("git", {}).get("revision", "")

        if signing_script:
            _run_custom_script(
                signing_script=signing_script,
                signing_args=signing_args or [],
                name=name,
                origin=origin,
                commit_sha=commit_sha,
                component_dir=component_dir,
                unsigned_digest=unsigned_digest,
                dest_quay_url=effective_dest_quay_url,
                dest_quay_user=dest_quay_user,
                dest_quay_pass=dest_quay_pass,
                **common_kwargs,
            )
        else:
            _run_default_script(
                name=name,
                component_dir=component_dir,
                unsigned_digest=unsigned_digest,
                apple_id=apple_id,
                team_id=team_id,
                app_specific_password=app_specific_password,
                **common_kwargs,
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument("--quay-url", required=True, help="Quay repository URL base")
    p.add_argument("--pipeline-run-uid", required=True, help="Unique ID for this pipeline run")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run Mac signing; return exit code."""
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
