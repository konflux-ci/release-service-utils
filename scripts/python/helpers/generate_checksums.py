#!/usr/bin/env python3
"""Generate and GPG-sign a merged sha256sum.txt for all component archives.

Generates checksums for all archive files across all components' ``ready_for_distribution``
directories into a single ``sha256sum.txt`` file, then signs it once on the checksum host:
* ``--clearsign`` → ``sha256sum.txt.sig``
* ``--gpgsign``   → ``sha256sum.txt.gpg``

The merged checksum file and its signatures are placed in the first component's
``ready_for_distribution`` directory.

Note: if multiple components targeting the same CGW product version are released in
separate releases, the sha256sum.txt from the later release will overwrite the earlier
one. All components for a given CGW product version should be in the same release.

CLI arguments:
  ``--kerberos-realm``
  ``--pipeline-run-uid``

Secret mounts:
  ``CHECKSUM_CREDENTIALS_MOUNT``  (default: ``/mnt/checksum_credentials``)

Other env vars:
  ``AUTHOR``             – release author for rpm-sign (set by task from ``params.author``)
  ``SIGNING_KEY_NAME``   – GPG key name (set by task from ``params.signingKeyName``)
  ``SNAPSHOT_JSON``      – JSON string of the Snapshot spec
  ``CONTENT_DIR``        – override base directory (default: ``/shared/artifacts``)
  ``SHARED_DIR``         – override shared volume root (default: ``/shared``)
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

import authentication
import file as file_utils
import retry

PROG = "generate_checksums.py"

CHECKSUM_CREDENTIALS_MOUNT = Path(
    os.environ.get("CHECKSUM_CREDENTIALS_MOUNT", "/mnt/checksum_credentials")
)
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))
SHARED_DIR = Path(os.environ.get("SHARED_DIR", "/shared"))

logger = logging.getLogger(__name__)


class _SSHConnectionError(Exception):
    """Transient SSH connection failure (exit code 255)."""


def _run_ssh_command(cmd: list[str], *, max_attempts: int = 3) -> None:
    """Run an SSH/SCP command, retrying on transient connection errors (RC 255)."""

    def _attempt() -> None:
        result = subprocess.run(cmd, check=False)
        if result.returncode == 255:
            logger.warning("SSH connection failed (exit 255), will retry: %s", shlex.join(cmd))
            raise _SSHConnectionError(f"SSH connection failed (exit 255): {shlex.join(cmd)}")
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, cmd)

    retry.retry_with_exponential_backoff(
        _attempt,
        max_attempts=max_attempts,
        retry_on=_SSHConnectionError,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument(
        "--kerberos-realm",
        default="IPA.REDHAT.COM",
        # IPA.REDHAT.COM is the production Red Hat Kerberos realm; override only for testing.
        help="Kerberos realm for the checksum host",
    )
    p.add_argument("--pipeline-run-uid", required=True, help="Unique ID for this pipeline run")
    return p.parse_args(argv)


def _kinit(checksum_user: str, kerberos_realm: str, keytab_b64: bytes) -> None:
    """Obtain a Kerberos ticket-granting ticket using the supplied base64-encoded keytab."""
    krb5cc = Path(f"/tmp/krb5cc_{os.getuid()}")
    os.environ["KRB5CCNAME"] = f"FILE:{krb5cc}"
    os.environ["KRB5_TRACE"] = "/dev/stderr"

    fd, keytab_path = tempfile.mkstemp(suffix=".keytab")
    keytab = Path(keytab_path)
    try:
        os.write(fd, base64.b64decode(keytab_b64))
        os.close(fd)
        authentication.kinit_with_retry(
            f"{checksum_user}@{kerberos_realm}",
            keytab,
            {"KRB5CCNAME": f"FILE:{krb5cc}", "KRB5_TRACE": "/dev/stderr"},
        )
    finally:
        keytab.unlink(missing_ok=True)


def run(kerberos_realm: str, pipeline_run_uid: str) -> None:
    """Generate sha256sum.txt and sign it on the checksum host via SSH."""
    author = os.environ.get("AUTHOR", "").strip()
    if not author:
        raise ValueError("AUTHOR env var is required (set by task from params.author)")
    signing_key_name = os.environ.get("SIGNING_KEY_NAME", "").strip()
    if not signing_key_name:
        raise ValueError(
            "SIGNING_KEY_NAME env var is required (set by task from params.signingKeyName)"
        )

    checksum_user = (CHECKSUM_CREDENTIALS_MOUNT / "user").read_text().strip()
    checksum_host = (CHECKSUM_CREDENTIALS_MOUNT / "host").read_text().strip()
    keytab_b64 = (CHECKSUM_CREDENTIALS_MOUNT / "keytab").read_bytes()

    _kinit(checksum_user, kerberos_realm, keytab_b64)

    ssh_dir = Path("/tmp/.ssh")
    ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    known_hosts = ssh_dir / "known_hosts"
    shutil.copy2(str(CHECKSUM_CREDENTIALS_MOUNT / "fingerprint"), str(known_hosts))
    known_hosts.chmod(0o600)

    ssh_opts = [
        "-v",
        "-o",
        "UserKnownHostsFile=/tmp/.ssh/known_hosts",
        "-o",
        "GSSAPIAuthentication=yes",
        "-o",
        "GSSAPIDelegateCredentials=yes",
        "-o",
        "IdentitiesOnly=yes",
    ]

    shared_snapshot = SHARED_DIR / "snapshot.json"
    if shared_snapshot.exists():
        snapshot = json.loads(shared_snapshot.read_text())
    else:
        snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    components = snapshot.get("components", [])

    sha_sums_path = Path("/tmp/sha256sum.txt")
    sha_sums_path.write_text("")
    first_ready_dir: Path | None = None

    for component in components:
        name = component.get("name", "")
        ready_dir = CONTENT_DIR / name / "ready_for_distribution"

        if first_ready_dir is None:
            first_ready_dir = ready_dir

        logger.info("Generating checksums for component: %s", name)

        archive_files = (
            sorted(
                f
                for f in ready_dir.iterdir()
                if f.is_file() and not f.name.startswith("sha256sum.txt")
            )
            if ready_dir.exists()
            else []
        )

        with sha_sums_path.open("a") as out:
            for archive in archive_files:
                checksum = file_utils.sha256(archive)
                out.write(f"{checksum}  {archive.name}\n")

    if first_ready_dir is None:
        raise RuntimeError("No components found in snapshot")

    if not sha_sums_path.stat().st_size:
        raise RuntimeError("No archives found to checksum across all components")

    remote_dir = f"{pipeline_run_uid}_merged"
    remote_base = f"/home/{checksum_user}/{remote_dir}"
    remote_checksum_dir = f"{remote_base}/checksum"
    remote_input = f"{remote_checksum_dir}/sha256sum.txt"
    remote_target = f"{checksum_user}@{checksum_host}"

    try:
        _run_ssh_command(
            [
                "ssh",
                *ssh_opts,
                remote_target,
                "mkdir -p " + shlex.quote(remote_checksum_dir),
            ]
        )
        _run_ssh_command(
            [
                "scp",
                *ssh_opts,
                str(sha_sums_path),
                f"{remote_target}:{remote_checksum_dir}",
            ]
        )

        logger.info("Signing merged sha256sum.txt with --clearsign")
        _run_ssh_command(
            [
                "ssh",
                *ssh_opts,
                remote_target,
                " ".join(
                    [
                        "rpm-sign",
                        "--nat",
                        "--clearsign",
                        "--key",
                        shlex.quote(signing_key_name),
                        f"--onbehalfof={shlex.quote(author)}",
                        "--output",
                        shlex.quote(f"{remote_input}.sig"),
                        shlex.quote(remote_input),
                    ]
                ),
            ]
        )

        logger.info("Signing merged sha256sum.txt with --gpgsign")
        _run_ssh_command(
            [
                "ssh",
                *ssh_opts,
                remote_target,
                " ".join(
                    [
                        "rpm-sign",
                        "--nat",
                        "--gpgsign",
                        "--key",
                        shlex.quote(signing_key_name),
                        f"--onbehalfof={shlex.quote(author)}",
                        "--output",
                        shlex.quote(f"{remote_input}.gpg"),
                        shlex.quote(remote_input),
                    ]
                ),
            ]
        )

        first_ready_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(sha_sums_path), str(first_ready_dir / "sha256sum.txt"))

        _run_ssh_command(
            [
                "scp",
                *ssh_opts,
                f"{remote_target}:{remote_checksum_dir}/sha256sum.txt.sig",
                str(first_ready_dir / "sha256sum.txt.sig"),
            ]
        )

        _run_ssh_command(
            [
                "scp",
                *ssh_opts,
                f"{remote_target}:{remote_checksum_dir}/sha256sum.txt.gpg",
                str(first_ready_dir / "sha256sum.txt.gpg"),
            ]
        )
    finally:
        cleanup = subprocess.run(
            [
                "ssh",
                *ssh_opts,
                remote_target,
                "rm -rf " + shlex.quote(remote_base),
            ],
            check=False,
        )
        if cleanup.returncode != 0:
            logger.warning(
                "Remote cleanup failed (exit code: %d) — %s may remain on the checksum host",
                cleanup.returncode,
                remote_base,
            )


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run checksum generation and signing; return exit code."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv[1:] if argv is not None else None)
    try:
        run(args.kerberos_realm, args.pipeline_run_uid)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
