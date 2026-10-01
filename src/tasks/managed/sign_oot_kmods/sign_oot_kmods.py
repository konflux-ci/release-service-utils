#!/usr/bin/env python3
"""Sign out-of-tree kernel modules on the internal signing server.

Uploads ``.ko`` files over GSSAPI SSH, runs ``rh-signing-client`` on the remote
host, copies the signed modules back, writes ``sha256sum``-compatible checksum
files, and packs the signed tree into ``signed-kmods.tar.gz``.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path

from release_service_utils.helpers import authentication, file, gssapi_ssh, tekton
from release_service_utils.helpers.logger import logger

PROG = "sign_oot_kmods.py"
ARCHIVE_NAME = "signed-kmods.tar.gz"

DEFAULT_SIGNING_SECRET_DIR = Path("/etc/secrets")
DEFAULT_KEYTAB_PATH = Path("/etc/sec-keytab/keytab-build-and-sign.keytab")
DEFAULT_FINGERPRINT_PATH = Path("/etc/sec-checksum/checksumFingerprint")
DEFAULT_SSH_DIR = Path("/tmp/.ssh")


@dataclass(frozen=True)
class SignResult:
    """Outcome of uploading, signing, and copying kernel modules back."""

    signed_count: int


def ko_files(directory: Path) -> list[Path]:
    """Return regular ``.ko`` files under *directory*, sorted by relative path."""
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.rglob("*.ko") if path.is_file())


def read_signing_secrets(mount: Path) -> tuple[str, str, str]:
    """Return the signing key, host, and user from files under *mount*."""
    sign_key = authentication.read_mounted_text(mount, "signKey")
    sign_host = authentication.read_mounted_text(mount, "signHost")
    sign_user = authentication.read_mounted_text(mount, "signUser")
    return sign_key, sign_host, sign_user


def kinit_for_signing(sign_user: str, kerberos_realm: str, keytab: Path) -> None:
    """Obtain a Kerberos ticket for ``sign_user@kerberos_realm``."""
    credential_cache = Path(f"/tmp/krb5cc_{os.getuid()}")
    os.environ["KRB5CCNAME"] = f"FILE:{credential_cache}"
    authentication.kinit_with_retry(
        f"{sign_user}@{kerberos_realm}",
        keytab,
        {"KRB5CCNAME": f"FILE:{credential_cache}"},
    )


def prepare_ssh(
    sign_user: str, sign_host: str, fingerprint: Path, ssh_dir: Path
) -> tuple[str, list[str]]:
    """Install known_hosts and return the SSH target plus GSSAPI options."""
    known_hosts = gssapi_ssh.install_known_hosts(fingerprint, ssh_dir)
    return f"{sign_user}@{sign_host}", gssapi_ssh.ssh_options(known_hosts)


def build_remote_signing_script(
    *,
    sign_key: str,
    arch_label: str,
    signing_author: str,
    remote_dir: str,
) -> str:
    """Build the remote script that signs every ``.ko`` file under *remote_dir*.

    The signing key is embedded for the remote shell only. Callers pass this
    script on SSH stdin and must not log it.
    """
    key = shlex.quote(sign_key)
    arch = shlex.quote(arch_label)
    author = shlex.quote(signing_author)
    return f"""set -euo pipefail
KEY={key}
ARCH={arch}
SIGNING_AUTHOR={author}
REMOTE_DIR={remote_dir}
echo "Remote: Signing OOT kernel modules for architecture $ARCH"
found=0
while IFS= read -r -d '' kmod; do
    found=$((found + 1))
    echo "Remote: Signing kernel module $kmod for $ARCH"
    rh-signing-client --key "$KEY" --onbehalfof "$SIGNING_AUTHOR" --lkmsign "$kmod"
done < <(find "$REMOTE_DIR" -name '*.ko' -type f -print0)
if [ "$found" -eq 0 ]; then
    echo "Remote: ERROR: No .ko files found in $REMOTE_DIR for $ARCH" >&2
    exit 1
fi
echo "Remote: Successfully signed $found .ko files for $ARCH"
echo "Remote: Finished signing process for architecture $ARCH"
"""


def write_ko_tarball(source_dir: Path, tarball: Path) -> None:
    """Write a gzip tarball of ``.ko`` files, preserving paths relative to *source_dir*."""
    with tarfile.open(tarball, "w:gz") as archive:
        for path in ko_files(source_dir):
            relative = "./" + path.relative_to(source_dir).as_posix()
            archive.add(path, arcname=relative)


def extract_tarball(tarball: Path, destination: Path) -> None:
    """Extract *tarball* into *destination*."""
    with tarfile.open(tarball, "r:gz") as archive:
        archive.extractall(destination, filter="data")


def write_checksums(directory: Path, filename: str) -> Path:
    """Write ``sha256sum`` text for ``.ko`` files in *directory* and return the path."""
    lines = []
    for path in ko_files(directory):
        relative = "./" + path.relative_to(directory).as_posix()
        lines.append(f"{file.sha256(path)}  {relative}")
    text = ("\n".join(lines) + "\n") if lines else ""
    destination = directory / filename
    destination.write_text(text, encoding="utf-8")
    return destination


def _temp_tarball() -> Path:
    """Return a new temporary ``.tar.gz`` path."""
    descriptor, name = tempfile.mkstemp(prefix="kmods_", suffix=".tar.gz")
    os.close(descriptor)
    return Path(name)


def _arch_dirs(root: Path) -> list[Path]:
    """Return architecture subdirectories of *root*, sorted by name."""
    if not root.is_dir():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir())


def cleanup_remote(target: str, options: list[str], task_run_uid: str) -> None:
    """Remove the TaskRun directory on the signing host.

    A cleanup failure is logged and does not replace an earlier signing error.
    """
    command = f"rm -rf {gssapi_ssh.remote_shell_path(task_run_uid)}"
    try:
        gssapi_ssh.run_ssh(["ssh", *options, target, command])
    except (subprocess.CalledProcessError, gssapi_ssh.SSHConnectionError) as exc:
        logger.warning("Remote cleanup failed: %s", exc)


def transfer_and_sign(
    *,
    local_dir: Path,
    target: str,
    options: list[str],
    task_run_uid: str,
    remote_parts: tuple[str, ...],
    upload_name: str,
    download_name: str,
    arch_label: str,
    sign_key: str,
    signing_author: str,
) -> SignResult:
    """Upload ``.ko`` files, sign them remotely, and extract the signed tarball."""
    remote_dir = gssapi_ssh.remote_shell_path(task_run_uid, *remote_parts)
    upload_shell = gssapi_ssh.remote_shell_path(task_run_uid, upload_name)
    upload_scp = gssapi_ssh.remote_scp_path(task_run_uid, upload_name)
    download_shell = gssapi_ssh.remote_shell_path(task_run_uid, download_name)
    download_scp = gssapi_ssh.remote_scp_path(task_run_uid, download_name)

    gssapi_ssh.run_ssh(["ssh", *options, target, f"mkdir -p {remote_dir}"])

    unsigned = ko_files(local_dir)
    if unsigned:
        local_tar = _temp_tarball()
        try:
            write_ko_tarball(local_dir, local_tar)
            gssapi_ssh.run_ssh(["scp", *options, str(local_tar), f"{target}:{upload_scp}"])
            gssapi_ssh.run_ssh(
                ["ssh", *options, target, f"cd {remote_dir} && tar -xzf {upload_shell}"]
            )
        finally:
            local_tar.unlink(missing_ok=True)

    script = build_remote_signing_script(
        sign_key=sign_key,
        arch_label=arch_label,
        signing_author=signing_author,
        remote_dir=remote_dir,
    )
    gssapi_ssh.run_ssh(["ssh", *options, target, "bash -s"], stdin=script)

    create_command = (
        f"cd {remote_dir} && find . -name '*.ko' -type f | "
        f"tar -czf {download_shell} --files-from=-"
    )
    gssapi_ssh.run_ssh(["ssh", *options, target, create_command])

    signed_tar = _temp_tarball()
    try:
        gssapi_ssh.run_ssh(["scp", *options, f"{target}:{download_scp}", str(signed_tar)])
        with tempfile.TemporaryDirectory(prefix="signed_kmods_") as staging_name:
            staging = Path(staging_name)
            extract_tarball(signed_tar, staging)
            signed = ko_files(staging)
            if unsigned and not signed:
                raise RuntimeError(f"{PROG}: No signed .ko files copied back for {arch_label}")
            for path in unsigned:
                path.unlink()
            for path in signed:
                destination = local_dir / path.relative_to(staging)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
            return SignResult(signed_count=len(signed))
    finally:
        signed_tar.unlink(missing_ok=True)


def package_signed_files(data_dir: Path, signed_kmods_path: str) -> Path:
    """Archive ``data_dir / signed_kmods_path`` and remove the unpacked tree.

    Raises ``RuntimeError`` when the tree contains no ``.ko`` files.
    """
    source = data_dir / signed_kmods_path
    modules = ko_files(source)
    logger.info("Packaging %d signed .ko files", len(modules))
    if not modules:
        raise RuntimeError(f"{PROG}: No signed .ko files found to package")

    archive_path = data_dir / ARCHIVE_NAME
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source, arcname=signed_kmods_path)

    with tarfile.open(archive_path, "r:gz") as archive:
        packed = [
            member.name for member in archive.getmembers() if member.name.endswith(".ko")
        ]
    logger.info("Created %s", ARCHIVE_NAME)
    logger.info("Archive contains %d .ko files", len(packed))
    shutil.rmtree(source)
    return archive_path


def write_signing_summary(root: Path, arch_count: int) -> None:
    """Write ``signing_summary.txt`` for a multi-architecture signing run."""
    lines = [
        "Multi-architecture signing summary:",
        f"Total architectures processed: {arch_count}",
        "Signing details:",
    ]
    for arch_dir in _arch_dirs(root):
        count = len(ko_files(arch_dir))
        lines.append(f"  {arch_dir.name}: {count} signed .ko files")
        checksum = arch_dir / f"signed_kmods_checksums_{arch_dir.name}.txt"
        if checksum.is_file():
            lines.append("    Checksums: verified")
    text = "\n".join(lines) + "\n"
    (root / "signing_summary.txt").write_text(text, encoding="utf-8")
    logger.info("Multi-architecture signing completed. Summary:\n%s", text)


def sign_tree(
    *,
    local_dir: Path,
    remote_parts: tuple[str, ...],
    target: str,
    options: list[str],
    task_run_uid: str,
    sign_key: str,
    signing_author: str,
) -> SignResult:
    """Sign ``.ko`` files in *local_dir* and write checksums."""
    arch_name = local_dir.name
    logger.info("Processing signing for architecture: %s", arch_name)
    modules = ko_files(local_dir)
    if not modules:
        logger.warning("No .ko files found for architecture %s, skipping", arch_name)
        return SignResult(signed_count=0)
    logger.info("Found %d .ko files to sign for %s", len(modules), arch_name)
    result = transfer_and_sign(
        local_dir=local_dir,
        target=target,
        options=options,
        task_run_uid=task_run_uid,
        remote_parts=remote_parts,
        upload_name=f"kmods_{arch_name}.tar.gz",
        download_name=f"signed_{arch_name}.tar.gz",
        arch_label=arch_name,
        sign_key=sign_key,
        signing_author=signing_author,
    )
    logger.info(
        "Successfully copied back %d signed .ko files for %s",
        result.signed_count,
        arch_name,
    )
    checksum = write_checksums(local_dir, f"signed_kmods_checksums_{arch_name}.txt")
    logger.info(
        "Generated checksums for %s (%d files)",
        arch_name,
        len(checksum.read_text(encoding="utf-8").splitlines()),
    )
    logger.info("Completed signing for %s", arch_name)
    return result


def run(
    data_dir: Path,
    signed_kmods_path: str,
    kerberos_realm: str,
    signing_author: str,
    task_run_uid: str,
) -> None:
    """Sign kernel modules under ``data_dir / signed_kmods_path``.

    On success the signed tree is replaced with ``signed-kmods.tar.gz``.
    """
    secret_dir = file.path_from_env_variable("SIGNING_SECRET_DIR", DEFAULT_SIGNING_SECRET_DIR)
    keytab = file.path_from_env_variable("KEYTAB_PATH", DEFAULT_KEYTAB_PATH)
    fingerprint = file.path_from_env_variable("FINGERPRINT_PATH", DEFAULT_FINGERPRINT_PATH)
    ssh_dir = file.path_from_env_variable("SSH_DIR", DEFAULT_SSH_DIR)
    sign_key, sign_host, sign_user = read_signing_secrets(secret_dir)
    logger.info("Signing OOT modules from %s", data_dir / signed_kmods_path)
    kinit_for_signing(sign_user, kerberos_realm, keytab)
    target, options = prepare_ssh(sign_user, sign_host, fingerprint, ssh_dir)
    root = data_dir / signed_kmods_path
    try:
        arch_dirs = _arch_dirs(root)
        if not arch_dirs:
            logger.error("No architecture directories found in %s", root)
            raise RuntimeError(f"{PROG}: No architecture directories found in {root}")
        logger.info("Detected %d architecture(s) from directory structure", len(arch_dirs))
        logger.info("Using unique remote directory: ~/%s", task_run_uid)
        signed_any = False
        for arch_dir in arch_dirs:
            result = sign_tree(
                local_dir=arch_dir,
                remote_parts=("kmods", arch_dir.name),
                target=target,
                options=options,
                task_run_uid=task_run_uid,
                sign_key=sign_key,
                signing_author=signing_author,
            )
            if result.signed_count > 0:
                signed_any = True
        if not signed_any:
            raise RuntimeError(f"{PROG}: No .ko files found after signing process")
        if len(arch_dirs) > 1:
            write_signing_summary(root, len(arch_dirs))
    finally:
        logger.info("Cleaning up remote directory ~/%s", task_run_uid)
        cleanup_remote(target, options, task_run_uid)
    package_signed_files(data_dir, signed_kmods_path)


def main() -> int:
    """Read Tekton environment variables and sign kernel modules."""
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        signed_kmods_path=tekton.require_env("PARAM_SIGNED_KMODS_PATH"),
        kerberos_realm=tekton.require_env("PARAM_KERBEROS_REALM"),
        signing_author=tekton.require_env("PARAM_SIGNING_AUTHOR"),
        task_run_uid=tekton.require_env("TASK_RUN_UID"),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
