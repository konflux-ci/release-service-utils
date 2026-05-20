"""Authentication oriented helpers for task scripts.

Store reusable, task-agnostic pieces here: krb5 configuration for container
runs, ``kinit`` with keytabs from the filesystem, and reading typical mounted
service-account / secret file layouts.
"""

from __future__ import annotations

import base64
import os
import subprocess
from collections.abc import Sequence
from pathlib import Path

import retry


def read_mounted_text(mount: Path, filename: str) -> str:
    """
    Read a UTF-8 file (``mount / filename``) and return the text with leading
    and trailing whitespace removed. Use for one-line secret values (API base URL,
    principal name) when a pod mounts a volume as a directory of small files.
    """
    return (mount / filename).read_text(encoding="utf-8").strip()


def load_keytab_from_mount(
    mount: Path,
    *,
    principal_file: str,
    keytab_b64_file: str,
) -> tuple[str, bytes]:
    """
    Load a Kerberos principal and raw keytab from files under *mount*.

    *principal_file* is a one-line text file (principal). *keytab_b64_file*
    is the same for a base64-encoded keytab. Returns the principal string and
    the decoded keytab bytes.
    """
    princ = read_mounted_text(mount, principal_file)
    b64 = read_mounted_text(mount, keytab_b64_file).encode("ascii")
    return princ, base64.b64decode(b64)


def load_service_account(
    mount: Path,
    text_files: Sequence[str] = (),
    *,
    principal_file: str,
    keytab_b64_file: str,
) -> tuple[str, bytes, dict[str, str]]:
    """
    Load a mounted Kubernetes-style service account: principal, keytab, and extra
    one-value string files (API base URL, usernames, etc.).

    *principal_file* and *keytab_b64_file* are passed through to
    ``load_keytab_from_mount``. It then reads each additional filename in
    ``text_files`` and returns a
    dict mapping that filename to stripped text (for example an API base URL
    in one file). You can list several files; each name becomes a key in the
    returned dict.
    """
    princ, keytab = load_keytab_from_mount(
        mount, principal_file=principal_file, keytab_b64_file=keytab_b64_file
    )
    extra: dict[str, str] = {name: read_mounted_text(mount, name) for name in text_files}
    return princ, keytab, extra


def patch_krb5_config(source: str) -> str:
    """
    Return ``source`` with one line added immediately after ``[libdefaults]``.

    Inserts ``dns_canonicalize_hostname = false`` to avoid
    "Invalid UID in persistent keyring name" / hostname canonicalization
    issues when some environments use a temporary KRB5_CONFIG derived from the
    system ``/etc/krb5.conf`` template.

    **source** is the full file text. If a ``[libdefaults]`` line is present, one
    setting line is inserted immediately after it; if that section is missing,
    the returned string matches **source** unchanged.
    """
    out: list[str] = []
    for line in source.splitlines(keepends=True):
        out.append(line)
        if line.strip() == "[libdefaults]":
            out.append("    dns_canonicalize_hostname = false\n")
    return "".join(out)


def kinit_with_retry(
    princ: str, keytab: Path, extra_env: dict[str, str], *, max_attempts: int = 5
) -> None:
    """
    Run ``kinit`` with a keytab, retrying a few times on non-zero exit.

    Merges ``os.environ`` with ``extra_env`` for the ``kinit`` child only.
    If every attempt fails, the last ``CalledProcessError`` is raised. **princ** is
    the principal; **keytab** is the keytab path; **extra_env** is merged in (for
    example ``KRB5CCNAME`` and ``KRB5_CONFIG``). For verbose libkrb5 trace, set
    ``KRB5_TRACE=/dev/stderr`` in the process environment (or in **extra_env**)
    on the parent before calling. **max_attempts** is how many times to run
    ``kinit`` before giving up, with exponential backoff (5s, 10s, 20s...).
    """

    def _run_once() -> None:
        p = subprocess.run(
            ["kinit", princ, "-k", "-t", str(keytab)],
            check=False,
            env={**os.environ, **extra_env},
        )
        if p.returncode == 0:
            return
        raise subprocess.CalledProcessError(
            p.returncode,
            f"kinit {princ} -k -t {keytab!s}",
        )

    retry.retry_with_exponential_backoff(
        _run_once,
        max_attempts=max_attempts,
        retry_on=subprocess.CalledProcessError,
        base_sleep_seconds=5,
    )
