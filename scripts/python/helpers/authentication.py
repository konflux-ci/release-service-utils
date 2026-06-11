"""Authentication oriented helpers for task scripts.

Store reusable, task-agnostic pieces here: krb5 configuration for container
runs, ``kinit`` with keytabs from the filesystem, and reading typical mounted
service-account / secret file layouts.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path

import file
import retry
from logger import logger


def read_mounted_text(mount: Path, filename: str) -> str:
    """Read a UTF-8 file (``mount / filename``) and return stripped text.

    Returns the text with leading and trailing whitespace removed.
    Use for one-line secret values (API base URL, principal name) when
    a pod mounts a volume as a directory of small files.
    """
    return (mount / filename).read_text(encoding="utf-8").strip()


def load_keytab_from_mount(
    mount: Path,
    *,
    principal_file: str,
    keytab_b64_file: str,
) -> tuple[str, bytes]:
    """Load a Kerberos principal and raw keytab from files under *mount*.

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
    """Load a mounted Kubernetes-style service account from the filesystem.

    Reads the principal, keytab, and extra one-value string files
    (API base URL, usernames, etc.) from *mount*.

    *principal_file* and *keytab_b64_file* are passed through to
    ``load_keytab_from_mount``. It then reads each additional filename in
    ``text_files`` and returns a dict mapping that filename to stripped text
    (for example an API base URL in one file). You can list several files;
    each name becomes a key in the returned dict.
    """
    princ, keytab = load_keytab_from_mount(
        mount, principal_file=principal_file, keytab_b64_file=keytab_b64_file
    )
    extra: dict[str, str] = {name: read_mounted_text(mount, name) for name in text_files}
    return princ, keytab, extra


def patch_krb5_config(source: str) -> str:
    """Return ``source`` with one line added immediately after ``[libdefaults]``.

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
    """Run ``kinit`` with a keytab, retrying a few times on non-zero exit.

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


def write_docker_config(config_json: str) -> None:
    """Write *config_json* to ``~/.docker/config.json``, creating the directory if needed.

    Shared by helpers that authenticate to container registries (Quay, Red Hat workloads
    registry) before pulling or pushing OCI artifacts.  The caller is responsible for
    obtaining the JSON string (reading a mounted secret file, stripping noise, etc.).

    The directory is created with mode 0700 and the file is written atomically with
    mode 0600 to prevent other processes from reading registry credentials.
    """
    docker_dir = Path.home() / ".docker"
    docker_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    config_path = docker_dir / "config.json"
    fd, tmp_path = tempfile.mkstemp(dir=docker_dir)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(config_json)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, config_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def setup_docker_config(
    path: Path,
    *,
    strip_noise: bool = False,
    optional: bool = False,
) -> None:
    """Read a dockerconfigjson file and write it to ``~/.docker/config.json``.

    *path* is the full path to a ``.dockerconfigjson`` file (typically a mounted
    Kubernetes secret).  When *optional* is ``True`` and the file is absent or empty,
    the function returns without writing anything — useful for mounts that may not be
    present in all environments.  When *strip_noise* is ``True``, any leading/trailing
    non-JSON characters (e.g. outer quotes added by some k8s secret encodings) are
    stripped before writing.
    """
    if optional and (not path.is_file() or path.stat().st_size == 0):
        return
    raw = path.read_text(encoding="utf-8")
    if strip_noise:
        first = raw.find("{")
        last = raw.rfind("}")
        if first == -1 or last == -1 or first > last:
            raise ValueError(f"No valid JSON object found in {path}")
        raw = json.dumps(json.loads(raw[first : last + 1]))
    write_docker_config(raw)


@contextmanager
def kerberos_login(
    principal: str,
    keytab_bytes: bytes,
    krb5_config: str,
    *,
    kinit_fn: Callable[..., None] = kinit_with_retry,
) -> Generator[None, None, None]:
    """Set up Kerberos auth with ephemeral temp files and clean up on exit.

    Create temporary files for the keytab, krb5 config, and credential
    cache, run ``kinit``, and update ``os.environ`` so that
    ``requests-kerberos`` can use the credentials.  All temp files are
    removed when the context exits.
    """
    keytab_path = file.make_tempfile_path("keytab-", keytab_bytes)
    krb5_path = file.make_tempfile_path("krb5-", krb5_config.encode("utf-8"))
    ccache_fd, ccache_name = tempfile.mkstemp()
    os.close(ccache_fd)
    ccache_path = Path(ccache_name)

    try:
        kenv = {
            "KRB5_CONFIG": str(krb5_path),
            "KRB5CCNAME": str(ccache_path),
            "KRB5_TRACE": "/dev/stderr",
        }
        logger.info("Logging in with Kerberos (kinit)...")
        kinit_fn(principal, keytab_path, kenv, max_attempts=5)
        os.environ.update(kenv)
        yield
    finally:
        for key in kenv:
            os.environ.pop(key, None)
        for p in (keytab_path, krb5_path, ccache_path):
            p.unlink(missing_ok=True)


def create_container_auth_config(
    from_index: str,
    publishing_credential: str,
) -> None:
    """Create ``~/.config/containers/auth.json`` for skopeo.

    For ``registry-proxy.engineering.redhat.com``, remove any existing auth
    entry (that registry uses Kerberos, not token auth).  For other
    registries, write base64-encoded publishing credentials.
    """
    auth_dir = Path.home() / ".config" / "containers"
    auth_dir.mkdir(parents=True, exist_ok=True)
    auth_file = auth_dir / "auth.json"

    auth_name = from_index.rsplit(":", 1)[0]

    if re.match(r"^registry-proxy(-stage)?\.engineering\.redhat\.com", auth_name):
        if auth_file.exists():
            try:
                data = json.loads(auth_file.read_text(encoding="utf-8"))
                data.get("auths", {}).pop(auth_name, None)
                auth_file.write_text(json.dumps(data), encoding="utf-8")
            except (json.JSONDecodeError, OSError):
                auth_file.write_text("{}", encoding="utf-8")
        else:
            auth_file.write_text("{}", encoding="utf-8")
        return

    if not publishing_credential:
        logger.warning("No publishing credentials available for %s", auth_name)
        auth_file.write_text("{}", encoding="utf-8")
        return

    token = base64.b64encode(publishing_credential.encode()).decode("ascii")
    auth_data = {"auths": {auth_name: {"auth": token}}}
    auth_file.write_text(json.dumps(auth_data), encoding="utf-8")
