"""Shared helpers for OCI artifact operations using the oras CLI."""

from __future__ import annotations

import json
import posixpath
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

from release_service_utils.helpers import file
from release_service_utils.helpers import subprocess_cmd
from release_service_utils.helpers.subprocess_cmd import run_cmd

FLAT_ARTIFACT_CONFIG_MEDIA_TYPE = "application/vnd.oci.empty.v1+json"

# OCI/Docker layer whiteout conventions: a file deleted by a later layer is
# represented by a same-named sibling entry prefixed with ".wh.", and an
# opaque directory marker (deleting everything a lower layer had underneath)
# is named ".wh..wh..opq".
_WHITEOUT_PREFIX = ".wh."
_WHITEOUT_OPAQUE_MARKER = ".wh..wh..opq"


def safe_relative_path(source: str) -> str:
    """Strip a leading slash from *source*, rejecting any ``..`` segment.

    Guards against a staged-file ``source``/``filename`` value escaping its
    intended destination directory (e.g. ``../../etc/passwd``).
    """
    stripped = source.lstrip("/")
    if ".." in stripped.split("/"):
        raise ValueError(f"path must not contain '..' segments: {source!r}")
    return stripped


def _normalize_member_name(name: str) -> str:
    stripped = name.lstrip("/")
    while stripped.startswith("./"):
        stripped = stripped[2:]
    return stripped


def oras_resolve(
    reference: str,
    *,
    auth_ref: str | None = None,
    check: bool = True,
) -> str | None:
    """Resolve the digest of an OCI image reference using oras.

    Obtains registry credentials via ``select-oci-auth`` and runs
    ``oras resolve``.

    *auth_ref* overrides the reference passed to ``select-oci-auth`` —
    useful when resolving a tagged reference (``repo:tag``) but the
    auth credentials should be obtained for the bare repository URL.
    Defaults to *reference* when not given.

    When *check* is ``True`` (the default), ``RuntimeError`` is raised on
    a non-zero exit code.  When ``False``, ``None`` is returned instead,
    which is convenient for "try to resolve, treat failure as not-found"
    callers.
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as auth_file:
        select_auth = run_cmd(["select-oci-auth", auth_ref or reference], check=False)
        auth_content = select_auth.stdout.strip()
        auth_file.write(auth_content if auth_content else "{}")
        auth_file.flush()

        result = run_cmd(
            ["oras", "resolve", "--registry-config", auth_file.name, reference],
            check=False,
        )

    if result.returncode != 0:
        if check:
            raise RuntimeError(
                f"oras resolve failed for {reference!r}"
                f" (exit {result.returncode}): {result.stderr.strip()}"
            )
        return None
    digest = result.stdout.strip()
    return digest or None


def oras_login(registry: str, username: str, password: str) -> None:
    """Log in to an OCI registry via oras using username/password credentials.

    Credentials are passed via stdin to avoid exposing them in process arguments.
    Raises ``subprocess.CalledProcessError`` if the login fails.
    """
    subprocess.run(
        ["oras", "login", registry, "-u", username, "--password-stdin"],
        input=password,
        text=True,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def oras_pull(
    pull_spec: str,
    download_dir: Path,
    *,
    stderr_path: Path | None = None,
) -> None:
    """Pull an OCI artifact into *download_dir* using select-oci-auth and oras."""
    auth_file = file.make_tempfile_path("oras-auth-")
    try:
        auth_out = subprocess_cmd.run_cmd(
            ["select-oci-auth", str(pull_spec)],
            stderr_path=stderr_path,
            check=True,
        ).stdout
        auth_file.write_text(auth_out, encoding="utf-8")
        subprocess_cmd.run_cmd(
            [
                "oras",
                "pull",
                "--registry-config",
                str(auth_file),
                str(pull_spec),
            ],
            cwd=download_dir,
            stderr_path=stderr_path,
            check=True,
            stream_stdout=True,
        )
    finally:
        # Always remove the auth file; subprocess failures still propagate to callers.
        auth_file.unlink(missing_ok=True)


def extract_disk_image_files(
    pull_spec: str,
    wanted_sources: list[str],
    destination: Path,
    *,
    stderr_path: Path | None = None,
) -> None:
    """Pull *pull_spec* and copy each entry of *wanted_sources* into *destination*.

    Complements ``oras_pull`` for callers that also need to handle disk-image
    references shaped like a normal (potentially multi-layer) container image,
    e.g. a ``docker-build-oci-ta`` test fixture, rather than the flat, single-blob
    OCI artifact ``oras pull`` expects (what ``bootc-image-builder``'s
    ``buildah manifest add --artifact`` produces in production).

    Pulls the manifest+blobs via ``skopeo copy`` and branches on
    ``config.mediaType``:
    - Flat OCI artifacts: files are matched to layers by basename via the
      ``org.opencontainers.image.title`` annotation (equivalent to what
      ``oras pull`` does).
    - Normal layered images: files are extracted from the tar layers by their
      full path, in manifest order, so a later layer's copy of a path wins
      (matching standard union-filesystem layering semantics).

    For each entry in *wanted_sources*, ``destination / entry.lstrip("/")`` is
    created if a match is found; entries not found in the image are silently
    skipped, same as a mismatched ``oras pull`` title -- callers are expected
    to detect and warn/handle missing files themselves.
    """
    stripped_sources = {source: safe_relative_path(source) for source in wanted_sources}

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        auth_file = file.make_tempfile_path("oci-auth-")
        try:
            auth_out = subprocess_cmd.run_cmd(
                ["select-oci-auth", str(pull_spec)],
                stderr_path=stderr_path,
                check=True,
            ).stdout
            auth_file.write_text(auth_out, encoding="utf-8")
            subprocess_cmd.run_cmd(
                [
                    "skopeo",
                    "copy",
                    "--retry-times",
                    "3",
                    "--authfile",
                    str(auth_file),
                    f"docker://{pull_spec}",
                    f"dir:{tmp_dir}",
                ],
                stderr_path=stderr_path,
                check=True,
                stream_stdout=True,
            )
        finally:
            auth_file.unlink(missing_ok=True)

        manifest = json.loads((tmp_dir / "manifest.json").read_text())
        config_media_type = manifest.get("config", {}).get("mediaType", "")

        if config_media_type == FLAT_ARTIFACT_CONFIG_MEDIA_TYPE:
            _copy_flat_artifact_files(manifest, tmp_dir, stripped_sources, destination)
        else:
            _copy_layered_image_files(manifest, tmp_dir, stripped_sources, destination)


def _copy_flat_artifact_files(
    manifest: dict,
    tmp_dir: Path,
    stripped_sources: dict[str, str],
    destination: Path,
) -> None:
    """Copy blobs from a flat OCI artifact's layers, matched by title basename."""
    title_to_blob: dict[str, Path] = {}
    for layer in manifest.get("layers", []):
        title = (layer.get("annotations") or {}).get("org.opencontainers.image.title")
        digest = layer.get("digest", "")
        if title and digest:
            title_to_blob[title] = tmp_dir / digest.removeprefix("sha256:")

    for stripped in stripped_sources.values():
        blob = title_to_blob.get(Path(stripped).name)
        if blob is None or not blob.is_file():
            continue
        dest = destination / stripped
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(blob), str(dest))


def _copy_layered_image_files(
    manifest: dict,
    tmp_dir: Path,
    stripped_sources: dict[str, str],
    destination: Path,
) -> None:
    """Extract wanted paths from a normal layered image's tar layers.

    Iterates layers in manifest order so a later layer's copy of a path
    overwrites an earlier one, and a later layer's whiteout entry
    (``.wh.<name>`` or the ``.wh..wh..opq`` opaque-directory marker) removes
    an earlier layer's already-extracted copy, matching standard OCI/Docker
    union-filesystem layer semantics.
    """
    wanted_paths = set(stripped_sources.values())
    for layer in manifest.get("layers", []):
        layer_file = tmp_dir / layer.get("digest", "").removeprefix("sha256:")
        if not layer_file.exists():
            continue
        with tarfile.open(str(layer_file)) as tf:
            for member in tf.getmembers():
                name = _normalize_member_name(member.name)
                dirname, base = posixpath.split(name)

                if base == _WHITEOUT_OPAQUE_MARKER:
                    prefix = f"{dirname}/" if dirname else ""
                    for wanted in wanted_paths:
                        if wanted == dirname or wanted.startswith(prefix):
                            (destination / wanted).unlink(missing_ok=True)
                    continue

                if base.startswith(_WHITEOUT_PREFIX):
                    deleted_name = base[len(_WHITEOUT_PREFIX) :]
                    deleted = (
                        posixpath.join(dirname, deleted_name) if dirname else deleted_name
                    )
                    if deleted in wanted_paths:
                        (destination / deleted).unlink(missing_ok=True)
                    continue

                if name not in wanted_paths or member.issym() or member.islnk():
                    continue
                if member.isdev():
                    continue
                extracted = tf.extractfile(member)
                if extracted is None:
                    continue
                dest = destination / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as out_f:
                    shutil.copyfileobj(extracted, out_f)


def oras_manifest_fetch(
    pullspec: str,
    auth_file: Path,
    *,
    platform: str | None = None,
) -> str:
    """Fetch an OCI manifest and return the raw JSON string.

    Runs ``oras manifest fetch`` with the given auth config.  When
    *platform* is provided (e.g. ``"linux/amd64"``), ``--platform`` is
    passed to select a specific architecture from a manifest list.
    """
    cmd: list[str | Path] = [
        "oras",
        "manifest",
        "fetch",
        "--registry-config",
        str(auth_file),
    ]
    if platform:
        cmd += ["--platform", platform]
    cmd.append(pullspec)
    result = run_cmd(cmd, check=True)
    return result.stdout


def oras_blob_fetch(
    pullspec: str,
    output_path: Path,
    auth_file: Path,
) -> None:
    """Download a single OCI blob to *output_path* using ``oras blob fetch``."""
    run_cmd(
        [
            "oras",
            "blob",
            "fetch",
            "--registry-config",
            str(auth_file),
            "--output",
            str(output_path),
            pullspec,
        ],
        check=True,
    )


def oras_push(tag: str, directory: Path, subdirectory: str, component_name: str) -> str:
    """Push *subdirectory* inside *directory* to an OCI registry via oras.

    Runs ``oras push --annotation=quay.expires-after=1d <tag> <subdirectory>`` with
    *directory* as the working directory and returns the ``sha256:<hex>`` digest string.

    Raises ``RuntimeError`` if the digest cannot be extracted from the oras output,
    which typically indicates a failed or incomplete push.
    """
    result = subprocess.check_output(
        [
            "oras",
            "push",
            "--annotation=quay.expires-after=1d",
            tag,
            subdirectory,
        ],
        cwd=str(directory),
        stderr=subprocess.STDOUT,
        text=True,
    )
    match = re.search(r"Digest:\s+(\S+)", result)
    if not match:
        raise RuntimeError(
            f"Could not extract digest from oras push output for {component_name}:\n{result}"
        )
    return match.group(1)


def os_arch_dir(
    os_name: str, arch: str, *, mac_windows_base: Path, linux_base: Path
) -> Path | None:
    """Return the OS/arch content directory for *os_name* and *arch*, or ``None``.

    For macOS and Windows, the directory sits under *mac_windows_base* (e.g.
    ``component_dir / "unsigned"`` or ``component_dir / "signed"``); for Linux it sits
    under *linux_base* (typically ``component_dir / "linux"``).  Returns ``None`` for
    unrecognised OS names so callers can skip or raise as appropriate.
    """
    if os_name == "darwin":
        return mac_windows_base / "macos" / arch
    if os_name == "linux":
        return linux_base / arch
    if os_name == "windows":
        return mac_windows_base / "windows" / arch
    return None
