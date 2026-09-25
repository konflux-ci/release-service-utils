#!/usr/bin/env python3
"""Extract Python packages from OCI artifacts and populate release notes.

Pulls snapshot images with oras, parses wheel SBOMs for pkg:pypi PURLs,
writes releaseNotes.content.artifacts, ensures mapping.components entries
have contentType ``generic``, and fetches Tekton Chains SLSA provenance.
"""

from __future__ import annotations

import base64
import binascii
import filecmp
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from packaging.utils import InvalidWheelFilename, canonicalize_name
from packaging.utils import parse_wheel_filename as parse_packaging_wheel_filename
from packaging.version import InvalidVersion, Version

from release_service_utils.helpers import (
    content_gateway,
    file,
    image_ref,
    oras_utils,
    pypi_purl,
    retry,
    subprocess_cmd,
    tekton,
)
from release_service_utils.helpers.logger import logger

PROG = "extract_py_artifacts.py"
GENERIC_CONTENT_TYPE = "generic"
COSIGN_KEY = "k8s://openshift-pipelines/public-key"
ORAS_PULL_ATTEMPTS = 3
DEFAULT_FILES_DIR = "files"
SBOM_MEMBER_SUFFIX = ".dist-info/sboms/redhat.spdx.json"
_MAX_SBOM_UNCOMPRESSED_BYTES = 16 * 1024 * 1024
_FILENAME_MAX_BYTES = 255
_REPO_DIGEST_HEX = 16
_PACKAGE_SUFFIXES = (".whl", ".tar.gz")
# Architectures whose PEP 427 tag contains an underscore.
_MULTI_SEGMENT_ARCHES = ("x86_64", "ppc64le")
_MACOS_UNIVERSAL2_ARCHES = ("arm64", "x86_64")


def _split_image(image: str) -> tuple[str, str]:
    """Return ``(repository, digest)`` for a digest-qualified *image*."""
    try:
        return image_ref.split_digest_qualified_ref(image)
    except ValueError as exc:
        raise tekton.CheckStepError("validating image digest", exc) from exc


def _image_identity(image: str) -> tuple[str, str]:
    """Return the tag-stripped repository and digest that identify *image*."""
    repo, digest = _split_image(image)
    return image_ref.repository(f"{repo}@{digest}"), digest


def _provenance_filename(image: str) -> str:
    """Return a filesystem-safe provenance name for *image*.

    Hashes the repository so two refs that share a digest cannot
    overwrite each other, and so a long slash-heavy repository cannot
    exceed the filesystem component-length limit. The image digest
    remains the filename prefix so catalog signing can still glob
    ``sha256:*.json``. If the name must be bounded, only the digest
    stem is truncated so the ``.json`` extension is kept.
    """
    repository, digest = _image_identity(image)
    repo_digest = hashlib.sha256(repository.encode()).hexdigest()[:_REPO_DIGEST_HEX]
    suffix = f"--{repo_digest}.json"
    stem_limit = max(0, _FILENAME_MAX_BYTES - len(suffix.encode()))
    stem = digest.encode()[:stem_limit].decode("utf-8", errors="ignore")
    return f"{stem}{suffix}"


def container_images_from_snapshot(snapshot: dict[str, Any]) -> list[str]:
    """Return non-empty digest-qualified ``containerImage`` values.

    Distinct repositories that share a digest are kept. The same
    repository and digest appearing more than once is recorded once.
    """
    components = snapshot.get("components")
    if not isinstance(components, list):
        raise tekton.CheckStepError(
            "reading snapshot components",
            ValueError("snapshot has no components"),
        )
    images: list[str] = []
    seen: set[tuple[str, str]] = set()
    for component in components:
        if not isinstance(component, dict):
            continue
        image = str(component.get("containerImage") or "").strip()
        if not image:
            continue
        identity = _image_identity(image)
        if identity in seen:
            continue
        seen.add(identity)
        images.append(image)
    if not images:
        raise tekton.CheckStepError(
            "reading snapshot components",
            ValueError("snapshot has no containerImage values"),
        )
    return images


def parse_wheel_filename(filename: str) -> tuple[str, str, list[str]]:
    """Return ``(name, version, platforms)`` from a PEP 427 wheel filename.

    Uses ``packaging.utils.parse_wheel_filename`` for validation. The
    distribution name keeps the spelling from the filename so artifact
    and mapping component values match catalog exact-name lookups.
    Canonicalized names are used only for SBOM and PURL matching.
    Platform tags are a sorted list of distinct values so multi-arch wheels
    keep every supported target.
    """
    try:
        _, version, _build, tags = parse_packaging_wheel_filename(filename)
    except InvalidWheelFilename as exc:
        raise tekton.CheckStepError(
            "parsing wheel filename",
            ValueError(f"Cannot parse wheel filename: {filename}"),
        ) from exc
    wheel_name = filename.removesuffix(".whl").split("-", 1)[0]
    platforms = sorted({tag.platform for tag in tags})
    return wheel_name, str(version), platforms


def parse_wheel_platform(platform: str) -> list[tuple[str, str]]:
    """Return architecture/OS targets for a PEP 427 wheel platform tag.

    ``os: "any"`` is reserved for the literal ``any`` and ``none`` tags.
    macOS ``universal2`` tags expand to both ``x86_64`` and ``arm64`` Darwin
    targets. Unknown platform-specific tags raise so artifacts are not
    labeled as platform-independent.
    """
    if platform in {"any", "none"}:
        return [("noarch", "any")]
    if platform == "universal2" or platform.endswith("_universal2"):
        return [(arch, "darwin") for arch in _MACOS_UNIVERSAL2_ARCHES]
    architecture = platform.rsplit("_", 1)[-1]
    for known in _MULTI_SEGMENT_ARCHES:
        if platform == known or platform.endswith(f"_{known}"):
            architecture = known
            break
    if platform.startswith(("manylinux", "musllinux", "linux")):
        operating_system = "linux"
    elif platform.startswith("macosx"):
        operating_system = "darwin"
    elif platform.startswith("win"):
        operating_system = "windows"
    elif platform.startswith("freebsd"):
        operating_system = "freebsd"
    else:
        raise tekton.CheckStepError(
            "parsing wheel platform",
            ValueError(f"Unsupported wheel platform tag: {platform}"),
        )
    return [(architecture, operating_system)]


def _dist_info_matches(member: str, name: str, version: str) -> bool:
    """Return True if *member* is the Red Hat SBOM for *name* and *version*."""
    if member.endswith("/") or not member.endswith(SBOM_MEMBER_SUFFIX):
        return False
    stem = member[: -len(SBOM_MEMBER_SUFFIX)].rsplit("/", 1)[-1]
    want_name = canonicalize_name(name)
    try:
        want_version = Version(version)
    except InvalidVersion:
        return False
    for index, char in enumerate(stem):
        if char != "-":
            continue
        dist, ver = stem[:index], stem[index + 1 :]
        if not dist or not ver:
            continue
        try:
            if canonicalize_name(dist) == want_name and Version(ver) == want_version:
                return True
        except InvalidVersion:
            continue
    return False


def extract_sbom_from_wheel(wheel: Path, name: str, version: str) -> dict[str, Any]:
    """Return the Red Hat SPDX SBOM JSON object from *wheel*.

    Rejects a selected ZIP member whose uncompressed size exceeds
    ``_MAX_SBOM_UNCOMPRESSED_BYTES`` and reads the JSON through a
    bounded reader so a compressed oversized SBOM cannot exhaust memory.
    Archive, I/O, decoding, and JSON failures become
    ``tekton.CheckStepError``.
    """
    try:
        with zipfile.ZipFile(wheel) as archive:
            matches = sorted(
                member
                for member in archive.namelist()
                if _dist_info_matches(member, name, version)
            )
            if not matches:
                raise tekton.CheckStepError(
                    "extracting SBOM from wheel",
                    ValueError(f"SBOM not found in wheel: {wheel.name}"),
                )
            sbom_path = matches[0]
            info = archive.getinfo(sbom_path)
            if info.file_size > _MAX_SBOM_UNCOMPRESSED_BYTES:
                cause = ValueError(
                    f"SBOM member {sbom_path!r} exceeds "
                    f"{_MAX_SBOM_UNCOMPRESSED_BYTES} uncompressed bytes"
                )
                raise tekton.CheckStepError("extracting SBOM from wheel", cause) from cause
            logger.info("Extracting SBOM: %s", sbom_path)
            with archive.open(sbom_path) as handle:
                raw = file.read_bounded(handle, max_bytes=_MAX_SBOM_UNCOMPRESSED_BYTES)
                parsed = json.loads(raw)
    except tekton.CheckStepError:
        raise
    except (
        OSError,
        ValueError,
        KeyError,
        RuntimeError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise tekton.CheckStepError("extracting SBOM from wheel", exc) from exc
    if not isinstance(parsed, dict):
        raise tekton.CheckStepError(
            "extracting SBOM from wheel",
            TypeError(f"SBOM root must be an object: {sbom_path}"),
        )
    return parsed


def extract_pypi_purl(sbom: dict[str, Any], name: str, version: str, filename: str) -> str:
    """Return the ``pkg:pypi/`` PURL that matches *name* and *version*."""
    packages = sbom.get("packages")
    if not isinstance(packages, list):
        packages = []
    for package in packages:
        if not isinstance(package, dict):
            continue
        refs = package.get("externalRefs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            if ref.get("referenceType") != "purl":
                continue
            locator = str(ref.get("referenceLocator") or "")
            if pypi_purl.pypi_purl_matches(locator, name, version):
                return locator
    raise tekton.CheckStepError(
        "reading pkg:pypi PURL from SBOM",
        ValueError(f"No pkg:pypi PURL found in SBOM for wheel: {filename}"),
    )


def _is_python_package_file(path: Path) -> bool:
    """Return True if *path* is a wheel or source distribution."""
    name = path.name
    return any(name.endswith(suffix) for suffix in _PACKAGE_SUFFIXES)


def _contained_regular_files(
    source_dir: Path,
    pattern: str,
    *,
    action: str,
) -> list[Path]:
    """Return regular files under *source_dir* matching *pattern*.

    Rejects symbolic links and resolved paths that escape *source_dir*
    before callers compare, open, or copy them.
    """
    try:
        return file.contained_regular_files(source_dir, pattern)
    except ValueError as exc:
        raise tekton.CheckStepError(action, exc) from exc


def _flatten_package_files(source_dir: Path, dest_dir: Path) -> None:
    """Copy wheels and source distributions to the root of *dest_dir*.

    Byte-identical files with the same name are kept. Different content
    at the same name raises so the catalog signing task cannot silently
    miss or replace a nested package. Source symbolic links and paths
    that resolve outside *source_dir* are rejected before comparison or
    copy. Destination symbolic links are rejected before the existence
    check so ``copy2`` cannot follow them outside *dest_dir*.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    for source in _contained_regular_files(source_dir, "*", action="flattening package files"):
        if not _is_python_package_file(source):
            continue
        dest = dest_dir / source.name
        if dest.is_symlink():
            cause = ValueError(f"Conflicting package file {source.name!r}")
            raise tekton.CheckStepError("flattening package files", cause) from cause
        if dest.exists():
            if dest.is_file() and filecmp.cmp(source, dest, shallow=False):
                continue
            raise tekton.CheckStepError(
                "flattening package files",
                ValueError(f"Conflicting package file {source.name!r}"),
            )
        try:
            dest = file.resolve_path_under_base(dest_dir, source.name)
        except ValueError as exc:
            raise tekton.CheckStepError("flattening package files", exc) from exc
        shutil.copy2(source, dest)


def collect_wheel_artifacts(files_dir: Path) -> list[dict[str, str]]:
    """Build deduplicated artifact rows from ``*.whl`` files under *files_dir*.

    Source symbolic links and paths that resolve outside *files_dir*
    are rejected before a wheel archive is opened.
    """
    artifacts: list[dict[str, str]] = []
    for wheel in _contained_regular_files(
        files_dir, "*.whl", action="collecting wheel artifacts"
    ):
        logger.info("Processing wheel: %s", wheel.name)
        name, version, platforms = parse_wheel_filename(wheel.name)
        sbom = extract_sbom_from_wheel(wheel, name, version)
        purl = extract_pypi_purl(sbom, name, version, wheel.name)
        logger.info("Found PURL: %s", purl)
        targets: set[tuple[str, str]] = set()
        for platform in platforms:
            for architecture, operating_system in parse_wheel_platform(platform):
                logger.info(
                    "Platform: %s -> arch=%s, os=%s",
                    platform,
                    architecture,
                    operating_system,
                )
                targets.add((architecture, operating_system))
        for architecture, operating_system in sorted(targets):
            artifacts.append(
                {
                    "component": name,
                    "purl": purl,
                    "architecture": architecture,
                    "os": operating_system,
                }
            )

    if not artifacts:
        raise tekton.CheckStepError(
            "collecting wheel artifacts",
            ValueError(f"No .whl files found in {files_dir}"),
        )

    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[dict[str, str]] = []
    for artifact in artifacts:
        key = (
            artifact["component"],
            artifact["purl"],
            artifact["architecture"],
            artifact["os"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(artifact)
    return deduped


def update_release_notes_artifacts(
    data: dict[str, Any],
    artifacts: list[dict[str, str]],
) -> None:
    """Append *artifacts* to ``releaseNotes.content.artifacts``.

    Missing, null, or non-dictionary ``releaseNotes`` values are
    replaced with an empty dictionary before ``content.artifacts`` is
    created.
    """
    release_notes = data.setdefault("releaseNotes", {})
    if not isinstance(release_notes, dict):
        release_notes = {}
        data["releaseNotes"] = release_notes
    content = release_notes.setdefault("content", {})
    if not isinstance(content, dict):
        content = {}
        release_notes["content"] = content
    existing = content.get("artifacts")
    if not isinstance(existing, list):
        existing = []
    content["artifacts"] = existing + artifacts


def update_mapping_components(data: dict[str, Any], component_names: list[str]) -> None:
    """Ensure mapping components exist and missing content types become generic.

    An empty nested ``contentGateway.contentType`` falls back to the
    top-level type. When the effective type is empty, both the top-level
    field and an explicitly empty nested type are set to ``generic`` so
    later PURL updates resolve the same value.
    """
    mapping = data.setdefault("mapping", {})
    if not isinstance(mapping, dict):
        mapping = {}
        data["mapping"] = mapping
    existing = mapping.get("components")
    if not isinstance(existing, list):
        existing = []

    names = set(component_names)
    existing_names = {
        component["name"]
        for component in existing
        if isinstance(component, dict) and isinstance(component.get("name"), str)
    }

    updated: list[Any] = []
    for component in existing:
        name = component.get("name") if isinstance(component, dict) else None
        if (
            isinstance(component, dict)
            and isinstance(name, str)
            and name in names
            and content_gateway.component_content_type(component) == ""
        ):
            updated_component = {**component, "contentType": GENERIC_CONTENT_TYPE}
            gateway = updated_component.get("contentGateway")
            if isinstance(gateway, dict) and gateway.get("contentType") == "":
                updated_component["contentGateway"] = {
                    **gateway,
                    "contentType": GENERIC_CONTENT_TYPE,
                }
            updated.append(updated_component)
        else:
            updated.append(component)

    for name in component_names:
        if name not in existing_names:
            updated.append({"name": name, "contentType": GENERIC_CONTENT_TYPE})

    mapping["components"] = updated


def _merge_pulled_files(source_dir: Path, dest_dir: Path, image: str) -> None:
    """Copy files from *source_dir* into *dest_dir*, rejecting content collisions.

    Byte-identical files at the same relative path are kept. A different
    file at that path, or a non-directory ancestor that would block the
    destination, raises so one image cannot silently replace another.
    Source symbolic links and paths that resolve outside *source_dir*
    are rejected before comparison or copy.
    """
    for source in _contained_regular_files(
        source_dir, "*", action="merging pulled OCI artifacts"
    ):
        relative = source.relative_to(source_dir)
        dest = dest_dir / relative
        if dest.exists():
            if dest.is_file() and filecmp.cmp(source, dest, shallow=False):
                continue
            raise tekton.CheckStepError(
                "merging pulled OCI artifacts",
                ValueError(f"Conflicting path {relative.as_posix()!r} from {image}"),
            )
        for ancestor in dest.parents:
            if ancestor == dest_dir:
                break
            if ancestor.exists() and not ancestor.is_dir():
                conflict = ancestor.relative_to(dest_dir).as_posix()
                raise tekton.CheckStepError(
                    "merging pulled OCI artifacts",
                    ValueError(f"Conflicting path {conflict!r} from {image}"),
                )
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)


def pull_oci_artifacts(images: list[str], files_dir: Path) -> None:
    """Pull each snapshot image into *files_dir* with oras.

    Each image is extracted into its own temporary directory, then merged
    into *files_dir* so equal relative paths cannot silently overwrite
    each other before wheel artifacts are collected. Each retry recreates
    that pull directory so a failed attempt cannot leave partial files.
    """
    files_dir.mkdir(parents=True, exist_ok=True)
    for image in images:
        logger.info("Processing %s", image)
        with tempfile.TemporaryDirectory(prefix="extract-py-pull-") as tmp:
            pull_dir = Path(tmp)

            def _pull(pull_spec: str = image, dest: Path = pull_dir) -> None:
                if dest.exists():
                    shutil.rmtree(dest)
                dest.mkdir(parents=True)
                oras_utils.oras_pull(pull_spec, download_dir=dest)

            retry.retry_with_exponential_backoff(
                _pull,
                max_attempts=ORAS_PULL_ATTEMPTS,
            )
            _merge_pulled_files(pull_dir, files_dir, image)
    logger.info("Extracted files: %s", sorted(path.name for path in files_dir.iterdir()))


def _decode_cosign_payload(envelope: dict[str, Any]) -> dict[str, Any]:
    """Return the JSON statement from a cosign DSSE envelope payload."""
    try:
        payload_b64 = str(envelope["payload"])
        padded = payload_b64 + "=" * ((4 - len(payload_b64) % 4) % 4)
        parsed = json.loads(base64.b64decode(padded))
    except (
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        binascii.Error,
        UnicodeDecodeError,
    ) as exc:
        raise tekton.CheckStepError("fetching Chains provenance", exc) from exc
    if not isinstance(parsed, dict):
        raise tekton.CheckStepError(
            "decoding Chains provenance",
            TypeError("cosign attestation payload must be a JSON object"),
        )
    return parsed


def _subject_identity(entry: object) -> tuple[str, str] | None:
    """Return ``(repository, digest)`` from a well-formed in-toto subject."""
    if not isinstance(entry, dict):
        return None
    name = entry.get("name")
    digest_field = entry.get("digest")
    if not isinstance(name, str) or not name:
        return None
    if not isinstance(digest_field, dict):
        return None
    sha256 = digest_field.get("sha256")
    if not isinstance(sha256, str) or not sha256:
        return None
    hex_digest = sha256.removeprefix("sha256:")
    candidate = f"{image_ref.repository(name)}@sha256:{hex_digest}"
    try:
        return _image_identity(candidate)
    except tekton.CheckStepError:
        return None


def _require_matching_subject(statement: dict[str, Any], image: str) -> None:
    """Require a subject that identifies the requested *image*."""
    expected = _image_identity(image)
    subjects = statement.get("subject")
    if subjects is None or subjects == []:
        cause = ValueError(f"Chains provenance for {image} is missing a subject")
        raise tekton.CheckStepError("fetching Chains provenance", cause) from cause
    if not isinstance(subjects, list):
        cause = ValueError(f"Chains provenance for {image} has a malformed subject")
        raise tekton.CheckStepError("fetching Chains provenance", cause) from cause
    identities = [_subject_identity(entry) for entry in subjects]
    if all(identity is None for identity in identities):
        cause = ValueError(f"Chains provenance for {image} has a malformed subject")
        raise tekton.CheckStepError("fetching Chains provenance", cause) from cause
    if expected not in identities:
        cause = ValueError(f"Chains provenance subject does not identify {image}")
        raise tekton.CheckStepError("fetching Chains provenance", cause) from cause


def _write_sibling_file(dest: Path, payload: str) -> Path:
    """Write *payload* to a sibling of *dest* and return that path."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(prefix=f".{dest.name}-", dir=dest.parent)
    path = Path(name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as out:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())
    except OSError:
        path.unlink(missing_ok=True)
        raise
    return path


def _commit_run_outputs(
    staged_files: Path,
    files_dir: Path,
    data_path: Path,
    payload: str,
) -> None:
    """Commit staged packages and *payload* or restore every live output.

    Writes the data file to a sibling first so the only remaining step
    after swapping *files_dir* is an atomic replace. If that replace
    fails, the previous *files_dir* is restored.
    """
    data_tmp = _write_sibling_file(data_path, payload)
    backup: Path | None = None
    try:
        backup = file.swap_directory(staged_files, files_dir)
    except Exception:
        data_tmp.unlink(missing_ok=True)
        raise
    try:
        os.replace(data_tmp, data_path)
    except OSError:
        data_tmp.unlink(missing_ok=True)
        file.restore_directory(backup, files_dir)
        raise
    if backup is not None:
        shutil.rmtree(backup, ignore_errors=True)


def _write_chains_provenance(images: list[str], dest_dir: Path) -> None:
    """Write verified Chains attestations into *dest_dir* without publishing."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    for image in images:
        logger.info("Fetching Chains provenance for %s", image)
        dest = dest_dir / _provenance_filename(image)

        try:
            with tempfile.TemporaryDirectory() as auth_tmp:
                docker_config = Path(auth_tmp)
                auth = subprocess_cmd.run_cmd(
                    ["select-oci-auth", image],
                    check=True,
                ).stdout
                (docker_config / "config.json").write_text(
                    auth if auth.strip() else "{}",
                    encoding="utf-8",
                )
                result = subprocess_cmd.run_cmd(
                    [
                        "cosign",
                        "verify-attestation",
                        "--type=slsaprovenance",
                        "--insecure-ignore-tlog=true",
                        "--insecure-ignore-sct=true",
                        "--key",
                        COSIGN_KEY,
                        image,
                    ],
                    env={"DOCKER_CONFIG": str(docker_config)},
                    check=True,
                )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise tekton.CheckStepError("fetching Chains provenance", exc) from exc

        first_line = (result.stdout or "").splitlines()[0] if result.stdout else ""
        if not first_line:
            raise tekton.CheckStepError(
                "fetching Chains provenance",
                ValueError(f"Failed to fetch Chains provenance for {image}"),
            )
        try:
            envelope = json.loads(first_line)
            statement = _decode_cosign_payload(envelope)
            _require_matching_subject(statement, image)
        except tekton.CheckStepError:
            raise
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise tekton.CheckStepError("fetching Chains provenance", exc) from exc
        payload = json.dumps(statement, indent=2) + "\n"
        if dest.exists():
            if dest.read_text(encoding="utf-8") == payload:
                logger.info("Reused identical Chains provenance at %s", dest)
                continue
            cause = ValueError(f"Conflicting Chains provenance at {dest.name!r} from {image}")
            raise tekton.CheckStepError("fetching Chains provenance", cause) from cause
        dest.write_text(payload, encoding="utf-8")
        logger.info("Saved Chains provenance to %s", dest)


def fetch_chains_provenance(images: list[str], files_dir: Path) -> None:
    """Verify and store Chains SLSA provenance for each image.

    Writes attestations into a temporary sibling of
    ``files_dir/chains-provenance`` and swaps that directory into place
    only after every image has a statement. A failed fetch or swap
    deletes staging and restores the previous directory. Output names
    include repository and digest. A later image that maps to the same
    file may reuse a byte-identical statement and raises on a conflict.
    """
    dest_dir = files_dir / "chains-provenance"
    files_dir.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".chains-provenance-", dir=files_dir))
    committed = False
    try:
        _write_chains_provenance(images, staging)
        try:
            file.replace_directory(staging, dest_dir)
        except ValueError as exc:
            raise tekton.CheckStepError("fetching Chains provenance", exc) from exc
        committed = True
    finally:
        if not committed:
            shutil.rmtree(staging, ignore_errors=True)

    logger.info(
        "Chains provenance files: %s",
        sorted(path.name for path in dest_dir.iterdir()),
    )


def run(*, snapshot_path: Path, data_path: Path, files_dir: Path) -> None:
    """Extract wheels, populate release notes, and fetch Chains provenance.

    Snapshot images are pulled into a clean staging directory so leftover
    wheels in *files_dir* cannot enter the current advisory. Flattened
    wheels, source distributions, Chains provenance, and the updated
    data file stay staged until every fallible step finishes. A sibling
    swap publishes *files_dir* and an atomic replace publishes
    ``data.json``; a later failure restores the previous live outputs.
    Packages that are not in the current snapshot are not left behind
    for later SBOM extraction. *files_dir* must not be the data
    directory or contain *data_path*, or the swap would move the data
    file before its atomic replace.
    """
    _reject_files_dir_containing_data(files_dir, data_path)
    snapshot = file.load_json_dict(snapshot_path)
    images = container_images_from_snapshot(snapshot)
    with tempfile.TemporaryDirectory(prefix="extract-py-artifacts-") as staging:
        staging_dir = Path(staging)
        pull_dir = staging_dir / "pull"
        packages_dir = staging_dir / "packages"
        pull_oci_artifacts(images, pull_dir)
        artifacts = collect_wheel_artifacts(pull_dir)
        _flatten_package_files(pull_dir, packages_dir)
        logger.info(
            "Adding %d artifact(s) to releaseNotes.content.artifacts",
            len(artifacts),
        )

        data = file.load_json_dict(data_path)
        update_release_notes_artifacts(data, artifacts)
        update_mapping_components(
            data,
            sorted({artifact["component"] for artifact in artifacts}),
        )
        files_dir.parent.mkdir(parents=True, exist_ok=True)
        live = Path(tempfile.mkdtemp(prefix=".extract-py-files-", dir=files_dir.parent))
        committed = False
        try:
            _flatten_package_files(packages_dir, live)
            _write_chains_provenance(images, live / "chains-provenance")
            try:
                _commit_run_outputs(
                    live,
                    files_dir,
                    data_path,
                    json.dumps(data, indent=2) + "\n",
                )
            except ValueError as exc:
                raise tekton.CheckStepError(
                    "publishing extract-py-artifacts outputs", exc
                ) from exc
            committed = True
        finally:
            if not committed:
                shutil.rmtree(live, ignore_errors=True)
    logger.info(
        "Updated artifacts: %s", json.dumps(data["releaseNotes"]["content"]["artifacts"])
    )


def resolve_files_dir(data_dir: Path, files_dir: str) -> Path:
    """Resolve *files_dir* under *data_dir*, allowing an in-tree absolute path."""
    text = files_dir.strip() or DEFAULT_FILES_DIR
    candidate = Path(text)
    if candidate.is_absolute():
        resolved = candidate.resolve()
        root = data_dir.resolve()
        if not resolved.is_relative_to(root):
            raise tekton.CheckStepError(
                "resolving files directory",
                ValueError(f"path must stay under {data_dir}: {files_dir!r}"),
            )
        return resolved
    return file.resolve_path_under_base(data_dir, text)


def _reject_files_dir_containing_data(files_dir: Path, data_path: Path) -> None:
    """Reject a files directory that would swallow *data_path* on swap.

    Swapping *files_dir* moves every child, including a data file that
    lives at or under that directory, so the later atomic replace would
    publish to a path that no longer exists.
    """
    files_root = files_dir.resolve()
    data_root = data_path.resolve()
    if not data_root.is_relative_to(files_root):
        return
    cause = ValueError(
        f"files directory {str(files_dir)!r} must not be the data "
        f"directory or contain the data file {str(data_path)!r}"
    )
    raise tekton.CheckStepError("resolving files directory", cause) from cause


def main() -> int:
    """Read Tekton PARAM_* environment variables and extract Python artifacts."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    snapshot_rel = tekton.require_env("PARAM_SNAPSHOT_PATH")
    data_rel = tekton.require_env("PARAM_DATA_PATH")
    files_raw = os.environ.get("PARAM_FILES_DIR", DEFAULT_FILES_DIR)

    data_path = file.resolve_path_under_base(data_dir, data_rel)
    files_dir = resolve_files_dir(data_dir, files_raw)
    _reject_files_dir_containing_data(files_dir, data_path)
    run(
        snapshot_path=file.resolve_path_under_base(data_dir, snapshot_rel),
        data_path=data_path,
        files_dir=files_dir,
    )
    logger.info("extract-py-artifacts completed successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
