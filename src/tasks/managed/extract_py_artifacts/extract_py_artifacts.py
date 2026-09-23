#!/usr/bin/env python3
"""Extract Python packages from OCI artifacts and populate release notes.

Port of the extract-py-artifacts task: list snapshot images, pull them
with oras into ``filesDir``, append wheel artifacts to
``releaseNotes.content.artifacts``, set mapping content types, and save
Chains provenance next to the extracted files.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, NoReturn

from release_service_utils.helpers import subprocess_cmd, tekton
from release_service_utils.helpers.logger import logger

PROG = "extract_py_artifacts.py"
GENERIC_CONTENT_TYPE = "generic"
COSIGN_KEY = "k8s://openshift-pipelines/public-key"
DEFAULT_FILES_DIR = "files"
SBOM_MEMBER_SUFFIX = ".dist-info/sboms/redhat.spdx.json"
# Same path the task's extract-artifacts step uses for ``select-oci-auth``.
_AUTH_FILE = Path("/tmp/auth.json")


def _fail(message: str) -> NoReturn:
    """Log *message* and stop the step with status 1."""
    logger.error("%s", message)
    raise SystemExit(1)


def _jq_or(left: Any, right: Any) -> Any:
    """Return *right* when *left* is null or false, matching jq ``//``."""
    if left is None or left is False:
        return right
    return left


def _joined(base: str, relative: str) -> Path:
    """Join *base* and *relative* the way the task concatenates paths."""
    return Path(f"{base}/{relative}")


def container_images_from_snapshot(snapshot: dict[str, Any]) -> list[str]:
    """Return ``.components[].containerImage`` in snapshot order.

    A missing ``containerImage`` is the string ``null``, matching
    ``jq -r``. Duplicate values are kept. A missing or null
    ``components`` array fails the step.
    """
    if "components" not in snapshot or snapshot["components"] is None:
        raise ValueError("Cannot iterate over null (null)")
    components = snapshot["components"]
    if not isinstance(components, list):
        raise ValueError("Cannot iterate over components")
    images: list[str] = []
    for component in components:
        if not isinstance(component, dict):
            raise ValueError('Cannot index component with string "containerImage"')
        image = component.get("containerImage")
        if image is None:
            images.append("null")
            continue
        if not isinstance(image, str):
            raise ValueError("containerImage must be a string")
        images.append(image)
    return images


def parse_wheel_filename(filename: str) -> tuple[str, str, str]:
    """Return ``(name, version, platform)`` from a wheel filename.

    Splits the stem on ``-`` and requires 5 or 6 fields, matching the
    task: ``{name}-{version}(-{build})?-{python}-{abi}-{platform}.whl``.
    """
    stem = filename.removesuffix(".whl")
    parts = stem.split("-")
    if len(parts) < 5 or len(parts) > 6:
        _fail(f"ERROR: Cannot parse wheel filename: {filename}")
    return parts[0], parts[1], parts[-1]


def parse_wheel_platform(platform: str) -> tuple[str, str]:
    """Return ``(architecture, os)`` for one wheel platform tag.

    ``any`` and ``none`` are ``noarch`` / ``any``. Otherwise the
    architecture is the text after the last ``_``. The OS is ``linux``,
    ``macos``, or ``windows`` from the tag prefix, and ``any`` when the
    prefix is anything else.
    """
    if platform in {"any", "none"}:
        return "noarch", "any"
    architecture = platform.rsplit("_", 1)[-1]
    operating_system = "any"
    if platform.startswith(("manylinux", "musllinux", "linux")):
        operating_system = "linux"
    elif platform.startswith("macosx"):
        operating_system = "macos"
    elif platform.startswith("win"):
        operating_system = "windows"
    return architecture, operating_system


def extract_sbom_from_wheel(wheel: Path, name: str, version: str) -> dict[str, Any]:
    """Return the SPDX object at ``{name}-{version}.dist-info/sboms/...``.

    The member path is the literal dist-info directory from the wheel
    filename. A missing member or an unreadable archive fails the step.
    """
    sbom_path = f"{name}-{version}{SBOM_MEMBER_SUFFIX}"
    logger.info("  Extracting SBOM: %s", sbom_path)
    try:
        with zipfile.ZipFile(wheel) as archive:
            raw = archive.read(sbom_path)
    except (OSError, KeyError, zipfile.BadZipFile, zipfile.LargeZipFile):
        _fail(f"ERROR: SBOM not found in wheel: {sbom_path}")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"SBOM root must be an object: {sbom_path}")
    return parsed


def extract_pypi_purl(sbom: dict[str, Any], filename: str) -> str:
    """Return the first ``pkg:pypi/`` PURL in *sbom*.

    Walks ``packages[].externalRefs[]`` and keeps the first purl locator
    with that prefix. The wheel name and version are not compared.
    """
    packages = sbom.get("packages")
    if isinstance(packages, list):
        for package in packages:
            if package is None:
                continue
            if not isinstance(package, dict):
                raise ValueError('Cannot index package with string "externalRefs"')
            refs = package.get("externalRefs")
            if not isinstance(refs, list):
                continue
            for ref in refs:
                if not isinstance(ref, dict) or ref.get("referenceType") != "purl":
                    continue
                locator = ref.get("referenceLocator")
                if not isinstance(locator, str):
                    raise ValueError("startswith() requires string inputs")
                if locator.startswith("pkg:pypi/"):
                    return locator
    _fail(f"ERROR: No pkg:pypi PURL found in SBOM for wheel: {filename}")


def collect_wheel_artifacts(files_dir: Path) -> list[dict[str, str]]:
    """Build artifact rows from top-level ``*.whl`` files in *files_dir*.

    Each wheel contributes one row. Rows are deduplicated by component,
    purl, architecture, and OS, then sorted by that key.
    """
    artifacts: list[dict[str, str]] = []
    for wheel in sorted(path for path in files_dir.glob("*.whl") if path.is_file()):
        filename = wheel.name
        logger.info("Processing wheel: %s", filename)
        name, version, platform = parse_wheel_filename(filename)
        sbom = extract_sbom_from_wheel(wheel, name, version)
        purl = extract_pypi_purl(sbom, filename)
        logger.info("  Found PURL: %s", purl)
        architecture, operating_system = parse_wheel_platform(platform)
        logger.info(
            "  Platform: %s -> arch=%s, os=%s",
            platform,
            architecture,
            operating_system,
        )
        artifacts.append(
            {
                "component": name,
                "purl": purl,
                "architecture": architecture,
                "os": operating_system,
            }
        )

    deduped = _dedupe_artifacts(artifacts)
    if not deduped:
        _fail(f"ERROR: No .whl files found in {files_dir}")
    return deduped


def _dedupe_artifacts(artifacts: list[dict[str, str]]) -> list[dict[str, str]]:
    """Drop duplicate artifact rows and sort them like jq ``group_by``."""

    def sort_key(artifact: dict[str, str]) -> str:
        return (
            f"{artifact['component']}|{artifact['purl']}|"
            f"{artifact['architecture']}|{artifact['os']}"
        )

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for artifact in sorted(artifacts, key=sort_key):
        key = sort_key(artifact)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(artifact)
    return deduped


def update_release_notes_artifacts(
    data: dict[str, Any],
    artifacts: list[dict[str, str]],
) -> None:
    """Append *artifacts* to ``.releaseNotes.content.artifacts``.

    Missing or null objects along that path are created. A null or false
    artifact list becomes ``[]``. A non-object along the path fails.
    """
    release_notes = _object_or_new(data, "releaseNotes")
    content = _object_or_new(release_notes, "content")
    existing = content.get("artifacts") if "artifacts" in content else None
    existing = _jq_or(existing, [])
    if not isinstance(existing, list):
        raise ValueError("cannot append artifacts to a non-array")
    content["artifacts"] = existing + artifacts


def _object_or_new(parent: dict[str, Any], key: str) -> dict[str, Any]:
    """Return ``parent[key]`` as an object, creating it when it is null."""
    if key not in parent or parent[key] is None:
        parent[key] = {}
        return parent[key]
    value = parent[key]
    if not isinstance(value, dict):
        raise ValueError(f"Cannot index {key} with a nested field")
    return value


def update_mapping_components(data: dict[str, Any], component_names: list[str]) -> None:
    """Ensure each artifact component exists and has a content type.

    An existing component whose jq content type is empty gets top-level
    ``contentType: generic``. The nested ``contentGateway.contentType``
    is left unchanged. Names that are not already present are appended
    in sorted order.
    """
    names = sorted(set(component_names))
    new_components = [{"name": name, "contentType": GENERIC_CONTENT_TYPE} for name in names]
    existing = _mapping_components(data)
    existing_names = [_component_name(component) for component in existing]

    updated: list[Any] = []
    for component in existing:
        name = _component_name(component)
        if name in names and _content_type_blank(component):
            updated.append({**component, "contentType": GENERIC_CONTENT_TYPE})
        else:
            updated.append(component)
    for component in new_components:
        if component["name"] not in existing_names:
            updated.append(component)

    mapping = _object_or_new(data, "mapping")
    mapping["components"] = updated


def _mapping_components(data: dict[str, Any]) -> list[Any]:
    """Return ``.mapping.components``, or ``[]`` when it is null or false."""
    if "mapping" not in data or data["mapping"] is None:
        return []
    mapping = data["mapping"]
    if not isinstance(mapping, dict):
        raise ValueError('Cannot index string with string "components"')
    if (
        "components" not in mapping
        or mapping["components"] is None
        or mapping["components"] is False
    ):
        return []
    components = mapping["components"]
    if not isinstance(components, list):
        raise ValueError("mapping.components must be an array")
    return components


def _component_name(component: Any) -> Any:
    """Return ``.name``, or null when *component* is null."""
    if component is None:
        return None
    if not isinstance(component, dict):
        raise ValueError('Cannot index component with string "name"')
    return component.get("name")


def _content_type_blank(component: dict[str, Any]) -> bool:
    """Return whether the task's jq content-type expression is empty.

    ``.contentGateway.contentType // .contentType // ""`` treats only
    null and false as missing. An empty nested string does not fall
    through to the top-level type.
    """
    if "contentGateway" not in component or component["contentGateway"] is None:
        nested = None
    else:
        gateway = component["contentGateway"]
        if gateway is False:
            nested = None
        elif not isinstance(gateway, dict):
            raise ValueError('Cannot index string with string "contentType"')
        else:
            nested = gateway.get("contentType")
    top = component.get("contentType")
    return _jq_or(_jq_or(nested, top), "") == ""


def _retry_command(cmd: list[str]) -> None:
    """Re-run *cmd* the way the oras image ``retry`` wrapper does.

    Tries up to ``RETRY_MAX_TRIES`` (default 10). Waits
    ``RETRY_FACTOR`` (default 2) times the previous interval, starting
    at 1 second. Stops immediately when stderr contains ``unauthorized``,
    unless that failure is the last attempt.
    """
    factor = int(os.environ.get("RETRY_FACTOR", "2"))
    max_tries = int(os.environ.get("RETRY_MAX_TRIES", "10"))
    interval = 1
    for attempt in range(1, max_tries + 1):
        logger.info("Executing: %s", " ".join(cmd))
        result = subprocess_cmd.run_cmd(cmd, check=False, stream_stdout=True)
        if result.returncode == 0:
            return
        stderr = result.stderr or ""
        if stderr:
            logger.error("%s", stderr.rstrip())
        if attempt >= max_tries:
            raise subprocess.CalledProcessError(result.returncode, cmd, stderr=stderr)
        logger.warning("warning: Command failed and will retry, %s try", attempt)
        if "unauthorized" in stderr.lower():
            logger.error(
                "error: Unauthorized error, wrong registry credentials provided, won't retry"
            )
            raise subprocess.CalledProcessError(1, cmd, stderr=stderr)
        interval *= factor
        time.sleep(interval)


def _list_dir(path: Path, heading: str) -> None:
    """Log ``ls -la`` of *path* under *heading*."""
    logger.info("%s", heading)
    listing = subprocess_cmd.run_cmd(["ls", "-la", str(path)], check=True)
    if listing.stdout:
        logger.info("%s", listing.stdout.rstrip())


def pull_oci_artifacts(images: list[str], files_dir: Path) -> None:
    """Pull each image into *files_dir* with ``oras pull -o``.

    Later images extract into the same directory and can overwrite
    earlier files. Each pull is retried by the oras image ``retry``
    policy.
    """
    files_dir.mkdir(parents=True, exist_ok=True)
    for image in images:
        logger.info("Processing %s", image)
        auth = subprocess_cmd.run_cmd(["select-oci-auth", image], check=True).stdout
        _AUTH_FILE.write_text(auth or "", encoding="utf-8")
        _retry_command(
            [
                "oras",
                "pull",
                "--registry-config",
                str(_AUTH_FILE),
                image,
                "-o",
                str(files_dir),
            ]
        )
    _list_dir(files_dir, "Extracted files:")


def _provenance_filename(image: str) -> str:
    """Return ``${image##*@}.json``, the task's provenance filename."""
    return f"{image.rsplit('@', 1)[-1]}.json"


def _write_chains_provenance(images: list[str], files_dir: Path) -> None:
    """Verify and save Chains attestations under ``files_dir/chains-provenance``.

    The filename is the image reference after the last ``@``. The saved
    document is the decoded cosign payload. An existing file is
    overwritten.
    """
    provenance_dir = files_dir / "chains-provenance"
    provenance_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as docker_config:
        for image in images:
            logger.info("Fetching Chains provenance for %s", image)
            auth = subprocess_cmd.run_cmd(["select-oci-auth", image], check=True).stdout
            (Path(docker_config) / "config.json").write_text(auth or "", encoding="utf-8")
            dest = provenance_dir / _provenance_filename(image)
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
                env={"DOCKER_CONFIG": docker_config},
                check=False,
            )
            stderr = result.stderr or ""
            try:
                if result.returncode != 0:
                    raise ValueError(f"cosign exited {result.returncode}")
                statement = _decode_attestation(result.stdout or "")
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                logger.error("ERROR: Failed to fetch Chains provenance for %s", image)
                if stderr:
                    logger.error("%s", stderr.rstrip())
                raise SystemExit(1) from exc
            dest.write_text(json.dumps(statement, indent=2) + "\n", encoding="utf-8")
            logger.info("  Saved Chains provenance to %s", dest)
    _list_dir(provenance_dir, "Chains provenance files:")


def _decode_attestation(stdout: str) -> Any:
    """Return the JSON value of the first cosign attestation line.

    Matches ``head -1 | jq '.payload | @base64d | fromjson'``.
    """
    lines = stdout.splitlines()
    if not lines or not lines[0]:
        raise ValueError("empty cosign attestation")
    envelope = json.loads(lines[0])
    if not isinstance(envelope, dict) or "payload" not in envelope:
        raise ValueError("cosign attestation is missing payload")
    payload = base64.b64decode(envelope["payload"])
    return json.loads(payload)


def _write_data(path: Path, data: dict[str, Any]) -> None:
    """Write *data* to *path* via a temporary file, then replace it."""
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}-",
        delete=False,
    )
    tmp = Path(handle.name)
    try:
        with handle:
            json.dump(data, handle, indent=2)
            handle.write("\n")
        os.replace(tmp, path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def run(*, snapshot_path: Path, data_path: Path, files_dir: Path) -> None:
    """Pull snapshot images, update data.json, and fetch Chains provenance.

    Steps follow the task order. ``data.json`` is updated before
    provenance is fetched, and extracted files stay in *files_dir*.
    """
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if not isinstance(snapshot, dict):
        raise ValueError(f"JSON root must be an object: {snapshot_path}")
    images = container_images_from_snapshot(snapshot)
    pull_oci_artifacts(images, files_dir)
    artifacts = collect_wheel_artifacts(files_dir)
    logger.info(
        "Adding %d artifact(s) to releaseNotes.content.artifacts",
        len(artifacts),
    )
    data = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON root must be an object: {data_path}")
    update_release_notes_artifacts(data, artifacts)
    _write_data(data_path, data)
    update_mapping_components(data, [artifact["component"] for artifact in artifacts])
    _write_data(data_path, data)
    logger.info("Done. Updated artifacts:")
    logger.info("%s", json.dumps(data["releaseNotes"]["content"]["artifacts"], indent=2))
    _write_chains_provenance(images, files_dir)


def main() -> int:
    """Read Tekton PARAM_* environment variables and extract Python artifacts."""
    data_dir = tekton.require_env("PARAM_DATA_DIR")
    snapshot_rel = tekton.require_env("PARAM_SNAPSHOT_PATH")
    data_rel = tekton.require_env("PARAM_DATA_PATH")
    files_rel = os.environ.get("PARAM_FILES_DIR", DEFAULT_FILES_DIR)
    run(
        snapshot_path=_joined(data_dir, snapshot_rel),
        data_path=_joined(data_dir, data_rel),
        files_dir=_joined(data_dir, files_rel),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
