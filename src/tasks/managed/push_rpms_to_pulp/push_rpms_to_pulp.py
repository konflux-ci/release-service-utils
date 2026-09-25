#!/usr/bin/env python3
"""Push RPM packages from an OCI artifact to a Pulp repository."""

from __future__ import annotations

import json
import os
import tarfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file as file_helper
from release_service_utils.helpers import oras_utils
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.pulp_client import (
    PulpClient,
    PulpDigestStatus,
    parse_pulp_config,
)
from release_service_utils.helpers.rpm_utils import (
    RpmNevra,
    list_rpm_files,
    parse_comma_list,
    parse_nevra,
)

DEFAULT_PULP_SECRET = Path("/etc/secrets/cli.toml")
DEFAULT_FILES_DIR = Path("/var/workdir/rpm-extract")
DEFAULT_DATA_DIR = Path("/var/workdir/release")
DEFAULT_EXCLUDES = "-debuginfo-, -debugsource-"
DEFAULT_ARCHITECTURES = "x86_64,aarch64,s390x,ppc64le"
DEFAULT_CHUNK_SIZE = "100MB"
DEFAULT_TASK_TIMEOUT = 7200


@dataclass(frozen=True)
class PushConfig:
    """Inputs for the RPM-to-Pulp push workflow."""

    snapshot_path: Path | None
    signed_rpms_oci_artifact: str
    pulp_domain: str
    pulp_config_file: Path
    default_excludes: list[str]
    default_architectures: list[str]
    data_dir: Path
    results_dir_path: str
    artifacts_json_dir_path: str
    files_dir: Path
    pulp_upload_chunk_size: str
    pulp_task_timeout: int


@dataclass(frozen=True)
class LocalRpm:
    """One RPM file on disk, with metadata parsed once."""

    filename: str
    path: Path
    nevra: RpmNevra
    sha256: str


@dataclass(frozen=True)
class RpmPlacement:
    """How one RPM is recorded against one Pulp repository."""

    repo: str
    result_arch: str
    artifact_arch: str


@dataclass
class OutputDocs:
    """In-memory results.json and artifacts.json documents."""

    results: dict[str, Any] = field(default_factory=lambda: {"rpmfiles": []})
    artifacts: dict[str, Any] = field(
        default_factory=lambda: {"artifacts": {}, "distributions": {}}
    )

    def append_rpmfile(self, rpm: LocalRpm, placement: RpmPlacement, domain: str) -> None:
        """Append one RPM metadata entry to the results document."""
        self.results["rpmfiles"].append(
            {
                "rpm": rpm.filename,
                "rpmname": rpm.nevra.name,
                "arch": placement.result_arch,
                "pulprepo": f"{domain}/{placement.repo}",
                "epoch": rpm.nevra.epoch,
                "version": rpm.nevra.version,
                "release": rpm.nevra.release,
                "sha256": rpm.sha256,
            }
        )

    def add_artifact(self, rpm: LocalRpm, placement: RpmPlacement, url: str) -> None:
        """Record one pulp-tool artifact entry."""
        self.artifacts["artifacts"][rpm.filename] = {
            "labels": {"arch": placement.artifact_arch},
            "url": url,
            "sha256": rpm.sha256,
        }

    def add_distribution(self, arch: str, url: str) -> None:
        """Record a distribution URL for *arch*."""
        self.artifacts["distributions"][arch] = url


@dataclass(frozen=True)
class PushContext:
    """Shared Pulp client, paths, and output documents for one run."""

    client: PulpClient
    domain: str
    chunk_size: str
    config_file: Path
    base_url: str
    docs: OutputDocs
    timeout: int
    first_arch: str


def build_rpm_repo_map(snapshot: dict[str, Any]) -> dict[str, list[str]]:
    """Build rpm filename -> unique target repository_name list from a snapshot."""
    mapping: dict[str, list[str]] = {}
    for component in snapshot.get("components") or []:
        for entry in component.get("rpmsToPublish") or []:
            rpm = entry.get("rpm")
            if not rpm:
                continue
            repos = mapping.setdefault(str(rpm), [])
            for repo in entry.get("targetRepos") or []:
                if not isinstance(repo, dict):
                    continue
                name = repo.get("repository_name")
                if name and name not in repos:
                    repos.append(str(name))
    return mapping


def extract_from_signed(oci_artifact: str, files_dir: Path) -> None:
    """Pull a signed-RPMs OCI artifact and unpack ``signed-rpms`` if present."""
    oci_ref = oci_artifact.removeprefix("oci:")
    logger.info("Extracting RPMs from signed RPMs OCI artifact: %s", oci_ref)
    oras_utils.oras_pull(oci_ref, files_dir)
    archive = files_dir / "signed-rpms"
    if archive.is_file():
        logger.info("Extracting signed-rpms archive")
        with tarfile.open(archive, "r:gz") as tf:
            oras_utils.safe_extract_archive(tf, files_dir, "signed-rpms")
        archive.unlink()


def extract_from_snapshot(snapshot_path: Path, files_dir: Path) -> dict[str, list[str]]:
    """Pull RPMs from each snapshot containerImage and return the rpm-to-repo map."""
    snapshot = file_helper.load_json_dict(snapshot_path)
    rpm_repo_map = build_rpm_repo_map(snapshot)
    logger.info("RPM pre-filter detected in snapshot: %s", bool(rpm_repo_map))
    for component in snapshot.get("components") or []:
        image = component.get("containerImage")
        if image is None or not str(image).strip():
            logger.info("Skipping component with no containerImage")
            continue
        image = str(image).strip()
        logger.info("Processing %s", image)
        oras_utils.oras_pull(image, files_dir)
    return rpm_repo_map


def collect_local_rpms(files_dir: Path, excludes: list[str]) -> list[LocalRpm]:
    """Return non-excluded RPMs in *files_dir* with NEVRA metadata.

    Raise ``ValueError`` when an included RPM's NEVRA cannot be parsed.
    """
    rpms: list[LocalRpm] = []
    for entry in list_rpm_files(files_dir, excludes):
        nevra = parse_nevra(entry)
        logger.info("Including %s for upload", entry.name)
        rpms.append(
            LocalRpm(
                filename=entry.name,
                path=entry,
                nevra=nevra,
                sha256=file_helper.sha256(entry),
            )
        )
    return rpms


def detect_arches(
    rpms: list[LocalRpm],
    default_architectures: list[str],
    rpm_repo_map: dict[str, list[str]],
) -> list[str]:
    """Return arch repo names that the upload loop should process.

    When a filter map is present, return unique targeted arch repos in
    sorted order so noarch artifact URLs pick a stable ``first_arch``.
    Otherwise use RPM NEVRA order, then any extra default arches needed
    for noarch packages.
    """
    if rpm_repo_map:
        arches = sorted(
            {repo for repos in rpm_repo_map.values() for repo in repos if repo != "source"}
        )
        logger.info("Filter enabled; processing only targeted arch repos: %s", arches)
        return arches

    binary_arches = list(
        dict.fromkeys(
            rpm.nevra.arch for rpm in rpms if rpm.nevra.arch not in ("noarch", "src")
        )
    )
    has_noarch = any(rpm.nevra.arch == "noarch" for rpm in rpms)
    if binary_arches:
        logger.info("Found architectures: %s", ",".join(binary_arches))
    if not has_noarch:
        return binary_arches
    if not binary_arches:
        logger.info(
            "No architecture-specific RPMs found; using default architectures: %s",
            ",".join(default_architectures),
        )
        return list(default_architectures)

    extras = [arch for arch in default_architectures if arch not in binary_arches]
    if extras:
        logger.info("Adding default arches for noarch: %s", ",".join(extras))
    return [*binary_arches, *extras]


def content_url(base_url: str, domain: str, repo: str, pkg: str, rpm_name: str) -> str:
    """Return the pulp-content URL for *pkg* in *repo*."""
    first_letter = rpm_name[0].lower()
    return f"{base_url}/api/pulp-content/{domain}/{repo}/Packages/{first_letter}/{pkg}"


def _is_targeted(rpm_repo_map: dict[str, list[str]], pkg: str, repo: str) -> bool:
    """Return True when filtering is off or *pkg* targets *repo*."""
    if not rpm_repo_map:
        return True
    return repo in rpm_repo_map.get(pkg, [])


def _placement_for(rpm: LocalRpm, repo: str) -> RpmPlacement | None:
    """Return how *rpm* is recorded in *repo*, or None if it does not belong there."""
    arch = rpm.nevra.arch
    if repo == "source":
        if arch == "src":
            return RpmPlacement(repo="source", result_arch="src", artifact_arch="source")
        return None
    if arch == "src":
        return None
    if arch == repo:
        return RpmPlacement(repo=repo, result_arch=repo, artifact_arch=repo)
    if arch == "noarch":
        return RpmPlacement(repo=repo, result_arch="noarch", artifact_arch="noarch")
    return None


def target_placements(
    rpm: LocalRpm,
    repos: list[str],
    rpm_repo_map: dict[str, list[str]],
) -> list[RpmPlacement]:
    """Return the repositories *rpm* should be published into."""
    placements: list[RpmPlacement] = []
    for repo in repos:
        if not _is_targeted(rpm_repo_map, rpm.filename, repo):
            continue
        placement = _placement_for(rpm, repo)
        if placement is not None:
            placements.append(placement)
    return placements


def publish_repo_order(
    rpms: list[LocalRpm],
    arches: list[str],
    rpm_repo_map: dict[str, list[str]],
) -> list[str]:
    """Return arch repos plus source when a source RPM will be published."""
    needs_source = any(
        rpm.nevra.arch == "src" and _is_targeted(rpm_repo_map, rpm.filename, "source")
        for rpm in rpms
    )
    if needs_source:
        return [*arches, "source"]
    return list(arches)


def _artifact_url_repo(placement: RpmPlacement, first_arch: str) -> str:
    """Return the pulp-content repo used in artifacts.json for *placement*."""
    if placement.artifact_arch == "noarch":
        return first_arch
    return placement.repo


def _record_artifact(ctx: PushContext, rpm: LocalRpm, placement: RpmPlacement) -> None:
    """Write one artifacts.json entry for *rpm*."""
    ctx.docs.add_artifact(
        rpm,
        placement,
        content_url(
            ctx.base_url,
            ctx.domain,
            _artifact_url_repo(placement, ctx.first_arch),
            rpm.filename,
            rpm.nevra.name,
        ),
    )


def _log_upload(rpm: LocalRpm, placements: list[RpmPlacement]) -> None:
    """Log that *rpm* is being uploaded into *placements*."""
    repos = ",".join(placement.repo for placement in placements)
    if rpm.nevra.arch == "noarch":
        logger.info("Uploading noarch %s to repositories: %s", rpm.filename, repos)
    elif rpm.nevra.arch == "src":
        logger.info("Uploading %s (arch: source)", rpm.filename)
    else:
        logger.info("Uploading %s to %s repository", rpm.filename, placements[0].repo)


def process_local_rpm(
    rpm: LocalRpm,
    placements: list[RpmPlacement],
    ctx: PushContext,
) -> dict[str, list[str]]:
    """Upload *rpm* once if needed. Return new content hrefs keyed by repo."""
    if not placements:
        return {}

    to_add: list[RpmPlacement] = []
    artifact_recorded = False
    for placement in placements:
        status = ctx.client.check_digest(
            placement.repo,
            rpm.nevra.name,
            rpm.nevra.epoch,
            rpm.nevra.version,
            rpm.nevra.release,
            rpm.nevra.arch,
            rpm.sha256,
            fallback_to_latest=False,
        )
        if status is PulpDigestStatus.MISMATCH:
            raise RuntimeError(
                f"Package exists in {placement.repo} with different digest: {rpm.filename}"
            )
        if status is PulpDigestStatus.MATCH:
            logger.info(
                "Already exists with matching digest: %s in %s",
                rpm.filename,
                placement.repo,
            )
            if not artifact_recorded:
                _record_artifact(ctx, rpm, placement)
                artifact_recorded = True
            continue
        to_add.append(placement)

    if not to_add:
        return {}

    _log_upload(rpm, to_add)
    href = ctx.client.upload_rpm(rpm.path, ctx.chunk_size, ctx.config_file)
    hrefs_by_repo: dict[str, list[str]] = {}
    for placement in to_add:
        ctx.docs.append_rpmfile(rpm, placement, ctx.domain)
        hrefs_by_repo.setdefault(placement.repo, []).append(href)
        if not artifact_recorded:
            _record_artifact(ctx, rpm, placement)
            artifact_recorded = True
    return hrefs_by_repo


def add_content_by_repo(
    ctx: PushContext,
    hrefs_by_repo: dict[str, list[str]],
    repo_order: list[str],
) -> None:
    """Add collected content units to each repository in *repo_order*."""
    for repo in repo_order:
        hrefs = hrefs_by_repo.get(repo)
        if not hrefs:
            continue
        if repo == "source":
            logger.info("Adding %s source packages to source repository", len(hrefs))
        else:
            logger.info("Adding %s packages to %s repository", len(hrefs), repo)
        ctx.client.add_content(repo, hrefs, ctx.timeout)


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as pretty-printed JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def run(config: PushConfig) -> None:
    """Extract RPMs, upload them to Pulp, and write results files."""
    writable_config = file_helper.make_tempfile_path(
        "pulp-cli-", config.pulp_config_file.read_bytes()
    )
    try:
        _run(replace(config, pulp_config_file=writable_config))
    finally:
        writable_config.unlink(missing_ok=True)


def _run(config: PushConfig) -> None:
    """Run the push using *config.pulp_config_file* as the Pulp CLI config."""
    pulp_cfg = parse_pulp_config(config.pulp_config_file)
    client = PulpClient.from_config(pulp_cfg, config.pulp_domain)
    logger.info("Auth method detected for Pulp API calls: %s", client.auth_method)
    client.ensure_domain_exists()

    results_file = config.data_dir / config.results_dir_path / "push-rpms-to-pulp-results.json"
    if config.artifacts_json_dir_path:
        artifacts_dir = config.data_dir / config.artifacts_json_dir_path
    else:
        artifacts_dir = config.data_dir / config.results_dir_path
    artifacts_file = artifacts_dir / "artifacts.json"

    docs = OutputDocs()
    config.files_dir.mkdir(parents=True, exist_ok=True)
    if config.signed_rpms_oci_artifact:
        extract_from_signed(config.signed_rpms_oci_artifact, config.files_dir)
        rpm_repo_map: dict[str, list[str]] = {}
    else:
        if config.snapshot_path is None:
            raise RuntimeError(
                "Either signedRpmsOciArtifact or SNAPSHOT_PATH must be provided"
            )
        rpm_repo_map = extract_from_snapshot(config.snapshot_path, config.files_dir)

    rpms = collect_local_rpms(config.files_dir, config.default_excludes)
    arches = detect_arches(rpms, config.default_architectures, rpm_repo_map)
    repo_order = publish_repo_order(rpms, arches, rpm_repo_map)
    client.ensure_repos_exist(repo_order)
    ctx = PushContext(
        client=client,
        domain=config.pulp_domain,
        chunk_size=config.pulp_upload_chunk_size,
        config_file=config.pulp_config_file,
        base_url=pulp_cfg["base_url"],
        docs=docs,
        timeout=config.pulp_task_timeout,
        first_arch=arches[0] if arches else "x86_64",
    )

    hrefs_by_repo: dict[str, list[str]] = {}
    for rpm in rpms:
        placements = target_placements(rpm, repo_order, rpm_repo_map)
        added = process_local_rpm(rpm, placements, ctx)
        for repo, hrefs in added.items():
            hrefs_by_repo.setdefault(repo, []).extend(hrefs)

    add_content_by_repo(ctx, hrefs_by_repo, repo_order)

    for dist_arch in arches:
        docs.add_distribution(
            dist_arch,
            f"{ctx.base_url}/api/pulp-content/{config.pulp_domain}/{dist_arch}/",
        )
    docs.add_distribution(
        "source",
        f"{ctx.base_url}/api/pulp-content/{config.pulp_domain}/source/",
    )

    _write_json(results_file, docs.results)
    _write_json(artifacts_file, docs.artifacts)
    logger.info("Results written to: %s", results_file)
    logger.info("Artifacts JSON written to: %s", artifacts_file)


def main() -> int:
    """Read Tekton env vars and push RPMs to Pulp."""
    timeout_raw = os.environ.get("PULP_TASK_TIMEOUT", str(DEFAULT_TASK_TIMEOUT))
    timeout = int(timeout_raw)
    if timeout <= 0:
        raise ValueError("PULP_TASK_TIMEOUT must be a positive integer")

    snapshot_raw = os.environ.get("SNAPSHOT_PATH", "").strip()
    config = PushConfig(
        snapshot_path=Path(snapshot_raw) if snapshot_raw else None,
        signed_rpms_oci_artifact=os.environ.get("SIGNED_RPMS_OCI_ARTIFACT", "").strip(),
        pulp_domain=tekton.require_env("PULP_DOMAIN"),
        pulp_config_file=file_helper.path_from_env_variable(
            "PULP_CONFIG_FILE", DEFAULT_PULP_SECRET
        ),
        default_excludes=parse_comma_list(
            os.environ.get("DEFAULT_EXCLUDES", DEFAULT_EXCLUDES)
        ),
        default_architectures=parse_comma_list(
            os.environ.get("DEFAULT_ARCHITECTURES", DEFAULT_ARCHITECTURES)
        ),
        data_dir=file_helper.path_from_env_variable("DATA_DIR", DEFAULT_DATA_DIR),
        results_dir_path=tekton.require_env("RESULTS_DIR_PATH"),
        artifacts_json_dir_path=os.environ.get("ARTIFACTS_JSON_DIR_PATH", "").strip(),
        files_dir=file_helper.path_from_env_variable("FILES_DIR", DEFAULT_FILES_DIR),
        pulp_upload_chunk_size=os.environ.get("PULP_UPLOAD_CHUNK_SIZE", DEFAULT_CHUNK_SIZE),
        pulp_task_timeout=timeout,
    )
    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
