#!/usr/bin/env python3
"""Filter RPMs already published in advisories and reduce the snapshot.

Pull RPM files from each component's OCI artifact, transform RPM metadata
into purls for advisory matching, trigger an InternalRequest to check
advisories, validate Pulp digests for RPMs in advisories (rebuild detection),
write RPMs still needing publishing under ``.components[].rpmsToPublish``,
remove components with an empty list, and overwrite the snapshot file.
"""

from __future__ import annotations

import base64
import dataclasses
import gzip
import json
import os
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any

import requests

import advisory_data
import file as file_helper
import http_client
import oras_utils
import subprocess_cmd
import tekton
from logger import logger
from pulp_client import PulpAuth, PulpClient, PulpDigestStatus, parse_pulp_config


@dataclasses.dataclass(frozen=True)
class RpmNevra:
    """Name-Epoch-Version-Release-Architecture for an RPM."""

    name: str
    epoch: str
    version: str
    release: str
    arch: str


@dataclasses.dataclass(frozen=True)
class RpmEntry:
    """One RPM-to-repository mapping, fully resolved."""

    component_name: str
    rpm_filename: str
    sha256: str
    nevra: RpmNevra
    purl: str
    target_repo: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class ResultPaths:
    """Tekton result file paths written by the filtering workflow."""

    skip_release: Path
    environment: Path
    latest_advisory_url: Path
    latest_advisory_internal_url: Path


@dataclasses.dataclass(frozen=True)
class FilterConfig:
    """Input configuration for the filtering workflow."""

    snapshot_file: Path
    data_file: Path
    rpa_file: Path
    pulp_config_file: Path
    pulp_domain: str
    default_excludes: list[str]
    default_architectures: list[str]
    pipeline_run_uid: str
    oci_storage: str
    oras_options: str
    task_git_url: str
    task_git_revision: str
    synchronously: bool


@dataclasses.dataclass(frozen=True)
class LoadedContext:
    """Validated inputs loaded from disk."""

    snapshot: dict[str, Any]
    data: dict[str, Any]
    origin: str
    pulp_config: dict[str, str]
    base_url: str
    environment: str
    advisory_secret_name: str


@dataclasses.dataclass(frozen=True)
class FilteringResult:
    """Output of the advisory filtering phase."""

    in_advisory_rpms: list[dict[str, Any]]
    unreleased_rpms: list[dict[str, Any]]
    advisory_url: str
    advisory_internal_url: str


def should_exclude_file(filename: str, patterns: list[str]) -> bool:
    """Return True if *filename* contains any non-blank *pattern* as a substring."""
    return any(p in filename for p in patterns if p.strip())


def determine_environment(data: dict[str, Any]) -> str:
    """Return ``"stage"`` or ``"production"`` based on data file content."""
    intention = data.get("intention", "")
    if intention == "staging":
        return "stage"
    if intention == "production":
        return "production"

    advisory_repo = (data.get("advisory") or {}).get("repo", "")
    if "rhtap-release" in advisory_repo or "stage" in advisory_repo:
        return "stage"
    return "production"


def extract_rpm_metadata(rpm_path: Path) -> RpmNevra | None:
    """Run ``rpm -qp`` to extract NEVRA metadata from an RPM file.

    Return an ``RpmNevra``, or ``None`` on failure.
    """
    try:
        result = subprocess_cmd.run_cmd(
            [
                "rpm",
                "-qp",
                "--qf",
                "%{NAME}|%{EPOCH}|%{VERSION}|%{RELEASE}|%{ARCH}\n",
                str(rpm_path),
            ],
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("extract_rpm_metadata: exception running rpm -qp: %s", exc)
        return None

    if result.returncode != 0:
        return None

    line = result.stdout.strip().split("\n")[0]
    parts = line.split("|")
    if len(parts) != 5:
        return None

    epoch = parts[1]
    if not epoch or epoch == "(none)":
        epoch = "0"

    return RpmNevra(
        name=parts[0],
        epoch=epoch,
        version=parts[2],
        release=parts[3],
        arch=parts[4],
    )


def _build_arch_repo_cache(
    rpm_repos: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build a mapping from architecture to repository info."""
    cache: dict[str, dict[str, Any]] = {}
    for repo in rpm_repos:
        arch = repo.get("arch", "")
        if arch and arch not in cache:
            cache[arch] = {
                "repository_id": repo.get("repository_id", ""),
                "repository_name": repo.get("repository_name", ""),
                "distro": repo.get("distro", ""),
            }
    return cache


def _build_purl(
    rpmname: str,
    version: str,
    release: str,
    arch: str,
    distro: str,
    repo_id: str,
) -> str:
    """Build a Package URL for an RPM."""
    purl = f"pkg:rpm/redhat/{rpmname}@{version}-{release}?arch={arch}"
    if distro:
        purl += f"&distro={distro}"
    if repo_id:
        purl += f"&repository_id={repo_id}"
    return purl


def _pull_rpm_files(
    image: str,
    files_dir: Path,
    excludes: list[str],
) -> list[str]:
    """Pull an OCI artifact and return filtered RPM filenames."""
    oras_utils.oras_pull(image, files_dir)

    kept = []
    for f in sorted(files_dir.iterdir()):
        if not f.is_file():
            continue
        if not f.name.endswith(".rpm"):
            continue
        if should_exclude_file(f.name, excludes):
            continue
        kept.append(f.name)

    return kept


def _resolve_target_repos(
    nevra: RpmNevra,
    arch_cache: dict[str, dict[str, Any]],
    noarch_repos: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return target repositories for an RPM based on its NEVRA architecture."""
    if nevra.arch == "src":
        repo = arch_cache.get("src")
        if repo:
            return [repo]
        logger.warning(
            "No repository mapping for source arch (available: %s), skipping %s",
            sorted(arch_cache.keys()),
            nevra.name,
        )
        return []

    if nevra.arch == "noarch":
        return list(noarch_repos)

    repo = arch_cache.get(nevra.arch)
    return [repo] if repo else []


def _process_component_rpms(
    name: str,
    files_dir: Path,
    rpm_files: list[str],
    arch_cache: dict[str, dict[str, Any]],
    noarch_repos: list[dict[str, Any]],
) -> list[RpmEntry]:
    """Build resolved RPM entries for a single component."""
    results: list[RpmEntry] = []

    for f in rpm_files:
        file_abs = files_dir / f
        local_sha = file_helper.sha256(file_abs)
        nevra = extract_rpm_metadata(file_abs)
        if nevra is None:
            continue

        if f.endswith(".src.rpm"):
            nevra = RpmNevra(
                name=nevra.name,
                epoch=nevra.epoch,
                version=nevra.version,
                release=nevra.release,
                arch="src",
            )

        target_repos = _resolve_target_repos(nevra, arch_cache, noarch_repos)
        if not target_repos:
            continue

        for repo_obj in target_repos:
            repo_id = repo_obj.get("repository_id", "")
            distro = repo_obj.get("distro", "")

            purl = _build_purl(
                nevra.name, nevra.version, nevra.release, nevra.arch, distro, repo_id
            )

            results.append(
                RpmEntry(
                    component_name=name,
                    rpm_filename=f,
                    sha256=local_sha,
                    nevra=nevra,
                    purl=purl,
                    target_repo=repo_obj,
                )
            )

    return results


def entries_to_ir_payload(entries: list[RpmEntry]) -> list[dict[str, Any]]:
    """Serialize RPM entries into the dict format expected by InternalRequest."""
    return [
        {
            "name": e.component_name,
            "purl": e.purl,
            "repository_name": e.target_repo.get("repository_name", ""),
            "rpm": e.rpm_filename,
            "sha256": e.sha256,
            "rpmname": e.nevra.name,
            "epoch": e.nevra.epoch,
            "version": e.nevra.version,
            "release": e.nevra.release,
            "arch": e.nevra.arch,
            "targetRepo": e.target_repo,
        }
        for e in entries
    ]


def entries_to_rpms_map(
    entries: list[RpmEntry],
) -> dict[str, list[dict[str, Any]]]:
    """Group RPM entries by component, one entry per (RPM, repo) pair.

    Return a mapping from component name to its ``rpmsToPublish`` list.
    Each RPM-to-repo combination produces a separate entry with a
    single-element ``targetRepos`` array, matching the original bash
    output format.
    """
    by_component: dict[str, list[dict[str, Any]]] = {}
    for e in entries:
        by_component.setdefault(e.component_name, []).append(
            {
                "rpm": e.rpm_filename,
                "sha256": e.sha256,
                "rpmname": e.nevra.name,
                "epoch": e.nevra.epoch,
                "version": e.nevra.version,
                "release": e.nevra.release,
                "arch": e.nevra.arch,
                "targetRepos": [e.target_repo],
            }
        )

    return by_component


def build_rpm_entries(
    snapshot: dict[str, Any],
    data: dict[str, Any],
    default_excludes: list[str],
    default_architectures: list[str],
) -> list[RpmEntry]:
    """Extract RPMs from snapshot components and build resolved entries.

    Return a flat list of ``RpmEntry`` objects, one per RPM-to-repo mapping.
    """
    rpm_repos = (data.get("mapping") or {}).get("rpm-repositories") or []

    arch_cache = _build_arch_repo_cache(rpm_repos)
    noarch_repos = [arch_cache[a] for a in default_architectures if a in arch_cache]

    all_entries: list[RpmEntry] = []
    components = snapshot.get("components", [])

    for component in components:
        image = component.get("containerImage", "")
        name = component.get("name", "")
        logger.info("Processing component '%s' (%s)", name, image)

        with tempfile.TemporaryDirectory() as workdir_str:
            files_dir = Path(workdir_str) / "files"
            files_dir.mkdir()

            rpm_files = _pull_rpm_files(image, files_dir, default_excludes)
            if not rpm_files:
                continue

            entries = _process_component_rpms(
                name, files_dir, rpm_files, arch_cache, noarch_repos
            )
            all_entries.extend(entries)

    return all_entries


def create_internal_request(
    transformed_snapshot_b64: str,
    origin: str,
    advisory_secret_name: str,
    pipeline_run_uid: str,
    oci_storage: str,
    oras_options: str,
    task_git_url: str,
    task_git_revision: str,
    synchronously: bool,
) -> dict[str, Any]:
    """Create an InternalRequest and return its status results.

    Call ``internal-request`` to create the IR, wait for it via
    ``kubectl``, and return the parsed results dict.
    """
    pipelinerun_label = "internal-services.appstudio.openshift.io/pipelinerun-uid"

    cmd = [
        "internal-request",
        "--pipeline",
        "filter-already-released-advisory-rpms",
        "-p",
        f"transformedSnapshot={transformed_snapshot_b64}",
        "-p",
        f"origin={origin}",
        "-p",
        f"advisory_secret_name={advisory_secret_name}",
        "-p",
        f"internalRequestPipelineRunName={pipeline_run_uid}",
        "-p",
        f"ociStorage={oci_storage}",
        "-p",
        f"orasOptions={oras_options}",
        "-p",
        f"taskGitUrl={task_git_url}",
        "-p",
        f"taskGitRevision={task_git_revision}",
        "-s",
        str(synchronously).lower(),
        "-l",
        f"{pipelinerun_label}={pipeline_run_uid}",
    ]

    ir_result = subprocess_cmd.run_cmd(cmd, check=True)

    ir_name = ""
    for line in ir_result.stdout.splitlines():
        if "created" in line:
            match = re.search(r"'([^']+)'", line)
            if match:
                ir_name = match.group(1)
                break

    if not ir_name:
        raise RuntimeError(
            f"Could not parse InternalRequest name from output: {ir_result.stdout}"
        )

    logger.info("Internal request created: %s", ir_name)

    results_out = subprocess_cmd.run_cmd(
        [
            "kubectl",
            "get",
            "internalrequest",
            ir_name,
            "-o=jsonpath={.status.results}",
        ],
        check=True,
    )

    results_text = results_out.stdout.strip()
    if not results_text:
        raise RuntimeError(f"No results found in internal request {ir_name}")

    results: dict[str, Any] = json.loads(results_text)
    if results.get("result") != "Success":
        raise RuntimeError(f"Filtering failed: {json.dumps(results)}")

    return results


def pull_filter_results(
    artifact_ref: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Pull filter results OCI artifact and extract the tarball.

    Return ``(in_advisory_rpms, unreleased_rpms)`` lists.
    """
    oci_ref = artifact_ref.removeprefix("oci:")

    with tempfile.TemporaryDirectory() as results_dir_str:
        results_dir = Path(results_dir_str)
        oras_utils.oras_pull(oci_ref, results_dir)

        tarball = results_dir / "filter-results.tar.gz"

        with tarfile.open(tarball, "r:gz") as tf:
            tf.extractall(path=results_dir, filter="data")

        in_advisory_file = results_dir / "in_advisory_rpms.json"
        unreleased_file = results_dir / "unreleased_rpms.json"

        in_advisory: list[dict[str, Any]] = []
        if in_advisory_file.is_file():
            in_advisory = json.loads(in_advisory_file.read_text(encoding="utf-8"))

        if not unreleased_file.is_file():
            raise RuntimeError("No unreleased_rpms.json found in filter results")
        unreleased: list[dict[str, Any]] = json.loads(
            unreleased_file.read_text(encoding="utf-8")
        )

        return in_advisory, unreleased


def validate_pulp_digests(
    in_advisory_rpms: list[dict[str, Any]],
    pulp: PulpClient,
) -> None:
    """Validate Pulp digests for in-advisory RPMs.

    Raise ``RuntimeError`` on digest mismatch or Pulp API failures.
    """
    logger.info(
        "Validating Pulp digests for %d in-advisory RPMs...",
        len(in_advisory_rpms),
    )

    for rpm_entry in in_advisory_rpms:
        rpmname = rpm_entry["rpmname"]
        epoch = rpm_entry["epoch"]
        version = rpm_entry["version"]
        release = rpm_entry["release"]
        arch = rpm_entry["arch"]
        sha256 = rpm_entry["sha256"]
        repo_name = rpm_entry["repository_name"]

        logger.info(
            "Checking Pulp digest for %s-%s-%s.%s...",
            rpmname,
            version,
            release,
            arch,
        )

        try:
            status = pulp.check_digest(
                repo_name, rpmname, epoch, version, release, arch, sha256
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Pulp API error while checking digest "
                f"for {rpmname}-{version}-{release}.{arch}"
            ) from exc

        if status == PulpDigestStatus.MATCH:
            logger.info("  -> Published in Pulp with matching digest")
        elif status == PulpDigestStatus.NOT_FOUND:
            logger.info("  -> Not published in Pulp yet (advisory is authoritative)")
        elif status == PulpDigestStatus.MISMATCH:
            raise RuntimeError(
                f"Cannot rebuild RPM with same NEVR but different digest. "
                f"RPM: {rpmname}-{version}-{release}.{arch}, "
                f"Snapshot sha256: {sha256}. "
                f"Action required: Bump version or release number"
            )


def filter_snapshot(
    snapshot: dict[str, Any],
    unreleased_rpms: list[dict[str, Any]],
    component_rpms_map: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Filter snapshot to keep only components with unreleased RPMs.

    Attach ``rpmsToPublish`` to each retained component.
    """
    unreleased_names = sorted(set(r["name"] for r in unreleased_rpms))

    components_by_name = {c["name"]: c for c in snapshot.get("components", []) if "name" in c}

    filtered_components = []
    for comp_name in unreleased_names:
        original = components_by_name.get(comp_name)
        if original is None:
            continue

        rpms = component_rpms_map.get(comp_name)
        if not rpms:
            continue

        component = dict(original)
        component["rpmsToPublish"] = rpms
        filtered_components.append(component)

    result = dict(snapshot)
    result["components"] = filtered_components
    return result


def _write_results(
    results: ResultPaths,
    snapshot_file: Path,
    snapshot: dict[str, Any],
    *,
    skip_release: bool,
    advisory_url: str = "",
    advisory_internal_url: str = "",
) -> None:
    """Write Tekton result files and the updated snapshot."""
    results.skip_release.write_text("true" if skip_release else "false", encoding="utf-8")
    results.latest_advisory_url.write_text(advisory_url, encoding="utf-8")
    results.latest_advisory_internal_url.write_text(advisory_internal_url, encoding="utf-8")
    snapshot_file.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")


def load_and_validate(config: FilterConfig) -> LoadedContext:
    """Load input files, parse Pulp config, and determine environment.

    Raise ``FileNotFoundError`` on missing files, ``TypeError`` if
    JSON root is not an object, or ``RuntimeError`` on invalid
    configuration.
    """
    snapshot = file_helper.load_json_dict(config.snapshot_file)
    data = file_helper.load_json_dict(config.data_file)
    rpa = file_helper.load_json_dict(config.rpa_file)

    origin = rpa["spec"]["origin"]
    if not origin:
        raise ValueError("'origin' in ReleasePlanAdmission spec is empty")

    pulp_config = parse_pulp_config(config.pulp_config_file)
    base_url = pulp_config["base_url"]

    environment = determine_environment(data)

    return LoadedContext(
        snapshot=snapshot,
        data=data,
        origin=origin,
        pulp_config=pulp_config,
        base_url=base_url,
        environment=environment,
        advisory_secret_name=advisory_data.advisory_secret_name(environment),
    )


def submit_advisory_filter(
    rpm_entries: list[RpmEntry],
    config: FilterConfig,
    ctx: LoadedContext,
) -> FilteringResult:
    """Compress RPM entries, submit InternalRequest, and pull back results."""
    ir_payload = entries_to_ir_payload(rpm_entries)
    entries_json = json.dumps(ir_payload).encode("utf-8")
    compressed = gzip.compress(entries_json)
    transformed_b64 = base64.b64encode(compressed).decode("ascii")

    ir_results = create_internal_request(
        transformed_b64,
        ctx.origin,
        ctx.advisory_secret_name,
        config.pipeline_run_uid,
        config.oci_storage,
        config.oras_options,
        config.task_git_url,
        config.task_git_revision,
        config.synchronously,
    )

    filter_artifact = ir_results.get("filter_results_artifact", "")
    if not filter_artifact:
        raise RuntimeError("No filter_results_artifact found in results")

    logger.info("Pulling filter results from: %s", filter_artifact)
    in_advisory_rpms, unreleased_rpms = pull_filter_results(filter_artifact)

    return FilteringResult(
        in_advisory_rpms=in_advisory_rpms,
        unreleased_rpms=unreleased_rpms,
        advisory_url=ir_results.get("advisory_url", ""),
        advisory_internal_url=ir_results.get("advisory_internal_url", ""),
    )


def make_pulp_client(
    ctx: LoadedContext,
    domain: str,
) -> PulpClient:
    """Build a PulpClient with retry session and auth configured."""
    session = http_client.get_retry_session(
        total=3,
        connect=3,
        read=3,
        status=2,
        backoff_factor=0.4,
        allowed_methods=frozenset({"GET", "POST"}),
    )
    session.auth = PulpAuth(ctx.pulp_config)
    return PulpClient(session, ctx.base_url, domain)


def run(
    config: FilterConfig,
    results: ResultPaths,
) -> None:
    """Orchestrate all filtering phases and write Tekton results."""
    ctx = load_and_validate(config)
    results.environment.write_text(ctx.environment, encoding="utf-8")
    logger.info("Environment: %s", ctx.environment)

    rpm_entries = build_rpm_entries(
        ctx.snapshot,
        ctx.data,
        config.default_excludes,
        config.default_architectures,
    )
    logger.info("Total transformed entries: %d", len(rpm_entries))

    if not rpm_entries:
        logger.info("No RPMs found in any component, skipping release")
        ctx.snapshot["components"] = []
        _write_results(results, config.snapshot_file, ctx.snapshot, skip_release=True)
        return

    filtering = submit_advisory_filter(rpm_entries, config, ctx)

    pulp = make_pulp_client(ctx, config.pulp_domain)
    if filtering.in_advisory_rpms:
        validate_pulp_digests(filtering.in_advisory_rpms, pulp)

    logger.info("Unreleased RPMs: %d", len(filtering.unreleased_rpms))

    if not filtering.unreleased_rpms:
        logger.info("All RPMs in the snapshot have already been released in advisories.")
        ctx.snapshot["components"] = []
        _write_results(
            results,
            config.snapshot_file,
            ctx.snapshot,
            skip_release=True,
            advisory_url=filtering.advisory_url,
            advisory_internal_url=filtering.advisory_internal_url,
        )
        return

    component_rpms_map = entries_to_rpms_map(rpm_entries)
    filtered = filter_snapshot(ctx.snapshot, filtering.unreleased_rpms, component_rpms_map)
    logger.info(
        "Filtered snapshot components: %d",
        len(filtered.get("components", [])),
    )
    _write_results(results, config.snapshot_file, filtered, skip_release=False)


def main() -> int:
    """Read environment variables and run the filtering workflow."""
    (
        result_skip_release,
        result_environment,
        result_latest_advisory_url,
        result_latest_advisory_internal_url,
    ) = tekton.result_paths_from_env(
        "RESULT_SKIP_RELEASE",
        "RESULT_ENVIRONMENT",
        "RESULT_LATEST_ADVISORY_URL",
        "RESULT_LATEST_ADVISORY_INTERNAL_URL",
    )

    config = FilterConfig(
        snapshot_file=Path(tekton.require_env("SNAPSHOT_FILE")),
        data_file=Path(tekton.require_env("DATA_FILE")),
        rpa_file=Path(tekton.require_env("RPA_FILE")),
        pulp_config_file=Path(tekton.require_env("PULP_CONFIG_FILE")),
        pulp_domain=tekton.require_env("PULP_DOMAIN"),
        default_excludes=[
            x.strip()
            for x in os.environ.get("DEFAULT_EXCLUDES", "-debuginfo-, -debugsource-").split(
                ","
            )
            if x.strip()
        ],
        default_architectures=[
            x.strip()
            for x in os.environ.get(
                "DEFAULT_ARCHITECTURES", "x86_64,aarch64,s390x,ppc64le"
            ).split(",")
            if x.strip()
        ],
        pipeline_run_uid=tekton.require_env("PIPELINE_RUN_UID"),
        oci_storage=os.environ.get("OCI_STORAGE", "empty"),
        oras_options=os.environ.get("ORAS_OPTIONS", ""),
        task_git_url=tekton.require_env("TASK_GIT_URL"),
        task_git_revision=tekton.require_env("TASK_GIT_REVISION"),
        synchronously=os.environ.get("SYNCHRONOUSLY", "true").lower() == "true",
    )
    result_paths = ResultPaths(
        skip_release=result_skip_release,
        environment=result_environment,
        latest_advisory_url=result_latest_advisory_url,
        latest_advisory_internal_url=result_latest_advisory_internal_url,
    )

    run(config, result_paths)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
