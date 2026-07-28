#!/usr/bin/env python3
"""Populate the releaseNotes key in a data JSON file.

Handle container images, binary/disk-image/RPM artifacts, GitHub release
artifacts, CVE validation against Jira and advisory type/references.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import advisory_data
import file as file_helper
import http_client
import jira as jira_helper
import requests
import snapshot as snapshot_helper
import tekton
from logger import logger
from requests.auth import HTTPBasicAuth
from subprocess_cmd import run_cmd_text

UNIQUE_TAG_RE = re.compile(r"(rhel-)?v?[0-9]+\.[0-9]+(\.[0-9]+)?-[0-9]{8,}")
CLASSIFICATION_URL = "https://access.redhat.com/security/updates/classification/"
CVE_REF_PREFIX = "https://access.redhat.com/security/cve/"
PULP_CONTENT_BASE_URL = "https://packages.redhat.com/api/pulp-content"


def build_cves_for_component(data: dict[str, Any], component_name: str) -> dict[str, Any]:
    """Construct CVE json for a single component."""
    fixed: dict[str, Any] = {}
    for cve in data.get("releaseNotes", {}).get("cves", []):
        if cve.get("component") == component_name:
            fixed[cve["key"]] = {"packages": cve.get("packages", [])}
    return {"cves": {"fixed": fixed}}


def get_timestamp_tag(component: dict[str, Any]) -> str:
    """Try to get a timestamp tag from the OCI image.created label.

    Return the label value as unix timestamp seconds or empty string
    if the label is missing or cannot be parsed.
    """
    created = snapshot_helper.component_label_value(
        component, "org.opencontainers.image.created"
    )
    if created:
        try:
            timestamp_tag = int(datetime.fromisoformat(created).timestamp())
            return str(timestamp_tag)
        except (ValueError, OverflowError):
            pass
        logger.warning(
            "Component '%s' has 'org.opencontainers.image.created' "
            "label ('%s') but it could not be parsed as a date.",
            component.get("name", ""),
            created,
        )
    else:
        # Conforma should have blocked this release
        logger.warning(
            "Component '%s' is missing the "
            "'org.opencontainers.image.created' label. "
            "Falling back to regex tag matching.",
            component.get("name", ""),
        )
    return ""


def get_unique_tag_from_tags(tags: list[str]) -> str:
    """Return the longest tag matching the timestamp regex or empty string."""
    longest = ""
    for tag in tags:
        if UNIQUE_TAG_RE.search(tag) and len(tag) > len(longest):
            longest = tag
    return longest


def get_image_architectures(image_ref: str) -> list[dict[str, Any]]:
    """Get all architectures and their digests for an image."""
    output = run_cmd_text(["get-image-architectures", image_ref])
    arch_digests: list[dict[str, Any]] = []
    for line in output.strip().splitlines():
        if line.strip():
            arch_digests.append(json.loads(line))
    return arch_digests


def parse_checksum_file(path: Path) -> dict[str, str]:
    """Parse a SHA256SUMS file into a filename-to-checksum mapping."""
    checksums: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        checksum, filename = parts
        if filename.endswith("_manifest.json"):
            continue
        checksums[filename] = checksum
    return checksums


def validate_cve_issues(
    data: dict[str, Any],
    session: requests.Session,
    auth: HTTPBasicAuth,
) -> None:
    """Check that each Jira Vulnerability issue has a CVE in releaseNotes.cves."""
    release_notes = data.get("releaseNotes", {})
    issues_fixed = release_notes.get("issues", {}).get("fixed")
    if not issues_fixed:
        logger.info("No issues.fixed found. Skipping CVE validation.")
        return

    # Drop duplicate issue references (same source + id); keeps first occurrence
    seen: set[tuple[str, str]] = set()
    unique_issues: list[dict[str, Any]] = []
    for issue in issues_fixed:
        key = (issue["source"], issue["id"])
        if key not in seen:
            seen.add(key)
            unique_issues.append(issue)
    if len(unique_issues) < len(issues_fixed):
        logger.info(
            "Removed duplicate issues.fixed: %d -> %d entries",
            len(issues_fixed),
            len(unique_issues),
        )
    data["releaseNotes"]["issues"]["fixed"] = unique_issues

    release_cves = {cve["key"] for cve in release_notes.get("cves", [])}
    errors: list[str] = []

    for issue in unique_issues:
        server = jira_helper.normalize_issue_server(issue["source"])
        issue_id = issue["id"]

        # Currently only handle redhat.atlassian.net
        if server != jira_helper.SUPPORTED_JIRA_SERVER:
            logger.info("Skipping non-JIRA issue: %s from %s", issue_id, server)
            continue

        url = f"https://{server}/rest/api/2/issue/{issue_id}"
        try:
            body = jira_helper.jira_get_json(session, url, auth)
        except requests.RequestException as exc:
            # we should not fail for issues that are not available
            logger.warning(
                "Could not fetch issue %s from %s. Skipping. %s",
                issue_id,
                server,
                exc,
            )
            continue

        issue_type = body.get("fields", {}).get("issuetype", {}).get("name")
        if issue_type != "Vulnerability":
            logger.info(
                "Issue %s is not a Vulnerability (type: %s). Skipping.",
                issue_id,
                issue_type,
            )
            continue

        cve_id = body.get("fields", {}).get(jira_helper.JIRA_CVE_CUSTOM_FIELD_ID)
        if not cve_id or cve_id == "null":
            logger.warning(
                "Issue %s is a Vulnerability but has no CVE ID. Skipping.",
                issue_id,
            )
            continue

        if cve_id not in release_cves:
            msg = (
                f"Issue {issue_id} lists 'CVE ID' {cve_id} but that "
                f"CVE is not present in the releaseNotes.cves section."
            )
            logger.error(msg)
            errors.append(msg)

    if errors:
        raise RuntimeError("Errors were found in the CVE validation:\n" + "\n".join(errors))


def populate_images(data: dict[str, Any], snapshot: dict[str, Any]) -> None:
    """Populate releaseNotes.content.images."""
    if data.get("github"):
        logger.info("Github release. Skipping image-specific release note generation.")
        return

    content_type = advisory_data.first_mapping_content_type(data)
    if content_type in ("binary", "disk-image"):
        logger.info(
            "Content type is %s. Skipping image-specific release note generation.",
            content_type,
        )
        return

    components = snapshot.get("components", [])
    content_images = (
        data.setdefault("releaseNotes", {}).setdefault("content", {}).setdefault("images", [])
    )

    for component in components:
        name = component["name"]
        canonical_name = component.get("canonicalName") or ""
        image = component["containerImage"]

        if not re.match(r"^[^:]+@sha256:[0-9a-f]+$", image):
            msg = f"Failed to extract sha256 tag from {image}. Exiting with failure"
            raise RuntimeError(msg)

        cves_dict = build_cves_for_component(data, name)
        has_cves = bool(cves_dict["cves"]["fixed"])

        arch_digests = get_image_architectures(image)

        # Timestamp tag from the OCI label applies to all repos
        timestamp_tag = get_timestamp_tag(component)

        for repo in component.get("repositories", []):
            delivery_repo = repo["rh-registry-repo"]
            tags = repo.get("tags", [])

            # Use timestamp tag if available, otherwise fall back to
            # regex matching against this repo's tags
            if timestamp_tag:
                unique_tag = timestamp_tag
            else:
                unique_tag = get_unique_tag_from_tags(tags)

            for arch_digest in arch_digests:
                arch = arch_digest["platform"]["architecture"]
                digest = arch_digest["digest"]

                # If canonicalName is present, use it.
                # Otherwise fallback to the last segment of the deliveryRepo
                purl_path = delivery_repo.rsplit("/", 1)[-1]
                if canonical_name:
                    purl_path = canonical_name

                purl = (
                    f"pkg:oci/{purl_path}"
                    f"@{digest.replace(':', '%3A')}"
                    f"?arch={arch}"
                    f"&repository_url={delivery_repo}"
                )
                if unique_tag:
                    purl += f"&tag={unique_tag}"

                entry: dict[str, Any] = {
                    "architecture": arch,
                    "containerImage": f"{delivery_repo}@{digest}",
                    "purl": purl,
                    "repository": delivery_repo,
                    "tags": tags,
                    "component": name,
                }
                if has_cves:
                    entry.update(cves_dict)
                content_images.append(entry)

    # Key is only created when entries exist remove if empty
    if not content_images:
        data["releaseNotes"]["content"].pop("images", None)


def populate_artifacts(data: dict[str, Any], snapshot: dict[str, Any]) -> None:
    """Populate releaseNotes.content.artifacts."""
    content_type = advisory_data.first_mapping_content_type(data)
    if content_type not in ("binary", "disk-image", "rpm"):
        logger.info(
            "Not binary or disk-image or rpm content. Skipping artifact-specific logic."
        )
        return

    components = snapshot.get("components", [])

    # For RPMs, at least one component must have rpmsToPublish
    if content_type == "rpm":
        has_rpms = any(component.get("rpmsToPublish") for component in components)
        if not has_rpms:
            msg = "No rpmsToPublish found in snapshot cannot generate RPM release notes."
            raise RuntimeError(msg)

    content_artifacts = (
        data.setdefault("releaseNotes", {})
        .setdefault("content", {})
        .setdefault("artifacts", [])
    )

    for component in components:
        name = component["name"]
        cves_dict = build_cves_for_component(data, name)
        has_cves = bool(cves_dict["cves"]["fixed"])

        if content_type == "binary":
            _populate_binary(data, name, cves_dict, has_cves, content_artifacts)
        elif content_type == "disk-image":
            _populate_disk_image(data, name, cves_dict, has_cves, content_artifacts)
        elif content_type == "rpm":
            _populate_rpm(
                data,
                component,
                name,
                cves_dict,
                has_cves,
                content_artifacts,
            )


def _populate_binary(
    data: dict[str, Any],
    name: str,
    cves_dict: dict[str, Any],
    has_cves: bool,
    content_artifacts: list[dict[str, Any]],
) -> None:
    """Append binary artifact entries."""
    for mapping_component in data.get("mapping", {}).get("components", []):
        if mapping_component.get("name") != name:
            continue
        # Binaries use .files, falling back to .staged.files
        files = mapping_component.get("files") or []
        if not files:
            files = mapping_component.get("staged", {}).get("files", [])
        for file_entry in files:
            # Will be filled in by a later task after signing
            entry: dict[str, Any] = {
                "architecture": file_entry["arch"],
                "os": file_entry["os"],
                "purl": "placeholder",
                "component": name,
            }
            if has_cves:
                entry.update(cves_dict)
            content_artifacts.append(entry)


def _populate_disk_image(
    data: dict[str, Any],
    name: str,
    cves_dict: dict[str, Any],
    has_cves: bool,
    content_artifacts: list[dict[str, Any]],
) -> None:
    """Append disk-image artifact entries."""
    marketplace = bool(data.get("mapping", {}).get("cloudMarketplacesSecret"))
    for mapping_component in data.get("mapping", {}).get("components", []):
        if mapping_component.get("name") != name:
            continue
        for file_entry in mapping_component.get("staged", {}).get("files", []):
            filename = file_entry["filename"]

            arch = "unknown"
            if "aarch64" in filename:
                arch = "aarch64"
            elif "x86_64" in filename:
                arch = "x86_64"

            if marketplace:
                # For marketplace releases, no checksum or download_url
                # since these images go to cloud marketplaces, not CDN
                version = mapping_component.get("staged", {}).get("version", "unknown")
                purl = f"pkg:generic/{name}@{version}"
            else:
                # For CDN releases, placeholder will be updated by create-advisory
                purl = "placeholder"

            entry: dict[str, Any] = {
                "architecture": arch,
                "os": "linux",
                "purl": purl,
                "component": name,
            }
            if has_cves:
                entry.update(cves_dict)
            content_artifacts.append(entry)


def _populate_rpm(
    data: dict[str, Any],
    component: dict[str, Any],
    name: str,
    cves_dict: dict[str, Any],
    has_cves: bool,
    content_artifacts: list[dict[str, Any]],
) -> None:
    """Append RPM artifact entries from rpmsToPublish in the snapshot."""
    rpms = component.get("rpmsToPublish", [])
    if not rpms:
        return

    # SBOMs/attestations are uploaded to the signed RPMs domain
    pulp_domain = (
        data.get("signOptions", {}).get("signedRpmsDomain")
        or data.get("pulp", {}).get("domain")
        or ""
    )
    signing_key = data.get("signOptions", {}).get("signKeyAlias", {}).get("key", "")

    for rpm in rpms:
        rpm_name = rpm["rpmname"]
        arch = rpm["arch"]
        version = rpm["version"]
        release = rpm["release"]
        # Distro lives in targetRepos when present
        # only used in the no-targetRepos fallback
        distro = rpm.get("distro", "")

        sbom_path = rpm.get("sbomPath", "")
        sbom_url = ""
        if sbom_path and pulp_domain:
            sbom_url = f"{PULP_CONTENT_BASE_URL}/{pulp_domain}/{sbom_path}"

        attestation_path = rpm.get("attestationPath", "")
        attestation_url = ""
        if attestation_path and pulp_domain:
            attestation_url = f"{PULP_CONTENT_BASE_URL}/{pulp_domain}/{attestation_path}"

        # Expand per-target repo to keep repository_id accurate
        # (noarch may target multiple repos)
        target_repos = rpm.get("targetRepos", [])
        if not target_repos:
            # If no targetRepos, fall back to best-effort purl without repository_id.
            purl = advisory_data.generate_purl_rpm(
                rpm_name, version, release, arch, distro, ""
            )
            logger.info("purl: %s", purl)
            entry: dict[str, Any] = {
                "architecture": arch,
                "os": "linux",
                "purl": purl,
                "component": name,
            }
            if signing_key:
                entry["signingKey"] = signing_key
            if has_cves:
                entry.update(cves_dict)
            content_artifacts.append(entry)
            continue

        for target_repo in target_repos:
            repo_id = target_repo["repository_id"]
            repo_name = target_repo.get("repository_name", "")
            repo_distro = target_repo.get("distro", "")
            arch_for_entry = "src" if repo_name == "source" else arch
            purl = advisory_data.generate_purl_rpm(
                rpm_name,
                version,
                release,
                arch_for_entry,
                repo_distro,
                repo_id,
            )
            logger.info("purl: %s", purl)
            entry = {
                "architecture": arch_for_entry,
                "os": "linux",
                "purl": purl,
                "component": name,
            }
            if arch_for_entry == "src" and sbom_url:
                logger.info("sbom: %s", sbom_url)
                entry["sbom"] = sbom_url
            if arch_for_entry == "src" and attestation_url:
                logger.info("attestation: %s", attestation_url)
                entry["attestation"] = attestation_url
            if signing_key:
                entry["signingKey"] = signing_key
            if has_cves:
                entry.update(cves_dict)
            content_artifacts.append(entry)


def populate_github(
    data: dict[str, Any],
    snapshot: dict[str, Any],
    github_release_version: str,
    github_release_url: str,
    binaries_dir: str,
) -> None:
    """Populate releaseNotes.content.artifacts for github releases."""
    if not data.get("github"):
        logger.info("Not a github release. Skipping github-specific release note generation.")
        return

    # GitHub releases are single-component snapshots
    name = snapshot["components"][0]["name"]
    cves_dict = build_cves_for_component(data, name)
    has_cves = bool(cves_dict["cves"]["fixed"])

    binaries_path = Path(binaries_dir) if binaries_dir else None
    if not binaries_path or not binaries_path.is_dir():
        msg = "Binaries directory does not exist."
        raise RuntimeError(msg)

    checksum_files = list(binaries_path.glob("*_SHA256SUMS"))
    if not checksum_files:
        msg = "No checksum file was provided."
        raise RuntimeError(msg)
    checksum_map = parse_checksum_file(checksum_files[0])

    # Parse owner/repo from the release URL
    match = re.search(r"https://github\.com/([^/]+/[^/]+)", github_release_url)
    if not match:
        msg = f"Could not parse owner/repo from URL: {github_release_url}"
        raise RuntimeError(msg)
    owner_repo = match.group(1)

    # GitHub release tags typically use v prefix (e.g., v1.7.2), but the
    # version extracted from filenames may not include it
    github_tag = github_release_version
    if not github_tag.startswith("v"):
        github_tag = f"v{github_release_version}"

    content_artifacts = (
        data.setdefault("releaseNotes", {})
        .setdefault("content", {})
        .setdefault("artifacts", [])
    )

    for mapping_component in data.get("mapping", {}).get("components", []):
        if mapping_component.get("name") != name:
            continue
        for file_entry in mapping_component.get("files", []):
            source = file_entry.get("source", "")
            filename = Path(source).name
            if filename.endswith("_manifest.json"):
                continue
            checksum = checksum_map.get(filename, "")
            download_url = (
                f"https://github.com/{owner_repo}/releases/"
                f"download/{github_tag}/{filename}"
            )
            purl = (
                f"pkg:generic/{name}@{github_release_version}"
                f"?checksum={checksum}"
                f"&download_url={download_url}"
            )
            entry: dict[str, Any] = {
                "architecture": file_entry["arch"],
                "os": file_entry["os"],
                "purl": purl,
                "component": name,
            }
            if has_cves:
                entry.update(cves_dict)
            content_artifacts.append(entry)


def update_type_and_references(data: dict[str, Any]) -> None:
    """Set type to RHSA and add per-CVE reference URLs."""
    release_notes = data.get("releaseNotes", {})
    content = release_notes.get("content", {})

    # Check key existence an empty [] still counts as content present
    if "images" not in content and "artifacts" not in content:
        return

    images = content.get("images")
    artifacts = content.get("artifacts")

    # Collect all CVE IDs across content items
    items = images if images else artifacts
    cve_ids: set[str] = set()
    for item in items:
        fixed = item.get("cves", {}).get("fixed", {})
        cve_ids.update(fixed.keys())

    references = release_notes.setdefault("references", [])

    if not cve_ids:
        return

    release_notes["type"] = "RHSA"

    references.append(CLASSIFICATION_URL)
    for cve_id in sorted(cve_ids):
        references.append(f"{CVE_REF_PREFIX}{cve_id}")

    # Remove duplicates and sort
    seen: set[str] = set()
    unique_references: list[str] = []
    for reference in references:
        if reference not in seen:
            seen.add(reference)
            unique_references.append(reference)
    release_notes["references"] = sorted(unique_references)


def run(
    *,
    data_file: Path,
    snapshot_file: Path,
    jira_secret_path: Path,
    binaries_dir: str,
    github_release_version: str,
    github_release_url: str,
) -> int:
    """Run all populate-release-notes steps and write the result."""
    data = file_helper.load_json_dict(data_file)
    snapshot = file_helper.load_json_dict(snapshot_file)

    email, token = jira_helper.read_jira_credentials(jira_secret_path)
    auth = HTTPBasicAuth(email, token)
    session = http_client.get_retry_session(
        total=5,
        connect=3,
        read=3,
        status=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    validate_cve_issues(data, session, auth)

    populate_images(data, snapshot)
    populate_artifacts(data, snapshot)
    populate_github(
        data,
        snapshot,
        github_release_version,
        github_release_url,
        binaries_dir,
    )
    update_type_and_references(data)

    output = json.dumps(data, indent=2)
    data_file.write_text(output + "\n", encoding="utf-8")
    logger.info("%s\n%s", data_file, output)
    return 0


def main() -> int:
    """Read environment variables and run the task."""
    data_dir = Path(tekton.require_env("DATA_DIR"))
    data_path = tekton.require_env("DATA_PATH")
    snapshot_path = tekton.require_env("SNAPSHOT_PATH")

    jira_secret_path = file_helper.path_from_env_variable("JIRA_SECRET_PATH", "/etc/secrets")
    binaries_dir_rel = os.environ.get("BINARIES_DIR", "")
    github_release_version = os.environ.get("GITHUB_RELEASE_VERSION", "")
    github_release_url = os.environ.get("GITHUB_RELEASE_URL", "")

    binaries_dir = ""
    if binaries_dir_rel:
        binaries_dir = str(data_dir / binaries_dir_rel)

    return run(
        data_file=data_dir / data_path,
        snapshot_file=data_dir / snapshot_path,
        jira_secret_path=jira_secret_path,
        binaries_dir=binaries_dir,
        github_release_version=github_release_version,
        github_release_url=github_release_url,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
