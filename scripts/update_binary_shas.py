#!/usr/bin/env python3
"""Update SHA-256 checksums for curl-downloaded binaries in the Dockerfile.

Usage:
    ./scripts/update_binary_shas.py <binary> <version>  # Update one binary
    ./scripts/update_binary_shas.py --refresh           # Refresh current versions
    ./scripts/update_binary_shas.py --all-latest        # Update all to latest

Examples:
    ./scripts/update_binary_shas.py yq 4.35.0
    ./scripts/update_binary_shas.py --refresh
    ./scripts/update_binary_shas.py --all-latest
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.request
import urllib.error
from dataclasses import dataclass
from pathlib import Path

# ANSI color codes
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
RED = "\033[0;31m"
NC = "\033[0m"  # No Color


def log_info(msg: str) -> None:
    """Log info message."""
    print(f"{GREEN}[INFO]{NC} {msg}")


def log_warn(msg: str) -> None:
    """Log warning message."""
    print(f"{YELLOW}[WARN]{NC} {msg}")


def log_error(msg: str) -> None:
    """Log error message."""
    print(f"{RED}[ERROR]{NC} {msg}", file=sys.stderr)


@dataclass
class BinaryMetadata:
    """Metadata for a binary that needs SHA verification."""

    name: str
    archs: list[str]
    url_template: str
    checksums_url: str | None
    latest_api: str
    latest_type: str = "github"  # github, gitlab, or raw

    def get_url(self, version: str, arch: str) -> str:
        """Get download URL for a specific version and arch."""
        return self.url_template.replace("${VERSION}", version).replace("${ARCH}", arch)

    def get_checksums_url(self, version: str, arch: str) -> str | None:
        """Get checksums URL for a specific version and arch."""
        if not self.checksums_url:
            return None
        return self.checksums_url.replace("${VERSION}", version).replace("${ARCH}", arch)


# Binary metadata configuration
BINARIES = {
    "yq": BinaryMetadata(
        name="yq",
        archs=["amd64", "arm64"],
        url_template="https://github.com/mikefarah/yq/releases/download/v${VERSION}/yq_linux_${ARCH}",
        checksums_url="https://github.com/mikefarah/yq/releases/download/v${VERSION}/checksums",
        latest_api="https://api.github.com/repos/mikefarah/yq/releases/latest",
    ),
    "kubectl": BinaryMetadata(
        name="kubectl",
        archs=["amd64", "arm64"],
        url_template="https://dl.k8s.io/release/v${VERSION}/bin/linux/${ARCH}/kubectl",
        checksums_url="https://dl.k8s.io/release/v${VERSION}/bin/linux/${ARCH}/kubectl.sha256",
        latest_api="https://dl.k8s.io/release/stable.txt",
        latest_type="raw",
    ),
    "opm": BinaryMetadata(
        name="opm",
        archs=["amd64", "arm64"],
        url_template="https://github.com/operator-framework/operator-registry/releases/download/${VERSION}/linux-${ARCH}-opm",
        checksums_url=None,  # No official checksums
        latest_api="https://api.github.com/repos/operator-framework/operator-registry/releases",
    ),
    "glab": BinaryMetadata(
        name="glab",
        archs=["amd64", "arm64"],
        url_template="https://gitlab.com/gitlab-org/cli/-/releases/v${VERSION}/downloads/glab_${VERSION}_linux_${ARCH}.tar.gz",
        checksums_url="https://gitlab.com/gitlab-org/cli/-/releases/v${VERSION}/downloads/checksums.txt",
        latest_api="https://gitlab.com/api/v4/projects/gitlab-org%2Fcli/releases",
        latest_type="gitlab",
    ),
    "gh": BinaryMetadata(
        name="gh",
        archs=["amd64", "arm64"],
        url_template="https://github.com/cli/cli/releases/download/v${VERSION}/gh_${VERSION}_linux_${ARCH}.tar.gz",
        checksums_url="https://github.com/cli/cli/releases/download/v${VERSION}/gh_${VERSION}_checksums.txt",
        latest_api="https://api.github.com/repos/cli/cli/releases/latest",
    ),
    "syft": BinaryMetadata(
        name="syft",
        archs=["amd64", "arm64"],
        url_template="https://github.com/anchore/syft/releases/download/v${VERSION}/syft_${VERSION}_linux_${ARCH}.tar.gz",
        checksums_url="https://github.com/anchore/syft/releases/download/v${VERSION}/syft_${VERSION}_checksums.txt",
        latest_api="https://api.github.com/repos/anchore/syft/releases/latest",
    ),
    "kubectl-ka": BinaryMetadata(
        name="kubectl-ka",
        archs=["amd64", "arm64"],
        url_template="https://github.com/kubearchive/kubearchive/releases/download/v${VERSION}/kubectl-ka-linux-${ARCH}",
        checksums_url=None,  # No official checksums published
        latest_api="https://api.github.com/repos/kubearchive/kubearchive/releases/latest",
    ),
    "cosign3": BinaryMetadata(
        name="cosign3",
        archs=["amd64", "arm64", "ppc64le", "s390x"],
        url_template="https://github.com/sigstore/cosign/releases/download/v${VERSION}/cosign-linux-${ARCH}",
        checksums_url="https://github.com/sigstore/cosign/releases/download/v${VERSION}/cosign_checksums.txt",
        latest_api="https://api.github.com/repos/sigstore/cosign/releases/latest",
    ),
    "uv": BinaryMetadata(
        name="uv",
        archs=["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"],
        url_template="https://github.com/astral-sh/uv/releases/download/${VERSION}/uv-${ARCH}.tar.gz",
        checksums_url="https://github.com/astral-sh/uv/releases/download/${VERSION}/uv-${ARCH}.tar.gz.sha256",
        latest_api="https://api.github.com/repos/astral-sh/uv/releases/latest",
    ),
    "exodus-rsync": BinaryMetadata(
        name="exodus-rsync",
        archs=["amd64"],
        url_template="https://github.com/release-engineering/exodus-rsync/releases/download/${VERSION}/exodus-rsync",
        checksums_url=None,  # No official checksums published
        latest_api="https://api.github.com/repos/release-engineering/exodus-rsync/releases/latest",
    ),
}


def get_current_version(binary: str, dockerfile_path: Path) -> str | None:
    """Extract current version of a binary from Dockerfile."""
    # Map binary name to Dockerfile ARG name (most use uppercase with _ for -)
    arg_name_map = {
        "kubectl-ka": "KUBEARCHIVE_VERSION",
    }

    var_name = arg_name_map.get(binary, binary.upper().replace("-", "_") + "_VERSION")
    pattern = rf"^ARG {var_name}=(.+)$"

    content = dockerfile_path.read_text()
    match = re.search(pattern, content, re.MULTILINE)
    if match:
        return match.group(1)
    return None


def get_latest_version(meta: BinaryMetadata) -> str | None:
    """Fetch latest version from the binary's release API."""
    # Some binaries keep the 'v' prefix in their VERSION ARG
    keep_v_prefix = meta.name in ["opm", "exodus-rsync"]

    try:
        with urllib.request.urlopen(meta.latest_api, timeout=10) as resp:
            content = resp.read().decode("utf-8")

            version = None
            if meta.latest_type == "raw":
                # kubectl: plain text file with version
                version = content.strip()
            elif meta.latest_type == "gitlab":
                # GitLab: array of releases, take first
                data = json.loads(content)
                version = data[0]["tag_name"]
            else:
                # GitHub: JSON with tag_name
                # Filter out pre-releases
                data = json.loads(content)
                if isinstance(data, dict):
                    # Single release object - check if it's a pre-release
                    if data.get("prerelease", False):
                        log_warn(f"{meta.name}: Latest release is a pre-release, skipping")
                        return None
                    version = data["tag_name"]
                else:
                    # Array of releases - find first stable release
                    for release in data:
                        if not release.get("prerelease", False):
                            version = release["tag_name"]
                            break

                    if not version:
                        log_warn(f"{meta.name}: No stable releases found")
                        return None

            # Strip 'v' prefix unless this binary needs to keep it
            if not keep_v_prefix:
                version = version.lstrip("v")

            return version
    except Exception as e:
        log_error(f"Failed to fetch latest version for {meta.name}: {e}")
        return None


def download_and_hash(url: str) -> str | None:
    """Download a file and return its SHA-256 hash."""
    try:
        log_info(f"  Downloading: {url}")
        with urllib.request.urlopen(url, timeout=30) as resp:
            content = resp.read()
            sha256 = hashlib.sha256(content).hexdigest()
            return sha256
    except Exception as e:
        log_error(f"Failed to download {url}: {e}")
        return None


def verify_against_official(
    meta: BinaryMetadata, version: str, arch: str, computed_sha: str
) -> bool:
    """Verify computed SHA against official checksums."""
    checksums_url = meta.get_checksums_url(version, arch)
    if not checksums_url:
        log_warn(
            f"No official checksums available for {meta.name}, "
            f"using locally computed SHA-256"
        )
        return True

    try:
        log_info(f"  Verifying against: {checksums_url}")
        with urllib.request.urlopen(checksums_url, timeout=10) as resp:
            checksums_content = resp.read().decode("utf-8")

        # Extract official SHA based on binary-specific patterns
        official_sha = extract_official_sha(meta.name, version, arch, checksums_content)

        if not official_sha:
            log_warn(f"Could not extract official checksum for {meta.name} {arch}")
            return True

        if computed_sha == official_sha:
            log_info("  ✓ Checksum verified against official source")
            return True
        else:
            log_error("  ✗ Checksum mismatch!")
            log_error(f"    Computed:  {computed_sha}")
            log_error(f"    Official:  {official_sha}")
            return False
    except Exception as e:
        log_warn(f"Failed to verify checksums: {e}")
        return True  # Don't fail if verification unavailable


def extract_official_sha(
    binary_name: str, version: str, arch: str, checksums_content: str
) -> str | None:
    """Extract the official SHA from checksums file based on binary-specific patterns."""
    lines = checksums_content.strip().split("\n")

    if binary_name == "yq":
        # yq uses a special format with 31 hash types per line
        # SHA-256 is at position 18 in checksums_hashes_order, which is field 19 (field 1 is filename)
        pattern = f"yq_linux_{arch} "
        for line in lines:
            if line.startswith(pattern):
                parts = line.split()
                if len(parts) >= 19:
                    return parts[18]  # Field 19 (0-indexed as 18)
        return None
    elif binary_name == "kubectl":
        # kubectl publishes single-file .sha256 with just the hash
        return checksums_content.strip()
    elif binary_name == "glab":
        pattern = f"glab_{version}_linux_{arch}.tar.gz"
    elif binary_name == "gh":
        pattern = f"gh_{version}_linux_{arch}.tar.gz"
    elif binary_name == "syft":
        pattern = f"syft_{version}_linux_{arch}.tar.gz"
    elif binary_name == "kubectl-ka":
        pattern = f"kubectl-ka-linux-{arch}"
    elif binary_name == "cosign3":
        pattern = f"cosign-linux-{arch}"
    elif binary_name == "uv":
        # uv publishes single .sha256 file per artifact with format: <sha>  <filename>
        return checksums_content.strip().split()[0]
    elif binary_name == "exodus-rsync":
        # exodus-rsync publishes .sha256 file with format: <sha>  <filename>
        parts = checksums_content.strip().split()
        return parts[0] if parts else checksums_content.strip()
    else:
        return None

    # Search for pattern in checksums file (format: <sha> <filename>)
    for line in lines:
        if pattern in line:
            parts = line.split()
            if len(parts) >= 2:
                return parts[0]

    return None


def update_dockerfile(
    binary: str, version: str, shas: dict[str, str], dockerfile_path: Path
) -> bool:
    """Update Dockerfile with new version and SHA-256 checksums."""
    # Map binary name to Dockerfile ARG name
    arg_name_map = {
        "kubectl-ka": "KUBEARCHIVE_VERSION",
    }

    var_name = arg_name_map.get(binary, binary.upper().replace("-", "_") + "_VERSION")

    # Read current Dockerfile
    lines = dockerfile_path.read_text().splitlines(keepends=True)

    # Update VERSION ARG
    updated = False
    for i, line in enumerate(lines):
        if line.startswith(f"ARG {var_name}="):
            old_version = line.split("=", 1)[1].strip()
            lines[i] = f"ARG {var_name}={version}\n"
            log_info(f"Updated {var_name}: {old_version} → {version}")
            updated = True
            break

    if not updated:
        log_warn(f"Could not find ARG {var_name} in Dockerfile")

    # Map binary name to SHA variable name used in Dockerfile
    sha_var_map = {
        "yq": "YQ_SHA256",
        "kubectl": "KUBECTL_SHA256",
        "opm": "OPM_SHA256",
        "glab": "GLAB_SHA256",
        "gh": "GH_SHA256",
        "syft": "SYFT_SHA256",
        "kubectl-ka": "KUBEARCHIVE_SHA256",
        "cosign3": "COSIGN3_SHA256",
        "uv": "UV_SHA256",
        "exodus-rsync": None,  # exodus-rsync uses inline echo, not variables
    }

    sha_var = sha_var_map.get(binary)
    if not sha_var:
        if binary == "exodus-rsync":
            # Special case: exodus-rsync has SHA inline in echo command
            for i, line in enumerate(lines):
                if 'echo "' in line and "/usr/local/bin/rsync" in line and "sha256sum" in line:
                    # Line format: echo "SHA  /usr/local/bin/rsync" | sha256sum -c - && \
                    old_sha = line.split('"')[1].split()[0]
                    new_sha = shas["amd64"]
                    lines[i] = line.replace(old_sha, new_sha)
                    log_info(f"Updated exodus-rsync SHA-256: {old_sha} → {new_sha}")
                    break
        dockerfile_path.write_text("".join(lines))
        return True

    # Update SHA variables in if/elif blocks
    # Map architecture names to their Dockerfile condition patterns
    arch_conditions = {
        "amd64": [
            'if [ "$GO_ARCH" = "amd64" ]',
            'if [ "$ARCH" = "x86_64" ]',
            'if [ "$ARCH" = "x86_64" ] || [ "$ARCH" = "amd64" ]',
        ],
        "arm64": [
            'elif [ "$GO_ARCH" = "arm64" ]',
            'elif [ "$ARCH" = "aarch64" ]',
            'elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]',
        ],
        "ppc64le": [
            'elif [ "$ARCH" = "ppc64le" ]',
        ],
        "s390x": [
            'elif [ "$ARCH" = "s390x" ]',
        ],
        "x86_64-unknown-linux-gnu": [
            'if [ "$ARCH" = "x86_64" ]',
        ],
        "aarch64-unknown-linux-gnu": [
            'elif [ "$ARCH" = "aarch64" ]',
        ],
    }

    for arch, sha in shas.items():
        conditions = arch_conditions.get(arch, [])
        if not conditions:
            log_warn(f"No condition mapping for architecture: {arch}")
            continue

        # Find and update the SHA assignment for this architecture
        in_correct_block = False
        for i, line in enumerate(lines):
            # Check if we're entering the right conditional block
            for cond in conditions:
                if cond in line:
                    in_correct_block = True
                    break

            # Check if we're leaving the block
            if in_correct_block and ("elif [" in line or "else" in line or "fi &&" in line):
                # Check if this is a different elif (not our target condition)
                is_our_condition = any(cond in line for cond in conditions)
                if not is_our_condition:
                    in_correct_block = False

            # Update SHA if we're in the right block
            if in_correct_block and f"{sha_var}=" in line:
                # Extract old SHA and replace it
                match = re.search(rf"{sha_var}=([a-f0-9]+);", line)
                if match:
                    old_sha = match.group(1)
                    if old_sha != sha:
                        lines[i] = line.replace(old_sha, sha)
                        log_info(f"Updated {sha_var} for {arch}")
                    in_correct_block = False  # Found and updated, move on
                    break

    # Write updated Dockerfile
    dockerfile_path.write_text("".join(lines))
    log_info(f"Dockerfile updated successfully")

    return True


def update_binary(
    meta: BinaryMetadata, version: str, dockerfile_path: Path
) -> dict[str, str] | None:
    """Update a binary to the specified version and return SHA mapping."""
    log_info(f"Updating {meta.name} to version {version}")

    shas: dict[str, str] = {}

    for arch in meta.archs:
        url = meta.get_url(version, arch)
        sha = download_and_hash(url)

        if not sha:
            log_error(f"Failed to download {meta.name} for {arch}")
            return None

        if not verify_against_official(meta, version, arch, sha):
            log_error(f"Verification failed for {meta.name} {arch}")
            return None

        shas[arch] = sha

    return shas


def display_updates(
    binary: str, version: str, shas: dict[str, str], applied: bool = False
) -> None:
    """Display the updates made to the Dockerfile."""
    var_name = binary.upper().replace("-", "_") + "_VERSION"

    print("\n" + "=" * 60)
    print(f"{'Applied updates' if applied else 'Updates'} for {binary}")
    print("=" * 60)
    print(f"ARG {var_name}={version}")
    print()
    for arch, sha in shas.items():
        print(f"{arch:30s}: {sha}")
    print("=" * 60)
    print()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update SHA-256 checksums for Dockerfile binaries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "binary",
        nargs="?",
        choices=list(BINARIES.keys()) + ["--refresh", "--all-latest"],
        help="Binary to update (or --refresh/--all-latest)",
    )
    parser.add_argument("version", nargs="?", help="Version to update to")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh SHAs for all binaries at current versions",
    )
    parser.add_argument(
        "--all-latest",
        action="store_true",
        help="Update all binaries to latest versions",
    )

    args = parser.parse_args()

    # Find Dockerfile
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    dockerfile = repo_root / "Dockerfile"

    if not dockerfile.exists():
        log_error(f"Dockerfile not found at {dockerfile}")
        return 1

    # Handle different modes
    if args.refresh or args.binary == "--refresh":
        # Refresh all at current versions
        for binary_name, meta in BINARIES.items():
            current = get_current_version(binary_name, dockerfile)
            if not current:
                log_error(f"Could not find current version for {binary_name}")
                continue

            log_info(f"Refreshing {binary_name} at current version {current}")
            shas = update_binary(meta, current, dockerfile)
            if shas:
                update_dockerfile(binary_name, current, shas, dockerfile)
                display_updates(binary_name, current, shas, applied=True)

    elif args.all_latest or args.binary == "--all-latest":
        # Update all to latest
        for binary_name, meta in BINARIES.items():
            log_info(f"Fetching latest version for {binary_name}...")
            latest = get_latest_version(meta)
            if not latest:
                log_error(f"Could not fetch latest version for {binary_name}")
                continue

            log_info(f"Latest {binary_name}: {latest}")
            shas = update_binary(meta, latest, dockerfile)
            if shas:
                update_dockerfile(binary_name, latest, shas, dockerfile)
                display_updates(binary_name, latest, shas, applied=True)

    elif args.binary and args.version:
        # Update single binary to specific version
        if args.binary not in BINARIES:
            log_error(f"Unknown binary: {args.binary}")
            log_error(f"Supported: {', '.join(BINARIES.keys())}")
            return 1

        meta = BINARIES[args.binary]
        shas = update_binary(meta, args.version, dockerfile)
        if not shas:
            return 1

        update_dockerfile(args.binary, args.version, shas, dockerfile)
        display_updates(args.binary, args.version, shas, applied=True)

    else:
        parser.print_help()
        return 1

    log_info("Done! Dockerfile has been updated.")
    log_info("Review the changes with: git diff Dockerfile")
    log_info("Then test the build locally with: docker build -f Dockerfile .")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
