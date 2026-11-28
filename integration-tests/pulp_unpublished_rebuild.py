"""Exercise published and unpublished RPM digest conflicts against a real Pulp server."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from release_service_utils.helpers.pulp_client import (
    PulpClient,
    PulpDigestStatus,
    parse_pulp_config,
)

logger = logging.getLogger("pulp_unpublished_rebuild_test")


def _pulp(config: Path, domain: str, *args: str) -> dict[str, Any]:
    """Run one authenticated Pulp CLI command and return its final JSON object."""
    command = ["pulp", "--config", str(config), "--domain", domain, *args]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(f"Pulp command failed: {' '.join(args)}: {result.stderr.strip()}")
    for line in reversed(result.stdout.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _build_rpm(root: Path, name: str, variant: str) -> Path:
    """Build a valid RPM with a stable NEVRA and variant-specific bytes."""
    top = root / variant
    for directory in ("BUILD", "BUILDROOT", "RPMS", "SOURCES", "SPECS", "SRPMS"):
        (top / directory).mkdir(parents=True)
    spec = top / "SPECS" / "probe.spec"
    spec.write_text(
        f"""Name: {name}
Version: 1.0
Release: 1
Summary: Pulp retry integration probe
License: MIT
BuildArch: noarch

%description
Pulp retry integration probe

%install
mkdir -p %{{buildroot}}/usr/share/{name}
printf '%s\\n' '{variant}' > %{{buildroot}}/usr/share/{name}/variant

%files
/usr/share/{name}/variant
""",
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "rpmbuild",
            "--define",
            f"_topdir {top}",
            "--define",
            f"_tmppath {top}",
            "-bb",
            str(spec),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"rpmbuild failed: {result.stderr.strip()}")
    return top / "RPMS" / "noarch" / f"{name}-1.0-1.noarch.rpm"


def _digest(path: Path) -> str:
    """Return the SHA256 digest of a local RPM."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _create_repo(config: Path, domain: str, name: str, created: list[str]) -> None:
    """Create an isolated repo with an empty published baseline."""
    _pulp(
        config,
        domain,
        "rpm",
        "repository",
        "create",
        "--name",
        name,
        "--no-autopublish",
    )
    created.append(name)
    baseline = _pulp(
        config,
        domain,
        "rpm",
        "publication",
        "create",
        "--repository",
        name,
        "--version",
        "0",
    )
    publication = str(baseline["pulp_href"])
    _pulp(
        config,
        domain,
        "rpm",
        "distribution",
        "create",
        "--name",
        name,
        "--base-path",
        name,
        "--publication",
        publication,
    )


def _delete_repo(config: Path, domain: str, name: str) -> None:
    """Remove this test's distribution and repository."""
    for args in (
        ("rpm", "distribution", "destroy", "--name", name),
        ("rpm", "repository", "destroy", "--name", name),
    ):
        try:
            _pulp(config, domain, *args)
        except RuntimeError as exc:
            logger.warning("Cleanup failed for %s: %s", name, exc)


def _check(
    client: PulpClient,
    repo: str,
    name: str,
    expected_sha: str,
    expected: PulpDigestStatus,
) -> None:
    """Assert the digest status using the real Pulp API."""
    status = client.check_digest(
        repo,
        name,
        "0",
        "1.0",
        "1",
        "noarch",
        expected_sha,
        fallback_to_latest=True,
        allow_unpublished_rebuild=True,
    )
    if status is not expected:
        raise AssertionError(f"{repo}: expected {expected.value}, got {status.value}")


def run(config: Path, domain: str) -> None:
    """Verify an unpublished replacement succeeds and a published conflict fails."""
    suffix = uuid4().hex[:8]
    name = f"hum11606-rebuild-{suffix}"
    unpublished = f"{name}-unpublished"
    published = f"{name}-published"
    client = PulpClient.from_config(parse_pulp_config(config), domain)
    client.ensure_domain_exists()
    created: list[str] = []
    try:
        with tempfile.TemporaryDirectory(prefix="pulp-rebuild-test-") as directory:
            root = Path(directory)
            old_rpm = _build_rpm(root, name, "old")
            new_rpm = _build_rpm(root, name, "new")
            old_sha, new_sha = _digest(old_rpm), _digest(new_rpm)
            if old_sha == new_sha:
                raise AssertionError("RPM variants have the same digest")

            for repo in (unpublished, published):
                _create_repo(config, domain, repo, created)

            old_href = client.upload_rpm(old_rpm, "100MB", config)
            new_href = client.upload_rpm(new_rpm, "100MB", config)
            client.add_content(unpublished, [old_href], 300)
            _check(
                client,
                unpublished,
                name,
                new_sha,
                PulpDigestStatus.UNPUBLISHED_MISMATCH,
            )
            client.replace_unpublished_rpm(
                unpublished, name, "0", "1.0", "1", "noarch", new_sha, new_href, 300
            )
            _check(client, unpublished, name, new_sha, PulpDigestStatus.MATCH)
            logger.info("Unpublished conflicting RPM was replaced atomically")

            client.add_content(published, [old_href], 300)
            publication = _pulp(
                config,
                domain,
                "rpm",
                "publication",
                "create",
                "--repository",
                published,
            )
            _pulp(
                config,
                domain,
                "rpm",
                "distribution",
                "update",
                "--name",
                published,
                "--publication",
                str(publication["pulp_href"]),
            )
            _check(client, published, name, new_sha, PulpDigestStatus.MISMATCH)
            try:
                client.replace_unpublished_rpm(
                    published, name, "0", "1.0", "1", "noarch", new_sha, new_href, 300
                )
            except RuntimeError as exc:
                if "Published RPM has a different digest" not in str(exc):
                    raise
            else:
                raise AssertionError("Published conflicting RPM was unexpectedly replaced")
            logger.info("Published conflicting RPM was rejected")
    finally:
        for repo in reversed(created):
            _delete_repo(config, domain, repo)


def main() -> int:
    """Parse CLI arguments and run the isolated Pulp integration test."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path, help="Path to Pulp cli.toml")
    parser.add_argument("--domain", required=True, help="Pulp test domain")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    run(args.config, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
