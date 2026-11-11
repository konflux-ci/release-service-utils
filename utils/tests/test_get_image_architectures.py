"""Tests for the get-image-architectures utility script.

Covers the dual-compression deduplication: when an image index contains
both gzip and zstd:chunked manifests per architecture, the script must
return only the non-zstd entry per (architecture, os).
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "get-image-architectures"

ZSTD_ANNOTATION = {"io.github.containers.compression.zstd": "true"}


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _manifest(
    digest: str,
    arch: str,
    os_name: str = "linux",
    zstd: bool = False,
) -> dict:
    manifest = {
        "digest": digest,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "platform": {"architecture": arch, "os": os_name},
        "size": 429,
    }
    if zstd:
        manifest["annotations"] = ZSTD_ANNOTATION
    return manifest


def _index(manifests: list[dict]) -> dict:
    return {
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "schemaVersion": 2,
        "manifests": manifests,
    }


def _run_get_image_architectures(
    tmp_path: Path,
    raw_json: dict,
    config_json: dict | None = None,
) -> subprocess.CompletedProcess:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    raw_file = tmp_path / "raw.json"
    raw_file.write_text(json.dumps(raw_json), encoding="utf-8")
    config_file = tmp_path / "config.json"
    if config_json is not None:
        config_file.write_text(json.dumps(config_json), encoding="utf-8")

    # The script calls skopeo inspect with --raw first (returns the index or
    # manifest). For single-arch images it calls skopeo again without --raw
    # (returns the image config with Architecture/Os/Digest).
    _write_executable(
        bin_dir / "skopeo",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"--raw"* ]]; then
    cat "$MOCK_RAW_FILE"
else
    cat "${MOCK_CONFIG_FILE:-$MOCK_RAW_FILE}"
fi
""",
    )

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["MOCK_RAW_FILE"] = str(raw_file)
    if config_json is not None:
        env["MOCK_CONFIG_FILE"] = str(config_file)

    return subprocess.run(
        ["bash", str(SCRIPT_PATH), "quay.io/test/image:tag"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def _output_entries(result: subprocess.CompletedProcess) -> list[dict]:
    return [json.loads(line) for line in result.stdout.strip().splitlines()]


def test_deduplicates_dual_compression_index(tmp_path):
    """Dual-compression index: only the gzip entry per arch is returned."""
    index = _index(
        [
            _manifest("sha256:amd64gzip", "amd64"),
            _manifest("sha256:amd64zstd", "amd64", zstd=True),
            _manifest("sha256:arm64gzip", "arm64"),
            _manifest("sha256:arm64zstd", "arm64", zstd=True),
            _manifest("sha256:attestation", "unknown", "unknown"),
        ]
    )

    result = _run_get_image_architectures(tmp_path, index)

    assert result.returncode == 0, result.stderr
    digests = [entry["digest"] for entry in _output_entries(result)]
    assert digests == ["sha256:amd64gzip", "sha256:arm64gzip", "sha256:attestation"]


def test_keeps_first_zstd_entry_when_all_entries_are_zstd(tmp_path):
    """All-zstd platform: keep the first entry so the error surfaces downstream."""
    index = _index(
        [
            _manifest("sha256:firstzstd", "amd64", zstd=True),
            _manifest("sha256:secondzstd", "amd64", zstd=True),
        ]
    )

    result = _run_get_image_architectures(tmp_path, index)

    assert result.returncode == 0, result.stderr
    entries = _output_entries(result)
    assert len(entries) == 1
    assert entries[0]["digest"] == "sha256:firstzstd"


def test_regular_multiarch_index_is_unchanged(tmp_path):
    """Regular multi-arch index without zstd annotations: all entries returned."""
    index = _index(
        [
            _manifest("sha256:amd64digest", "amd64"),
            _manifest("sha256:arm64digest", "arm64"),
        ]
    )

    result = _run_get_image_architectures(tmp_path, index)

    assert result.returncode == 0, result.stderr
    entries = _output_entries(result)
    assert [entry["digest"] for entry in entries] == [
        "sha256:amd64digest",
        "sha256:arm64digest",
    ]
    assert all(entry["multiarch"] is True for entry in entries)


def test_single_arch_manifest(tmp_path):
    """Single-arch manifest (not an index): one entry with multiarch=false."""
    manifest = {
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "schemaVersion": 2,
    }
    config = {
        "Architecture": "amd64",
        "Os": "linux",
        "Digest": "sha256:singlearch",
    }

    result = _run_get_image_architectures(tmp_path, manifest, config)

    assert result.returncode == 0, result.stderr
    entries = _output_entries(result)
    assert entries == [
        {
            "platform": {"architecture": "amd64", "os": "linux"},
            "digest": "sha256:singlearch",
            "multiarch": False,
        }
    ]
