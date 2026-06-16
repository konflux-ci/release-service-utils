#!/usr/bin/env python3
"""Base64-encode SHA256 checksum files for signing."""

from __future__ import annotations

import base64
from pathlib import Path

import tekton
from logger import logger

PROG = "base64_encode_checksum.py"


def encode_checksums(binaries_dir: Path) -> str:
    """Concatenate all *SHA256SUMS files in *binaries_dir* and base64-encode.

    Files are read in sorted order for deterministic output. Raises
    ``FileNotFoundError`` when *binaries_dir* does not exist or contains no
    matching files.
    """
    if not binaries_dir.is_dir():
        raise FileNotFoundError(f"Binaries directory does not exist: {binaries_dir}")

    files = sorted(binaries_dir.glob("*SHA256SUMS"))
    if not files:
        raise FileNotFoundError(f"No *SHA256SUMS files found in {binaries_dir}")

    data = b"".join(f.read_bytes() for f in files)
    return base64.b64encode(data).decode("ascii")


def main() -> int:
    """Encode checksums and write the blob to the Tekton result file."""
    (rpath,) = tekton.result_paths_from_env("RESULT_BLOB")

    data_dir = tekton.require_env("DATA_DIR")
    binaries_dir_name = tekton.require_env("BINARIES_DIR")

    binaries_dir = Path(data_dir) / binaries_dir_name

    try:
        blob = encode_checksums(binaries_dir)
    except Exception as e:
        logger.error("%s: %s", PROG, e)
        return 1

    logger.info("%s", blob)
    rpath.write_text(blob, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
