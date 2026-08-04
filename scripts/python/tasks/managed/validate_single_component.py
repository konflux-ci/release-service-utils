#!/usr/bin/env python3
"""Validate that a Snapshot contains at most one component."""

from __future__ import annotations

from pathlib import Path

import file
import tekton
from logger import logger


def run(*, data_dir: Path, snapshot_path: Path) -> None:
    """Load the snapshot under *data_dir* and enforce the single-component rule."""
    snapshot_file = data_dir / snapshot_path
    logger.info("Loading snapshot from %s", snapshot_file)
    snapshot = file.load_json_dict(snapshot_file)

    components = snapshot.get("components", [])
    if not isinstance(components, list):
        msg = "snapshot.components must be a JSON array"
        raise TypeError(msg)

    count = len(components)
    if count > 1:
        msg = f"found {count} components, only one component per application is supported."
        raise ValueError(msg)
    logger.info("Snapshot has %d component(s); single-component check passed", count)


def main() -> int:
    """Read Tekton env vars and run the single-component validation."""
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        snapshot_path=Path(tekton.require_env("PARAM_SNAPSHOT_PATH")),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
