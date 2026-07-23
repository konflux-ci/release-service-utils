#!/usr/bin/env python3
"""Download maven repo zips from OCI registries for publish-to-mrrc.

Tekton injects ``DATA_DIR``, ``CHARON_PARAM_FILE_PATH`` and optionally
``WORK_DIR`` which defaults to ``/var/workdir/mrrc`` via env.
"""

from __future__ import annotations

from pathlib import Path

import charon_env
import file
import oras_utils
import tekton
from logger import logger


def prepare_repo(
    *,
    charon_param_file: Path,
    work_dir: Path,
) -> None:
    """Pull maven repo zips from each OCI registry into a short-hash subdir."""
    env = charon_env.load_charon_env(charon_param_file)
    registries = charon_env.require_oci_registries(env)

    work_dir.mkdir(parents=True, exist_ok=True)

    for registry in registries:
        logger.info("Downloading the maven repo zip %s", registry)
        short_hash = charon_env.short_sha256_prefix(registry)
        subdir = work_dir / short_hash
        subdir.mkdir(parents=True, exist_ok=True)
        oras_utils.oras_pull(registry, subdir)


def main() -> int:
    """Parse env vars and pull maven repo zips."""
    data_dir = Path(tekton.require_env("DATA_DIR"))
    work_dir = charon_env.mrrc_work_dir()
    charon_param_file = file.resolve_path_under_base(
        data_dir,
        tekton.require_env("CHARON_PARAM_FILE_PATH"),
    )
    prepare_repo(
        charon_param_file=charon_param_file,
        work_dir=work_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
