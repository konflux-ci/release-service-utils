#!/usr/bin/env python3
"""Download npm archives from OCI registries for publish-to-nrrc.

Tekton injects ``DATA_DIR``, ``CHARON_PARAM_FILE_PATH``, and optionally
``WORK_DIR`` (default ``/var/workdir/nrrc``; catalog sets ``/workdir/nrrc``) via env.
"""

from __future__ import annotations

from pathlib import Path

import charon_env
import file
import oras_utils
import subprocess_cmd
import tekton
from logger import logger


def prepare_repo(
    *,
    charon_param_file: Path,
    work_dir: Path,
) -> None:
    """Download OCI archives and collect gzip/tar files under ``shared/``."""
    env = charon_env.load_charon_env(charon_param_file)
    registries = charon_env.require_oci_registries(env)

    repo_dir = work_dir
    shared_repo = repo_dir / "shared"
    shared_repo.mkdir(parents=True, exist_ok=True)

    for registry in registries:
        logger.info("Downloading the npm archive from %s", registry)
        short_hash = charon_env.short_sha256_prefix(registry)
        subdir = repo_dir / short_hash
        subdir.mkdir(parents=True, exist_ok=True)

        oras_utils.oras_pull(registry, subdir)

        for found in subdir.rglob("*"):
            if not found.is_file():
                continue
            if not file.is_gzip_or_tar_archive(found, file_cmd=subprocess_cmd.run_cmd):
                continue
            move_to = shared_repo / f"{short_hash}_{found.name}"
            if move_to.exists():
                logger.warning("%s already exists, skipped", move_to)
                continue
            found.rename(move_to)


def main() -> int:
    """Read Tekton env vars and prepare npm archives for upload."""
    data_dir = Path(tekton.require_env("DATA_DIR"))
    work_dir = charon_env.nrrc_work_dir()
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
