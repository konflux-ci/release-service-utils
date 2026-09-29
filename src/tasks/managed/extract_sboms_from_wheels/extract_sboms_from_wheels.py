#!/usr/bin/env python3
"""Extract Red Hat SBOMs from Python wheels.

Walks ``*.whl`` files under a data directory, pulls ``redhat.spdx.json`` from
each wheel's ``.dist-info/sboms/`` path, and writes it into ``<data_dir>/sboms``
for a later Atlas/TPA upload step. Other SBOM files in the wheel are ignored:
only the Red Hat-generated SBOM is known to pass Atlas validation.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from release_service_utils.helpers import file, tekton
from release_service_utils.helpers.logger import logger

_SBOM_MEMBER_SUFFIX = ".dist-info/sboms/redhat.spdx.json"
SBOMS_SUBDIR = "sboms"


def _is_sbom_zip_member(name: str) -> bool:
    """Return True if *name* is ``redhat.spdx.json`` under ``.dist-info/sboms/``."""
    return name.endswith(_SBOM_MEMBER_SUFFIX)


def extract_sboms_from_wheel(wheel: Path, sboms_dir: Path) -> int:
    """Extract ``redhat.spdx.json`` from *wheel* into *sboms_dir*.

    Returns the number of SBOM files written. Wheels with no matching SBOM
    are skipped (count ``0``) after a log message.
    """
    wheel_name = wheel.name.removesuffix(".whl")
    with zipfile.ZipFile(wheel) as zf:
        members = [n for n in zf.namelist() if _is_sbom_zip_member(n)]
        if not members:
            logger.info("No SBOMs found in %s", wheel_name)
            return 0

        count = 0
        for sbom_path in members:
            output_name = f"{wheel_name}-{Path(sbom_path).name}"
            output_path = sboms_dir / output_name
            with zf.open(sbom_path) as src, output_path.open("wb") as dest:
                dest.write(src.read())
            logger.info("Extracted %s -> %s", sbom_path, output_name)
            count += 1
        return count


def run(data_dir: Path, files_dir: str) -> int:
    """Extract ``redhat.spdx.json`` from every wheel under *data_dir* / *files_dir*.

    Writes extracted files to ``data_dir / sboms``. Raises ``ValueError`` if
    *files_dir* is not relative to *data_dir*, and ``RuntimeError`` when no
    matching SBOM is found in any wheel.
    """
    wheels_dir = file.resolve_path_under_base(data_dir, files_dir)
    sboms_dir = data_dir / SBOMS_SUBDIR
    sboms_dir.mkdir(parents=True, exist_ok=True)

    found = 0
    for wheel in sorted(wheels_dir.glob("*.whl")):
        if not wheel.is_file():
            continue
        found += extract_sboms_from_wheel(wheel, sboms_dir)

    if found == 0:
        raise RuntimeError("No SBOMs found in any wheel")

    logger.info("Extracted %d SBOM(s)", found)
    for path in sorted(sboms_dir.iterdir()):
        logger.info("  %s", path.name)
    return found


def main() -> int:
    """Read environment variables and extract SBOMs from wheels."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    files_dir = tekton.require_env("PARAM_FILES_DIR")
    run(data_dir=data_dir, files_dir=files_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
