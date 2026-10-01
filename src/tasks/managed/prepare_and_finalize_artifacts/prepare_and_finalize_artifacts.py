#!/usr/bin/env python3
"""Select the input trusted artifact for prepare-and-finalize-artifacts."""

from __future__ import annotations

import os

from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

_BOTH_EMPTY_MSG = (
    "Both rpmDataArtifact and baseDataArtifact are empty. At least one must be provided."
)


def select_artifact(rpm_data_artifact: str, base_data_artifact: str) -> str:
    """Return the rpm artifact if set, otherwise the base artifact.

    Raises:
        ValueError: When both artifacts are empty.

    """
    if rpm_data_artifact:
        return rpm_data_artifact
    if base_data_artifact:
        return base_data_artifact
    raise ValueError(_BOTH_EMPTY_MSG)


def main() -> int:
    """Read Tekton env vars, select an artifact, and write the result."""
    rpm_data_artifact = os.environ.get("PARAM_RPM_DATA_ARTIFACT", "").strip()
    base_data_artifact = os.environ.get("PARAM_BASE_DATA_ARTIFACT", "").strip()
    (result_path,) = tekton.result_paths_from_env("RESULT_SELECTED_ARTIFACT")

    selected = select_artifact(rpm_data_artifact, base_data_artifact)
    logger.info("Selected artifact: %s", selected)
    result_path.write_text(selected, encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
