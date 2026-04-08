#!/usr/bin/env python3
"""Functions used for orchestrating catalog e2e."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


def require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"ERROR: {name} is required", file=sys.stderr)
        sys.exit(1)
    return v


# Any registry/repo path ending in /release-service-utils with :tag or @digest.
_UTILS_IMAGE_REF = re.compile(
    r"(?:[\w.-]+/)+release-service-utils(?::[^\s\n\"'#]+|@[^\s\n\"'#]+)"
)
_MULTILINE_UTILS_REF = re.compile(
    r"(image:\s*\n\s*)(?:[\w.-]+/)+release-service-utils(?::[^\s\n\"'#]+|@[^\s\n\"'#]+)",
    re.MULTILINE,
)


def patch_catalog_utils_image_refs(root: Path, utils_image: str) -> int:
    """Replace release-service-utils container image refs under ``root``.

    Skips ``tasks/**/tests/*.yaml`` (Tekton unit-test fixtures). Returns the
    number of YAML files modified.
    """
    root = root.resolve()
    tasks_root = root / "tasks"

    def _under_task_tests(p: Path) -> bool:
        try:
            rel = p.relative_to(tasks_root)
        except ValueError:
            return False
        return "tests" in rel.parts

    modified = 0
    for path in root.rglob("*.yaml"):
        if tasks_root.is_dir() and _under_task_tests(path):
            continue
        text = path.read_text(encoding="utf-8")
        new = _UTILS_IMAGE_REF.sub(utils_image, text)
        new = _MULTILINE_UTILS_REF.sub(r"\1" + utils_image, new)
        if new != text:
            path.write_text(new, encoding="utf-8")
            modified += 1
    return modified


if __name__ == "__main__":
    utils_image = require_env("UTILS_IMAGE")
    n = patch_catalog_utils_image_refs(Path.cwd(), utils_image)
    if n == 0:
        print(
            "ERROR: No YAML changes after patching release-service-utils image refs.",
            file=sys.stderr,
        )
        print(
            "       Check that catalog tasks still reference "
            "quay.io/konflux-ci/release-service-utils@",
            file=sys.stderr,
        )
        sys.exit(1)
