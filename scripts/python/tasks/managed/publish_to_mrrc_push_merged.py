#!/usr/bin/env python3
"""Push merged maven zip to OCI registry for publish-to-mrrc.

Tekton injects ``IMAGE``, ``IMAGE_EXPIRES_AFTER`` which is optional and
``WORK_DIR`` which defaults to ``/var/workdir/mrrc`` via env.  Result paths
come from ``RESULT_IMAGE_DIGEST`` and ``RESULT_IMAGE_TAG``.
"""

from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import charon_env
import file
import oras_utils
import tekton
from logger import logger


def generate_tag() -> str:
    """Build a tag from the current UTC time and a uuid4."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{uuid.uuid4()}"


def push_merged_maven_repo(
    *,
    work_dir: Path,
    image: str,
    image_expires_after: str,
    result_image_digest: Path,
    result_image_tag: Path,
) -> None:
    """Push merged.zip to the registry and write digest and tag to results."""
    merge_dir = work_dir / "merged"
    merged_zip = merge_dir / "merged.zip"

    if not merged_zip.is_file():
        logger.info("No merged maven zip found, skipping push")
        return

    tag = generate_tag()
    tagged_ref = f"{image}:{tag}"

    auth_out = subprocess.check_output(
        ["select-oci-auth", image],
        stderr=subprocess.STDOUT,
        text=True,
    )
    auth_file = file.make_tempfile_path("oras-auth-", auth_out.encode("utf-8"))

    cmd: list[str] = [
        "oras",
        "push",
        "--registry-config",
        str(auth_file),
        "--artifact-type",
        "application/vnd.maven+zip",
    ]
    if image_expires_after:
        logger.info("Setting image expiration to %s", image_expires_after)
        cmd.extend(["--annotation", f"quay.expires-after={image_expires_after}"])
    cmd.extend([tagged_ref, "merged.zip"])

    logger.info("Pushing image %s to registry", tagged_ref)
    subprocess.check_output(cmd, cwd=str(merge_dir), stderr=subprocess.STDOUT, text=True)

    digest = oras_utils.oras_resolve(tagged_ref)
    result_image_digest.write_text(digest, encoding="utf-8")
    result_image_tag.write_text(tag, encoding="utf-8")
    logger.info("Push merged zip %s@%s successfully", tagged_ref, digest)


def main() -> int:
    """Parse env vars and push the merged maven zip."""
    image = tekton.require_env("IMAGE")
    image_expires_after = os.environ.get("IMAGE_EXPIRES_AFTER", "").strip()
    work_dir = charon_env.mrrc_work_dir()
    result_digest, result_tag = tekton.result_paths_from_env(
        "RESULT_IMAGE_DIGEST",
        "RESULT_IMAGE_TAG",
    )
    push_merged_maven_repo(
        work_dir=work_dir,
        image=image,
        image_expires_after=image_expires_after,
        result_image_digest=result_digest,
        result_image_tag=result_tag,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
