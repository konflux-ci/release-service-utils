"""Wrapper for the ``get-image-architectures`` CLI utility.

``get-image-architectures`` inspects a container image (including OCI
artifacts) and reports one JSON object per architecture, handling the
distinct manifest media types (OCI artifact, single-arch OCI/Docker image,
Docker manifest list) that ``skopeo inspect`` can return. Rather than
duplicating that logic in Python, this helper shells out to the existing,
well-exercised utility and parses its newline-delimited JSON output.
"""

from __future__ import annotations

import json

from subprocess_cmd import run_cmd


def get_image_architectures(
    image_ref: str,
    *,
    retry_times: int | None = None,
) -> list[dict]:
    """Return architecture/platform/digest info for ``image_ref``.

    Each returned dict has at least ``platform`` (with ``architecture`` and
    ``os``) and ``digest`` keys; multi-arch images yield one dict per
    manifest, while single-arch images and OCI artifacts yield exactly one.

    Raises:
        RuntimeError: if the underlying command fails.

    """
    cmd = ["get-image-architectures"]
    if retry_times is not None:
        cmd += ["--skopeo-retries", str(retry_times)]
    cmd.append(image_ref)

    result = run_cmd(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"get-image-architectures failed for {image_ref}: {result.stderr.strip()}"
        )
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
