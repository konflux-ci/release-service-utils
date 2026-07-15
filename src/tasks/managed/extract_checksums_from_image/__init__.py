"""Extract SHA256SUMS checksum files from container images for GitHub releases.

Processes container images listed in a snapshot specification, extracts
binaries to a temporary directory, retains only checksum files (``*SHA256SUMS``)
in the output, and writes the relative output path to a Tekton result file.
"""

from . import extract_checksums_from_image  # noqa: F401
from .extract_checksums_from_image import (  # noqa: F401
    BINARIES_DIR,
    copy_to_binaries,
    extract_binaries_from_layers,
    extract_checksums,
    load_components,
    load_snapshot,
    main,
    remove_non_checksum_files,
)
