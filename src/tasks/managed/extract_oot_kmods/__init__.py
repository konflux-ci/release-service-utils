"""Extract out-of-tree kernel modules from a container image.

Detects the architectures present in the source container image,
copies each architecture-specific image via skopeo, scans the image
layers for ``.ko`` files under the configured kmods path, and extracts
them preserving directory structure.  An ``envfile`` adjacent to the
kmods directory is also extracted when present and used to determine
the final architecture directory name.
"""

from .extract_oot_kmods import (  # noqa: F401
    extract_single_arch,
    get_image_architectures,
    main,
    resolve_arch_name,
    run,
)
