"""Generate and GPG-sign a merged sha256sum.txt for all component archives."""

from .generate_checksums import (  # noqa: F401
    CHECKSUM_CREDENTIALS_MOUNT,
    CONTENT_DIR,
    PROG,
    SHARED_DIR,
    main,
    run,
)
