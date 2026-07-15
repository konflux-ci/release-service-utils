"""Sign macOS binaries on a remote Mac host via SSH."""

from .sign_mac import (  # noqa: F401
    CONTENT_DIR,
    MAC_HOST_CREDS_MOUNT,
    MAC_SIGNING_CREDS_MOUNT,
    MAC_SSH_KEY_MOUNT,
    PROG,
    QUAY_SECRET_MOUNT,
    main,
    run,
)
