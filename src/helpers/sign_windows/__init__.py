"""Sign Windows binaries on a remote Windows host via SSH."""

from .sign_windows import (  # noqa: F401
    CONTENT_DIR,
    PROG,
    QUAY_SECRET_MOUNT,
    WINDOWS_CREDS_MOUNT,
    WINDOWS_SSH_KEY_MOUNT,
    main,
    run,
)
