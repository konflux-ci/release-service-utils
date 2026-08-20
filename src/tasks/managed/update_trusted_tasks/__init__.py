"""Update trusted-tasks list OCI artifact with released task bundles."""

from . import update_trusted_tasks  # noqa: F401
from .update_trusted_tasks import (  # noqa: F401
    DEFAULT_DATA_DIR,
    check_latest_exists,
    derive_acceptable_bundles_repo,
    main,
    run,
)
