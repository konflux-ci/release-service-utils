"""Upload signed out-of-tree kernel modules to an S3-compatible bucket."""

from .push_oot_kmods_to_s3 import (  # noqa: F401
    main,
    run,
)
