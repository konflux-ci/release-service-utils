"""Filter already-released images from a snapshot before downstream validation.

Check target registries to determine if push-snapshot has completed successfully
for each component by validating that ALL required tags exist with the correct
digest.  Components that are fully released (all tags present in at least one
target repository) are filtered out.  The snapshot file is overwritten in place.
"""

from . import filter_already_released_images  # noqa: F401
from .filter_already_released_images import (  # noqa: F401
    PROG,
    filter_snapshot,
    is_component_released,
    main,
    run,
)
