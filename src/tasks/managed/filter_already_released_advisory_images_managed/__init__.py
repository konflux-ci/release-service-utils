"""Filter advisory-published images from a snapshot before advisory creation.

For each snapshot component, resolve architecture-specific image digests,
classify the component's mapped repository URLs as pending (stage) or
production advisories, and submit the resulting entries to the
`filter-already-released-advisory-images` internal pipeline via an
InternalRequest. Components the internal pipeline reports as still needing
release are kept in the snapshot; fully-released snapshots are reduced to an
empty component list and the run is marked skippable.
"""

from . import filter_already_released_advisory_images_managed  # noqa: F401
from .filter_already_released_advisory_images_managed import (  # noqa: F401
    FilterConfig,
    ResultPaths,
    check_skip_filter,
    decode_unreleased_components,
    determine_environment,
    filter_snapshot,
    main,
    run,
    run_filter_request,
    transform_component,
    transform_snapshot,
)
