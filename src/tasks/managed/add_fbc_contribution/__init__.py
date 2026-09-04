"""Add FBC contributions to index images via InternalRequests.

Create InternalRequests to add FBC (File-Based Catalog) contributions to index
images. This script batches multiple fragments into single IIB requests and can
split requests according to their OCP versions.
"""

from . import add_fbc_contribution  # noqa: F401
from .add_fbc_contribution import (  # noqa: F401
    AddFBCContributionConfig,
    BatchResult,
    OCPGroup,
    calculate_timeouts,
    compute_target_index_with_timestamp,
    deduplicate_results,
    execute_batch,
    get_batch_fragments,
    get_ocp_versions,
    group_components_by_ocp_version,
    main,
    process_batch_results,
    process_ocp_group,
    run,
    setup_argparser,
    validate_snapshot,
)
