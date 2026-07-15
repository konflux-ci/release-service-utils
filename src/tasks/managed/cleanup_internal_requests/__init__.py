"""Delete InternalRequest CRs associated with a specific PipelineRun."""

from . import cleanup_internal_requests  # noqa: F401
from .cleanup_internal_requests import (  # noqa: F401
    LABEL_KEY,
    PROG,
    cleanup,
    main,
    run,
)
