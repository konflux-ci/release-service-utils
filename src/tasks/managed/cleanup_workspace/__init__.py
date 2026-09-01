"""Clean up a workspace directory and InternalRequest CRs for a PipelineRun."""

from . import cleanup_workspace  # noqa: F401
from .cleanup_workspace import (  # noqa: F401
    PROG,
    cleanup_directory,
    main,
    run,
)
