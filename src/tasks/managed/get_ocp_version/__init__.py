"""Validate that all FBC fragment components in a snapshot target one OCP version."""

from . import get_ocp_version  # noqa: F401
from .get_ocp_version import (  # noqa: F401
    PROG,
    main,
    resolve_ocp_version,
    run,
    validate_ocp_versions,
)
