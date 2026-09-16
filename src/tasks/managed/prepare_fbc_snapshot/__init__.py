"""Update snapshot with multi-OCP version data and resolved index templates."""

from __future__ import annotations

from . import prepare_fbc_snapshot  # noqa: F401
from .prepare_fbc_snapshot import (  # noqa: F401
    MAX_TAG_LENGTH,
    OCP_VERSION_LENGTH,
    RESERVED_TAG_NAMES,
    build_suffix,
    generate_target_index,
    main,
    replace_ocp_version,
    run_prepare,
    sanitize_tag_component,
    validate_ocp_version,
)
