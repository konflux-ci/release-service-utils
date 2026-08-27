"""Inspect FBC fragment images to read their target OCP version."""

from .ocp_version import (  # noqa: F401
    FBC_OPENSHIFT_VERSION_LABEL,
    base_name_tag,
    read_openshift_version_label,
    resolve_ocp_version,
)
