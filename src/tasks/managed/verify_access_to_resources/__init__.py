"""Verify access to Release pipeline resources via kubectl auth can-i."""

from .verify_access_to_resources import (  # noqa: F401
    main,
    parse_namespaced_resource,
    run,
)
