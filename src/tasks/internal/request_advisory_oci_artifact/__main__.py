"""Entry point for the request-advisory-oci-artifact task."""

from __future__ import annotations

from release_service_utils.tasks.internal.request_advisory_oci_artifact import (
    request_advisory_oci_artifact,
)

if __name__ == "__main__":
    raise SystemExit(request_advisory_oci_artifact.main())
