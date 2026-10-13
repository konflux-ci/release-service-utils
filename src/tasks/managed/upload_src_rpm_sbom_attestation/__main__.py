"""Entry point for upload_src_rpm_sbom_attestation task."""

from __future__ import annotations

from release_service_utils.tasks.managed.upload_src_rpm_sbom_attestation.upload_src_rpm_sbom_attestation import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
