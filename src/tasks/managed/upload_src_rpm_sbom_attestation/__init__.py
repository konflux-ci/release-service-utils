"""Download image SBOMs and attestations and upload them to Pulp."""

from .upload_src_rpm_sbom_attestation import (  # noqa: F401
    main,
    run,
)
