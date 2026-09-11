"""Extract artifacts from OCI artifact images (pushed via ORAS)."""

from .extract_oci_artifacts import (  # noqa: F401
    CONTENT_DIR,
    PROG,
    main,
    process_component,
    run,
)
