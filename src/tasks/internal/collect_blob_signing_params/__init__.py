"""Collect blob-signing parameters from a Kubernetes ConfigMap."""

from .collect_blob_signing_params import (  # noqa: F401
    RESULT_KEYS,
    collect_blob_signing_params,
    extract_blob_signing_params,
    main,
)
