"""Sign binary artifacts via the middleware-signing pipeline."""

from __future__ import annotations

from . import rh_direct_sign_artifacts  # noqa: F401
from .rh_direct_sign_artifacts import (  # noqa: F401
    SubmitConfig,
    main,
    prepare_all_components,
    prepare_component,
    pull_and_extract,
    setup_argparser,
    submit_all_signing_requests,
    submit_signing_request,
)
