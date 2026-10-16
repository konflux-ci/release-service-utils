"""Sign binary artifacts and delegate publishing to an internal task."""

from __future__ import annotations

from . import gpg_sign_and_push_to_cdn  # noqa: F401
from .gpg_sign_and_push_to_cdn import (  # noqa: F401
    SubmitConfig,
    main,
    prepare_all_components,
    prepare_component,
    pull_and_extract,
    setup_argparser,
    submit_all_signing_requests,
    submit_publish_request,
    submit_signing_request,
)
