"""Create an InternalRequest to sign RPMs via the direct-rpm-signing pipeline."""

from __future__ import annotations

from . import rh_sign_rpm  # noqa: F401
from .rh_sign_rpm import (  # noqa: F401
    build_sign_request_params,
    main,
    run,
)
