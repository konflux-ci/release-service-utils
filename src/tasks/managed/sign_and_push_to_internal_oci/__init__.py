"""Sign & push oci via InternalRequest to internal quay repo."""

from __future__ import annotations

from . import sign_and_push_to_internal_oci  # noqa: F401
from .sign_and_push_to_internal_oci import (  # noqa: F401
    extract_origin,
    main,
    run,
)
