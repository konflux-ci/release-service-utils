"""Sign FBC index images via the container-signing pipeline."""

from . import direct_sign_index_image  # noqa: F401
from .direct_sign_index_image import (  # noqa: F401
    collect_fbc_signing_items,
    main,
    setup_argparser,
    translate_reference,
)
