"""Extract Red Hat SBOMs from Python wheels.

Recursively walk ``*.whl`` files under a data directory, pull
``redhat.spdx.json`` from each wheel's ``.dist-info/sboms/`` path, and
write it into ``<data_dir>/sboms`` for a later Atlas/TPA upload step.
"""

from __future__ import annotations

from .extract_sboms_from_wheels import (  # noqa: F401
    SBOMS_SUBDIR,
    extract_sboms_from_wheel,
    main,
    run,
)
