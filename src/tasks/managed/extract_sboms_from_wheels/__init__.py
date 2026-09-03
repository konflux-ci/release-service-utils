"""Extract Fromager-generated SBOMs from Python wheels.

Walks ``*.whl`` files under a data directory, pulls SBOM files from each
wheel's ``.dist-info/sboms/`` path, and writes them into ``<data_dir>/sboms``
for a later Atlas/TPA upload step.
"""

from .extract_sboms_from_wheels import (  # noqa: F401
    SBOMS_SUBDIR,
    extract_sboms_from_wheel,
    main,
    run,
)
