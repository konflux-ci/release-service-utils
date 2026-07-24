"""Collect three parameters for the create-github-release task.

The githubSecret from the Data file, the repository from the
snapshot file, and the release_version from the binaries of the
extract-checksums-from-image task.
"""

from . import collect_gh_params  # noqa: F401
from .collect_gh_params import (  # noqa: F401
    collect_params,
    main,
    validate_input_files,
)
