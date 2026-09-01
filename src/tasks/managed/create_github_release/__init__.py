"""Create a GitHub release with binaries extracted from a container image.

Extract binary files from the container image layers, then upload them
along with SHA256SUMS and signature files (from the Trusted Artifacts chain)
to a new GitHub release. If the release already exists, writes the existing
release URL to the result file.
"""

from . import create_github_release  # noqa: F401
from .create_github_release import (  # noqa: F401
    check_release_exists,
    copy_binaries_to_temp,
    create_release,
    main,
    run_create_github_release,
    write_results_json,
)
