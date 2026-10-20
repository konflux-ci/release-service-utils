"""Filter snapshot RPMs already published in GitLab-stored advisories.

Reads advisory repository metadata from a mounted secret, sparse-clones the
advisory Git repository, and splits snapshot RPM entries into unreleased vs
already-in-advisory lists. Filter results are pushed to OCI storage.

Reads task inputs from environment variables and writes Tekton result files
from ``RESULT_*``. After those env vars are present, the process exits with
status ``0`` even on logical failure so callers can read ``RESULT_RESULT``.
Missing env before result handling exits ``1``.
"""

from . import filter_already_released_advisory_rpms  # noqa: F401
