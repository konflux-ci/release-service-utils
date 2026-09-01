"""Filter snapshot images already published in GitLab-stored advisories.

Reads advisory repository metadata from a mounted secret, sparse-clones the
advisory Git repository, and progressively removes arch-specific snapshot rows
that match ``spec.content.images`` entries in existing advisories.

Writes Tekton result files from ``RESULT_*`` environment variables.
The process exits with status ``0`` even on logical failure so callers can
read ``RESULT_RESULT``.
"""

from . import filter_already_released_advisory_images  # noqa: F401
