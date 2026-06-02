"""Process managed index image publishing requests.

This script processes image publishing requests by extracting component details from
an Internal Request (IR) results JSON file. For each component, it extracts the
source index, target index, and build timestamp. It then initiates parallel, internal
Tekton requests to publish the target images (optionally including a timestamped version).
The script awaits the completion of all spawned requests and reports the status.
"""

from . import managed_publish_index_image  # noqa: F401
