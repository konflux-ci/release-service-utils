"""Extract artifacts from container images.

For each component in SNAPSHOT_JSON that has ``files`` or ``staged.files`` entries:
* Pulls the container image with ``skopeo copy`` (authenticated via ``select-oci-auth``).
* Identifies the specific files listed in the RPA and extracts them from the container layers.
* Creates OS flag files (``has_mac``, ``has_windows``, ``has_linux``) to indicate
  which signing paths are needed.

Components are processed in parallel, bounded by ``--concurrent-limit``.

CLI arguments:
  ``--concurrent-limit``

Secret mounts (paths can be overridden via env vars for testing):
  ``REDHAT_WORKLOADS_TOKEN_MOUNT``  (default: ``/mnt/redhat-workloads-token``)

Other env vars:
  ``SNAPSHOT_JSON``   – JSON string of the Snapshot spec (set by the task)
  ``CONTENT_DIR``     – override base directory (default: ``/shared/artifacts``)
"""

from .extract_artifacts import (  # noqa: F401
    CONTENT_DIR,
    PROG,
    REDHAT_WORKLOADS_TOKEN_MOUNT,
    main,
    process_component,
    run,
)
