"""Pull signed binaries from Quay, restore supplementary files, and compress artifacts.

For each component:
* Pulls signed macOS and Windows OCI artifacts from Quay into a ``signed/`` directory.
* Restores supplementary files (readme, license, changelog) that were held during signing.
* Compresses each file entry into the final deliverable format:
  - macOS / Linux → ``.tar.gz`` (from ``os/arch/`` directory)
  - Windows → ``.zip`` (from ``os/arch/`` directory, extension corrected from
    ``.tar.gz``/``.tar``)
* Updates ``SNAPSHOT_JSON`` to reflect corrected Windows filenames in ``files[]``.
* Saves the modified snapshot to ``/shared/snapshot.json`` for downstream use.

CLI arguments:
  ``--quay-url``

Secret mounts:
  ``QUAY_SECRET_MOUNT``  (default: ``/mnt/quaySecret``)

Other env vars:
  ``SNAPSHOT_JSON``   – JSON string of the Snapshot spec
  ``CONTENT_DIR``     – override base directory (default: ``/shared/artifacts``)
  ``SHARED_DIR``      – override shared volume root (default: ``/shared``)
"""

from .compress_artifacts import (  # noqa: F401
    CONTENT_DIR,
    PROG,
    QUAY_SECRET_MOUNT,
    SHARED_DIR,
    compress_component,
    main,
    run,
)
