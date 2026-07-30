"""Build an OCI artifact containing a checksum map of published files.

For each component's ``ready_for_distribution`` directory, computes sha256 checksums
for all files (excluding ``sha256sum.txt*``) and assembles them into a JSON manifest.
The manifest is packaged as a tar archive and pushed to OCI using ``oras push``.
The resulting ``store@digest`` reference is returned by ``run()`` for the caller to record.
The manifest is stored as an OCI artifact so that downstream advisory tooling has a stable,
addressable pointer it can pull independently to construct PURLs for released files.

Secret mounts:
  ``TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT``  (default: ``/mnt/trusted_artifacts_dockerconfig``)

Other env vars:
  ``SNAPSHOT_JSON``        – JSON string of the Snapshot spec
  ``CONTENT_DIR``          – override base directory (default: ``/shared/artifacts``)
  ``SHARED_DIR``           – override shared volume root (default: ``/shared``)
  ``OCI_STORE``            – OCI repository for checksum map artifacts
                             (default:
                             ``quay.io/konflux-ci/release-service-trusted-artifacts``)
"""

from .build_checksum_map import (  # noqa: F401
    CONTENT_DIR,
    OCI_STORE,
    PROG,
    SHARED_DIR,
    TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT,
    main,
    run,
)
