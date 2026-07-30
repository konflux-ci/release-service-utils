"""Push disk images with Pulp and publish metadata to the Developer Portal / CGW.

Reads snapshot JSON and credentials from mounted secrets, pulls OCI artifacts with
oras, stages files, invokes pulp_push_wrapper and developer_portal_wrapper.

Environment variables (set by the pulp-push-disk-images Tekton task):
  SNAPSHOT_JSON, CERT_EXPIRATION_WARN_DAYS, CONCURRENT_LIMIT, EXODUS_GW_ENV,
  CGW_HOSTNAME, RESULT_RESULT

Mount paths default to /mnt/exodusGwSecret, /mnt/pulpSecret, /mnt/udcacheSecret,
/mnt/redhat-workloads-token, /mnt/cgwSecret and can be overridden in tests.
"""

from . import pulp_push_disk_images  # noqa: F401
