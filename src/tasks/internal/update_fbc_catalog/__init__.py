"""Submit and monitor IIB FBC catalog update builds.

Authenticate with Kerberos, check for reusable previous builds, submit a
new FBC operation to IIB if needed, poll for completion, validate the
resulting index image, and retrieve manifest digests.
"""

from . import update_fbc_catalog  # noqa: F401
