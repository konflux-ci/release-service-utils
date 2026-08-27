"""Merge a ReleasePlanAdmission mapping into a Snapshot's components.

The mapping is expected to live under the ``mapping`` key of the data file
(the merged ReleasePlanAdmission data). If the data file is missing, or has
no ``mapping`` key, the Snapshot spec file is left untouched and the task
reports ``mapped=false``. Otherwise, the mapping's components are merged with
the Snapshot's components by name, tag templates are expanded, image
metadata (labels, annotations, env vars, media type) is attached to each
component, and repository URLs are translated between the quay.io and
registry.redhat.io namespaces.

Supported tag template variables:
* ``{{ timestamp }}`` -- the image's ``build-date`` label (or ``Created``
  fallback), formatted with the component's ``timestampFormat`` (or the
  mapping-level default, or ``%s``).
* ``{{ release_timestamp }}`` -- the current time, formatted the same way.
* ``{{ git_sha }}`` / ``{{ git_short_sha }}`` -- the git revision that
  triggered the Snapshot (and its first 7 characters).
* ``{{ digest_sha }}`` -- the image digest (without the ``sha256:`` prefix).
* ``{{ oci_version }}`` -- ``org.opencontainers.image.version`` from
  annotations (falling back to labels), with ``+`` replaced by ``_``.
* ``{{ incrementer }}`` -- the next sequential numeric tag in the repository.
* ``{{ component-incrementer }}`` -- like ``incrementer``, but computed
  uniformly across every repository the component is pushed to.
* ``{{ labels.mylabel }}`` -- the value of image label ``mylabel``.
"""

from . import apply_mapping  # noqa: F401
