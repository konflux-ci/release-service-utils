"""Download maven repo zips from OCI registries for publish-to-mrrc.

Tekton injects ``DATA_DIR``, ``CHARON_PARAM_FILE_PATH`` and optionally
``WORK_DIR`` which defaults to ``/var/workdir/mrrc`` via env.
"""

from .publish_to_mrrc_prepare_repo import prepare_repo, main  # noqa: F401
