"""Download npm archives from OCI registries for publish-to-nrrc.

Tekton injects ``DATA_DIR``, ``CHARON_PARAM_FILE_PATH``, and optionally
``WORK_DIR`` (default ``/var/workdir/nrrc``; catalog sets ``/workdir/nrrc``) via env.
"""

from . import publish_to_nrrc  # noqa: F401
from .publish_to_nrrc import main, prepare_repo  # noqa: F401
