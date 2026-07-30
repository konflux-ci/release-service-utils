"""Push merged maven zip to OCI registry for publish-to-mrrc.

Tekton injects ``IMAGE``, ``IMAGE_EXPIRES_AFTER`` which is optional and
``WORK_DIR`` which defaults to ``/var/workdir/mrrc`` via env.  Result paths
come from ``RESULT_IMAGE_DIGEST`` and ``RESULT_IMAGE_TAG``.
"""

from .publish_to_mrrc_push_merged import (  # noqa: F401
    generate_tag,
    push_merged_maven_repo,
    main,
)
