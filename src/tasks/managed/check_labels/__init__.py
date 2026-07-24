"""Validate container image labels against expected values from snapshot and data files.

Check that each image component's ``name`` label matches either its
``canonicalName`` or the repository URL, and that its ``cpe`` label matches
the ``releaseNotes.cpe`` value from the data file.

With ``--enforce true``, mismatches cause a non-zero exit. Without it, mismatches
are logged as warnings and the script exits successfully.
"""

from .check_labels import (  # noqa: F401
    IMAGE_MEDIA_TYPES,
    PROG,
    LabelValidationError,
    check_labels,
    derive_name_from_url,
    get_label_value,
    is_image_media_type,
    main,
    parse_args,
)
