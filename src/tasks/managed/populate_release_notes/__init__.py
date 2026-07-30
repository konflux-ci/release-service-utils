"""Populate the releaseNotes key in a data JSON file."""

from . import populate_release_notes  # noqa: F401
from .populate_release_notes import (  # noqa: F401
    CLASSIFICATION_URL,
    CVE_REF_PREFIX,
    PULP_CONTENT_BASE_URL,
    UNIQUE_TAG_RE,
    build_cves_for_component,
    get_image_architectures,
    get_timestamp_tag,
    get_unique_tag_from_tags,
    main,
    parse_checksum_file,
    populate_artifacts,
    populate_github,
    populate_images,
    run,
    update_type_and_references,
    validate_cve_issues,
)
