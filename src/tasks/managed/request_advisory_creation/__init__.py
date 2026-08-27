"""Update artifact PURLs and request advisory creation via InternalRequest.

This script is used for the create-advisory managed task. It uses a different name
so it does not conflict with the create-advisory internal task script.
"""

from .request_advisory_creation import (  # noqa: F401
    release_notes_purl,
    run_request_advisory_creation,
    TaskParams,
    main,
)
