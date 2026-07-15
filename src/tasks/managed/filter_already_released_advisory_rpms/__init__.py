"""Filter RPMs already published in advisories and reduce the snapshot.

Pull RPM files from each component's OCI artifact, transform RPM metadata
into purls for advisory matching, trigger an InternalRequest to check
advisories, validate Pulp digests for RPMs in advisories (rebuild detection),
write RPMs still needing publishing under ``.components[].rpmsToPublish``,
remove components with an empty list, and overwrite the snapshot file.
"""

from . import filter_already_released_advisory_rpms  # noqa: F401
from .filter_already_released_advisory_rpms import (  # noqa: F401
    FilterConfig,
    FilteringResult,
    LoadedContext,
    ResultPaths,
    RpmEntry,
    RpmNevra,
    build_rpm_entries,
    create_internal_request,
    determine_environment,
    entries_to_ir_payload,
    entries_to_rpms_map,
    extract_rpm_metadata,
    filter_snapshot,
    load_and_validate,
    main,
    make_pulp_client,
    pull_filter_results,
    run,
    should_exclude_file,
    submit_advisory_filter,
    validate_pulp_digests,
)
