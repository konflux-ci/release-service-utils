"""Collect release data from K8s resources and merge into data.json.

Fetch Release, ReleasePlan, ReleasePlanAdmission, ReleaseServiceConfig, and
Snapshot CRs; merge collector results with spec.data from those resources
(priority: RPA > RP > Release > collectors); resolve pipeline ref metadata;
validate disallowed data key sources; and write JSON files plus Tekton results.
"""

from .collect_data import (  # noqa: F401
    CollectDataResult,
    check_data_key_sources,
    collect,
    deep_merge,
    flatten_collectors,
    main,
    resolve_pipeline_ref,
    run,
    transform_snapshot_spec,
    write_outputs,
)
