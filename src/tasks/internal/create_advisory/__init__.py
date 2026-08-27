"""Create advisory YAML under `data/advisories` in a Git repo (Tekton task).

* Reads advisory credentials from `/mnt/advisory_secret` (or
  `ADVISORY_SECRET_MOUNT`) and Errata credentials from `/mnt/errata_secret`
  (or `ERRATA_SECRET_MOUNT`).
* Reserves an Errata `live_id` when the decoded advisory JSON has no
  `live_id`.
* Writes task results from `RESULT_RESULT`, `RESULT_ADVISORY_URL`,
  `RESULT_ADVISORY_INTERNAL_URL`, `RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME`,
  and `RESULT_INTERNAL_REQUEST_TASK_RUN_NAME`.
* After a valid invocation with those env vars, always exits with status `0`;
  success or failure is in the result files.
* Missing env before result handling exits `1`.
"""

from . import create_advisory  # noqa: F401
