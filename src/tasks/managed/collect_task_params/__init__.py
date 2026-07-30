"""Extract values from a data JSON file based on specified keys and result indices.

This script takes an array of resultIndex/key pairs and extracts the corresponding
values from a JSON data file. Each extracted value is placed at the specified
resultIndex in the output array for use by downstream Tekton tasks. Optional default
values can be provided for keys that may not exist in the data file.
"""

from .collect_task_params import (  # noqa: F401
    collect_task_params,
    extract_value_from_data,
    main,
    parse_jq_key_path,
    parse_keys_to_extract,
    run_collect_task_params,
    validate_key_spec,
)
