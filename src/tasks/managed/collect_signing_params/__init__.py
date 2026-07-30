"""Collect Konflux signing configuration parameters from a Kubernetes ConfigMap.

Retrieve keyless signing parameters from the cluster-config ConfigMap and write
them to Tekton result files. If the ConfigMap is not found, output empty strings
for all parameters except enableKeylessSigning which defaults to "false".
"""

from .collect_signing_params import (  # noqa: F401
    RESULT_KEYS,
    USAGE,
    collect_signing_params,
    extract_signing_params_from_configmap,
    get_empty_signing_params,
    main,
    parse_args,
    write_result_files,
)
