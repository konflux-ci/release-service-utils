"""Collect TPA (Trusted Profile Analyzer) parameters from cluster config or data file.

This script collects the TPA server configuration either from a Kubernetes
cluster ConfigMap (TSF workflow) or from a JSON data file. It outputs the
configuration values to Tekton result files.

The script first attempts to read configuration from a ConfigMap in the
specified namespace. If that fails or is incomplete, it falls back to
reading from a data file and determining stage/production configuration.
"""

from . import collect_tpa_params  # noqa: F401
from .collect_tpa_params import (  # noqa: F401
    PROD_DEFAULTS,
    STAGE_DEFAULTS,
    TSF_RETRY_AWS_SECRET_NAME,
    TSF_SECRET_NAME,
    TPAParams,
    get_tpa_config,
    main,
    params_from_data_file,
    parse_args,
    run_collect_tpa_params,
    try_tsf_config,
    write_results,
)
