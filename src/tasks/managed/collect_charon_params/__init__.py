"""Collect charon configuration from data, snapshot, and release files.

Extract parameters needed by charon (MRRC/NRRC publishing tool) and
write them as a shell-sourceable env file, a config file, and Tekton
result files.
"""

from .collect_charon_params import (  # noqa: F401
    PROG,
    CharonParams,
    collect_charon_params,
    main,
    run,
    write_charon_config,
    write_charon_env,
)
