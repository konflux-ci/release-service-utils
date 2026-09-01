"""Reduce a Snapshot to a single component based on CR labels."""

from . import reduce_snapshot  # noqa: F401
from .reduce_snapshot import (  # noqa: F401
    get_cr_labels,
    main,
    reduce_snapshot as reduce_snapshot_fn,
    resolve_namespace,
    run,
    validate_labels,
)
