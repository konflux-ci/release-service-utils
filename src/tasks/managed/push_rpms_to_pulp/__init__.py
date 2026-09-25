"""Push RPM packages from an OCI artifact to a Pulp repository."""

from . import push_rpms_to_pulp  # noqa: F401
from .push_rpms_to_pulp import (  # noqa: F401
    PushConfig,
    main,
    run,
)
