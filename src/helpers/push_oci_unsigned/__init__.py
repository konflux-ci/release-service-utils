"""Push unsigned OCI artifacts to Quay via ORAS."""

from .push_oci_unsigned import (  # noqa: F401
    PROG,
    main,
    parse_args,
    run,
)
