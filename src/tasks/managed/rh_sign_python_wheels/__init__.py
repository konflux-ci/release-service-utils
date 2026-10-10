"""Sign Python wheels and sdists with SLSA provenance attestations via cosign."""

from __future__ import annotations

from .rh_sign_python_wheels import (  # noqa: F401
    build_slsa_predicate,
    convert_dsse_to_pep740,
    load_chains_predicate,
    main,
    run,
)
