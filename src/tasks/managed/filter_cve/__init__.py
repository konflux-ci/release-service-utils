"""Filter the ``cve`` collector output down to a Conforma-facing RHSA summary.

The script reads the ``cve`` collector output from the collectors results directory,
keeps the Critical/Important CVEs that carry an RHSA, groups them per RHSA, attaches
each RHSA's ``released_on`` date from the Red Hat Security Data API, and overwrites
the raw collector file in place. It performs no date math and emits no container
grading verdict.
"""

from __future__ import annotations

from .filter_cve import (  # noqa: F401
    build_rhsa_summary,
    fetch_errata,
    filter_cve,
    find_cve_outputs,
    gating_records,
    main,
)
