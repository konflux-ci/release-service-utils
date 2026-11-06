"""Entry point for filter_cve task."""

from __future__ import annotations

from release_service_utils.tasks.managed.filter_cve.filter_cve import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
