"""Entry point for close_advisory_issues task."""

from __future__ import annotations

from release_service_utils.tasks.managed.close_advisory_issues.close_advisory_issues import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
