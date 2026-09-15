"""Entry point for get_advisory_severity task."""

from __future__ import annotations

from release_service_utils.tasks.internal.get_advisory_severity.get_advisory_severity import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
