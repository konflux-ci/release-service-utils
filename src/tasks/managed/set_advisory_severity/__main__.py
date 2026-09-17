"""Entry point for set_advisory_severity task."""

from __future__ import annotations

from release_service_utils.tasks.managed.set_advisory_severity.set_advisory_severity import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
