"""Run the push_artifacts_to_cdn task as a module entry point."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_artifacts_to_cdn.push_artifacts_to_cdn import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
