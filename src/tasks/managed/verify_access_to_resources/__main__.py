"""Entry point for verify_access_to_resources task."""

from __future__ import annotations

from release_service_utils.tasks.managed.verify_access_to_resources import (
    verify_access_to_resources,
)

if __name__ == "__main__":
    raise SystemExit(verify_access_to_resources.main())
