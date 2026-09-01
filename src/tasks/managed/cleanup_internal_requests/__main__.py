"""Entry point for cleanup_internal_requests task."""

from __future__ import annotations
from release_service_utils.tasks.managed.cleanup_internal_requests.cleanup_internal_requests import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
