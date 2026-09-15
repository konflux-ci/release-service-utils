"""Entry point for request_advisory_creation task."""

from __future__ import annotations

from release_service_utils.tasks.managed.request_advisory_creation.request_advisory_creation import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
