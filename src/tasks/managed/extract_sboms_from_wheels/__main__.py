"""Entry point for extract_sboms_from_wheels task."""

from __future__ import annotations

from release_service_utils.tasks.managed.extract_sboms_from_wheels.extract_sboms_from_wheels import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
