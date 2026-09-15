"""Entry point for validate_single_component task."""

from __future__ import annotations

from release_service_utils.tasks.managed.validate_single_component.validate_single_component import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
