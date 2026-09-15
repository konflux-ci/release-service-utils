"""Entry point for filter_already_released_advisory_rpms task."""

from __future__ import annotations

from release_service_utils.tasks.managed.filter_already_released_advisory_rpms.filter_already_released_advisory_rpms import (  # noqa:E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
