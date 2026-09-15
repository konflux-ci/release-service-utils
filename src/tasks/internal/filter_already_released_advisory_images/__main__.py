"""Entry point for filter_already_released_advisory_images task."""

from __future__ import annotations

from release_service_utils.tasks.internal.filter_already_released_advisory_images.filter_already_released_advisory_images import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
