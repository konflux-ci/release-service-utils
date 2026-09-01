"""Entry point for managed_publish_index_image task."""

from __future__ import annotations

from release_service_utils.tasks.managed.managed_publish_index_image.managed_publish_index_image import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
