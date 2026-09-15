"""Entry point for marketplacesvm_push_disk_images task."""

from __future__ import annotations

from release_service_utils.tasks.managed.marketplacesvm_push_disk_images.marketplacesvm_push_disk_images import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
