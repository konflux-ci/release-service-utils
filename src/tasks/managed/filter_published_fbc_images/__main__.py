"""Entry point for filter_published_fbc_images task."""

from __future__ import annotations

from release_service_utils.tasks.managed.filter_published_fbc_images.filter_published_fbc_images import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
