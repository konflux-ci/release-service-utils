"""Entry point for extract_checksums_from_image task."""

from __future__ import annotations

from release_service_utils.tasks.managed.extract_checksums_from_image.extract_checksums_from_image import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
