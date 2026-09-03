"""Entry point for direct_sign_index_image task."""

from __future__ import annotations

from release_service_utils.tasks.managed.direct_sign_index_image.direct_sign_index_image import (  # noqa:E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
