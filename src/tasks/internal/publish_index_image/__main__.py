"""Entry point for publish_index_image task."""

from __future__ import annotations

from release_service_utils.tasks.internal.publish_index_image.publish_index_image import main

if __name__ == "__main__":
    raise SystemExit(main())
