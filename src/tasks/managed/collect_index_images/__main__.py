"""Entry point for collect_index_images task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_index_images.collect_index_images import main

if __name__ == "__main__":
    raise SystemExit(main())
