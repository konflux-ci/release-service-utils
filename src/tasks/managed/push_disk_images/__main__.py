"""Entry point for push_disk_images task."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_disk_images.push_disk_images import main

if __name__ == "__main__":
    raise SystemExit(main())
