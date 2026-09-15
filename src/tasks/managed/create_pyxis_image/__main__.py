"""Entry point for create_pyxis_image task."""

from __future__ import annotations

from release_service_utils.tasks.managed.create_pyxis_image.create_pyxis_image import main

if __name__ == "__main__":
    raise SystemExit(main())
