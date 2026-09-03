"""Entry point for publish_pyxis_repository task."""

from __future__ import annotations

from release_service_utils.tasks.managed.publish_pyxis_repository.publish_pyxis_repository import (  # noqa:E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
