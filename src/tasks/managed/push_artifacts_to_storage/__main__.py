"""Entry point for push_artifacts_to_storage task."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_artifacts_to_storage.push_artifacts_to_storage import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
