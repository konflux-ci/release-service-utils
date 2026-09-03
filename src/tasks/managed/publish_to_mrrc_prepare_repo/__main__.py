"""Entry point for publish_to_mrrc_prepare_repo task."""

from __future__ import annotations

from release_service_utils.tasks.managed.publish_to_mrrc_prepare_repo.publish_to_mrrc_prepare_repo import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
