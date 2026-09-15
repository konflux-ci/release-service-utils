"""Entry point for publish_to_mrrc_push_merged task."""

from __future__ import annotations

from release_service_utils.tasks.managed.publish_to_mrrc_push_merged.publish_to_mrrc_push_merged import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
