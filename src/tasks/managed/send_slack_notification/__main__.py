"""Entry point for send_slack_notification task."""

from __future__ import annotations

from release_service_utils.tasks.managed.send_slack_notification.send_slack_notification import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
