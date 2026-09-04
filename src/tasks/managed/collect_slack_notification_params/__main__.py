"""Entry point for collect_slack_notification_params task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_slack_notification_params.collect_slack_notification_params import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
