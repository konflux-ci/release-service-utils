"""Send a Slack notification via an incoming webhook URL."""

from . import send_slack_notification  # noqa: F401
from .send_slack_notification import (  # noqa: F401
    circle_type_for_status,
    main,
    post_slack_webhook,
    run,
    substitute_message_placeholders,
)
