#!/usr/bin/env python3
"""Send a Slack notification via an incoming webhook URL."""

from __future__ import annotations

import os
from pathlib import Path

import requests

from release_service_utils.helpers import file
from release_service_utils.helpers.logger import logger

DEFAULT_SECRET_MOUNT = "/etc/secrets"


def circle_type_for_status(tasks_status: str) -> str:
    """Return the Slack emoji name used for *tasks_status* in message placeholders."""
    if tasks_status == "Failed":
        return "red_circle"
    if tasks_status == "Succeeded":
        return "large_green_circle"
    return "white_circle"


def substitute_message_placeholders(message: str, tasks_status: str) -> str:
    """Replace `@@CIRCLE_TYPE@@` and `@@STATUS_TEXT@@` in *message*."""
    circle_type = circle_type_for_status(tasks_status)
    return message.replace("@@CIRCLE_TYPE@@", circle_type).replace(
        "@@STATUS_TEXT@@",
        tasks_status,
    )


def post_slack_webhook(webhook_url: str, message_body: str) -> None:
    """POST *message_body* to the Slack incoming webhook at *webhook_url*."""
    response = requests.post(
        webhook_url,
        headers={"Content-type": "application/json"},
        data=message_body.encode("utf-8"),
        timeout=60.0,
    )
    response.raise_for_status()


def run(
    message: str,
    key_name: str,
    tasks_status: str,
    secret_mount: Path,
) -> None:
    """Send *message* to Slack when configuration and secrets are present."""
    tasks_status = tasks_status.strip()

    if not message:
        logger.info("Message is empty - No message will be sent to Slack")
        return

    if not key_name:
        logger.info(
            "No secret key name provided via "
            "'slack.slack-webhook-notification-secret-keyname' key in Data."
        )
        logger.info("No message will be sent to Slack")
        return

    try:
        secret_path = file.resolve_path_under_base(secret_mount, key_name)
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid secret key name ({key_name}): {exc}",
        ) from exc

    if not secret_path.is_file():
        raise RuntimeError(
            f"Error: Secret not defined properly. The key to use ({key_name}) "
            "is defined in the Release data but the Secret does not contain the key"
        )

    logger.info("Setting WEBHOOK_URL from secret....")
    webhook_url = secret_path.read_text(encoding="utf-8").strip()

    final_message = substitute_message_placeholders(message, tasks_status)

    logger.info("Posting Slack notification via webhook")
    post_slack_webhook(webhook_url, final_message)


def main() -> int:
    """Read Tekton environment variables and send the Slack notification."""
    message = os.environ.get("MESSAGE", "")
    key_name = os.environ.get("KEYNAME", "")
    tasks_status = os.environ.get("TASKSSTATUS", "")
    secret_mount = file.path_from_env_variable("SECRET_MOUNT", DEFAULT_SECRET_MOUNT)

    run(message, key_name, tasks_status, secret_mount)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
