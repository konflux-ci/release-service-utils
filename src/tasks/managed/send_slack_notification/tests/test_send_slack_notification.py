"""Tests for send_slack_notification."""

from __future__ import annotations

import json
import runpy
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests

from release_service_utils.tasks.managed.send_slack_notification import send_slack_notification
from release_service_utils.tasks.managed.collect_slack_notification_params import (
    ReleaseMetadata,
    build_slack_message,
)

TASK = "release_service_utils.tasks.managed.send_slack_notification.send_slack_notification"

SCRIPT_PATH = Path(send_slack_notification.__file__)


def _status_rich_text_elements(parsed_message: dict) -> list[dict]:
    """Return rich_text_section elements from the status block in *parsed_message*."""
    status_block = parsed_message["blocks"][4]
    return status_block["elements"][0]["elements"]


def _mock_post_response() -> MagicMock:
    """Return a mock Response whose raise_for_status succeeds."""
    response = MagicMock()
    response.raise_for_status.return_value = None
    return response


def test_circle_type_for_status_failed() -> None:
    """Failed task status maps to the red circle emoji name."""
    assert send_slack_notification.circle_type_for_status("Failed") == "red_circle"


def test_circle_type_for_status_succeeded() -> None:
    """Succeeded task status maps to the large green circle emoji name."""
    assert send_slack_notification.circle_type_for_status("Succeeded") == "large_green_circle"


def test_circle_type_for_status_default() -> None:
    """Other task statuses map to the white circle emoji name."""
    assert send_slack_notification.circle_type_for_status("Running") == "white_circle"


def test_substitute_message_placeholders() -> None:
    """Replace circle type and status text placeholders in the message."""
    message = "{ @@CIRCLE_TYPE@@ @@STATUS_TEXT@@ }"
    assert (
        send_slack_notification.substitute_message_placeholders(message, "Succeeded")
        == "{ large_green_circle Succeeded }"
    )


def test_post_slack_webhook_posts_json_body() -> None:
    """POST the message body with the Slack JSON content type header."""
    with mock.patch(
        f"{TASK}.requests.post",
        return_value=_mock_post_response(),
    ) as post:
        send_slack_notification.post_slack_webhook(
            "https://hooks.example.com/abc",
            '{"text":"hello"}',
        )

    post.assert_called_once_with(
        "https://hooks.example.com/abc",
        headers={"Content-type": "application/json"},
        data=b'{"text":"hello"}',
        timeout=60.0,
    )
    post.return_value.raise_for_status.assert_called_once_with()


def test_run_skips_when_message_empty(tmp_path: Path) -> None:
    """Do not call Slack when the message is empty."""
    with mock.patch(f"{TASK}.post_slack_webhook") as post:
        send_slack_notification.run("", "my-key", "Succeeded", tmp_path)

    post.assert_not_called()


def test_run_skips_when_key_name_empty(tmp_path: Path) -> None:
    """Do not call Slack when the secret key name is empty."""
    with mock.patch(f"{TASK}.post_slack_webhook") as post:
        send_slack_notification.run("hello", "", "Succeeded", tmp_path)

    post.assert_not_called()


def test_run_raises_when_secret_key_missing(tmp_path: Path) -> None:
    """Raise when the configured secret key file is not mounted."""
    with pytest.raises(RuntimeError, match="Secret does not contain the key"):
        send_slack_notification.run("hello", "missing-key", "Succeeded", tmp_path)


def test_run_rejects_path_traversal(tmp_path: Path) -> None:
    """Reject secret key names that traverse outside the secret mount."""
    secret_mount = tmp_path / "secrets"
    secret_mount.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("leaked", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Invalid secret key name"):
        send_slack_notification.run("hello", "../outside.txt", "Succeeded", secret_mount)


def test_run_rejects_absolute_key_name(tmp_path: Path) -> None:
    """Reject absolute secret key names."""
    secret_mount = tmp_path / "secrets"
    secret_mount.mkdir()

    with pytest.raises(RuntimeError, match="Invalid secret key name"):
        send_slack_notification.run(
            "hello",
            "/etc/passwd",
            "Succeeded",
            secret_mount,
        )


def test_run_posts_substituted_message(tmp_path: Path) -> None:
    """Read the webhook URL from the secret mount and post the final message."""
    secret_mount = tmp_path / "secrets"
    secret_mount.mkdir()
    (secret_mount / "my-team").write_text("ABCDEF", encoding="utf-8")

    with mock.patch(f"{TASK}.post_slack_webhook") as post:
        send_slack_notification.run(
            "{ @@CIRCLE_TYPE@@ @@STATUS_TEXT@@ }",
            "my-team",
            "Succeeded",
            secret_mount,
        )

    posted_message = post.call_args.args[1]
    assert "large_green_circle" in posted_message
    assert "Succeeded" in posted_message


def test_run_posts_valid_json_block_kit_message(tmp_path: Path) -> None:
    """Substitute placeholders in a Block Kit JSON payload and keep it parseable."""
    secret_mount = tmp_path / "secrets"
    secret_mount.mkdir()
    (secret_mount / "my-team").write_text("https://hooks.example.com", encoding="utf-8")

    meta = ReleaseMetadata("ws", "tws", "rel", "plr")
    message = build_slack_message(meta, "app", "http://r", "http://p")

    with mock.patch(
        f"{TASK}.requests.post",
        return_value=_mock_post_response(),
    ) as post:
        send_slack_notification.run(message, "my-team", "Succeeded", secret_mount)

    parsed = json.loads(post.call_args.kwargs["data"].decode())
    elements = _status_rich_text_elements(parsed)
    assert elements[0]["name"] == "large_green_circle"
    assert elements[2]["text"] == " Succeeded "


def test_run_normalizes_tasks_status_whitespace(tmp_path: Path) -> None:
    """Strip TASKSSTATUS whitespace so emoji mapping matches Tekton statuses."""
    secret_mount = tmp_path / "secrets"
    secret_mount.mkdir()
    (secret_mount / "my-team").write_text("https://hooks.example.com", encoding="utf-8")

    meta = ReleaseMetadata("ws", "tws", "rel", "plr")
    message = build_slack_message(meta, "app", "http://r", "http://p")

    with mock.patch(
        f"{TASK}.requests.post",
        return_value=_mock_post_response(),
    ) as post:
        send_slack_notification.run(message, "my-team", "Succeeded\n", secret_mount)

    parsed = json.loads(post.call_args.kwargs["data"].decode())
    elements = _status_rich_text_elements(parsed)
    assert elements[0]["name"] == "large_green_circle"
    assert elements[2]["text"] == " Succeeded "


def test_run_matches_catalog_happy_path(tmp_path: Path) -> None:
    """Mirror test-send-slack-notification.yaml: one webhook call with substitutions."""
    secret_mount = tmp_path / "secrets"
    secret_mount.mkdir()
    (secret_mount / "my-team").write_text("ABCDEF", encoding="utf-8")

    with mock.patch(
        f"{TASK}.requests.post",
        return_value=_mock_post_response(),
    ) as post:
        send_slack_notification.run(
            "{ @@CIRCLE_TYPE@@ @@STATUS_TEXT@@ }",
            "my-team",
            "Succeeded",
            secret_mount,
        )

    post.assert_called_once()
    call_kwargs = post.call_args.kwargs
    assert post.call_args.args[0] == "ABCDEF"
    assert call_kwargs["headers"] == {"Content-type": "application/json"}
    posted = call_kwargs["data"].decode()
    assert "large_green_circle" in posted
    assert "Succeeded" in posted


def test_run_matches_catalog_no_secret_scenario(tmp_path: Path) -> None:
    """Mirror test-send-slack-notification-no-secret.yaml: skip when inputs are empty."""
    with mock.patch(
        f"{TASK}.requests.post",
        return_value=_mock_post_response(),
    ) as post:
        send_slack_notification.run("", "", "Succeeded", tmp_path)

    post.assert_not_called()


def test_main_reads_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads MESSAGE, KEYNAME, and TASKSSTATUS from the environment."""
    secret_mount = tmp_path / "secrets"
    secret_mount.mkdir()
    (secret_mount / "webhook").write_text("https://hooks.example.com", encoding="utf-8")

    monkeypatch.setenv("MESSAGE", "status @@STATUS_TEXT@@")
    monkeypatch.setenv("KEYNAME", "webhook")
    monkeypatch.setenv("TASKSSTATUS", "Failed")
    monkeypatch.setenv("SECRET_MOUNT", str(secret_mount))

    with mock.patch(
        f"{TASK}.requests.post",
        return_value=_mock_post_response(),
    ) as post:
        assert send_slack_notification.main() == 0

    assert post.call_args.kwargs["data"] == b"status Failed"


def test_post_slack_webhook_propagates_request_errors() -> None:
    """Let connection errors propagate for managed task failure semantics."""
    with mock.patch(
        f"{TASK}.requests.post",
        side_effect=requests.ConnectionError("timeout"),
    ):
        with pytest.raises(requests.ConnectionError):
            send_slack_notification.post_slack_webhook("https://hooks.example.com", "{}")


def test_post_slack_webhook_raises_on_http_error() -> None:
    """Propagate HTTP errors from raise_for_status on non-2xx responses."""
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")

    with mock.patch(
        f"{TASK}.requests.post",
        return_value=response,
    ):
        with pytest.raises(requests.HTTPError):
            send_slack_notification.post_slack_webhook("https://hooks.example.com", "{}")


def test_script_entrypoint_exits_zero_when_skipping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Running the script module exits 0 when there is nothing to send."""
    monkeypatch.setenv("MESSAGE", "")
    monkeypatch.setenv("KEYNAME", "")
    monkeypatch.setenv("TASKSSTATUS", "Succeeded")

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(str(SCRIPT_PATH), run_name="__main__")

    assert exc_info.value.code == 0
